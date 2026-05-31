"""Synchronous device-side client.

This module provides :class:`RISELDevice`, the main entry point for device
firmware.  It handles:

* Wi-Fi connection (MicroPython only).
* Initial device registration via a heartbeat on startup.
* Periodic heartbeat loop running on a background daemon thread.
* Telemetry reporting via :meth:`RISELDevice.report`.
* A middleware pipeline that can add retry, throttling, caching, and logging.
* Lifecycle hooks so application code can react to events without subclassing.

The class is designed to work on both CPython (for testing and server-side
scripts) and MicroPython (for K230 / RP2040 firmware).
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from typing import Any

from .._compat import MICROPYTHON, get_mac_address, monotonic, now_unix, sleep
from .._logging import get_logger
from ..exceptions import TransportError
from .middleware import Middleware
from .transport import HTTPTransport, Transport

log = get_logger("client")

# Type alias for hook callbacks — any callable that accepts arbitrary positional args.
HookCallback = Callable[..., None]

# Defaults used when the caller does not supply explicit values.
DEFAULT_HEARTBEAT_INTERVAL = 30  # seconds between heartbeats
DEFAULT_FIRMWARE = "1.0.0"


class RISELDevice:
    """Synchronous RISE.L.net device client.

    Three-line bootstrap::

        from rise_l_net import RISELDevice
        device = RISELDevice("http://server:8080", wifi_ssid="WiFi", wifi_password="pwd")
        device.start()

    Args:
        server_url:          Base URL of the RISE.L.net server.
        wifi_ssid:           Wi-Fi network name (MicroPython only; ignored on CPython).
        wifi_password:       Wi-Fi password (MicroPython only).
        device_id:           Unique device identifier.  Defaults to the MAC address.
        heartbeat_interval:  Seconds between periodic heartbeats.
        metadata:            Arbitrary key-value pairs sent with every heartbeat
                             (e.g. ``{"location": "lab", "device_name": "Sensor-01"}``).
        api_key:             API key added as ``X-API-Key`` header if provided.
        firmware_version:    Version string reported in heartbeats.
        transport:           Custom transport implementation.  Defaults to
                             :class:`HTTPTransport`.
    """

    # All valid hook event names.  Registering an unknown name raises ValueError.
    _SUPPORTED_HOOKS = (
        "on_ready",  # fired once after successful startup
        "on_error",  # fired on any transport or heartbeat failure
        "on_heartbeat_success",  # fired after each successful heartbeat
        "on_heartbeat_fail",  # fired after a failed heartbeat
        "on_report_success",  # fired after a successful report
        "on_report_fail",  # fired after a failed report
    )

    def __init__(
        self,
        server_url: str,
        wifi_ssid: str | None = None,
        wifi_password: str | None = None,
        device_id: str | None = None,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        metadata: dict[str, Any] | None = None,
        api_key: str | None = None,
        firmware_version: str = DEFAULT_FIRMWARE,
        transport: Transport | None = None,
    ) -> None:
        if not server_url:
            raise ValueError("server_url is required")
        self.server_url = server_url
        self.wifi_ssid = wifi_ssid
        self.wifi_password = wifi_password
        self.heartbeat_interval = heartbeat_interval
        # Copy the metadata dict so mutations by the caller don't affect us.
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.api_key = api_key
        self.firmware_version = firmware_version
        # Use the provided device_id, fall back to MAC address, then a placeholder.
        self.device_id = device_id or get_mac_address() or "unknown-device"
        # Record the start time for uptime calculation in heartbeats.
        self.start_time = monotonic()

        # Build the default HTTP transport if none was provided.
        if transport is None:
            # Inject the API key as a header if one was supplied.
            headers = {"X-API-Key": api_key} if api_key else None
            self._transport: Transport = HTTPTransport(server_url, headers=headers)
        else:
            self._transport = transport

        # Ordered list of middleware; applied in registration order.
        self._middlewares: list[Middleware] = []
        # Hook registry: event name → list of callbacks.
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        # Runtime state flags.
        self._running = False
        # Event used to signal the heartbeat thread to stop.
        self._stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

        log.info("device initialized id=%s", self.device_id)

    # ---- public configuration API ------------------------------------------------

    def use(self, middleware: Middleware) -> RISELDevice:
        """Register a middleware in the send pipeline.

        Middleware is applied in registration order.  Returns ``self`` for
        method chaining::

            device.use(RetryMiddleware()).use(LoggingMiddleware())
        """
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        return self

    def transport(self, transport_impl: Transport) -> RISELDevice:
        """Replace the active transport with a new implementation.

        The old transport is closed before being replaced.  Returns ``self``
        for method chaining.
        """
        if not isinstance(transport_impl, Transport):
            raise TypeError(f"{transport_impl!r} is not a Transport")
        # Best-effort close of the old transport; ignore errors.
        try:
            self._transport.close()
        except Exception:
            pass
        self._transport = transport_impl
        return self

    def set_api_key(self, api_key: str, header_name: str = "X-API-Key") -> RISELDevice:
        """Update the API key sent with every request.

        Only works when the active transport is an :class:`HTTPTransport`.
        Returns ``self`` for method chaining.
        """
        self.api_key = api_key
        # Directly mutate the transport's header dict so the change takes effect
        # immediately without needing to recreate the transport.
        if isinstance(self._transport, HTTPTransport):
            self._transport.headers[header_name] = api_key
        return self

    def hook(self, event_name: str, callback: HookCallback) -> RISELDevice:
        """Register a callback for a lifecycle event.

        Args:
            event_name: One of the names in ``_SUPPORTED_HOOKS``.
            callback:   Callable invoked when the event fires.  Arguments vary
                        by event; see the class docstring for details.

        Returns:
            ``self`` for method chaining.

        Raises:
            ValueError: If *event_name* is not a supported hook.
        """
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    def on_ready(self, cb: HookCallback) -> RISELDevice:
        """Shortcut for ``hook("on_ready", cb)``."""
        return self.hook("on_ready", cb)

    def on_error(self, cb: HookCallback) -> RISELDevice:
        """Shortcut for ``hook("on_error", cb)``."""
        return self.hook("on_error", cb)

    # ---- internals ---------------------------------------------------------------

    def _trigger_hook(self, event: str, *args: Any) -> None:
        """Fire all callbacks registered for *event*.

        Exceptions raised by individual callbacks are logged and swallowed so
        that a buggy callback cannot crash the heartbeat thread.
        """
        for cb in self._hooks.get(event, []):
            try:
                cb(*args)
            except Exception:
                log.exception("hook %s failed", event)

    def _connect_wifi(self) -> bool:
        """Connect to Wi-Fi on MicroPython.  Always returns True on CPython.

        Blocks for up to 10 seconds waiting for the connection to be
        established.  Returns False if the connection times out.
        """
        # Wi-Fi management is only needed on MicroPython embedded devices.
        if not MICROPYTHON or not self.wifi_ssid:
            return True
        try:
            import network  # type: ignore[import-not-found]
        except ImportError:
            # Running on CPython with MicroPython flag somehow set — skip.
            return True

        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)  # power on the Wi-Fi radio

        # If already connected (e.g. after a soft reset), skip the handshake.
        if wlan.isconnected():
            return True

        log.info("connecting wifi ssid=%s", self.wifi_ssid)
        wlan.connect(self.wifi_ssid, self.wifi_password)

        # Poll until connected or timeout.
        deadline = monotonic() + 10
        while not wlan.isconnected():
            if monotonic() > deadline:
                log.warning("wifi connect timed out")
                return False
            sleep(0.5)  # yield to other tasks while waiting

        log.info("wifi connected")
        return True

    def _local_ip(self) -> str:
        """Return the device's current IP address, or ``"0.0.0.0"`` if unknown.

        On CPython the IP is managed by the OS and not easily queried here,
        so we return the placeholder.  On MicroPython we read it from the
        WLAN interface.
        """
        if not MICROPYTHON:
            # CPython: the OS manages networking; return a placeholder.
            return "0.0.0.0"
        try:
            import network  # type: ignore[import-not-found,unused-ignore]

            wlan = network.WLAN(network.STA_IF)
            if wlan.isconnected():
                # ifconfig() returns (ip, subnet, gateway, dns).
                return str(wlan.ifconfig()[0])
        except Exception:
            pass
        return "0.0.0.0"

    def _send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send *data* through the middleware pipeline to *endpoint*.

        The loop handles retries: if any middleware's ``on_error`` returns
        True, the entire pipeline (before_send → transport.send) is repeated.

        Raises:
            TransportError: If the send fails and no middleware requests a retry.
        """
        while True:
            # Pass the payload through each middleware's before_send hook.
            payload = data
            for mw in self._middlewares:
                try:
                    payload = mw.before_send(endpoint, payload)
                except Exception:
                    log.exception("middleware before_send failed")

            # Attempt the actual network send.
            try:
                response = self._transport.send(endpoint, payload)
            except TransportError as exc:
                # Give each middleware a chance to handle the error and request
                # a retry.  If any middleware returns True, we loop again.
                should_retry = False
                for mw in self._middlewares:
                    try:
                        if mw.on_error(endpoint, payload, exc):
                            should_retry = True
                    except Exception:
                        log.exception("middleware on_error failed")
                if not should_retry:
                    raise  # propagate the error to the caller
                continue  # retry from the top of the loop

            # Send succeeded — pass the response through after_send hooks.
            for mw in self._middlewares:
                try:
                    response = mw.after_send(endpoint, payload, response)
                except Exception:
                    log.exception("middleware after_send failed")
            return response

    def _build_heartbeat(self) -> dict[str, Any]:
        """Construct the heartbeat payload dictionary."""
        return {
            "device_id": self.device_id,
            "ip": self._local_ip(),
            # Uptime is the elapsed time since start() was called.
            "uptime": int(monotonic() - self.start_time),
            "version": self.firmware_version,
            "metadata": self.metadata,
        }

    def _send_heartbeat(self) -> bool:
        """Send a single heartbeat to the server.

        Returns True on success, False on failure.  Fires the appropriate
        hooks in both cases.
        """
        payload = self._build_heartbeat()
        try:
            response = self._send("/api/heartbeat", payload)
        except TransportError as exc:
            log.warning("heartbeat failed: %s", exc)
            self._trigger_hook("on_heartbeat_fail", str(exc))
            self._trigger_hook("on_error", str(exc))
            return False
        self._trigger_hook("on_heartbeat_success", response)
        return True

    def _heartbeat_loop(self) -> None:
        """Background thread target: send periodic heartbeats.

        The initial heartbeat is sent synchronously in :meth:`start` before
        this thread is launched, so we wait for the interval first to avoid
        sending two heartbeats back-to-back.

        ``_stop_event.wait(interval)`` serves double duty: it sleeps for the
        interval *and* wakes up immediately when ``stop()`` sets the event,
        so the thread exits promptly without sleeping through a long interval.
        """
        # Wait for the interval before the first loop heartbeat.
        while not self._stop_event.wait(self.heartbeat_interval):
            try:
                self._send_heartbeat()
            except Exception:
                # Catch-all so a crash in the heartbeat logic doesn't kill the
                # thread silently — the exception is logged and the loop continues.
                log.exception("heartbeat loop iteration crashed")

    # ---- public API --------------------------------------------------------------

    def report(self, event_type: str, data: dict[str, Any], severity: str = "info") -> bool:
        """Send a telemetry event to the server.

        Args:
            event_type: Application-defined category string (e.g. ``"temperature"``).
            data:       Arbitrary payload dictionary with the event details.
            severity:   One of ``"info"``, ``"warning"``, ``"error"``, ``"critical"``.

        Returns:
            True if the server accepted the event, False on failure.
        """
        payload = {
            "device_id": self.device_id,
            "timestamp": now_unix(),  # record the time of the event on the device
            "event_type": event_type,
            "data": data,
            "severity": severity,
        }
        try:
            response = self._send("/api/report", payload)
        except TransportError as exc:
            log.warning("report failed type=%s: %s", event_type, exc)
            self._trigger_hook("on_report_fail", str(exc))
            self._trigger_hook("on_error", str(exc))
            return False
        self._trigger_hook("on_report_success", response)
        return True

    def start(self, *, block: bool = False) -> bool:
        """Connect to the server, register the device, and start the heartbeat loop.

        Steps:
        1. Call ``transport.connect()`` to open any persistent resources.
        2. Connect to Wi-Fi (MicroPython only).
        3. Send the initial heartbeat to register the device.
        4. Start the background heartbeat thread.
        5. Fire the ``on_ready`` hook.

        Args:
            block: If True, block the calling thread until :meth:`stop` is
                   called or the process is interrupted.  If False (default),
                   the heartbeat thread runs in the background and this method
                   returns immediately.

        Returns:
            True if startup succeeded, False if any step failed.
        """
        if self._running:
            raise RuntimeError("device already running")

        # Step 1: open transport resources (e.g. aiohttp session).
        try:
            self._transport.connect()
        except Exception as exc:
            log.error("transport connect failed: %s", exc)
            self._trigger_hook("on_error", str(exc))
            return False

        # Step 2: connect to Wi-Fi (no-op on CPython).
        if not self._connect_wifi():
            return False

        # Step 3: send the initial heartbeat to register with the server.
        if not self._send_heartbeat():
            return False

        # Step 4: start the background heartbeat thread.
        self._running = True
        self._stop_event.clear()  # reset in case stop() was called before
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop,
            daemon=True,  # daemon threads are killed when the main thread exits
            name="risel-heartbeat",
        )
        self._heartbeat_thread.start()

        # Step 5: notify listeners that the device is ready.
        self._trigger_hook("on_ready")
        log.info("device started")

        if block:
            # Block the calling thread, waking every second to check _running.
            try:
                while self._running:
                    self._stop_event.wait(1)
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()

        return True

    def stop(self) -> None:
        """Stop the heartbeat loop and release all resources.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if not self._running:
            return
        self._running = False
        # Signal the heartbeat thread to wake up and exit.
        self._stop_event.set()
        if self._heartbeat_thread is not None:
            # Wait up to 5 seconds for the thread to finish its current iteration.
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
        try:
            self._transport.close()
        except Exception:
            log.exception("transport close failed")
        log.info("device stopped")

    def __enter__(self) -> RISELDevice:
        """Start the device when used as a context manager."""
        self.start(block=False)
        return self

    def __exit__(self, *_: Any) -> None:
        """Stop the device when the context manager exits."""
        self.stop()


def quick_start(
    server_url: str,
    wifi_ssid: str | None = None,
    wifi_password: str | None = None,
) -> RISELDevice:
    """Convenience function: create a device, start it, and return it.

    Equivalent to::

        device = RISELDevice(server_url, wifi_ssid=wifi_ssid, wifi_password=wifi_password)
        device.start()
        return device
    """
    device = RISELDevice(server_url, wifi_ssid=wifi_ssid, wifi_password=wifi_password)
    device.start()
    return device
