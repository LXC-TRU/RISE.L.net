"""Synchronous device-side client."""

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

HookCallback = Callable[..., None]

DEFAULT_HEARTBEAT_INTERVAL = 30
DEFAULT_FIRMWARE = "1.0.0"


class RISELDevice:
    """Synchronous RISE.L.net device client.

    Three-line bootstrap::

        from rise_l_net import RISELDevice
        device = RISELDevice("http://server:8080", wifi_ssid="WiFi", wifi_password="pwd")
        device.start()
    """

    _SUPPORTED_HOOKS = (
        "on_ready",
        "on_error",
        "on_heartbeat_success",
        "on_heartbeat_fail",
        "on_report_success",
        "on_report_fail",
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
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.api_key = api_key
        self.firmware_version = firmware_version
        self.device_id = device_id or get_mac_address() or "unknown-device"
        self.start_time = monotonic()

        if transport is None:
            headers = {"X-API-Key": api_key} if api_key else None
            self._transport: Transport = HTTPTransport(server_url, headers=headers)
        else:
            self._transport = transport

        self._middlewares: list[Middleware] = []
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        self._running = False
        self._stop_event = threading.Event()
        self._heartbeat_thread: threading.Thread | None = None

        log.info("device initialized id=%s", self.device_id)

    # ---- public configuration -----------------------------------------------------

    def use(self, middleware: Middleware) -> RISELDevice:
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        return self

    def transport(self, transport_impl: Transport) -> RISELDevice:
        if not isinstance(transport_impl, Transport):
            raise TypeError(f"{transport_impl!r} is not a Transport")
        try:
            self._transport.close()
        except Exception:
            pass
        self._transport = transport_impl
        return self

    def set_api_key(self, api_key: str, header_name: str = "X-API-Key") -> RISELDevice:
        self.api_key = api_key
        if isinstance(self._transport, HTTPTransport):
            self._transport.headers[header_name] = api_key
        return self

    def hook(self, event_name: str, callback: HookCallback) -> RISELDevice:
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    def on_ready(self, cb: HookCallback) -> RISELDevice:
        return self.hook("on_ready", cb)

    def on_error(self, cb: HookCallback) -> RISELDevice:
        return self.hook("on_error", cb)

    # ---- internals ---------------------------------------------------------------

    def _trigger_hook(self, event: str, *args: Any) -> None:
        for cb in self._hooks.get(event, []):
            try:
                cb(*args)
            except Exception:
                log.exception("hook %s failed", event)

    def _connect_wifi(self) -> bool:
        if not MICROPYTHON or not self.wifi_ssid:
            return True
        try:
            import network  # type: ignore[import-not-found]
        except ImportError:
            return True
        wlan = network.WLAN(network.STA_IF)
        wlan.active(True)
        if wlan.isconnected():
            return True
        log.info("connecting wifi ssid=%s", self.wifi_ssid)
        wlan.connect(self.wifi_ssid, self.wifi_password)
        deadline = monotonic() + 10
        while not wlan.isconnected():
            if monotonic() > deadline:
                log.warning("wifi connect timed out")
                return False
            sleep(0.5)
        log.info("wifi connected")
        return True

    def _local_ip(self) -> str:
        if not MICROPYTHON:
            return "0.0.0.0"
        try:
            import network  # type: ignore[import-not-found,unused-ignore]

            wlan = network.WLAN(network.STA_IF)
            if wlan.isconnected():
                return str(wlan.ifconfig()[0])
        except Exception:
            pass
        return "0.0.0.0"

    def _send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send `data` through the middleware pipeline. Raises TransportError."""
        while True:
            payload = data
            for mw in self._middlewares:
                try:
                    payload = mw.before_send(endpoint, payload)
                except Exception:
                    log.exception("middleware before_send failed")
            try:
                response = self._transport.send(endpoint, payload)
            except TransportError as exc:
                should_retry = False
                for mw in self._middlewares:
                    try:
                        if mw.on_error(endpoint, payload, exc):
                            should_retry = True
                    except Exception:
                        log.exception("middleware on_error failed")
                if not should_retry:
                    raise
                continue
            for mw in self._middlewares:
                try:
                    response = mw.after_send(endpoint, payload, response)
                except Exception:
                    log.exception("middleware after_send failed")
            return response

    def _build_heartbeat(self) -> dict[str, Any]:
        return {
            "device_id": self.device_id,
            "ip": self._local_ip(),
            "uptime": int(monotonic() - self.start_time),
            "version": self.firmware_version,
            "metadata": self.metadata,
        }

    def _send_heartbeat(self) -> bool:
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
        # The initial heartbeat is sent synchronously in start(); wait first
        # so we don't send two back-to-back.
        while not self._stop_event.wait(self.heartbeat_interval):
            try:
                self._send_heartbeat()
            except Exception:
                log.exception("heartbeat loop iteration crashed")

    # ---- public API --------------------------------------------------------------

    def report(self, event_type: str, data: dict[str, Any], severity: str = "info") -> bool:
        payload = {
            "device_id": self.device_id,
            "timestamp": now_unix(),
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
        """Connect, register, and start the heartbeat loop.

        With block=True the call blocks until stop() is called or the process
        is interrupted. With block=False the heartbeat thread runs in the
        background.
        """
        if self._running:
            raise RuntimeError("device already running")
        try:
            self._transport.connect()
        except Exception as exc:
            log.error("transport connect failed: %s", exc)
            self._trigger_hook("on_error", str(exc))
            return False
        if not self._connect_wifi():
            return False
        if not self._send_heartbeat():
            return False
        self._running = True
        self._stop_event.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop, daemon=True, name="risel-heartbeat"
        )
        self._heartbeat_thread.start()
        self._trigger_hook("on_ready")
        log.info("device started")
        if block:
            try:
                while self._running:
                    self._stop_event.wait(1)
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()
        return True

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=5)
            self._heartbeat_thread = None
        try:
            self._transport.close()
        except Exception:
            log.exception("transport close failed")
        log.info("device stopped")

    def __enter__(self) -> RISELDevice:
        self.start(block=False)
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()


def quick_start(
    server_url: str,
    wifi_ssid: str | None = None,
    wifi_password: str | None = None,
) -> RISELDevice:
    device = RISELDevice(server_url, wifi_ssid=wifi_ssid, wifi_password=wifi_password)
    device.start()
    return device
