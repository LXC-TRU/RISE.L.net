"""Asynchronous device-side client.

This module provides :class:`AsyncRISELDevice`, the async counterpart of
:class:`~rise_l_net.client.device.RISELDevice`.  It uses ``asyncio`` tasks
instead of threads and ``aiohttp`` for HTTP transport.

The API mirrors the sync version as closely as possible so that code can be
ported between the two with minimal changes.

Requires the ``aiohttp`` package::

    pip install "rise-l-net[async]"

Example::

    async with AsyncRISELDevice("http://server:8080") as device:
        await device.report("temperature", {"value": 23.5})
        await device.wait_closed()
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from .._compat import get_mac_address, monotonic, now_unix
from .._logging import get_logger
from ..exceptions import TransportError
from .async_transport import AsyncHTTPTransport, AsyncTransport
from .middleware import Middleware

log = get_logger("client.async")

# Hook callbacks may be plain callables or async coroutine functions.
HookCallback = Callable[..., Awaitable[None] | None]

DEFAULT_HEARTBEAT_INTERVAL = 30  # seconds between heartbeats
DEFAULT_FIRMWARE = "1.0.0"


class AsyncRISELDevice:
    """Async RISE.L.net device client.

    Usage::

        async with AsyncRISELDevice("http://server:8080") as device:
            await device.report("temperature", {"value": 23.5})
            await device.wait_closed()

    Args:
        server_url:          Base URL of the RISE.L.net server.
        device_id:           Unique device identifier.  Defaults to the MAC address.
        heartbeat_interval:  Seconds between periodic heartbeats.
        metadata:            Arbitrary key-value pairs sent with every heartbeat.
        api_key:             API key added as ``X-API-Key`` header if provided.
        firmware_version:    Version string reported in heartbeats.
        transport:           Custom async transport.  Defaults to
                             :class:`AsyncHTTPTransport`.
    """

    # All valid hook event names.
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
        device_id: str | None = None,
        heartbeat_interval: int = DEFAULT_HEARTBEAT_INTERVAL,
        metadata: dict[str, Any] | None = None,
        api_key: str | None = None,
        firmware_version: str = DEFAULT_FIRMWARE,
        transport: AsyncTransport | None = None,
    ) -> None:
        if not server_url:
            raise ValueError("server_url is required")
        self.server_url = server_url
        self.heartbeat_interval = heartbeat_interval
        # Copy the metadata dict so mutations by the caller don't affect us.
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.api_key = api_key
        self.firmware_version = firmware_version
        # Use the provided device_id, fall back to MAC address, then a placeholder.
        self.device_id = device_id or get_mac_address() or "unknown-device"
        # Record the start time for uptime calculation in heartbeats.
        self.start_time = monotonic()

        # Build the default async HTTP transport if none was provided.
        if transport is None:
            headers = {"X-API-Key": api_key} if api_key else None
            self._transport: AsyncTransport = AsyncHTTPTransport(server_url, headers=headers)
        else:
            self._transport = transport

        # Ordered list of middleware; applied in registration order.
        self._middlewares: list[Middleware] = []
        # Hook registry: event name → list of callbacks.
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        # Runtime state.
        self._running = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        # Event set when stop() completes; used by wait_closed().
        self._closed = asyncio.Event()

    # ---- public configuration API ------------------------------------------------

    def use(self, middleware: Middleware) -> AsyncRISELDevice:
        """Register a middleware in the send pipeline.  Returns ``self``."""
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        return self

    def transport(self, transport_impl: AsyncTransport) -> AsyncRISELDevice:
        """Replace the active transport.  Returns ``self``."""
        if not isinstance(transport_impl, AsyncTransport):
            raise TypeError(f"{transport_impl!r} is not an AsyncTransport")
        self._transport = transport_impl
        return self

    def hook(self, event_name: str, callback: HookCallback) -> AsyncRISELDevice:
        """Register a callback for a lifecycle event.  Returns ``self``.

        Callbacks may be plain functions or ``async`` coroutine functions.
        """
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    # ---- internals ---------------------------------------------------------------

    async def _trigger_hook(self, event: str, *args: Any) -> None:
        """Fire all callbacks registered for *event*.

        Supports both sync and async callbacks.  Exceptions are logged and
        swallowed so a buggy callback cannot crash the heartbeat task.
        """
        for cb in self._hooks.get(event, []):
            try:
                result = cb(*args)
                # Await the result if the callback is a coroutine function.
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                log.exception("hook %s failed", event)

    async def _send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send *data* through the middleware pipeline to *endpoint*.

        Handles retries: if any middleware's ``on_error`` returns True, the
        entire pipeline is repeated.

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
                response = await self._transport.send(endpoint, payload)
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
            # Async devices don't have direct access to the local IP; use placeholder.
            "ip": "0.0.0.0",
            "uptime": int(monotonic() - self.start_time),
            "version": self.firmware_version,
            "metadata": self.metadata,
        }

    async def _send_heartbeat(self) -> bool:
        """Send a single heartbeat.  Returns True on success, False on failure."""
        try:
            response = await self._send("/api/heartbeat", self._build_heartbeat())
        except TransportError as exc:
            log.warning("heartbeat failed: %s", exc)
            await self._trigger_hook("on_heartbeat_fail", str(exc))
            await self._trigger_hook("on_error", str(exc))
            return False
        await self._trigger_hook("on_heartbeat_success", response)
        return True

    async def _heartbeat_loop(self) -> None:
        """Asyncio task: send periodic heartbeats.

        The initial heartbeat is sent in :meth:`start` before this task is
        created, so we sleep first to avoid back-to-back heartbeats.

        ``asyncio.CancelledError`` is re-raised so that the task can be
        cancelled cleanly by :meth:`stop`.
        """
        # Wait for the interval before the first loop heartbeat.
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                raise  # propagate cancellation immediately
            try:
                await self._send_heartbeat()
            except asyncio.CancelledError:
                raise  # propagate cancellation from within the heartbeat
            except Exception:
                # Log and continue so a single failure doesn't kill the loop.
                log.exception("heartbeat loop iteration crashed")

    # ---- public API --------------------------------------------------------------

    async def report(self, event_type: str, data: dict[str, Any], severity: str = "info") -> bool:
        """Send a telemetry event to the server.

        Args:
            event_type: Application-defined category string.
            data:       Arbitrary payload dictionary.
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
            response = await self._send("/api/report", payload)
        except TransportError as exc:
            log.warning("report failed type=%s: %s", event_type, exc)
            await self._trigger_hook("on_report_fail", str(exc))
            await self._trigger_hook("on_error", str(exc))
            return False
        await self._trigger_hook("on_report_success", response)
        return True

    async def start(self) -> bool:
        """Connect to the server, register the device, and start the heartbeat task.

        Returns:
            True if startup succeeded, False if any step failed.
        """
        if self._running:
            raise RuntimeError("device already running")

        # Open the transport session (creates the aiohttp ClientSession).
        try:
            await self._transport.connect()
        except Exception as exc:
            log.error("transport connect failed: %s", exc)
            await self._trigger_hook("on_error", str(exc))
            return False

        # Send the initial heartbeat to register with the server.
        if not await self._send_heartbeat():
            return False

        # Mark as running and start the periodic heartbeat task.
        self._running = True
        self._closed.clear()  # reset in case stop() was called before
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(),
            name="risel-async-heartbeat",
        )

        # Notify listeners that the device is ready.
        await self._trigger_hook("on_ready")
        return True

    async def stop(self) -> None:
        """Stop the heartbeat task and release all resources.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if not self._running:
            return
        self._running = False

        # Cancel the heartbeat task and wait for it to finish.
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                # CancelledError is expected; other exceptions are swallowed.
                pass
            self._heartbeat_task = None

        # Close the transport session.
        try:
            await self._transport.close()
        except Exception:
            log.exception("transport close failed")

        # Signal wait_closed() that shutdown is complete.
        self._closed.set()

    async def wait_closed(self) -> None:
        """Block until :meth:`stop` has completed.

        Useful in scripts that want to keep the event loop alive until the
        device is explicitly stopped::

            device = AsyncRISELDevice("http://server:8080")
            await device.start()
            # ... do work ...
            await device.stop()
            await device.wait_closed()
        """
        await self._closed.wait()

    async def __aenter__(self) -> AsyncRISELDevice:
        """Start the device when used as an async context manager."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Stop the device when the async context manager exits."""
        await self.stop()
