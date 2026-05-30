"""Asynchronous device-side client (aiohttp)."""

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

HookCallback = Callable[..., Awaitable[None] | None]

DEFAULT_HEARTBEAT_INTERVAL = 30
DEFAULT_FIRMWARE = "1.0.0"


class AsyncRISELDevice:
    """Async RISE.L.net device client.

    Usage::

        async with AsyncRISELDevice("http://server:8080") as device:
            await device.report("temperature", {"value": 23.5})
            await device.wait_closed()
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
        self.metadata: dict[str, Any] = dict(metadata or {})
        self.api_key = api_key
        self.firmware_version = firmware_version
        self.device_id = device_id or get_mac_address() or "unknown-device"
        self.start_time = monotonic()

        if transport is None:
            headers = {"X-API-Key": api_key} if api_key else None
            self._transport: AsyncTransport = AsyncHTTPTransport(server_url, headers=headers)
        else:
            self._transport = transport

        self._middlewares: list[Middleware] = []
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        self._running = False
        self._heartbeat_task: asyncio.Task[None] | None = None
        self._closed = asyncio.Event()

    # ---- public configuration -----------------------------------------------------

    def use(self, middleware: Middleware) -> AsyncRISELDevice:
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        return self

    def transport(self, transport_impl: AsyncTransport) -> AsyncRISELDevice:
        if not isinstance(transport_impl, AsyncTransport):
            raise TypeError(f"{transport_impl!r} is not an AsyncTransport")
        self._transport = transport_impl
        return self

    def hook(self, event_name: str, callback: HookCallback) -> AsyncRISELDevice:
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    # ---- internals ---------------------------------------------------------------

    async def _trigger_hook(self, event: str, *args: Any) -> None:
        for cb in self._hooks.get(event, []):
            try:
                result = cb(*args)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                log.exception("hook %s failed", event)

    async def _send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        while True:
            payload = data
            for mw in self._middlewares:
                try:
                    payload = mw.before_send(endpoint, payload)
                except Exception:
                    log.exception("middleware before_send failed")
            try:
                response = await self._transport.send(endpoint, payload)
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
            "ip": "0.0.0.0",
            "uptime": int(monotonic() - self.start_time),
            "version": self.firmware_version,
            "metadata": self.metadata,
        }

    async def _send_heartbeat(self) -> bool:
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
        # Initial heartbeat already sent in start(); wait first.
        while self._running:
            try:
                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                raise
            try:
                await self._send_heartbeat()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("heartbeat loop iteration crashed")

    # ---- public API --------------------------------------------------------------

    async def report(self, event_type: str, data: dict[str, Any], severity: str = "info") -> bool:
        payload = {
            "device_id": self.device_id,
            "timestamp": now_unix(),
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
        if self._running:
            raise RuntimeError("device already running")
        try:
            await self._transport.connect()
        except Exception as exc:
            log.error("transport connect failed: %s", exc)
            await self._trigger_hook("on_error", str(exc))
            return False
        if not await self._send_heartbeat():
            return False
        self._running = True
        self._closed.clear()
        self._heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(), name="risel-async-heartbeat"
        )
        await self._trigger_hook("on_ready")
        return True

    async def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except (asyncio.CancelledError, Exception):
                pass
            self._heartbeat_task = None
        try:
            await self._transport.close()
        except Exception:
            log.exception("transport close failed")
        self._closed.set()

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def __aenter__(self) -> AsyncRISELDevice:
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.stop()
