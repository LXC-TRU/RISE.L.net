"""Asynchronous RISE.L.net server (aiohttp-backed).

This module provides :class:`AsyncRISELServer`, the async counterpart of
:class:`~rise_l_net.server.server.RISELServer`.  It uses ``aiohttp`` for the
HTTP layer and ``asyncio`` tasks for the timeout checker, which means it can
handle many concurrent connections without the overhead of one thread per
connection.

The public API mirrors the sync server as closely as possible so that code
can be ported between the two with minimal changes.

Requires the ``aiohttp`` and ``aiosqlite`` packages::

    pip install "rise-l-net[async]"

Example::

    async with AsyncRISELServer(port=8080) as server:
        server.hook("on_report", my_async_handler)
        await server.wait_closed()
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import TYPE_CHECKING, Any

from .._compat import now_unix
from .._logging import get_logger
from ..exceptions import RISELError, ValidationError
from ..models import Device, Event
from .async_storage import AsyncSQLiteStorage, AsyncStorage
from .handlers import event_response, heartbeat_response, parse_event, parse_heartbeat
from .middleware import Middleware, Request

if TYPE_CHECKING:
    # Deferred import: only used for type annotations so that the module can be
    # imported without aiohttp installed (the ImportError is raised in __init__).
    from aiohttp import web

log = get_logger("server.async")

# Type aliases.
AsyncRouteHandler = Callable[[Request], Awaitable[dict[str, Any]]]
HookCallback = Callable[..., Awaitable[None] | None]

# Default configuration constants.
DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024  # 1 MiB
DEFAULT_DEVICE_TIMEOUT = 90  # seconds without a heartbeat before marking offline
DEFAULT_TIMEOUT_CHECK_INTERVAL = 30  # how often the timeout checker runs


class AsyncRISELServer:
    """Async device-management server built on aiohttp.

    Usage (non-blocking start)::

        server = AsyncRISELServer(port=8080)
        await server.start()
        try:
            await asyncio.Event().wait()  # keep the event loop alive
        finally:
            await server.stop()

    Usage (async context manager)::

        async with AsyncRISELServer(port=8080) as server:
            await server.wait_closed()

    Args:
        port:           TCP port to listen on.
        host:           Bind address.
        db_path:        Path to the SQLite database file.  Ignored if *storage*
                        is provided.
        device_timeout: Seconds without a heartbeat before a device is marked
                        offline.
        max_body_bytes: Maximum allowed request body size in bytes.
        storage:        Custom async storage backend.  Defaults to
                        :class:`~rise_l_net.server.async_storage.AsyncSQLiteStorage`.
    """

    # All valid hook event names.
    _SUPPORTED_HOOKS = (
        "on_device_registered",
        "on_heartbeat",
        "on_report",
        "on_device_online",
        "on_device_offline",
    )

    def __init__(
        self,
        port: int = 8080,
        host: str = "0.0.0.0",
        db_path: str = "riselnet.db",
        device_timeout: int = DEFAULT_DEVICE_TIMEOUT,
        max_body_bytes: int = DEFAULT_MAX_BODY_BYTES,
        storage: AsyncStorage | None = None,
    ) -> None:
        # Fail fast at construction time if aiohttp is not installed.
        try:
            import aiohttp  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "AsyncRISELServer requires aiohttp. Install with: pip install rise-l-net[async]"
            ) from exc

        self.host = host
        self.port = port
        self.device_timeout = device_timeout
        self.max_body_bytes = max_body_bytes

        # Use the provided storage backend or create a default async SQLite one.
        self._storage: AsyncStorage = (
            storage if storage is not None else AsyncSQLiteStorage(db_path)
        )
        self._middlewares: list[Middleware] = []
        # Plugins may be sync or async; we handle both in _trigger_plugin.
        self._plugins: list[Any] = []
        self._custom_routes: dict[str, AsyncRouteHandler] = {}
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        # aiohttp runner and site are created in start().
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        # Background asyncio task running the device timeout checker.
        self._timeout_task: asyncio.Task[None] | None = None
        # Set of background tasks created by plugin.on_load() coroutines.
        # Keeping strong references prevents them from being garbage-collected.
        self._background_tasks: set[asyncio.Task[Any]] = set()
        # Event set when stop() completes; used by wait_closed().
        self._closed = asyncio.Event()
        self._running = False

        log.info("async server initialized port=%d", port)

    # ---- public configuration API ------------------------------------------------

    def use(self, middleware: Middleware) -> AsyncRISELServer:
        """Register a middleware in the request pipeline.  Returns ``self``."""
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        return self

    def storage(self, storage: AsyncStorage) -> AsyncRISELServer:
        """Replace the active async storage backend.  Returns ``self``."""
        if not isinstance(storage, AsyncStorage):
            raise TypeError(f"{storage!r} is not an AsyncStorage")
        self._storage = storage
        return self

    def plugin(self, plugin: Any) -> AsyncRISELServer:
        """Register a plugin and call its ``on_load`` hook.

        If ``on_load`` returns a coroutine, it is scheduled as a background
        task.  A strong reference is kept until the task completes to prevent
        premature garbage collection.

        Returns ``self`` for method chaining.
        """
        self._plugins.append(plugin)
        on_load = getattr(plugin, "on_load", None)
        if on_load is not None:
            try:
                result = on_load(self)
                if asyncio.iscoroutine(result):
                    # Schedule on the running loop and keep a strong reference
                    # so the task can't be garbage-collected mid-flight.
                    task = asyncio.ensure_future(result)
                    self._background_tasks.add(task)
                    # Remove the reference once the task is done.
                    task.add_done_callback(self._background_tasks.discard)
            except Exception:
                log.exception("plugin on_load failed: %s", plugin.__class__.__name__)
        return self

    def route(self, path: str, handler: AsyncRouteHandler) -> AsyncRISELServer:
        """Register a custom async POST route handler.  Returns ``self``."""
        if not path.startswith("/"):
            raise ValueError("route path must start with '/'")
        self._custom_routes[path] = handler
        return self

    def hook(self, event_name: str, callback: HookCallback) -> AsyncRISELServer:
        """Register a callback for a lifecycle event.  Returns ``self``.

        Callbacks may be plain functions or ``async`` coroutine functions.
        """
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    # ---- internals ---------------------------------------------------------------

    async def _trigger_hook(self, event_name: str, *args: Any) -> None:
        """Fire all callbacks registered for *event_name*.

        Supports both sync and async callbacks.  Exceptions are logged and
        swallowed so a buggy callback cannot crash the server.
        """
        for cb in self._hooks.get(event_name, []):
            try:
                result = cb(*args)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                log.exception("hook %s failed", event_name)

    async def _trigger_plugin(self, method_name: str, *args: Any) -> None:
        """Call *method_name* on every registered plugin.

        Supports both sync and async plugin methods.  Exceptions are logged
        and swallowed.
        """
        for plugin in self._plugins:
            method = getattr(plugin, method_name, None)
            if method is None:
                continue
            try:
                result = method(*args)
                if asyncio.iscoroutine(result):
                    await result
            except Exception:
                log.exception("plugin %s.%s failed", plugin.__class__.__name__, method_name)

    def _apply_before(self, request: Request) -> Request | None:
        """Run the request through all ``before_request`` middleware hooks.

        Note: middleware hooks are synchronous even in the async server.
        This keeps the middleware API consistent between sync and async servers.
        """
        for mw in self._middlewares:
            try:
                result = mw.before_request(request)
            except Exception as exc:
                log.exception("middleware before_request failed")
                mw.on_error(request, exc)
                return None
            if result is None:
                return None
            request = result
        return request

    def _apply_after(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        """Run the response through all ``after_request`` middleware hooks."""
        for mw in self._middlewares:
            try:
                response = mw.after_request(request, response)
            except Exception as exc:
                log.exception("middleware after_request failed")
                mw.on_error(request, exc)
        return response

    async def _handle_heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process a heartbeat payload and return the response dictionary."""
        heartbeat = parse_heartbeat(payload)
        existing = await self._storage.get_device(heartbeat.device_id)
        was_online = existing is not None and existing.status == "online"
        device, is_new = await self._storage.upsert_device(heartbeat)

        if is_new:
            log.info("device registered: %s", device.device_id)
            await self._trigger_hook("on_device_registered", device.device_id, payload)
            await self._trigger_plugin("on_device_registered", device, heartbeat)
        elif not was_online:
            log.info("device online: %s", device.device_id)
            await self._trigger_hook("on_device_online", device.device_id)
            await self._trigger_plugin("on_device_online", device.device_id)

        await self._trigger_hook("on_heartbeat", device.device_id, payload)
        await self._trigger_plugin("on_heartbeat", device, heartbeat)
        return heartbeat_response(registered=is_new)

    async def _handle_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process a telemetry event payload and return the response dictionary."""
        event = parse_event(payload)
        if event.timestamp == 0:
            event.timestamp = now_unix()
        event_id = await self._storage.save_event(event)
        log.info("event saved id=%d device=%s type=%s", event_id, event.device_id, event.event_type)
        await self._trigger_hook("on_report", event.device_id, payload)
        await self._trigger_plugin("on_report", event.device_id, event)
        return event_response(event_id)

    async def _timeout_loop(self) -> None:
        """Asyncio task: periodically mark stale devices as offline.

        Runs every ``DEFAULT_TIMEOUT_CHECK_INTERVAL`` seconds.
        ``asyncio.CancelledError`` is re-raised so the task can be cancelled
        cleanly by :meth:`stop`.
        """
        while True:
            try:
                cutoff = now_unix() - self.device_timeout
                transitioned = await self._storage.mark_offline(cutoff)
                for device_id in transitioned:
                    log.info("device offline: %s", device_id)
                    await self._trigger_hook("on_device_offline", device_id)
                    await self._trigger_plugin("on_device_offline", device_id)
            except asyncio.CancelledError:
                raise  # propagate cancellation immediately
            except Exception:
                log.exception("timeout checker iteration failed")
            try:
                await asyncio.sleep(DEFAULT_TIMEOUT_CHECK_INTERVAL)
            except asyncio.CancelledError:
                raise

    # ---- aiohttp view factories --------------------------------------------------

    async def _make_app(self) -> web.Application:
        """Build and return the aiohttp Application with all routes registered."""
        from aiohttp import web

        # Set the maximum body size at the application level.
        app = web.Application(client_max_size=self.max_body_bytes)
        # Register built-in routes using the _view_builtin factory.
        app.router.add_post("/api/heartbeat", self._view_builtin(self._handle_heartbeat))
        app.router.add_post("/api/report", self._view_builtin(self._handle_report))
        # Register custom routes using the _view_custom factory.
        for path, handler in self._custom_routes.items():
            app.router.add_post(path, self._view_custom(handler))
        return app

    async def _parse_request(self, req: web.Request) -> tuple[Request | None, web.Response | None]:
        """Parse and validate an aiohttp request.

        Returns ``(request_obj, None)`` on success, or ``(None, error_response)``
        if the request is invalid or blocked by middleware.
        """
        from aiohttp import web

        # Parse the JSON body; return 400 on any decode error.
        try:
            payload = await req.json()
        except Exception:
            return None, web.json_response({"error": "invalid JSON"}, status=400)

        if not isinstance(payload, dict):
            return None, web.json_response({"error": "JSON object required"}, status=400)

        # Extract device_id only if it is a string (guard against wrong types).
        device_id = payload.get("device_id") if isinstance(payload.get("device_id"), str) else None

        request_obj = Request(
            path=req.path,
            headers={k: v for k, v in req.headers.items()},
            body=payload,
            device_id=device_id,
            remote_addr=req.remote or "",
        )

        # Run the middleware pipeline; return 403 if any middleware blocks.
        applied = self._apply_before(request_obj)
        if applied is None:
            return None, web.json_response({"error": "forbidden"}, status=403)
        return applied, None

    def _view_builtin(
        self, handler: Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Return an aiohttp view function for a built-in handler.

        Built-in handlers (heartbeat, report) receive the raw body dict.
        """

        async def view(req: web.Request) -> web.Response:
            from aiohttp import web

            request_obj, error = await self._parse_request(req)
            if error is not None:
                return error  # parsing or middleware blocked the request
            assert request_obj is not None

            try:
                # Pass the raw body dict to the built-in handler.
                response = await handler(request_obj.body)
            except ValidationError as exc:
                return web.json_response({"error": str(exc)}, status=400)
            except RISELError:
                log.exception("handler error")
                return web.json_response({"error": "internal error"}, status=500)

            response = self._apply_after(request_obj, response)
            return web.json_response(response)

        return view

    def _view_custom(
        self, handler: AsyncRouteHandler
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Return an aiohttp view function for a custom route handler.

        Custom handlers receive the full :class:`~rise_l_net.server.middleware.Request`
        object (not just the body) so they can access headers and metadata.
        """

        async def view(req: web.Request) -> web.Response:
            from aiohttp import web

            request_obj, error = await self._parse_request(req)
            if error is not None:
                return error

            assert request_obj is not None
            try:
                # Pass the full Request object to the custom handler.
                response = await handler(request_obj)
            except ValidationError as exc:
                return web.json_response({"error": str(exc)}, status=400)
            except RISELError:
                log.exception("handler error")
                return web.json_response({"error": "internal error"}, status=500)

            response = self._apply_after(request_obj, response)
            return web.json_response(response)

        return view

    # ---- read-only accessors -----------------------------------------------------

    async def get_devices(self, status: str | None = None) -> list[Device]:
        """Return all registered devices, optionally filtered by status."""
        return await self._storage.list_devices(status)

    async def get_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return telemetry events, optionally filtered by device."""
        return await self._storage.list_events(device_id, limit)

    # ---- lifecycle ---------------------------------------------------------------

    async def start(self) -> None:
        """Start the aiohttp server and the device timeout task.

        Non-blocking: returns immediately after the server is listening.
        Use :meth:`wait_closed` to block until :meth:`stop` is called.
        """
        from aiohttp import web

        if self._running:
            raise RuntimeError("server already running")

        # Build the aiohttp application with all routes.
        app = await self._make_app()
        # AppRunner manages the application lifecycle.
        self._runner = web.AppRunner(app, access_log=None)  # suppress aiohttp access logs
        await self._runner.setup()
        # TCPSite binds to the host/port and starts accepting connections.
        self._site = web.TCPSite(self._runner, host=self.host, port=self.port)
        await self._site.start()

        # Start the background timeout checker task.
        self._timeout_task = asyncio.create_task(self._timeout_loop(), name="risel-async-timeout")
        self._running = True
        self._closed.clear()
        log.info("async server listening on http://%s:%d", self.host, self.port)

    async def stop(self) -> None:
        """Stop the server and release all resources.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if not self._running:
            return
        self._running = False

        # Cancel and await the timeout checker task.
        if self._timeout_task is not None:
            self._timeout_task.cancel()
            try:
                await self._timeout_task
            except (asyncio.CancelledError, Exception):
                # CancelledError is expected; other exceptions are swallowed.
                pass
            self._timeout_task = None

        # Stop the aiohttp site and clean up the runner.
        if self._site is not None:
            await self._site.stop()
            self._site = None
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

        # Close the storage backend.
        try:
            await self._storage.close()
        except Exception:
            log.exception("storage close failed")

        # Signal wait_closed() that shutdown is complete.
        self._closed.set()
        log.info("async server stopped")

    async def wait_closed(self) -> None:
        """Block until :meth:`stop` has completed.

        Useful for keeping the event loop alive until the server is stopped
        externally (e.g. by a signal handler).
        """
        await self._closed.wait()

    async def __aenter__(self) -> AsyncRISELServer:
        """Start the server when used as an async context manager."""
        await self.start()
        return self

    async def __aexit__(self, *_: Any) -> None:
        """Stop the server when the async context manager exits."""
        await self.stop()
