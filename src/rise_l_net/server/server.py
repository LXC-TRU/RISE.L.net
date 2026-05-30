"""Synchronous RISE.L.net server."""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Callable
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .._compat import now_unix
from .._logging import get_logger
from ..exceptions import RISELError, ValidationError
from ..models import Device, Event
from .handlers import event_response, heartbeat_response, parse_event, parse_heartbeat
from .middleware import Middleware, Request
from .plugins import Plugin
from .storage import SQLiteStorage, Storage

log = get_logger("server")

# Type aliases
RouteHandler = Callable[[Request], dict[str, Any]]
HookCallback = Callable[..., None]

EventName = str

DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024  # 1 MiB
DEFAULT_DEVICE_TIMEOUT = 90  # seconds
DEFAULT_TIMEOUT_CHECK_INTERVAL = 30  # seconds


class RISELServer:
    """Synchronous device-management server.

    Basic usage::

        server = RISELServer(port=8080)
        server.start()  # blocks

    Extended usage::

        server = RISELServer(port=8080)
        server.use(AuthMiddleware(api_key="secret"))
        server.use(RateLimitMiddleware(max_requests_per_minute=120))
        server.storage(MyCustomStorage())
        server.route("/api/custom", custom_handler)
        server.plugin(WebhookPlugin("https://hook.url"))
        server.start()
    """

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
        storage: Storage | None = None,
    ) -> None:
        self.host = host
        self.port = port
        self.device_timeout = device_timeout
        self.max_body_bytes = max_body_bytes

        self._storage: Storage = storage if storage is not None else SQLiteStorage(db_path)
        self._middlewares: list[Middleware] = []
        self._plugins: list[Plugin] = []
        self._custom_routes: dict[str, RouteHandler] = {}
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        self._running = False
        self._httpd: ThreadingHTTPServer | None = None
        self._serve_thread: threading.Thread | None = None
        self._timeout_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        log.info("server initialized port=%d", port)

    # ---- public configuration API -------------------------------------------------

    def use(self, middleware: Middleware) -> RISELServer:
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        log.info("middleware registered: %s", middleware.__class__.__name__)
        return self

    def storage(self, storage: Storage) -> RISELServer:
        if not isinstance(storage, Storage):
            raise TypeError(f"{storage!r} is not a Storage")
        self._storage.close()
        self._storage = storage
        log.info("storage replaced: %s", storage.__class__.__name__)
        return self

    def plugin(self, plugin: Plugin) -> RISELServer:
        if not isinstance(plugin, Plugin):
            raise TypeError(f"{plugin!r} is not a Plugin")
        self._plugins.append(plugin)
        try:
            plugin.on_load(self)
        except Exception:
            log.exception("plugin on_load failed: %s", plugin.__class__.__name__)
        log.info("plugin registered: %s", plugin.__class__.__name__)
        return self

    def route(self, path: str, handler: RouteHandler) -> RISELServer:
        if not path.startswith("/"):
            raise ValueError("route path must start with '/'")
        self._custom_routes[path] = handler
        log.info("route registered: %s", path)
        return self

    def hook(self, event_name: EventName, callback: HookCallback) -> RISELServer:
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    # Backwards-compat shortcuts
    def on_device_registered(self, cb: HookCallback) -> RISELServer:
        return self.hook("on_device_registered", cb)

    def on_heartbeat(self, cb: HookCallback) -> RISELServer:
        return self.hook("on_heartbeat", cb)

    def on_report(self, cb: HookCallback) -> RISELServer:
        return self.hook("on_report", cb)

    def on_device_online(self, cb: HookCallback) -> RISELServer:
        return self.hook("on_device_online", cb)

    def on_device_offline(self, cb: HookCallback) -> RISELServer:
        return self.hook("on_device_offline", cb)

    # ---- read-only accessors ------------------------------------------------------

    def get_devices(self, status: str | None = None) -> list[Device]:
        return self._storage.list_devices(status)

    def get_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        return self._storage.list_events(device_id, limit)

    # ---- internals ---------------------------------------------------------------

    def _trigger_hook(self, event_name: str, *args: Any) -> None:
        for cb in self._hooks.get(event_name, []):
            try:
                cb(*args)
            except Exception:
                log.exception("hook %s failed", event_name)

    def _trigger_plugin(self, method_name: str, *args: Any) -> None:
        for plugin in self._plugins:
            method = getattr(plugin, method_name, None)
            if method is None:
                continue
            try:
                method(*args)
            except Exception:
                log.exception("plugin %s.%s failed", plugin.__class__.__name__, method_name)

    def _apply_before(self, request: Request) -> Request | None:
        for mw in self._middlewares:
            try:
                result = mw.before_request(request)
            except Exception as exc:
                log.exception("middleware before_request failed: %s", mw.__class__.__name__)
                mw.on_error(request, exc)
                return None
            if result is None:
                return None
            request = result
        return request

    def _apply_after(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        for mw in self._middlewares:
            try:
                response = mw.after_request(request, response)
            except Exception as exc:
                log.exception("middleware after_request failed: %s", mw.__class__.__name__)
                mw.on_error(request, exc)
        return response

    def _handle_heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        heartbeat = parse_heartbeat(payload)
        existing = self._storage.get_device(heartbeat.device_id)
        was_online = existing is not None and existing.status == "online"
        device, is_new = self._storage.upsert_device(heartbeat)
        if is_new:
            log.info("device registered: %s", device.device_id)
            self._trigger_hook("on_device_registered", device.device_id, payload)
            self._trigger_plugin("on_device_registered", device, heartbeat)
        elif not was_online:
            log.info("device online: %s", device.device_id)
            self._trigger_hook("on_device_online", device.device_id)
            self._trigger_plugin("on_device_online", device.device_id)
        self._trigger_hook("on_heartbeat", device.device_id, payload)
        self._trigger_plugin("on_heartbeat", device, heartbeat)
        return heartbeat_response(registered=is_new)

    def _handle_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        event = parse_event(payload)
        if event.timestamp == 0:
            event.timestamp = now_unix()
        event_id = self._storage.save_event(event)
        log.info("event saved id=%d device=%s type=%s", event_id, event.device_id, event.event_type)
        self._trigger_hook("on_report", event.device_id, payload)
        self._trigger_plugin("on_report", event.device_id, event)
        return event_response(event_id)

    def _check_device_timeout(self) -> None:
        while not self._stop_event.is_set():
            try:
                cutoff = now_unix() - self.device_timeout
                transitioned = self._storage.mark_offline(cutoff)
                for device_id in transitioned:
                    log.info("device offline: %s", device_id)
                    self._trigger_hook("on_device_offline", device_id)
                    self._trigger_plugin("on_device_offline", device_id)
            except Exception:
                log.exception("timeout checker iteration failed")
            self._stop_event.wait(DEFAULT_TIMEOUT_CHECK_INTERVAL)

    # ---- HTTP request dispatch ---------------------------------------------------

    def _dispatch(self, request: Request) -> tuple[int, dict[str, Any]]:
        applied = self._apply_before(request)
        if applied is None:
            return 403, {"error": "forbidden"}
        request = applied

        try:
            if request.path == "/api/heartbeat":
                response = self._handle_heartbeat(request.body)
            elif request.path == "/api/report":
                response = self._handle_report(request.body)
            elif request.path in self._custom_routes:
                response = self._custom_routes[request.path](request)
            else:
                return 404, {"error": "not found"}
        except ValidationError as exc:
            return 400, {"error": str(exc)}
        except RISELError as exc:
            log.warning("handler error: %s", exc)
            return 500, {"error": "internal error"}

        response = self._apply_after(request, response)
        return 200, response

    # ---- lifecycle ---------------------------------------------------------------

    def start(self, *, block: bool = True) -> None:
        """Start the HTTP listener and the timeout checker.

        With block=True (default) this method blocks until the server stops.
        With block=False the listener runs on a background thread and the
        method returns immediately.
        """
        if self._running:
            raise RuntimeError("server already running")
        self._running = True
        self._stop_event.clear()
        self._timeout_thread = threading.Thread(
            target=self._check_device_timeout, daemon=True, name="risel-timeout"
        )
        self._timeout_thread.start()

        handler_cls = _make_handler_class(self)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler_cls)
        log.info("server listening on http://%s:%d", self.host, self.port)
        if block:
            try:
                self._httpd.serve_forever()
            except KeyboardInterrupt:
                pass
            finally:
                self.stop()
        else:
            self._serve_thread = threading.Thread(
                target=self._httpd.serve_forever, daemon=True, name="risel-http"
            )
            self._serve_thread.start()

    def stop(self) -> None:
        if not self._running:
            return
        self._running = False
        self._stop_event.set()
        if self._httpd is not None:
            self._httpd.shutdown()
            self._httpd.server_close()
            self._httpd = None
        if self._serve_thread is not None:
            self._serve_thread.join(timeout=5)
            self._serve_thread = None
        if self._timeout_thread is not None:
            self._timeout_thread.join(timeout=5)
            self._timeout_thread = None
        try:
            self._storage.close()
        except Exception:
            log.exception("storage close failed")
        log.info("server stopped")

    def __enter__(self) -> RISELServer:
        self.start(block=False)
        # Briefly yield so the listener thread is up before tests connect.
        for _ in range(50):
            if self._httpd is not None:
                break
            time.sleep(0.01)
        return self

    def __exit__(self, *_: Any) -> None:
        self.stop()


def _make_handler_class(server: RISELServer) -> type[BaseHTTPRequestHandler]:
    """Build a BaseHTTPRequestHandler subclass bound to a server instance."""
    max_body = server.max_body_bytes

    class RequestHandler(BaseHTTPRequestHandler):
        # Silence default access logs; we use the library logger.
        def log_message(self, format: str, *args: Any) -> None:
            log.debug("http %s - %s", self.address_string(), format % args)

        def _write_json(self, status: int, body: dict[str, Any]) -> None:
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:
            try:
                length_header = self.headers.get("Content-Length")
                if length_header is None:
                    self._write_json(411, {"error": "Content-Length required"})
                    return
                try:
                    content_length = int(length_header)
                except ValueError:
                    self._write_json(400, {"error": "invalid Content-Length"})
                    return
                if content_length < 0 or content_length > max_body:
                    self._write_json(413, {"error": "request body too large"})
                    return

                raw = self.rfile.read(content_length) if content_length else b""
                try:
                    body = json.loads(raw.decode("utf-8")) if raw else {}
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self._write_json(400, {"error": "invalid JSON"})
                    return
                if not isinstance(body, dict):
                    self._write_json(400, {"error": "JSON object required"})
                    return

                request = Request(
                    path=self.path,
                    headers={k: v for k, v in self.headers.items()},
                    body=body,
                    device_id=body.get("device_id"),
                    remote_addr=self.client_address[0] if self.client_address else "",
                )
                status, response = server._dispatch(request)
                self._write_json(status, response)
            except Exception:
                log.exception("unhandled exception in request handler")
                try:
                    self._write_json(500, {"error": "internal error"})
                except Exception:
                    pass

    return RequestHandler


def quick_server(
    port: int = 8080,
    on_report: HookCallback | None = None,
) -> RISELServer:
    """Three-line server bootstrap. Blocks until interrupted."""
    server = RISELServer(port=port)
    if on_report is not None:
        server.on_report(on_report)
    server.start()
    return server
