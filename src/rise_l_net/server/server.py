"""Synchronous RISE.L.net server.

This module provides :class:`RISELServer`, the main entry point for running
a device-management server.  It handles:

* Receiving heartbeats from devices and updating their status in storage.
* Receiving telemetry events and persisting them.
* Detecting offline devices via a background timeout-checker thread.
* A middleware pipeline for authentication, rate limiting, and validation.
* A plugin system for side-effect integrations (webhooks, alerts, etc.).
* Custom route registration for application-specific endpoints.
* Lifecycle hooks so application code can react to device events.

The HTTP layer uses :class:`~http.server.ThreadingHTTPServer` so that slow
requests on one thread do not block other devices from sending heartbeats.

Security notes
--------------
* Request bodies are limited to ``max_body_bytes`` (default 1 MiB) to prevent
  memory exhaustion attacks.
* JSON parse errors return HTTP 400 without leaking internal details.
* Unhandled exceptions return HTTP 500 with a generic message; the full
  traceback is logged internally.
* API key comparison uses :func:`hmac.compare_digest` (via
  :class:`~rise_l_net.server.middleware.AuthMiddleware`) to prevent timing
  attacks.
"""

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

# Type aliases for readability.
RouteHandler = Callable[[Request], dict[str, Any]]
HookCallback = Callable[..., None]
EventName = str

# Default configuration constants.
DEFAULT_MAX_BODY_BYTES = 1 * 1024 * 1024  # 1 MiB — protects against large payloads
DEFAULT_DEVICE_TIMEOUT = 90  # seconds without a heartbeat before marking offline
DEFAULT_TIMEOUT_CHECK_INTERVAL = 30  # how often the timeout checker runs


class RISELServer:
    """Synchronous device-management server.

    Basic usage::

        server = RISELServer(port=8080)
        server.start()  # blocks until Ctrl-C

    Extended usage::

        server = RISELServer(port=8080)
        server.use(AuthMiddleware(api_key="secret"))
        server.use(RateLimitMiddleware(max_requests_per_minute=120))
        server.storage(MyCustomStorage())
        server.route("/api/custom", custom_handler)
        server.plugin(WebhookPlugin("https://hook.url"))
        server.start()

    Args:
        port:           TCP port to listen on.
        host:           Bind address.  ``"0.0.0.0"`` listens on all interfaces.
        db_path:        Path to the SQLite database file.  Ignored if *storage*
                        is provided.
        device_timeout: Seconds without a heartbeat before a device is marked
                        offline.
        max_body_bytes: Maximum allowed request body size in bytes.
        storage:        Custom storage backend.  Defaults to
                        :class:`~rise_l_net.server.storage.SQLiteStorage`.
    """

    # All valid hook event names.
    _SUPPORTED_HOOKS = (
        "on_device_registered",  # first heartbeat from a new device
        "on_heartbeat",  # every heartbeat (including first)
        "on_report",  # telemetry event received
        "on_device_online",  # device came back online after being offline
        "on_device_offline",  # device timed out and was marked offline
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

        # Use the provided storage backend or create a default SQLite one.
        self._storage: Storage = storage if storage is not None else SQLiteStorage(db_path)
        # Ordered list of middleware; applied in registration order.
        self._middlewares: list[Middleware] = []
        # List of registered plugins.
        self._plugins: list[Plugin] = []
        # Custom route handlers: path → callable.
        self._custom_routes: dict[str, RouteHandler] = {}
        # Hook registry: event name → list of callbacks.
        self._hooks: dict[str, list[HookCallback]] = {h: [] for h in self._SUPPORTED_HOOKS}

        # Runtime state.
        self._running = False
        self._httpd: ThreadingHTTPServer | None = None
        # Background thread running serve_forever() when block=False.
        self._serve_thread: threading.Thread | None = None
        # Background thread running the device timeout checker.
        self._timeout_thread: threading.Thread | None = None
        # Event used to signal background threads to stop.
        self._stop_event = threading.Event()

        log.info("server initialized port=%d", port)

    # ---- public configuration API ------------------------------------------------

    def use(self, middleware: Middleware) -> RISELServer:
        """Register a middleware in the request pipeline.

        Returns ``self`` for method chaining.
        """
        if not isinstance(middleware, Middleware):
            raise TypeError(f"{middleware!r} is not a Middleware")
        self._middlewares.append(middleware)
        log.info("middleware registered: %s", middleware.__class__.__name__)
        return self

    def storage(self, storage: Storage) -> RISELServer:
        """Replace the active storage backend.

        The old backend is closed before being replaced.  Returns ``self``.
        """
        if not isinstance(storage, Storage):
            raise TypeError(f"{storage!r} is not a Storage")
        # Close the old backend to release its database connection.
        self._storage.close()
        self._storage = storage
        log.info("storage replaced: %s", storage.__class__.__name__)
        return self

    def plugin(self, plugin: Plugin) -> RISELServer:
        """Register a plugin and call its ``on_load`` hook.

        Returns ``self`` for method chaining.
        """
        if not isinstance(plugin, Plugin):
            raise TypeError(f"{plugin!r} is not a Plugin")
        self._plugins.append(plugin)
        # Call on_load immediately so the plugin can perform setup.
        try:
            plugin.on_load(self)
        except Exception:
            log.exception("plugin on_load failed: %s", plugin.__class__.__name__)
        log.info("plugin registered: %s", plugin.__class__.__name__)
        return self

    def route(self, path: str, handler: RouteHandler) -> RISELServer:
        """Register a custom POST route handler.

        Args:
            path:    Server-relative path starting with ``"/"``.
            handler: Callable that receives a :class:`~rise_l_net.server.middleware.Request`
                     and returns a response dictionary.

        Returns:
            ``self`` for method chaining.
        """
        if not path.startswith("/"):
            raise ValueError("route path must start with '/'")
        self._custom_routes[path] = handler
        log.info("route registered: %s", path)
        return self

    def hook(self, event_name: EventName, callback: HookCallback) -> RISELServer:
        """Register a callback for a lifecycle event.

        Returns ``self`` for method chaining.

        Raises:
            ValueError: If *event_name* is not a supported hook.
        """
        if event_name not in self._hooks:
            raise ValueError(f"unknown hook event: {event_name!r}")
        self._hooks[event_name].append(callback)
        return self

    # Backwards-compatible shortcut methods for common hooks.
    def on_device_registered(self, cb: HookCallback) -> RISELServer:
        """Shortcut for ``hook("on_device_registered", cb)``."""
        return self.hook("on_device_registered", cb)

    def on_heartbeat(self, cb: HookCallback) -> RISELServer:
        """Shortcut for ``hook("on_heartbeat", cb)``."""
        return self.hook("on_heartbeat", cb)

    def on_report(self, cb: HookCallback) -> RISELServer:
        """Shortcut for ``hook("on_report", cb)``."""
        return self.hook("on_report", cb)

    def on_device_online(self, cb: HookCallback) -> RISELServer:
        """Shortcut for ``hook("on_device_online", cb)``."""
        return self.hook("on_device_online", cb)

    def on_device_offline(self, cb: HookCallback) -> RISELServer:
        """Shortcut for ``hook("on_device_offline", cb)``."""
        return self.hook("on_device_offline", cb)

    # ---- read-only accessors -----------------------------------------------------

    def get_devices(self, status: str | None = None) -> list[Device]:
        """Return all registered devices, optionally filtered by status."""
        return self._storage.list_devices(status)

    def get_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return telemetry events, optionally filtered by device."""
        return self._storage.list_events(device_id, limit)

    # ---- internals ---------------------------------------------------------------

    def _trigger_hook(self, event_name: str, *args: Any) -> None:
        """Fire all callbacks registered for *event_name*.

        Exceptions are logged and swallowed so a buggy callback cannot crash
        the server or affect the device's response.
        """
        for cb in self._hooks.get(event_name, []):
            try:
                cb(*args)
            except Exception:
                log.exception("hook %s failed", event_name)

    def _trigger_plugin(self, method_name: str, *args: Any) -> None:
        """Call *method_name* on every registered plugin.

        Uses ``getattr`` so that plugins only need to implement the hooks they
        care about.  Exceptions are logged and swallowed.
        """
        for plugin in self._plugins:
            method = getattr(plugin, method_name, None)
            if method is None:
                continue  # plugin does not implement this hook
            try:
                method(*args)
            except Exception:
                log.exception("plugin %s.%s failed", plugin.__class__.__name__, method_name)

    def _apply_before(self, request: Request) -> Request | None:
        """Run the request through all ``before_request`` middleware hooks.

        Returns None if any middleware short-circuits the pipeline (i.e.
        returns None from its ``before_request`` hook).
        """
        for mw in self._middlewares:
            try:
                result = mw.before_request(request)
            except Exception as exc:
                log.exception("middleware before_request failed: %s", mw.__class__.__name__)
                mw.on_error(request, exc)
                return None  # treat middleware exceptions as a block
            if result is None:
                return None  # middleware explicitly blocked the request
            request = result
        return request

    def _apply_after(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        """Run the response through all ``after_request`` middleware hooks."""
        for mw in self._middlewares:
            try:
                response = mw.after_request(request, response)
            except Exception as exc:
                log.exception("middleware after_request failed: %s", mw.__class__.__name__)
                mw.on_error(request, exc)
        return response

    def _handle_heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process a heartbeat payload and return the response dictionary.

        Determines whether this is a new registration or an update, fires the
        appropriate hooks and plugins, and returns the acknowledgement.
        """
        heartbeat = parse_heartbeat(payload)
        # Check the current status before upserting so we can detect transitions.
        existing = self._storage.get_device(heartbeat.device_id)
        was_online = existing is not None and existing.status == "online"
        device, is_new = self._storage.upsert_device(heartbeat)

        if is_new:
            # First heartbeat from this device — fire registration hooks.
            log.info("device registered: %s", device.device_id)
            self._trigger_hook("on_device_registered", device.device_id, payload)
            self._trigger_plugin("on_device_registered", device, heartbeat)
        elif not was_online:
            # Device was offline and just came back — fire online hooks.
            log.info("device online: %s", device.device_id)
            self._trigger_hook("on_device_online", device.device_id)
            self._trigger_plugin("on_device_online", device.device_id)

        # Always fire the heartbeat hook regardless of registration status.
        self._trigger_hook("on_heartbeat", device.device_id, payload)
        self._trigger_plugin("on_heartbeat", device, heartbeat)
        return heartbeat_response(registered=is_new)

    def _handle_report(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Process a telemetry event payload and return the response dictionary."""
        event = parse_event(payload)
        # Fill in the server timestamp if the device did not provide one.
        if event.timestamp == 0:
            event.timestamp = now_unix()
        event_id = self._storage.save_event(event)
        log.info("event saved id=%d device=%s type=%s", event_id, event.device_id, event.event_type)
        self._trigger_hook("on_report", event.device_id, payload)
        self._trigger_plugin("on_report", event.device_id, event)
        return event_response(event_id)

    def _check_device_timeout(self) -> None:
        """Background thread: periodically mark stale devices as offline.

        Runs every ``DEFAULT_TIMEOUT_CHECK_INTERVAL`` seconds.  Uses
        ``_stop_event.wait()`` instead of ``time.sleep()`` so the thread
        wakes up immediately when ``stop()`` is called.
        """
        while not self._stop_event.is_set():
            try:
                # Any device whose last heartbeat is older than device_timeout
                # seconds is considered offline.
                cutoff = now_unix() - self.device_timeout
                transitioned = self._storage.mark_offline(cutoff)
                for device_id in transitioned:
                    log.info("device offline: %s", device_id)
                    self._trigger_hook("on_device_offline", device_id)
                    self._trigger_plugin("on_device_offline", device_id)
            except Exception:
                log.exception("timeout checker iteration failed")
            # Wait for the interval or until stop() signals us to exit.
            self._stop_event.wait(DEFAULT_TIMEOUT_CHECK_INTERVAL)

    # ---- HTTP request dispatch ---------------------------------------------------

    def _dispatch(self, request: Request) -> tuple[int, dict[str, Any]]:
        """Route a request to the appropriate handler and return (status, body).

        Applies middleware before and after the handler.  Returns HTTP 403 if
        middleware blocks the request, 404 for unknown paths, 400 for
        validation errors, and 500 for unexpected errors.
        """
        # Run before_request middleware; returns None if the request is blocked.
        applied = self._apply_before(request)
        if applied is None:
            return 403, {"error": "forbidden"}
        request = applied

        try:
            # Route to the appropriate built-in or custom handler.
            if request.path == "/api/heartbeat":
                response = self._handle_heartbeat(request.body)
            elif request.path == "/api/report":
                response = self._handle_report(request.body)
            elif request.path in self._custom_routes:
                response = self._custom_routes[request.path](request)
            else:
                return 404, {"error": "not found"}
        except ValidationError as exc:
            # Client sent a malformed payload — return 400 with the error message.
            return 400, {"error": str(exc)}
        except RISELError as exc:
            # Internal library error — log it but don't leak details to the client.
            log.warning("handler error: %s", exc)
            return 500, {"error": "internal error"}

        # Run after_request middleware on the successful response.
        response = self._apply_after(request, response)
        return 200, response

    # ---- lifecycle ---------------------------------------------------------------

    def start(self, *, block: bool = True) -> None:
        """Start the HTTP listener and the device timeout checker.

        Args:
            block: If True (default), block the calling thread until the server
                   is stopped (e.g. by Ctrl-C or :meth:`stop`).  If False, the
                   listener runs on a background daemon thread and this method
                   returns immediately.
        """
        if self._running:
            raise RuntimeError("server already running")
        self._running = True
        self._stop_event.clear()

        # Start the timeout checker on a daemon thread so it exits automatically
        # when the main thread exits.
        self._timeout_thread = threading.Thread(
            target=self._check_device_timeout, daemon=True, name="risel-timeout"
        )
        self._timeout_thread.start()

        # Build the request handler class bound to this server instance.
        handler_cls = _make_handler_class(self)
        self._httpd = ThreadingHTTPServer((self.host, self.port), handler_cls)
        log.info("server listening on http://%s:%d", self.host, self.port)

        if block:
            # Blocking mode: serve_forever() runs in the calling thread.
            try:
                self._httpd.serve_forever()
            except KeyboardInterrupt:
                pass  # Ctrl-C is a normal way to stop the server
            finally:
                self.stop()
        else:
            # Non-blocking mode: serve_forever() runs on a background thread.
            self._serve_thread = threading.Thread(
                target=self._httpd.serve_forever, daemon=True, name="risel-http"
            )
            self._serve_thread.start()

    def stop(self) -> None:
        """Stop the server and release all resources.

        Safe to call multiple times; subsequent calls are no-ops.
        """
        if not self._running:
            return
        self._running = False
        # Signal background threads to stop.
        self._stop_event.set()

        # Shut down the HTTP server gracefully.
        if self._httpd is not None:
            self._httpd.shutdown()  # stops serve_forever()
            self._httpd.server_close()  # closes the listening socket
            self._httpd = None

        # Wait for background threads to finish.
        if self._serve_thread is not None:
            self._serve_thread.join(timeout=5)
            self._serve_thread = None
        if self._timeout_thread is not None:
            self._timeout_thread.join(timeout=5)
            self._timeout_thread = None

        # Close the storage backend to flush any pending writes.
        try:
            self._storage.close()
        except Exception:
            log.exception("storage close failed")
        log.info("server stopped")

    def __enter__(self) -> RISELServer:
        """Start the server (non-blocking) when used as a context manager."""
        self.start(block=False)
        # Poll briefly until the listener thread is accepting connections.
        # This prevents tests from connecting before the socket is ready.
        for _ in range(50):
            if self._httpd is not None:
                break
            time.sleep(0.01)
        return self

    def __exit__(self, *_: Any) -> None:
        """Stop the server when the context manager exits."""
        self.stop()


def _make_handler_class(server: RISELServer) -> type[BaseHTTPRequestHandler]:
    """Build a ``BaseHTTPRequestHandler`` subclass bound to *server*.

    We use a factory function (closure) rather than a class attribute because
    ``BaseHTTPRequestHandler`` is instantiated per-request by the HTTP server
    framework, so we cannot pass the server instance via ``__init__``.
    """
    # Capture max_body in the closure to avoid an attribute lookup per request.
    max_body = server.max_body_bytes

    class RequestHandler(BaseHTTPRequestHandler):
        """Per-request HTTP handler.  One instance is created per connection."""

        def log_message(self, format: str, *args: Any) -> None:
            """Override the default access log to use the library logger."""
            log.debug("http %s - %s", self.address_string(), format % args)

        def _write_json(self, status: int, body: dict[str, Any]) -> None:
            """Serialise *body* to JSON and write the HTTP response."""
            payload = json.dumps(body).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            # Always include Content-Length so clients know when the body ends.
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_POST(self) -> None:
            """Handle an inbound POST request."""
            try:
                # Require Content-Length to prevent reading an unbounded stream.
                length_header = self.headers.get("Content-Length")
                if length_header is None:
                    self._write_json(411, {"error": "Content-Length required"})
                    return

                try:
                    content_length = int(length_header)
                except ValueError:
                    self._write_json(400, {"error": "invalid Content-Length"})
                    return

                # Reject bodies that exceed the configured size limit.
                if content_length < 0 or content_length > max_body:
                    self._write_json(413, {"error": "request body too large"})
                    return

                # Read exactly content_length bytes from the socket.
                raw = self.rfile.read(content_length) if content_length else b""

                # Parse the JSON body; return 400 on any decode error.
                try:
                    body = json.loads(raw.decode("utf-8")) if raw else {}
                except (json.JSONDecodeError, UnicodeDecodeError):
                    self._write_json(400, {"error": "invalid JSON"})
                    return

                # The API only accepts JSON objects (dicts), not arrays or scalars.
                if not isinstance(body, dict):
                    self._write_json(400, {"error": "JSON object required"})
                    return

                # Build the normalised Request object for the middleware pipeline.
                request = Request(
                    path=self.path,
                    # Copy headers into a plain dict for easier access.
                    headers={k: v for k, v in self.headers.items()},
                    body=body,
                    device_id=body.get("device_id"),
                    remote_addr=self.client_address[0] if self.client_address else "",
                )

                # Dispatch through middleware and route handlers.
                status, response = server._dispatch(request)
                self._write_json(status, response)

            except Exception:
                # Catch-all: log the full traceback but return a generic 500
                # to avoid leaking internal details to the client.
                log.exception("unhandled exception in request handler")
                try:
                    self._write_json(500, {"error": "internal error"})
                except Exception:
                    pass  # ignore errors writing the error response

    return RequestHandler


def quick_server(
    port: int = 8080,
    on_report: HookCallback | None = None,
) -> RISELServer:
    """Convenience function: create a server, optionally register a report hook, and start it.

    Blocks until the server is stopped (Ctrl-C or :meth:`RISELServer.stop`).

    Args:
        port:      TCP port to listen on.
        on_report: Optional callback invoked for every telemetry event.

    Returns:
        The :class:`RISELServer` instance (after it has stopped).
    """
    server = RISELServer(port=port)
    if on_report is not None:
        server.on_report(on_report)
    server.start()  # blocks
    return server
