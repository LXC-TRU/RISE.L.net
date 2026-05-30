"""Server-side middleware for inbound HTTP requests.

Server middleware intercepts every inbound request and can:

* Inspect or modify the request before it reaches the route handler
  (``before_request``).
* Inspect or modify the response before it is sent back to the device
  (``after_request``).
* React to errors raised during request processing (``on_error``).

Middleware is applied in registration order.  If any ``before_request`` hook
returns ``None``, the pipeline is short-circuited and the server returns
HTTP 403 Forbidden without calling the route handler.

Example::

    server = RISELServer(port=8080)
    server.use(AuthMiddleware(api_key="secret"))
    server.use(RateLimitMiddleware(max_requests_per_minute=120))
    server.use(LoggingMiddleware())
"""

from __future__ import annotations

import hmac
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any

from .._logging import get_logger

log = get_logger("server.middleware")


@dataclass(slots=True)
class Request:
    """Normalised representation of an inbound HTTP request.

    Created by the HTTP handler and passed through the middleware pipeline
    and into route handlers.  Using a dataclass instead of passing raw
    ``BaseHTTPRequestHandler`` objects keeps the middleware API clean and
    testable without a real HTTP server.

    Attributes:
        path:        The request path, e.g. ``"/api/heartbeat"``.
        headers:     Raw HTTP headers as a case-preserving dictionary.
        body:        Parsed JSON request body as a dictionary.
        device_id:   Value of ``body["device_id"]`` if present, else None.
                     Pre-extracted for convenient access in middleware.
        remote_addr: Client IP address string.
    """

    path: str
    headers: dict[str, str]
    body: dict[str, Any]
    device_id: str | None = field(default=None)
    remote_addr: str = ""

    def header(self, name: str, default: str | None = None) -> str | None:
        """Return the value of an HTTP header by name, case-insensitively.

        HTTP headers are case-insensitive per RFC 7230.  This method normalises
        the lookup so that ``req.header("x-api-key")`` matches a header sent
        as ``"X-API-Key"`` or ``"X-Api-Key"``.

        Args:
            name:    Header name to look up (any case).
            default: Value to return if the header is absent.

        Returns:
            The header value string, or *default* if not found.
        """
        target = name.lower()
        for key, value in self.headers.items():
            if key.lower() == target:
                return value
        return default


class Middleware:
    """Base server middleware.  All hooks are pass-through no-ops by default.

    Subclass this and override only the hooks you need.
    """

    def before_request(self, request: Request) -> Request | None:
        """Called before the request reaches the route handler.

        Args:
            request: The normalised inbound request.

        Returns:
            The (possibly modified) request to pass to the next middleware, or
            ``None`` to short-circuit the pipeline and return HTTP 403.
        """
        return request

    def after_request(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        """Called after the route handler produces a response.

        Args:
            request:  The request that was processed.
            response: The response dictionary from the route handler.

        Returns:
            The (possibly modified) response to pass to the next middleware.
        """
        return response

    def on_error(self, request: Request | None, error: BaseException) -> None:
        """Called when an exception is raised during request processing.

        Args:
            request: The request being processed when the error occurred,
                     or None if the error happened before the request was parsed.
            error:   The exception that was raised.
        """


class LoggingMiddleware(Middleware):
    """Log every inbound request and outbound response at INFO level.

    Useful during development.  In production, set the
    ``rise_l_net.server.middleware`` logger to WARNING to reduce noise.
    """

    def before_request(self, request: Request) -> Request | None:
        """Log the request path, device ID, and remote address."""
        log.info(
            "request path=%s device=%s remote=%s",
            request.path,
            request.device_id,
            request.remote_addr,
        )
        return request

    def after_request(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        """Log the response status."""
        log.info("response path=%s status=%s", request.path, response.get("status", "ok"))
        return response

    def on_error(self, request: Request | None, error: BaseException) -> None:
        """Log the error at WARNING level."""
        path = request.path if request else "?"
        log.warning("error path=%s error=%s", path, error)


class AuthMiddleware(Middleware):
    """Reject requests that do not carry a valid API key.

    Uses :func:`hmac.compare_digest` for the comparison to prevent timing
    attacks — a naive ``==`` comparison leaks information about how many
    characters of the key are correct.

    The header lookup is case-insensitive (HTTP headers are case-insensitive
    per RFC 7230).

    Args:
        api_key:     The expected API key value.  Must be non-empty.
        header_name: Name of the HTTP header that carries the key.
                     Defaults to ``"X-API-Key"``.
    """

    def __init__(self, api_key: str, header_name: str = "X-API-Key") -> None:
        if not api_key:
            raise ValueError("api_key must be a non-empty string")
        # Store as bytes for hmac.compare_digest, which requires bytes or str
        # of the same type.
        self._expected = api_key.encode("utf-8")
        self.header_name = header_name

    def before_request(self, request: Request) -> Request | None:
        """Return None (block) if the API key is missing or incorrect."""
        provided = request.header(self.header_name)
        if provided is None or not hmac.compare_digest(provided.encode("utf-8"), self._expected):
            log.warning("auth: invalid or missing api key on %s", request.path)
            # Return None to short-circuit the pipeline → HTTP 403.
            return None
        return request


class RateLimitMiddleware(Middleware):
    """Sliding-window per-device rate limiter (in-process only).

    Tracks the number of requests from each device (or remote IP if no
    device_id is available) within a rolling 60-second window.  Requests
    that exceed the limit are blocked with HTTP 403.

    Note: This is an in-process limiter.  It does not share state across
    multiple server processes.  For distributed rate limiting, implement a
    custom middleware backed by Redis or a similar shared store.

    Args:
        max_requests_per_minute: Maximum allowed requests per device per minute.
    """

    def __init__(self, max_requests_per_minute: int = 60) -> None:
        if max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")
        self.max_rpm = max_requests_per_minute
        # Lock to protect the _windows dict from concurrent access.
        self._lock = threading.Lock()
        # Maps device key → deque of monotonic timestamps within the window.
        self._windows: dict[str, deque[float]] = {}

    def before_request(self, request: Request) -> Request | None:
        """Block the request if the device has exceeded its rate limit."""
        # Use device_id as the rate-limit key; fall back to remote IP.
        key = request.device_id or request.remote_addr or "unknown"
        now = time.monotonic()
        # Timestamps older than 60 seconds are outside the window.
        cutoff = now - 60.0
        with self._lock:
            window = self._windows.setdefault(key, deque())
            # Evict expired timestamps from the front of the deque.
            while window and window[0] < cutoff:
                window.popleft()
            if len(window) >= self.max_rpm:
                log.warning("rate limit exceeded key=%s", key)
                return None  # block the request
            # Record this request's timestamp.
            window.append(now)
        return request


class ValidationMiddleware(Middleware):
    """Reject requests that are missing the required ``device_id`` field.

    This middleware runs before the route handler so that handlers can assume
    ``request.device_id`` is always a non-empty string.
    """

    def before_request(self, request: Request) -> Request | None:
        """Return None (block) if device_id is absent or empty."""
        if not request.device_id:
            log.warning("validation: missing device_id on %s", request.path)
            return None
        return request
