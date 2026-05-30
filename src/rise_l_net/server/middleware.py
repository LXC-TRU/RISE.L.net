"""Server-side middleware for inbound HTTP requests."""

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
    """Normalized representation of an inbound request handed to middleware."""

    path: str
    headers: dict[str, str]
    body: dict[str, Any]
    device_id: str | None = field(default=None)
    remote_addr: str = ""

    def header(self, name: str, default: str | None = None) -> str | None:
        """Case-insensitive header lookup."""
        target = name.lower()
        for key, value in self.headers.items():
            if key.lower() == target:
                return value
        return default


class Middleware:
    """Base middleware. Override the relevant hooks.

    `before_request` may return None to short-circuit the pipeline. In that
    case it should also set the response by raising an `RISELError` subclass
    or returning a tuple `(None, status, body)`.
    """

    def before_request(self, request: Request) -> Request | None:
        return request

    def after_request(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        return response

    def on_error(self, request: Request | None, error: BaseException) -> None:
        pass


class LoggingMiddleware(Middleware):
    def before_request(self, request: Request) -> Request | None:
        log.info(
            "request path=%s device=%s remote=%s",
            request.path,
            request.device_id,
            request.remote_addr,
        )
        return request

    def after_request(self, request: Request, response: dict[str, Any]) -> dict[str, Any]:
        log.info("response path=%s status=%s", request.path, response.get("status", "ok"))
        return response

    def on_error(self, request: Request | None, error: BaseException) -> None:
        path = request.path if request else "?"
        log.warning("error path=%s error=%s", path, error)


class AuthMiddleware(Middleware):
    """API-key auth using a constant-time comparison."""

    def __init__(self, api_key: str, header_name: str = "X-API-Key") -> None:
        if not api_key:
            raise ValueError("api_key must be a non-empty string")
        self._expected = api_key.encode("utf-8")
        self.header_name = header_name

    def before_request(self, request: Request) -> Request | None:
        provided = request.header(self.header_name)
        if provided is None or not hmac.compare_digest(provided.encode("utf-8"), self._expected):
            log.warning("auth: invalid or missing api key on %s", request.path)
            return None
        return request


class RateLimitMiddleware(Middleware):
    """Sliding-window per-device rate limiter (in-process only)."""

    def __init__(self, max_requests_per_minute: int = 60) -> None:
        if max_requests_per_minute <= 0:
            raise ValueError("max_requests_per_minute must be positive")
        self.max_rpm = max_requests_per_minute
        self._lock = threading.Lock()
        self._windows: dict[str, deque[float]] = {}

    def before_request(self, request: Request) -> Request | None:
        key = request.device_id or request.remote_addr or "unknown"
        now = time.monotonic()
        cutoff = now - 60.0
        with self._lock:
            window = self._windows.setdefault(key, deque())
            while window and window[0] < cutoff:
                window.popleft()
            if len(window) >= self.max_rpm:
                log.warning("rate limit exceeded key=%s", key)
                return None
            window.append(now)
        return request


class ValidationMiddleware(Middleware):
    """Reject requests that don't carry a device_id."""

    def before_request(self, request: Request) -> Request | None:
        if not request.device_id:
            log.warning("validation: missing device_id on %s", request.path)
            return None
        return request
