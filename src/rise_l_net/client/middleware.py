"""Client-side middleware. All hooks may raise to abort the send."""

from __future__ import annotations

import json
import os
import random
from typing import Any

from .._compat import monotonic, sleep
from .._logging import get_logger
from ..exceptions import TransportError

log = get_logger("client.middleware")


class Middleware:
    """Base class. Hooks are invoked in registration order."""

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        """Return True to ask the device to retry, False to give up."""
        return False


class LoggingMiddleware(Middleware):
    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        log.info("send %s payload=%s", endpoint, _short_repr(data))
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        log.info("recv %s response=%s", endpoint, _short_repr(response))
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        log.warning("error %s: %s", endpoint, error)
        return False


class RetryMiddleware(Middleware):
    """Retry with exponential backoff and jitter.

    The middleware tracks attempts per endpoint. When `on_error` returns True
    the device's send loop calls `before_send` and `send` again.
    """

    def __init__(
        self,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 30.0,
        jitter: float = 0.2,
    ) -> None:
        if max_retries < 0:
            raise ValueError("max_retries must be >= 0")
        self.max_retries = max_retries
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter = jitter
        self._attempts: dict[str, int] = {}

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        # Clear after a successful retry chain when a fresh send begins.
        # This middleware relies on `on_error` to manage state; nothing to do.
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        self._attempts.pop(endpoint, None)
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        attempts = self._attempts.get(endpoint, 0)
        if attempts >= self.max_retries:
            self._attempts.pop(endpoint, None)
            return False
        delay = min(self.base_delay * (2**attempts), self.max_delay)
        if self.jitter:
            delay = delay * (1 + random.uniform(-self.jitter, self.jitter))
        delay = max(0.0, delay)
        log.info(
            "retry %s attempt=%d/%d delay=%.2fs",
            endpoint,
            attempts + 1,
            self.max_retries,
            delay,
        )
        sleep(delay)
        self._attempts[endpoint] = attempts + 1
        return True


class ThrottleMiddleware(Middleware):
    """Cap outbound requests to a maximum rate per second."""

    def __init__(self, max_requests_per_second: float = 10.0) -> None:
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be positive")
        self._interval = 1.0 / max_requests_per_second
        self._last_send = 0.0

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        now = monotonic()
        elapsed = now - self._last_send
        if elapsed < self._interval:
            sleep(self._interval - elapsed)
        self._last_send = monotonic()
        return data


class CompressionMiddleware(Middleware):
    """Placeholder. Real compression is intentionally not implemented because
    the device must agree with the server on the wire format. Subclass this
    to add gzip/zlib/cbor support that matches your deployment.
    """

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        return data


class CacheMiddleware(Middleware):
    """Persist failed sends to disk and try to flush them later."""

    def __init__(
        self,
        cache_path: str = "riselnet_cache.json",
        max_entries: int = 100,
    ) -> None:
        self.cache_path = cache_path
        self.max_entries = max_entries
        self._cache: list[dict[str, Any]] = []
        self._load()

    def _load(self) -> None:
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                self._cache = json.load(f)
        except FileNotFoundError:
            self._cache = []
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("cache load failed: %s", exc)
            self._cache = []

    def _persist(self) -> None:
        try:
            tmp = f"{self.cache_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._cache, f)
            os.replace(tmp, self.cache_path)
        except OSError as exc:
            log.warning("cache persist failed: %s", exc)

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        if not isinstance(error, TransportError):
            return False
        if len(self._cache) >= self.max_entries:
            return False
        self._cache.append({"endpoint": endpoint, "data": data})
        self._persist()
        log.info("cached request endpoint=%s size=%d", endpoint, len(self._cache))
        return False

    def pending(self) -> list[dict[str, Any]]:
        return list(self._cache)

    def drain(self) -> list[dict[str, Any]]:
        """Atomically take and clear the cached entries. Caller resends them."""
        items = list(self._cache)
        self._cache = []
        self._persist()
        return items


def _short_repr(value: Any, limit: int = 100) -> str:
    text = repr(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "..."
