"""Client-side middleware pipeline.

Middleware objects intercept every outbound request and can:

* Modify the payload before it is sent (``before_send``).
* Inspect or transform the response after a successful send (``after_send``).
* Decide whether to retry after a transport failure (``on_error``).

Middleware is applied in registration order.  The first middleware registered
is the first to run on the way out and the last to run on the way back in
(like an onion).

Example::

    device = RISELDevice("http://server:8080")
    device.use(RetryMiddleware(max_retries=5))
    device.use(LoggingMiddleware())
    device.use(ThrottleMiddleware(max_requests_per_second=2))
"""

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
    """Base middleware class.  All hooks are no-ops by default.

    Subclass this and override only the hooks you need.  The device's send
    loop calls hooks in registration order; returning the (possibly modified)
    value passes control to the next middleware.
    """

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Called before the payload is handed to the transport.

        Args:
            endpoint: The API path being called (e.g. ``"/api/report"``).
            data:     The outbound payload dictionary.

        Returns:
            The (possibly modified) payload to pass to the next middleware or
            the transport.
        """
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        """Called after a successful send with the parsed server response.

        Args:
            endpoint: The API path that was called.
            data:     The payload that was sent.
            response: The parsed JSON response from the server.

        Returns:
            The (possibly modified) response to pass to the next middleware or
            back to the caller.
        """
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        """Called when the transport raises a :class:`TransportError`.

        Args:
            endpoint: The API path that failed.
            data:     The payload that was being sent.
            error:    The exception that was raised.

        Returns:
            ``True`` to ask the device to retry the send, ``False`` to give up
            and propagate the error to the caller.
        """
        return False


class LoggingMiddleware(Middleware):
    """Log every outbound request and inbound response at INFO level.

    Useful during development and debugging.  In production, consider setting
    the ``rise_l_net.client.middleware`` logger to WARNING to reduce noise.
    """

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Log the outbound payload (truncated to 100 chars)."""
        log.info("send %s payload=%s", endpoint, _short_repr(data))
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        """Log the server response (truncated to 100 chars)."""
        log.info("recv %s response=%s", endpoint, _short_repr(response))
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        """Log the error at WARNING level and do not request a retry."""
        log.warning("error %s: %s", endpoint, error)
        return False


class RetryMiddleware(Middleware):
    """Retry failed sends with exponential backoff and optional jitter.

    The retry counter is tracked per endpoint so that a failure on
    ``/api/report`` does not consume retry budget for ``/api/heartbeat``.

    The counter is reset to zero after a successful send (``after_send``),
    so each new send starts with a fresh budget.

    Args:
        max_retries: Maximum number of retry attempts per send.  0 means no
                     retries (fail immediately).
        base_delay:  Initial delay in seconds before the first retry.
        max_delay:   Upper bound on the computed delay (prevents very long waits
                     after many failures).
        jitter:      Fraction of the computed delay to randomise.  0.2 means
                     ±20 % random variation, which spreads retries across time
                     when many devices fail simultaneously.
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
        # Maps endpoint path → number of attempts already made for the current send.
        self._attempts: dict[str, int] = {}

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        # Nothing to do on the way out; state is managed in on_error/after_send.
        return data

    def after_send(
        self, endpoint: str, data: dict[str, Any], response: dict[str, Any]
    ) -> dict[str, Any]:
        """Reset the attempt counter for this endpoint after a successful send."""
        self._attempts.pop(endpoint, None)
        return response

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        """Sleep for a back-off delay and signal the device to retry.

        Returns False (give up) once ``max_retries`` attempts have been made.
        """
        attempts = self._attempts.get(endpoint, 0)
        if attempts >= self.max_retries:
            # Budget exhausted — clear the counter and tell the device to stop.
            self._attempts.pop(endpoint, None)
            return False

        # Exponential back-off: delay doubles with each attempt.
        delay = min(self.base_delay * (2**attempts), self.max_delay)
        if self.jitter:
            # Add random noise to avoid thundering-herd when many devices retry
            # at the same time after a server restart.
            delay = delay * (1 + random.uniform(-self.jitter, self.jitter))
        delay = max(0.0, delay)  # guard against negative values from jitter

        log.info(
            "retry %s attempt=%d/%d delay=%.2fs",
            endpoint,
            attempts + 1,
            self.max_retries,
            delay,
        )
        sleep(delay)
        # Increment the counter so the next failure knows how many have occurred.
        self._attempts[endpoint] = attempts + 1
        return True  # ask the device to retry


class ThrottleMiddleware(Middleware):
    """Limit outbound requests to a maximum rate (requests per second).

    Useful on constrained devices where flooding the server could exhaust
    memory or battery.  The throttle is applied globally across all endpoints.

    Args:
        max_requests_per_second: Maximum allowed send rate.  Must be > 0.
    """

    def __init__(self, max_requests_per_second: float = 10.0) -> None:
        if max_requests_per_second <= 0:
            raise ValueError("max_requests_per_second must be positive")
        # Minimum time between consecutive sends in seconds.
        self._interval = 1.0 / max_requests_per_second
        # Monotonic timestamp of the last send; 0 means "never sent".
        self._last_send = 0.0

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Sleep if the time since the last send is less than the interval."""
        now = monotonic()
        elapsed = now - self._last_send
        if elapsed < self._interval:
            # Block until the minimum interval has passed.
            sleep(self._interval - elapsed)
        # Record the send time *after* the sleep so the next call measures from
        # the actual send moment, not from when we woke up.
        self._last_send = monotonic()
        return data


class CompressionMiddleware(Middleware):
    """Placeholder for payload compression.

    Real compression is intentionally not implemented here because the device
    and server must agree on the wire format (Content-Encoding header, etc.).
    Subclass this and override ``before_send`` to add gzip/zlib/CBOR support
    that matches your specific deployment.
    """

    def before_send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        # No-op: subclasses should override this to compress the payload.
        return data


class CacheMiddleware(Middleware):
    """Persist failed sends to disk so they can be retried later.

    When a :class:`TransportError` occurs (e.g. the device is offline), the
    payload is appended to a JSON file on disk.  The caller can later call
    :meth:`drain` to retrieve the cached entries and resend them.

    This middleware does **not** automatically retry cached entries — it only
    stores them.  The application is responsible for calling ``drain()`` and
    resending the payloads when connectivity is restored.

    Args:
        cache_path:  Path to the JSON cache file.  Created if it does not exist.
        max_entries: Maximum number of entries to keep.  Older entries are
                     silently dropped when the cache is full.
    """

    def __init__(
        self,
        cache_path: str = "riselnet_cache.json",
        max_entries: int = 100,
    ) -> None:
        self.cache_path = cache_path
        self.max_entries = max_entries
        self._cache: list[dict[str, Any]] = []
        # Load any previously cached entries from disk on startup.
        self._load()

    def _load(self) -> None:
        """Read the cache file from disk into memory.

        Silently resets to an empty cache if the file does not exist or is
        corrupted, rather than crashing the device on startup.
        """
        try:
            with open(self.cache_path, encoding="utf-8") as f:
                self._cache = json.load(f)
        except FileNotFoundError:
            # Normal case on first run — no cache file yet.
            self._cache = []
        except (OSError, json.JSONDecodeError) as exc:
            log.warning("cache load failed: %s", exc)
            self._cache = []

    def _persist(self) -> None:
        """Write the in-memory cache to disk atomically.

        Uses a temporary file + rename to avoid leaving a half-written cache
        file if the process is interrupted mid-write.
        """
        try:
            tmp = f"{self.cache_path}.tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._cache, f)
            # Atomic rename: either the old file or the new file is visible,
            # never a partial write.
            os.replace(tmp, self.cache_path)
        except OSError as exc:
            log.warning("cache persist failed: %s", exc)

    def on_error(self, endpoint: str, data: dict[str, Any], error: BaseException) -> bool:
        """Cache the payload when a TransportError occurs.

        Non-transport errors (e.g. programming mistakes) are not cached because
        retrying them would not help.  Returns False in all cases — this
        middleware stores the payload but does not request an immediate retry.
        """
        # Only cache network-level failures, not logic errors.
        if not isinstance(error, TransportError):
            return False
        # Drop the payload if the cache is already full to avoid unbounded growth.
        if len(self._cache) >= self.max_entries:
            return False
        self._cache.append({"endpoint": endpoint, "data": data})
        self._persist()
        log.info("cached request endpoint=%s size=%d", endpoint, len(self._cache))
        return False  # do not retry immediately

    def pending(self) -> list[dict[str, Any]]:
        """Return a snapshot of the cached entries without clearing them."""
        return list(self._cache)

    def drain(self) -> list[dict[str, Any]]:
        """Atomically take all cached entries and clear the cache.

        The caller is responsible for resending the returned payloads.  If
        resending fails, the caller should add them back via ``on_error``.
        """
        items = list(self._cache)
        self._cache = []
        self._persist()  # write the empty cache to disk
        return items


def _short_repr(value: Any, limit: int = 100) -> str:
    """Return a truncated repr of *value* for log messages.

    Keeps log lines readable when payloads are large.
    """
    text = repr(value)
    if len(text) <= limit:
        return text
    return text[:limit] + "..."
