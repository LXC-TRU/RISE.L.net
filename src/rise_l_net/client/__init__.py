"""Device-side client — sync and async."""

from __future__ import annotations

from .device import RISELDevice, quick_start
from .middleware import (
    CacheMiddleware,
    CompressionMiddleware,
    LoggingMiddleware,
    Middleware,
    RetryMiddleware,
    ThrottleMiddleware,
)
from .transport import HTTPTransport, Transport

__all__ = [
    "AsyncHTTPTransport",
    "AsyncRISELDevice",
    "CacheMiddleware",
    "CompressionMiddleware",
    "HTTPTransport",
    "LoggingMiddleware",
    "Middleware",
    "RISELDevice",
    "RetryMiddleware",
    "ThrottleMiddleware",
    "Transport",
    "quick_start",
]


def __getattr__(name: str) -> object:
    if name == "AsyncRISELDevice":
        from .async_device import AsyncRISELDevice

        return AsyncRISELDevice
    if name == "AsyncHTTPTransport":
        from .async_transport import AsyncHTTPTransport

        return AsyncHTTPTransport
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
