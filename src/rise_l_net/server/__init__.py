"""Server-side device manager — sync and async."""

from __future__ import annotations

from .middleware import (
    AuthMiddleware,
    LoggingMiddleware,
    Middleware,
    RateLimitMiddleware,
    ValidationMiddleware,
)
from .plugins import AlertPlugin, Plugin, WebhookPlugin
from .server import RISELServer, quick_server
from .storage import InMemoryStorage, SQLiteStorage, Storage

__all__ = [
    "AlertPlugin",
    "AsyncInMemoryStorage",
    "AsyncRISELServer",
    "AsyncSQLiteStorage",
    "AsyncStorage",
    "AuthMiddleware",
    "InMemoryStorage",
    "LoggingMiddleware",
    "Middleware",
    "Plugin",
    "RISELServer",
    "RateLimitMiddleware",
    "SQLiteStorage",
    "Storage",
    "ValidationMiddleware",
    "WebhookPlugin",
    "quick_server",
]


def __getattr__(name: str) -> object:
    if name == "AsyncRISELServer":
        from .async_server import AsyncRISELServer

        return AsyncRISELServer
    if name == "AsyncStorage":
        from .async_storage import AsyncStorage

        return AsyncStorage
    if name == "AsyncSQLiteStorage":
        from .async_storage import AsyncSQLiteStorage

        return AsyncSQLiteStorage
    if name == "AsyncInMemoryStorage":
        from .async_storage import AsyncInMemoryStorage

        return AsyncInMemoryStorage
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
