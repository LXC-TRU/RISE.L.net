"""rise-l-net: lightweight IoT device management toolkit.

Public API::

    from rise_l_net import RISELDevice, RISELServer
    from rise_l_net import AsyncRISELDevice, AsyncRISELServer  # requires [async] extra

Submodules::

    rise_l_net.client     — device-side client (sync + async)
    rise_l_net.server     — server-side device manager (sync + async)
    rise_l_net.models     — Heartbeat, Event, Device dataclasses
    rise_l_net.exceptions — RISELError tree
"""

from __future__ import annotations

from ._logging import configure as configure_logging
from .client.device import RISELDevice, quick_start
from .exceptions import (
    AuthError,
    ConfigError,
    RateLimitedError,
    RISELError,
    StorageError,
    TransportError,
    ValidationError,
)
from .models import Device, Event, Heartbeat
from .server.server import RISELServer, quick_server

__version__ = "0.1.1"

__all__ = [
    "AsyncRISELDevice",
    "AsyncRISELServer",
    "AuthError",
    "ConfigError",
    "Device",
    "Event",
    "Heartbeat",
    "RISELDevice",
    "RISELError",
    "RISELServer",
    "RateLimitedError",
    "StorageError",
    "TransportError",
    "ValidationError",
    "__version__",
    "configure_logging",
    "quick_server",
    "quick_start",
]


def __getattr__(name: str) -> object:
    # Async classes are imported lazily to avoid pulling in aiohttp at import
    # time when only the sync API is used.  mypy resolves these via the
    # TYPE_CHECKING block in each submodule, so type checking still works.
    if name == "AsyncRISELDevice":
        from .client.async_device import AsyncRISELDevice

        return AsyncRISELDevice
    if name == "AsyncRISELServer":
        from .server.async_server import AsyncRISELServer

        return AsyncRISELServer
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
