"""rise-l-net: lightweight IoT device management toolkit.

Public API:

    from rise_l_net import RISELDevice, RISELServer
    from rise_l_net import AsyncRISELDevice, AsyncRISELServer  # requires [async] extra

Submodules:

    rise_l_net.client   — device-side client (sync + async)
    rise_l_net.server   — server-side device manager (sync + async)
    rise_l_net.models   — Heartbeat, Event, Device dataclasses
    rise_l_net.exceptions — RISELError tree
"""

from __future__ import annotations

from ._logging import configure as configure_logging
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

__version__ = "0.1.0"

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
    # Lazy imports keep the base install zero-dep and avoid pulling aiohttp
    # unless async classes are actually requested.
    if name == "RISELDevice":
        from .client.device import RISELDevice

        return RISELDevice
    if name == "RISELServer":
        from .server.server import RISELServer

        return RISELServer
    if name == "AsyncRISELDevice":
        from .client.async_device import AsyncRISELDevice

        return AsyncRISELDevice
    if name == "AsyncRISELServer":
        from .server.async_server import AsyncRISELServer

        return AsyncRISELServer
    if name == "quick_start":
        from .client.device import quick_start

        return quick_start
    if name == "quick_server":
        from .server.server import quick_server

        return quick_server
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
