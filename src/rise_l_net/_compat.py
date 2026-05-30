"""Platform compatibility shims for CPython and MicroPython."""

from __future__ import annotations

import sys

MICROPYTHON: bool = sys.implementation.name == "micropython"
"""True when running under MicroPython."""


def monotonic() -> float:
    """Monotonic clock in seconds. Falls back to time.time() on MicroPython."""
    import time

    fn = getattr(time, "monotonic", None)
    if fn is not None:
        return float(fn())
    return float(time.time())


def now_unix() -> int:
    """Current Unix timestamp as integer seconds."""
    import time

    return int(time.time())


def get_mac_address() -> str | None:
    """Best-effort MAC address. Returns None when unavailable."""
    if MICROPYTHON:
        try:
            import network  # type: ignore[import-not-found,unused-ignore]

            wlan = network.WLAN(network.STA_IF)
            mac = wlan.config("mac")
            return ":".join(f"{b:02X}" for b in mac)
        except Exception:
            return None
    try:
        import uuid

        node = uuid.getnode()
        return ":".join(f"{(node >> i) & 0xFF:02X}" for i in range(40, -1, -8))
    except Exception:
        return None


def sleep(seconds: float) -> None:
    """Sleep helper that works on both runtimes."""
    import time

    time.sleep(seconds)
