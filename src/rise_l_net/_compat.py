"""Platform compatibility shims for CPython and MicroPython.

This module provides a thin abstraction layer so that the rest of the library
can import a single set of helpers without caring which Python runtime is
executing the code.  All public symbols are re-exported from here; nothing
else in the package should import ``time``, ``network``, or ``uuid`` directly.
"""

from __future__ import annotations

import sys

# Detect the runtime once at import time.  The rest of the library reads this
# flag to branch between CPython and MicroPython code paths.
MICROPYTHON: bool = sys.implementation.name == "micropython"
"""True when running under MicroPython (e.g. ESP32, RP2040)."""


def monotonic() -> float:
    """Return a monotonically increasing clock value in seconds.

    On CPython this delegates to ``time.monotonic``, which is guaranteed never
    to go backwards.  MicroPython does not expose ``time.monotonic``, so we
    fall back to ``time.time`` — good enough for heartbeat interval tracking
    where sub-second precision is not required.
    """
    import time

    # Prefer monotonic when available (CPython 3.3+).
    fn = getattr(time, "monotonic", None)
    if fn is not None:
        return float(fn())
    # MicroPython fallback: time.time() is monotonic in practice on embedded
    # hardware because there is no NTP adjustment during a single run.
    return float(time.time())


def now_unix() -> int:
    """Return the current UTC time as an integer Unix timestamp (seconds).

    Used for event timestamps and heartbeat payloads.  Integer precision is
    sufficient — sub-second granularity is not needed for IoT telemetry.
    """
    import time

    return int(time.time())


def get_mac_address() -> str | None:
    """Return the primary network interface MAC address, or None on failure.

    The address is formatted as six colon-separated uppercase hex octets,
    e.g. ``"AA:BB:CC:DD:EE:FF"``.

    On MicroPython the WLAN station interface is queried directly.
    On CPython the ``uuid`` module is used, which reads the hardware MAC from
    the OS network stack.  Both paths are best-effort: if anything goes wrong
    (no network interface, permission error, etc.) we return None and let the
    caller fall back to a generated device ID.
    """
    if MICROPYTHON:
        # MicroPython: read MAC from the Wi-Fi station interface.
        try:
            import network  # type: ignore[import-not-found,unused-ignore]

            wlan = network.WLAN(network.STA_IF)
            mac = wlan.config("mac")  # returns bytes on MicroPython
            # Format each byte as two uppercase hex digits separated by colons.
            return ":".join(f"{b:02X}" for b in mac)
        except Exception:
            # Network module unavailable or interface not initialised yet.
            return None

    # CPython: uuid.getnode() returns the MAC as a 48-bit integer.
    try:
        import uuid

        node = uuid.getnode()
        # Unpack the integer into six bytes, most-significant byte first.
        return ":".join(f"{(node >> i) & 0xFF:02X}" for i in range(40, -1, -8))
    except Exception:
        return None


def sleep(seconds: float) -> None:
    """Block the current thread (or coroutine on MicroPython) for *seconds*.

    This thin wrapper exists so that tests can monkeypatch ``sleep`` in a
    single place rather than patching ``time.sleep`` everywhere.
    """
    import time

    time.sleep(seconds)
