"""Domain models exchanged between client, server, and storage layers.

All models are plain ``dataclasses`` with ``slots=True`` for memory efficiency
on constrained devices.  They are intentionally free of business logic — they
carry data, nothing more.

The ``from_dict`` / ``to_dict`` helpers make it easy to serialise models to
JSON for HTTP transport and to deserialise them from raw request payloads.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# Default severity level used when a caller does not specify one.
DEFAULT_SEVERITY = "info"


@dataclass(slots=True)
class Heartbeat:
    """A single heartbeat payload sent from a device to the server.

    The server uses heartbeats to track device presence and update the
    ``last_heartbeat`` timestamp.  A device that stops sending heartbeats
    will eventually be marked offline by the timeout checker.

    Attributes:
        device_id:  Unique identifier for the device (MAC address or custom).
        ip:         Current IP address of the device on the local network.
        uptime:     Seconds since the device last booted or restarted.
        version:    Firmware / software version string reported by the device.
        metadata:   Arbitrary key-value pairs (location, device_name, etc.).
    """

    device_id: str
    ip: str = "0.0.0.0"
    uptime: int = 0
    version: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the heartbeat to a plain dictionary suitable for JSON."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Heartbeat:
        """Deserialise a heartbeat from a raw request payload dictionary.

        Unknown keys are silently ignored so that future protocol extensions
        do not break older server versions.
        """
        return cls(
            device_id=str(data["device_id"]),
            ip=str(data.get("ip", "0.0.0.0")),
            uptime=int(data.get("uptime", 0)),
            version=str(data.get("version", "unknown")),
            # Coerce None to an empty dict to avoid downstream AttributeErrors.
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(slots=True)
class Event:
    """A telemetry or report event sent from a device to the server.

    Events represent discrete observations (sensor readings, state changes,
    errors) that the device wants to persist on the server side.

    Attributes:
        device_id:   Identifier of the originating device.
        event_type:  Application-defined category string (e.g. "temperature").
        data:        Arbitrary payload dictionary with the event details.
        severity:    One of ``"info"``, ``"warning"``, ``"error"``, ``"critical"``.
        timestamp:   Unix timestamp when the event occurred (0 = use server time).
    """

    device_id: str
    event_type: str
    data: dict[str, Any] = field(default_factory=dict)
    severity: str = DEFAULT_SEVERITY
    timestamp: int = 0  # 0 means "not set"; the server will fill it in

    def to_dict(self) -> dict[str, Any]:
        """Serialise the event to a plain dictionary suitable for JSON."""
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        """Deserialise an event from a raw request payload dictionary."""
        return cls(
            device_id=str(data["device_id"]),
            event_type=str(data.get("event_type", "unknown")),
            data=dict(data.get("data") or {}),
            severity=str(data.get("severity", DEFAULT_SEVERITY)),
            timestamp=int(data.get("timestamp", 0)),
        )


@dataclass(slots=True)
class Device:
    """Server-side view of a registered device.

    This model is populated by the storage layer and returned to callers of
    ``RISELServer.get_devices()``.  It is never sent over the wire directly.

    Attributes:
        device_id:        Unique identifier (primary key in the database).
        device_name:      Human-readable name, derived from metadata or auto-generated.
        ip_address:       Last known IP address reported via heartbeat.
        first_seen:       Unix timestamp of the first heartbeat ever received.
        last_heartbeat:   Unix timestamp of the most recent heartbeat.
        status:           ``"online"`` or ``"offline"``.
        firmware_version: Last reported firmware version string.
        uptime:           Last reported uptime in seconds.
        metadata:         Last reported metadata dictionary.
    """

    device_id: str
    device_name: str = ""
    ip_address: str = "0.0.0.0"
    first_seen: int = 0
    last_heartbeat: int = 0
    status: str = "offline"  # devices start offline until first heartbeat
    firmware_version: str = "unknown"
    uptime: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Serialise the device record to a plain dictionary."""
        return asdict(self)
