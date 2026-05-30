"""Domain models exchanged between client, server, and storage layers."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

DEFAULT_SEVERITY = "info"


@dataclass(slots=True)
class Heartbeat:
    """A single heartbeat from a device."""

    device_id: str
    ip: str = "0.0.0.0"
    uptime: int = 0
    version: str = "unknown"
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Heartbeat:
        return cls(
            device_id=str(data["device_id"]),
            ip=str(data.get("ip", "0.0.0.0")),
            uptime=int(data.get("uptime", 0)),
            version=str(data.get("version", "unknown")),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass(slots=True)
class Event:
    """A telemetry / report event from a device."""

    device_id: str
    event_type: str
    data: dict[str, Any] = field(default_factory=dict)
    severity: str = DEFAULT_SEVERITY
    timestamp: int = 0

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Event:
        return cls(
            device_id=str(data["device_id"]),
            event_type=str(data.get("event_type", "unknown")),
            data=dict(data.get("data") or {}),
            severity=str(data.get("severity", DEFAULT_SEVERITY)),
            timestamp=int(data.get("timestamp", 0)),
        )


@dataclass(slots=True)
class Device:
    """Server-side view of a registered device."""

    device_id: str
    device_name: str = ""
    ip_address: str = "0.0.0.0"
    first_seen: int = 0
    last_heartbeat: int = 0
    status: str = "offline"
    firmware_version: str = "unknown"
    uptime: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)
