"""Storage abstractions and built-in synchronous backends."""

from __future__ import annotations

import json
import sqlite3
import threading
from abc import ABC, abstractmethod
from typing import Any

from .._compat import now_unix
from .._logging import get_logger
from ..exceptions import StorageError
from ..models import Device, Event, Heartbeat

log = get_logger("server.storage")


class Storage(ABC):
    """Abstract storage backend.

    Implementations must be safe to call from multiple threads.
    """

    @abstractmethod
    def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device row from a heartbeat.

        Returns the resulting Device and a flag indicating whether the row
        was newly inserted (True = new device, False = update).
        """

    @abstractmethod
    def get_device(self, device_id: str) -> Device | None:
        """Return a device by id or None if not found."""

    @abstractmethod
    def list_devices(self, status: str | None = None) -> list[Device]:
        """Return all devices, optionally filtered by status."""

    @abstractmethod
    def mark_offline(self, before_unix: int) -> list[str]:
        """Mark online devices whose last heartbeat is older than `before_unix`
        as offline. Returns the list of device ids that were transitioned.
        """

    @abstractmethod
    def save_event(self, event: Event) -> int:
        """Persist an event and return its id (>= 1)."""

    @abstractmethod
    def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events newest-first, optionally filtered by device."""

    def close(self) -> None:
        """Release any resources. Default is a no-op."""


class InMemoryStorage(Storage):
    """Thread-safe in-memory store. Useful for tests and small deployments."""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._devices: dict[str, Device] = {}
        self._events: list[Event] = []
        self._next_event_id = 1

    def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        with self._lock:
            now = now_unix()
            existing = self._devices.get(heartbeat.device_id)
            is_new = existing is None
            device = Device(
                device_id=heartbeat.device_id,
                device_name=(
                    str(heartbeat.metadata.get("device_name", ""))
                    or (existing.device_name if existing else f"Device-{heartbeat.device_id[-8:]}")
                ),
                ip_address=heartbeat.ip,
                first_seen=existing.first_seen if existing else now,
                last_heartbeat=now,
                status="online",
                firmware_version=heartbeat.version,
                uptime=heartbeat.uptime,
                metadata=heartbeat.metadata,
            )
            self._devices[heartbeat.device_id] = device
            return device, is_new

    def get_device(self, device_id: str) -> Device | None:
        with self._lock:
            return self._devices.get(device_id)

    def list_devices(self, status: str | None = None) -> list[Device]:
        with self._lock:
            rows = list(self._devices.values())
        if status is not None:
            rows = [d for d in rows if d.status == status]
        rows.sort(key=lambda d: d.last_heartbeat, reverse=True)
        return rows

    def mark_offline(self, before_unix: int) -> list[str]:
        transitioned: list[str] = []
        with self._lock:
            for device in self._devices.values():
                if device.status == "online" and device.last_heartbeat < before_unix:
                    device.status = "offline"
                    transitioned.append(device.device_id)
        return transitioned

    def save_event(self, event: Event) -> int:
        with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1
            stored = Event(
                device_id=event.device_id,
                event_type=event.event_type,
                data=event.data,
                severity=event.severity,
                timestamp=event.timestamp or now_unix(),
            )
            self._events.append(stored)
            return event_id

    def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            rows = list(self._events)
        if device_id is not None:
            rows = [e for e in rows if e.device_id == device_id]
        rows.sort(key=lambda e: e.timestamp, reverse=True)
        return rows[:limit]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id TEXT PRIMARY KEY,
    device_name TEXT NOT NULL,
    ip_address TEXT NOT NULL,
    first_seen INTEGER NOT NULL,
    last_heartbeat INTEGER NOT NULL,
    status TEXT NOT NULL,
    firmware_version TEXT NOT NULL,
    uptime INTEGER NOT NULL DEFAULT 0,
    metadata TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_devices_status ON devices(status);
CREATE INDEX IF NOT EXISTS idx_devices_last_heartbeat ON devices(last_heartbeat);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id TEXT NOT NULL,
    timestamp INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    data TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_device ON events(device_id);
CREATE INDEX IF NOT EXISTS idx_events_timestamp ON events(timestamp);
"""


class SQLiteStorage(Storage):
    """SQLite-backed storage. Uses WAL mode and a per-instance lock."""

    def __init__(self, db_path: str = "riselnet.db") -> None:
        self.db_path = db_path
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def _row_to_device(self, row: sqlite3.Row) -> Device:
        try:
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        except json.JSONDecodeError:
            metadata = {}
        return Device(
            device_id=row["device_id"],
            device_name=row["device_name"],
            ip_address=row["ip_address"],
            first_seen=int(row["first_seen"]),
            last_heartbeat=int(row["last_heartbeat"]),
            status=row["status"],
            firmware_version=row["firmware_version"],
            uptime=int(row["uptime"]),
            metadata=metadata,
        )

    def _row_to_event(self, row: sqlite3.Row) -> Event:
        try:
            data: dict[str, Any] = json.loads(row["data"]) if row["data"] else {}
        except json.JSONDecodeError:
            data = {}
        return Event(
            device_id=row["device_id"],
            event_type=row["event_type"],
            data=data,
            severity=row["severity"],
            timestamp=int(row["timestamp"]),
        )

    def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        now = now_unix()
        device_name = str(heartbeat.metadata.get("device_name", "")) or (
            f"Device-{heartbeat.device_id[-8:]}"
        )
        metadata_json = json.dumps(heartbeat.metadata)
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                cur = self._conn.execute(
                    "SELECT device_id FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                )
                is_new = cur.fetchone() is None
                if is_new:
                    self._conn.execute(
                        """
                        INSERT INTO devices (device_id, device_name, ip_address,
                                             first_seen, last_heartbeat, status,
                                             firmware_version, uptime, metadata)
                        VALUES (?, ?, ?, ?, ?, 'online', ?, ?, ?)
                        """,
                        (
                            heartbeat.device_id,
                            device_name,
                            heartbeat.ip,
                            now,
                            now,
                            heartbeat.version,
                            heartbeat.uptime,
                            metadata_json,
                        ),
                    )
                else:
                    self._conn.execute(
                        """
                        UPDATE devices
                           SET ip_address = ?,
                               last_heartbeat = ?,
                               status = 'online',
                               firmware_version = ?,
                               uptime = ?,
                               metadata = ?
                         WHERE device_id = ?
                        """,
                        (
                            heartbeat.ip,
                            now,
                            heartbeat.version,
                            heartbeat.uptime,
                            metadata_json,
                            heartbeat.device_id,
                        ),
                    )
                row = self._conn.execute(
                    "SELECT * FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                ).fetchone()
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                self._conn.execute("ROLLBACK")
                raise StorageError(str(exc)) from exc
        if row is None:
            raise StorageError("device row vanished after upsert")
        return self._row_to_device(row), is_new

    def get_device(self, device_id: str) -> Device | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM devices WHERE device_id = ?", (device_id,)
            ).fetchone()
        return self._row_to_device(row) if row else None

    def list_devices(self, status: str | None = None) -> list[Device]:
        with self._lock:
            if status is None:
                rows = self._conn.execute(
                    "SELECT * FROM devices ORDER BY last_heartbeat DESC"
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM devices WHERE status = ? ORDER BY last_heartbeat DESC",
                    (status,),
                ).fetchall()
        return [self._row_to_device(r) for r in rows]

    def mark_offline(self, before_unix: int) -> list[str]:
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                rows = self._conn.execute(
                    "SELECT device_id FROM devices WHERE status = 'online' AND last_heartbeat < ?",
                    (before_unix,),
                ).fetchall()
                if rows:
                    self._conn.execute(
                        "UPDATE devices SET status = 'offline' "
                        "WHERE status = 'online' AND last_heartbeat < ?",
                        (before_unix,),
                    )
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                self._conn.execute("ROLLBACK")
                raise StorageError(str(exc)) from exc
        return [row["device_id"] for row in rows]

    def save_event(self, event: Event) -> int:
        timestamp = event.timestamp or now_unix()
        data_json = json.dumps(event.data)
        with self._lock:
            try:
                cur = self._conn.execute(
                    """
                    INSERT INTO events (device_id, timestamp, event_type, severity, data)
                    VALUES (?, ?, ?, ?, ?)
                    """,
                    (event.device_id, timestamp, event.event_type, event.severity, data_json),
                )
            except sqlite3.Error as exc:
                raise StorageError(str(exc)) from exc
        event_id = cur.lastrowid
        if event_id is None:
            raise StorageError("event insert returned no rowid")
        return int(event_id)

    def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        if limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            if device_id is None:
                rows = self._conn.execute(
                    "SELECT * FROM events ORDER BY timestamp DESC, id DESC LIMIT ?",
                    (limit,),
                ).fetchall()
            else:
                rows = self._conn.execute(
                    "SELECT * FROM events WHERE device_id = ? "
                    "ORDER BY timestamp DESC, id DESC LIMIT ?",
                    (device_id, limit),
                ).fetchall()
        return [self._row_to_event(r) for r in rows]
