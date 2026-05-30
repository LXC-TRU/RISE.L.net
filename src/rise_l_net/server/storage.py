"""Storage abstractions and built-in synchronous backends.

The storage layer is responsible for persisting device registrations and
telemetry events.  It is intentionally decoupled from the HTTP layer so that:

* The server can be unit-tested with :class:`InMemoryStorage` without touching
  the filesystem.
* Production deployments can swap in a different backend (Redis, Postgres, etc.)
  by implementing the :class:`Storage` abstract base class.

Thread safety
-------------
All implementations in this module are safe to call from multiple threads
simultaneously.  :class:`SQLiteStorage` uses a single shared connection
protected by a reentrant lock; :class:`InMemoryStorage` uses the same lock
pattern for its in-memory dictionaries.

Schema notes
------------
Timestamps are stored as INTEGER (Unix seconds) rather than TEXT so that the
timeout checker can use a single SQL comparison (``last_heartbeat < ?``)
instead of parsing strings on every check.
"""

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

    All methods must be safe to call from multiple threads concurrently.
    Implementations should document their isolation guarantees.
    """

    @abstractmethod
    def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device row from a heartbeat.

        If the device does not exist, it is inserted with ``status='online'``
        and ``first_seen`` set to the current time.  If it already exists,
        only the mutable fields (ip, last_heartbeat, status, firmware_version,
        uptime, metadata) are updated; ``first_seen`` is preserved.

        Returns:
            A tuple of ``(device, is_new)`` where *is_new* is True if the row
            was newly inserted, False if it was updated.
        """

    @abstractmethod
    def get_device(self, device_id: str) -> Device | None:
        """Return the device with *device_id*, or None if not found."""

    @abstractmethod
    def list_devices(self, status: str | None = None) -> list[Device]:
        """Return all devices, ordered by ``last_heartbeat`` descending.

        Args:
            status: If provided, only return devices with this status
                    (``"online"`` or ``"offline"``).
        """

    @abstractmethod
    def mark_offline(self, before_unix: int) -> list[str]:
        """Mark online devices as offline if their last heartbeat is stale.

        A device is considered stale if ``last_heartbeat < before_unix``.
        Typically called as ``mark_offline(now_unix() - device_timeout)``.

        Returns:
            List of device IDs that were transitioned from online to offline.
        """

    @abstractmethod
    def save_event(self, event: Event) -> int:
        """Persist a telemetry event and return its auto-assigned integer ID."""

    @abstractmethod
    def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events ordered by timestamp descending (newest first).

        Args:
            device_id: If provided, only return events from this device.
            limit:     Maximum number of events to return.  Must be >= 0.

        Raises:
            ValueError: If *limit* is negative.
        """

    def close(self) -> None:
        """Release any resources held by the storage backend.

        Called by the server on shutdown.  The default implementation is a
        no-op; override when the backend holds open connections.
        """


class InMemoryStorage(Storage):
    """Thread-safe in-memory storage backend.

    Stores devices and events in plain Python dictionaries and lists.
    All data is lost when the process exits.

    Suitable for:
    * Unit tests (fast, no filesystem I/O).
    * Small deployments where persistence is not required.
    * Development and prototyping.
    """

    def __init__(self) -> None:
        # Reentrant lock so that methods can call each other without deadlocking.
        self._lock = threading.RLock()
        # device_id → Device mapping.
        self._devices: dict[str, Device] = {}
        # Ordered list of events (appended in arrival order).
        self._events: list[Event] = []
        # Auto-incrementing event ID counter.
        self._next_event_id = 1

    def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device.  Thread-safe via the instance lock."""
        with self._lock:
            now = now_unix()
            existing = self._devices.get(heartbeat.device_id)
            is_new = existing is None

            # Prefer the device_name from metadata; fall back to the existing
            # name (if updating) or an auto-generated name (if new).
            device = Device(
                device_id=heartbeat.device_id,
                device_name=(
                    str(heartbeat.metadata.get("device_name", ""))
                    or (existing.device_name if existing else f"Device-{heartbeat.device_id[-8:]}")
                ),
                ip_address=heartbeat.ip,
                # Preserve first_seen on updates so we know when the device
                # was originally registered.
                first_seen=existing.first_seen if existing else now,
                last_heartbeat=now,
                status="online",  # receiving a heartbeat means the device is online
                firmware_version=heartbeat.version,
                uptime=heartbeat.uptime,
                metadata=heartbeat.metadata,
            )
            self._devices[heartbeat.device_id] = device
            return device, is_new

    def get_device(self, device_id: str) -> Device | None:
        """Return the device or None.  Thread-safe."""
        with self._lock:
            return self._devices.get(device_id)

    def list_devices(self, status: str | None = None) -> list[Device]:
        """Return devices sorted by last_heartbeat descending."""
        with self._lock:
            # Snapshot the values under the lock to avoid mutation during sort.
            rows = list(self._devices.values())
        if status is not None:
            rows = [d for d in rows if d.status == status]
        rows.sort(key=lambda d: d.last_heartbeat, reverse=True)
        return rows

    def mark_offline(self, before_unix: int) -> list[str]:
        """Transition stale online devices to offline.  Thread-safe."""
        transitioned: list[str] = []
        with self._lock:
            for device in self._devices.values():
                # Only transition devices that are currently online and stale.
                if device.status == "online" and device.last_heartbeat < before_unix:
                    device.status = "offline"
                    transitioned.append(device.device_id)
        return transitioned

    def save_event(self, event: Event) -> int:
        """Append the event and return its ID.  Thread-safe."""
        with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1
            # Store a copy with the timestamp filled in if not provided.
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
        """Return events newest-first, optionally filtered by device."""
        if limit < 0:
            raise ValueError("limit must be non-negative")
        with self._lock:
            rows = list(self._events)
        if device_id is not None:
            rows = [e for e in rows if e.device_id == device_id]
        rows.sort(key=lambda e: e.timestamp, reverse=True)
        return rows[:limit]


# SQL DDL executed once when the database is first opened.
# Using INTEGER for timestamps avoids repeated strptime() calls in the timeout
# checker and enables efficient index-based range queries.
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
    """SQLite-backed persistent storage.

    Uses WAL (Write-Ahead Logging) journal mode for better concurrent read
    performance and a reentrant lock to serialise writes from multiple threads.

    The connection is opened with ``check_same_thread=False`` because the
    server's ``ThreadingHTTPServer`` may call storage methods from different
    threads.  The lock ensures that only one thread executes SQL at a time.

    Args:
        db_path: Path to the SQLite database file.  Created if it does not
                 exist.  Use ``":memory:"`` for an in-process database (useful
                 for testing when you need SQL semantics but not persistence).
    """

    def __init__(self, db_path: str = "riselnet.db") -> None:
        self.db_path = db_path
        # Reentrant lock to serialise all database access from multiple threads.
        self._lock = threading.RLock()
        # Open the connection once and reuse it for the lifetime of the server.
        # isolation_level=None enables autocommit mode; we manage transactions
        # explicitly with BEGIN / COMMIT / ROLLBACK.
        self._conn = sqlite3.connect(db_path, check_same_thread=False, isolation_level=None)
        # Row factory makes column access by name possible (row["device_id"]).
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            # WAL mode allows concurrent reads while a write is in progress.
            self._conn.execute("PRAGMA journal_mode=WAL")
            # NORMAL sync is safe with WAL and much faster than FULL.
            self._conn.execute("PRAGMA synchronous=NORMAL")
            # Enforce foreign key constraints (not enabled by default in SQLite).
            self._conn.execute("PRAGMA foreign_keys=ON")
            # Create tables and indexes if they don't exist yet.
            self._conn.executescript(_SCHEMA)

    def close(self) -> None:
        """Close the database connection.  Called by the server on shutdown."""
        with self._lock:
            self._conn.close()

    def _row_to_device(self, row: sqlite3.Row) -> Device:
        """Convert a database row to a :class:`Device` model.

        Handles JSON decode errors gracefully by falling back to an empty dict
        rather than crashing the server on a corrupted metadata column.
        """
        try:
            metadata = json.loads(row["metadata"]) if row["metadata"] else {}
        except json.JSONDecodeError:
            # Corrupted metadata — return an empty dict rather than crashing.
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
        """Convert a database row to an :class:`Event` model."""
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
        """Insert or update a device row inside an explicit transaction.

        We use a SELECT-then-INSERT/UPDATE pattern rather than
        ``INSERT OR REPLACE`` to preserve the ``first_seen`` timestamp on
        updates (REPLACE would delete and re-insert the row, resetting it).
        """
        now = now_unix()
        # Derive the display name from metadata or generate one from the ID.
        device_name = str(heartbeat.metadata.get("device_name", "")) or (
            f"Device-{heartbeat.device_id[-8:]}"
        )
        metadata_json = json.dumps(heartbeat.metadata)
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                # Check whether the device already exists.
                cur = self._conn.execute(
                    "SELECT device_id FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                )
                is_new = cur.fetchone() is None

                if is_new:
                    # First heartbeat from this device — insert a new row.
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
                            now,  # first_seen = now
                            now,  # last_heartbeat = now
                            heartbeat.version,
                            heartbeat.uptime,
                            metadata_json,
                        ),
                    )
                else:
                    # Subsequent heartbeat — update mutable fields only.
                    # first_seen is intentionally NOT updated here.
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

                # Re-read the row to get the final state (including first_seen).
                row = self._conn.execute(
                    "SELECT * FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                ).fetchone()
                self._conn.execute("COMMIT")
            except sqlite3.Error as exc:
                self._conn.execute("ROLLBACK")
                raise StorageError(str(exc)) from exc

        if row is None:
            # Should never happen, but guard against race conditions.
            raise StorageError("device row vanished after upsert")
        return self._row_to_device(row), is_new

    def get_device(self, device_id: str) -> Device | None:
        """Return the device row or None.  Read-only; no transaction needed."""
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM devices WHERE device_id = ?", (device_id,)
            ).fetchone()
        return self._row_to_device(row) if row else None

    def list_devices(self, status: str | None = None) -> list[Device]:
        """Return all device rows, newest heartbeat first."""
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
        """Transition stale online devices to offline in a single transaction.

        Using a single UPDATE instead of per-device updates reduces lock
        contention and is more efficient for large device fleets.
        """
        with self._lock:
            try:
                self._conn.execute("BEGIN")
                # Identify which devices will be transitioned before updating.
                rows = self._conn.execute(
                    "SELECT device_id FROM devices WHERE status = 'online' AND last_heartbeat < ?",
                    (before_unix,),
                ).fetchall()
                if rows:
                    # Batch update all stale devices in one statement.
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
        """Insert an event row and return its auto-assigned ID."""
        # Use the event's timestamp if provided; otherwise record the server time.
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
            # lastrowid is None only if the INSERT somehow produced no row.
            raise StorageError("event insert returned no rowid")
        return int(event_id)

    def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events newest-first, optionally filtered by device.

        The secondary sort on ``id DESC`` ensures a stable order when multiple
        events share the same timestamp (e.g. batch inserts).
        """
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
