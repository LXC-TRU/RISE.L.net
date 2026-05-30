"""Async storage backends for the RISE.L.net server.

This module provides async counterparts of the sync storage backends in
:mod:`rise_l_net.server.storage`.  The API is identical except that all
methods are coroutines.

:class:`AsyncInMemoryStorage` is useful for tests and small deployments.
:class:`AsyncSQLiteStorage` uses ``aiosqlite`` for non-blocking SQLite access,
which is important in an async server where blocking I/O would stall the event
loop and delay responses to other devices.

Both classes share the same SQL schema as the sync backend (imported from
:mod:`rise_l_net.server.storage`) so that a database created by one can be
read by the other.

Requires the ``aiosqlite`` package for :class:`AsyncSQLiteStorage`::

    pip install "rise-l-net[async]"
"""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .._compat import now_unix
from .._logging import get_logger
from ..exceptions import StorageError
from ..models import Device, Event, Heartbeat

# Re-use the SQL schema string from the sync backend to keep them in sync.
from .storage import _SCHEMA

if TYPE_CHECKING:
    # Only imported for type annotations; the runtime import is deferred to
    # _get_conn() so the module can be imported without aiosqlite installed.
    import aiosqlite

log = get_logger("server.async_storage")


class AsyncStorage(ABC):
    """Abstract async storage backend.

    All methods are coroutines.  Implementations must be safe to call
    concurrently from multiple coroutines.
    """

    @abstractmethod
    async def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device row from a heartbeat.

        Returns ``(device, is_new)`` where *is_new* is True for new devices.
        """
        ...

    @abstractmethod
    async def get_device(self, device_id: str) -> Device | None:
        """Return the device with *device_id*, or None if not found."""
        ...

    @abstractmethod
    async def list_devices(self, status: str | None = None) -> list[Device]:
        """Return all devices, optionally filtered by status."""
        ...

    @abstractmethod
    async def mark_offline(self, before_unix: int) -> list[str]:
        """Mark stale online devices as offline.  Returns transitioned IDs."""
        ...

    @abstractmethod
    async def save_event(self, event: Event) -> int:
        """Persist an event and return its auto-assigned integer ID."""
        ...

    @abstractmethod
    async def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events newest-first, optionally filtered by device."""
        ...

    async def close(self) -> None:
        """Release resources.  Default is a no-op."""


class AsyncInMemoryStorage(AsyncStorage):
    """Async in-memory storage backend.

    Uses an ``asyncio.Lock`` to serialise concurrent coroutine access.
    All data is lost when the process exits.

    Suitable for unit tests and small deployments where persistence is not
    required.
    """

    def __init__(self) -> None:
        # asyncio.Lock (not threading.Lock) because this class is used in
        # async contexts where threading primitives would block the event loop.
        self._lock = asyncio.Lock()
        self._devices: dict[str, Device] = {}
        self._events: list[Event] = []
        self._next_event_id = 1

    async def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device.  Coroutine-safe via asyncio.Lock."""
        async with self._lock:
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
                # Preserve first_seen on updates.
                first_seen=existing.first_seen if existing else now,
                last_heartbeat=now,
                status="online",
                firmware_version=heartbeat.version,
                uptime=heartbeat.uptime,
                metadata=heartbeat.metadata,
            )
            self._devices[heartbeat.device_id] = device
            return device, is_new

    async def get_device(self, device_id: str) -> Device | None:
        """Return the device or None.  Coroutine-safe."""
        async with self._lock:
            return self._devices.get(device_id)

    async def list_devices(self, status: str | None = None) -> list[Device]:
        """Return devices sorted by last_heartbeat descending."""
        async with self._lock:
            # Snapshot under the lock to avoid mutation during sort.
            rows = list(self._devices.values())
        if status is not None:
            rows = [d for d in rows if d.status == status]
        rows.sort(key=lambda d: d.last_heartbeat, reverse=True)
        return rows

    async def mark_offline(self, before_unix: int) -> list[str]:
        """Transition stale online devices to offline.  Coroutine-safe."""
        transitioned: list[str] = []
        async with self._lock:
            for device in self._devices.values():
                if device.status == "online" and device.last_heartbeat < before_unix:
                    device.status = "offline"
                    transitioned.append(device.device_id)
        return transitioned

    async def save_event(self, event: Event) -> int:
        """Append the event and return its ID.  Coroutine-safe."""
        async with self._lock:
            event_id = self._next_event_id
            self._next_event_id += 1
            self._events.append(
                Event(
                    device_id=event.device_id,
                    event_type=event.event_type,
                    data=event.data,
                    severity=event.severity,
                    timestamp=event.timestamp or now_unix(),
                )
            )
            return event_id

    async def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events newest-first, optionally filtered by device."""
        async with self._lock:
            rows = list(self._events)
        if device_id is not None:
            rows = [e for e in rows if e.device_id == device_id]
        rows.sort(key=lambda e: e.timestamp, reverse=True)
        return rows[:limit]


class AsyncSQLiteStorage(AsyncStorage):
    """Async SQLite storage backend using ``aiosqlite``.

    ``aiosqlite`` wraps SQLite in a background thread and exposes an async
    interface, so database operations do not block the event loop.

    The connection is opened lazily on the first call to ``_get_conn()`` and
    reused for the lifetime of the server.  An ``asyncio.Lock`` serialises
    write operations to avoid SQLite's "database is locked" errors.

    Args:
        db_path: Path to the SQLite database file.  Created if it does not
                 exist.
    """

    def __init__(self, db_path: str = "riselnet.db") -> None:
        # Fail fast at construction time if aiosqlite is not installed.
        try:
            import aiosqlite  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "AsyncSQLiteStorage requires aiosqlite. Install with: pip install rise-l-net[async]"
            ) from exc
        self.db_path = db_path
        # Connection is None until _get_conn() is called for the first time.
        self._conn: aiosqlite.Connection | None = None
        # Lock to serialise write transactions.
        self._lock = asyncio.Lock()

    async def _get_conn(self) -> aiosqlite.Connection:
        """Return the open database connection, opening it if necessary."""
        import aiosqlite

        if self._conn is None:
            # isolation_level=None enables autocommit; we manage transactions
            # explicitly with BEGIN / COMMIT / ROLLBACK.
            self._conn = await aiosqlite.connect(self.db_path, isolation_level=None)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            # Create tables and indexes if they don't exist yet.
            await self._conn.executescript(_SCHEMA)
        return self._conn

    async def close(self) -> None:
        """Close the database connection.  Called by the server on shutdown."""
        async with self._lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None

    @staticmethod
    def _row_to_device(row: Any) -> Device:
        """Convert an aiosqlite row to a :class:`Device` model."""
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

    @staticmethod
    def _row_to_event(row: Any) -> Event:
        """Convert an aiosqlite row to an :class:`Event` model."""
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

    async def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
        """Insert or update a device row inside an explicit transaction."""
        conn = await self._get_conn()
        now = now_unix()
        device_name = str(heartbeat.metadata.get("device_name", "")) or (
            f"Device-{heartbeat.device_id[-8:]}"
        )
        metadata_json = json.dumps(heartbeat.metadata)
        async with self._lock:
            try:
                await conn.execute("BEGIN")
                # Check whether the device already exists.
                async with conn.execute(
                    "SELECT device_id FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                ) as cur:
                    is_new = await cur.fetchone() is None

                if is_new:
                    # First heartbeat — insert a new row.
                    await conn.execute(
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
                    await conn.execute(
                        """
                        UPDATE devices
                           SET ip_address = ?, last_heartbeat = ?, status = 'online',
                               firmware_version = ?, uptime = ?, metadata = ?
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

                # Re-read the row to get the final state.
                async with conn.execute(
                    "SELECT * FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                ) as cur:
                    row = await cur.fetchone()
                await conn.execute("COMMIT")
            except Exception as exc:
                await conn.execute("ROLLBACK")
                raise StorageError(str(exc)) from exc

        if row is None:
            raise StorageError("device row vanished after upsert")
        return self._row_to_device(row), is_new

    async def get_device(self, device_id: str) -> Device | None:
        """Return the device row or None.  Read-only; no lock needed."""
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)) as cur:
            row = await cur.fetchone()
        return self._row_to_device(row) if row else None

    async def list_devices(self, status: str | None = None) -> list[Device]:
        """Return all device rows, newest heartbeat first."""
        conn = await self._get_conn()
        if status is None:
            query = "SELECT * FROM devices ORDER BY last_heartbeat DESC"
            params: tuple[Any, ...] = ()
        else:
            query = "SELECT * FROM devices WHERE status = ? ORDER BY last_heartbeat DESC"
            params = (status,)
        async with conn.execute(query, params) as cur:
            rows = await cur.fetchall()
        return [self._row_to_device(r) for r in rows]

    async def mark_offline(self, before_unix: int) -> list[str]:
        """Transition stale online devices to offline in a single transaction."""
        conn = await self._get_conn()
        async with self._lock:
            try:
                await conn.execute("BEGIN")
                # Identify which devices will be transitioned before updating.
                async with conn.execute(
                    "SELECT device_id FROM devices WHERE status = 'online' AND last_heartbeat < ?",
                    (before_unix,),
                ) as cur:
                    rows = await cur.fetchall()
                if rows:
                    # Batch update all stale devices in one statement.
                    await conn.execute(
                        "UPDATE devices SET status = 'offline' "
                        "WHERE status = 'online' AND last_heartbeat < ?",
                        (before_unix,),
                    )
                await conn.execute("COMMIT")
            except Exception as exc:
                await conn.execute("ROLLBACK")
                raise StorageError(str(exc)) from exc
        return [row["device_id"] for row in rows]

    async def save_event(self, event: Event) -> int:
        """Insert an event row and return its auto-assigned ID."""
        conn = await self._get_conn()
        timestamp = event.timestamp or now_unix()
        data_json = json.dumps(event.data)
        async with conn.execute(
            """
            INSERT INTO events (device_id, timestamp, event_type, severity, data)
            VALUES (?, ?, ?, ?, ?)
            """,
            (event.device_id, timestamp, event.event_type, event.severity, data_json),
        ) as cur:
            event_id = cur.lastrowid
        if event_id is None:
            raise StorageError("event insert returned no rowid")
        return int(event_id)

    async def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]:
        """Return events newest-first, optionally filtered by device."""
        if limit < 0:
            raise ValueError("limit must be non-negative")
        conn = await self._get_conn()
        if device_id is None:
            query = "SELECT * FROM events ORDER BY timestamp DESC, id DESC LIMIT ?"
            params: tuple[Any, ...] = (limit,)
        else:
            query = (
                "SELECT * FROM events WHERE device_id = ? ORDER BY timestamp DESC, id DESC LIMIT ?"
            )
            params = (device_id, limit)
        async with conn.execute(query, params) as cur:
            rows = await cur.fetchall()
        return [self._row_to_event(r) for r in rows]
