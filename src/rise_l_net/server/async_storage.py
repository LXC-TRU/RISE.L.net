"""Async storage backends."""

from __future__ import annotations

import asyncio
import json
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any

from .._compat import now_unix
from .._logging import get_logger
from ..exceptions import StorageError
from ..models import Device, Event, Heartbeat
from .storage import _SCHEMA  # share schema with sync backend

if TYPE_CHECKING:
    import aiosqlite

log = get_logger("server.async_storage")


class AsyncStorage(ABC):
    @abstractmethod
    async def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]: ...

    @abstractmethod
    async def get_device(self, device_id: str) -> Device | None: ...

    @abstractmethod
    async def list_devices(self, status: str | None = None) -> list[Device]: ...

    @abstractmethod
    async def mark_offline(self, before_unix: int) -> list[str]: ...

    @abstractmethod
    async def save_event(self, event: Event) -> int: ...

    @abstractmethod
    async def list_events(self, device_id: str | None = None, limit: int = 100) -> list[Event]: ...

    async def close(self) -> None:
        """Release resources."""


class AsyncInMemoryStorage(AsyncStorage):
    """Async in-memory store. Useful for tests."""

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        self._devices: dict[str, Device] = {}
        self._events: list[Event] = []
        self._next_event_id = 1

    async def upsert_device(self, heartbeat: Heartbeat) -> tuple[Device, bool]:
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
        async with self._lock:
            return self._devices.get(device_id)

    async def list_devices(self, status: str | None = None) -> list[Device]:
        async with self._lock:
            rows = list(self._devices.values())
        if status is not None:
            rows = [d for d in rows if d.status == status]
        rows.sort(key=lambda d: d.last_heartbeat, reverse=True)
        return rows

    async def mark_offline(self, before_unix: int) -> list[str]:
        transitioned: list[str] = []
        async with self._lock:
            for device in self._devices.values():
                if device.status == "online" and device.last_heartbeat < before_unix:
                    device.status = "offline"
                    transitioned.append(device.device_id)
        return transitioned

    async def save_event(self, event: Event) -> int:
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
        async with self._lock:
            rows = list(self._events)
        if device_id is not None:
            rows = [e for e in rows if e.device_id == device_id]
        rows.sort(key=lambda e: e.timestamp, reverse=True)
        return rows[:limit]


class AsyncSQLiteStorage(AsyncStorage):
    """SQLite-backed async storage using aiosqlite."""

    def __init__(self, db_path: str = "riselnet.db") -> None:
        try:
            import aiosqlite  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "AsyncSQLiteStorage requires aiosqlite. Install with: pip install rise-l-net[async]"
            ) from exc
        self.db_path = db_path
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def _get_conn(self) -> aiosqlite.Connection:
        import aiosqlite

        if self._conn is None:
            self._conn = await aiosqlite.connect(self.db_path, isolation_level=None)
            self._conn.row_factory = aiosqlite.Row
            await self._conn.execute("PRAGMA journal_mode=WAL")
            await self._conn.execute("PRAGMA synchronous=NORMAL")
            await self._conn.executescript(_SCHEMA)
        return self._conn

    async def close(self) -> None:
        async with self._lock:
            if self._conn is not None:
                await self._conn.close()
                self._conn = None

    @staticmethod
    def _row_to_device(row: Any) -> Device:
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
        conn = await self._get_conn()
        now = now_unix()
        device_name = str(heartbeat.metadata.get("device_name", "")) or (
            f"Device-{heartbeat.device_id[-8:]}"
        )
        metadata_json = json.dumps(heartbeat.metadata)
        async with self._lock:
            try:
                await conn.execute("BEGIN")
                async with conn.execute(
                    "SELECT device_id FROM devices WHERE device_id = ?",
                    (heartbeat.device_id,),
                ) as cur:
                    is_new = await cur.fetchone() is None
                if is_new:
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
                            now,
                            now,
                            heartbeat.version,
                            heartbeat.uptime,
                            metadata_json,
                        ),
                    )
                else:
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
        conn = await self._get_conn()
        async with conn.execute("SELECT * FROM devices WHERE device_id = ?", (device_id,)) as cur:
            row = await cur.fetchone()
        return self._row_to_device(row) if row else None

    async def list_devices(self, status: str | None = None) -> list[Device]:
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
        conn = await self._get_conn()
        async with self._lock:
            try:
                await conn.execute("BEGIN")
                async with conn.execute(
                    "SELECT device_id FROM devices WHERE status = 'online' AND last_heartbeat < ?",
                    (before_unix,),
                ) as cur:
                    rows = await cur.fetchall()
                if rows:
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
