"""Async server + client integration tests (aiohttp)."""

from __future__ import annotations

from collections.abc import AsyncIterator

import pytest

aiohttp = pytest.importorskip("aiohttp")
pytest.importorskip("aiosqlite")

from rise_l_net.client.async_device import AsyncRISELDevice
from rise_l_net.server.async_server import AsyncRISELServer
from rise_l_net.server.async_storage import AsyncInMemoryStorage


@pytest.fixture
async def async_server(free_port: int) -> AsyncIterator[tuple[AsyncRISELServer, str]]:
    server = AsyncRISELServer(port=free_port, host="127.0.0.1", storage=AsyncInMemoryStorage())
    async with server:
        yield server, f"http://127.0.0.1:{free_port}"


async def test_async_heartbeat_and_report(
    async_server: tuple[AsyncRISELServer, str],
) -> None:
    server, base = async_server
    async with aiohttp.ClientSession() as session:
        async with session.post(
            f"{base}/api/heartbeat", json={"device_id": "async-dev", "version": "9.9"}
        ) as resp:
            assert resp.status == 200
            body = await resp.json()
            assert body["registered"] is True
        async with session.post(
            f"{base}/api/report",
            json={"device_id": "async-dev", "event_type": "tick", "data": {"v": 1}},
        ) as resp:
            assert resp.status == 200

    devices = await server.get_devices()
    assert {d.device_id for d in devices} == {"async-dev"}
    events = await server.get_events(device_id="async-dev")
    assert events[0].event_type == "tick"


async def test_async_invalid_json_returns_400(
    async_server: tuple[AsyncRISELServer, str],
) -> None:
    _, base = async_server
    async with (
        aiohttp.ClientSession() as session,
        session.post(
            f"{base}/api/heartbeat",
            data=b"not-json",
            headers={"Content-Type": "application/json"},
        ) as resp,
    ):
        assert resp.status == 400


async def test_async_device_against_async_server(
    async_server: tuple[AsyncRISELServer, str],
) -> None:
    server, base = async_server
    device = AsyncRISELDevice(base, device_id="async-e2e", heartbeat_interval=3600)
    async with device:
        assert await device.report("ping", {"x": 7}) is True

    events = await server.get_events(device_id="async-e2e")
    assert events and events[0].event_type == "ping"


async def test_async_storage_inmemory_basic() -> None:
    storage = AsyncInMemoryStorage()
    from rise_l_net.models import Event, Heartbeat

    device, is_new = await storage.upsert_device(Heartbeat(device_id="x"))
    assert is_new is True and device.status == "online"
    assert (await storage.get_device("x")).device_id == "x"  # type: ignore[union-attr]
    eid = await storage.save_event(Event(device_id="x", event_type="t", timestamp=10))
    assert eid >= 1
    events = await storage.list_events(device_id="x")
    assert len(events) == 1
    transitioned = await storage.mark_offline(before_unix=10**12)
    assert "x" in transitioned
