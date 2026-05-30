from __future__ import annotations

from pathlib import Path

import pytest

from rise_l_net.models import Event, Heartbeat
from rise_l_net.server.storage import InMemoryStorage, SQLiteStorage, Storage


@pytest.fixture(params=["memory", "sqlite"])
def storage(request: pytest.FixtureRequest, tmp_path: Path) -> Storage:
    if request.param == "memory":
        return InMemoryStorage()
    db_path = tmp_path / "test.db"
    store = SQLiteStorage(str(db_path))
    request.addfinalizer(store.close)
    return store


def _heartbeat(device_id: str = "dev-1", **kwargs: object) -> Heartbeat:
    return Heartbeat(device_id=device_id, **kwargs)  # type: ignore[arg-type]


def test_upsert_inserts_then_updates(storage: Storage) -> None:
    device, is_new = storage.upsert_device(_heartbeat())
    assert is_new is True
    assert device.device_id == "dev-1"
    assert device.status == "online"

    device2, is_new = storage.upsert_device(_heartbeat(ip="192.168.1.5", uptime=42))
    assert is_new is False
    assert device2.ip_address == "192.168.1.5"
    assert device2.uptime == 42
    assert device2.first_seen == device.first_seen


def test_get_device_returns_none_for_unknown(storage: Storage) -> None:
    assert storage.get_device("missing") is None


def test_list_devices_filters_by_status(storage: Storage) -> None:
    storage.upsert_device(_heartbeat("a"))
    storage.upsert_device(_heartbeat("b"))
    online = storage.list_devices(status="online")
    assert {d.device_id for d in online} == {"a", "b"}
    assert storage.list_devices(status="offline") == []


def test_mark_offline_transitions_only_stale_devices(storage: Storage) -> None:
    storage.upsert_device(_heartbeat("fresh"))
    storage.upsert_device(_heartbeat("stale"))
    # Force "stale" to look old by setting a far-future cutoff.
    transitioned = storage.mark_offline(before_unix=10**12)
    assert set(transitioned) == {"fresh", "stale"}
    assert all(d.status == "offline" for d in storage.list_devices())
    # Second call is a no-op
    assert storage.mark_offline(before_unix=10**12) == []


def test_save_event_returns_increasing_ids(storage: Storage) -> None:
    storage.upsert_device(_heartbeat("dev"))
    id1 = storage.save_event(Event(device_id="dev", event_type="t1"))
    id2 = storage.save_event(Event(device_id="dev", event_type="t2"))
    assert id2 > id1


def test_list_events_orders_newest_first_and_filters(storage: Storage) -> None:
    storage.upsert_device(_heartbeat("a"))
    storage.upsert_device(_heartbeat("b"))
    storage.save_event(Event(device_id="a", event_type="x", timestamp=10))
    storage.save_event(Event(device_id="b", event_type="y", timestamp=20))
    storage.save_event(Event(device_id="a", event_type="z", timestamp=30))

    all_events = storage.list_events(limit=10)
    assert [e.event_type for e in all_events] == ["z", "y", "x"]

    a_events = storage.list_events(device_id="a", limit=10)
    assert [e.event_type for e in a_events] == ["z", "x"]

    assert storage.list_events(limit=2) == all_events[:2]


def test_save_event_persists_data_payload(storage: Storage) -> None:
    storage.upsert_device(_heartbeat("dev"))
    storage.save_event(Event(device_id="dev", event_type="t", data={"k": 1, "nested": {"a": "b"}}))
    events = storage.list_events(limit=1)
    assert events[0].data == {"k": 1, "nested": {"a": "b"}}


def test_list_events_negative_limit_raises(storage: Storage) -> None:
    with pytest.raises(ValueError):
        storage.list_events(limit=-1)
