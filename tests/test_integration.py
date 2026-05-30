"""End-to-end test using the real HTTPTransport against a live RISELServer."""

from __future__ import annotations

from collections.abc import Iterator

import pytest

from rise_l_net.client.device import RISELDevice
from rise_l_net.server import InMemoryStorage, RISELServer


@pytest.fixture
def server(free_port: int) -> Iterator[tuple[RISELServer, str]]:
    srv = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    with srv:
        yield srv, f"http://127.0.0.1:{free_port}"


def test_real_client_against_real_server(server: tuple[RISELServer, str]) -> None:
    srv, base = server
    device = RISELDevice(base, device_id="e2e-1", heartbeat_interval=3600)
    assert device.start(block=False) is True
    try:
        assert device.report("temperature", {"value": 24.0}) is True
    finally:
        device.stop()

    devices = srv.get_devices()
    assert any(d.device_id == "e2e-1" for d in devices)
    events = srv.get_events(device_id="e2e-1")
    assert len(events) == 1
    assert events[0].event_type == "temperature"
    assert events[0].data == {"value": 24.0}
