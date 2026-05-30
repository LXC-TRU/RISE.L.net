from __future__ import annotations

from typing import Any

import pytest

from rise_l_net.client.device import RISELDevice
from rise_l_net.client.middleware import RetryMiddleware
from rise_l_net.client.transport import Transport
from rise_l_net.exceptions import TransportError


class FakeTransport(Transport):
    def __init__(
        self,
        responses: list[Any] | None = None,
        connect_error: Exception | None = None,
    ) -> None:
        self.responses: list[Any] = responses or []
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.connect_calls = 0
        self.close_calls = 0
        self._connect_error = connect_error

    def connect(self) -> None:
        self.connect_calls += 1
        if self._connect_error is not None:
            raise self._connect_error

    def close(self) -> None:
        self.close_calls += 1

    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((endpoint, dict(data)))
        if not self.responses:
            return {"ok": True}
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_device_start_sends_initial_heartbeat() -> None:
    transport = FakeTransport()
    device = RISELDevice(
        "http://localhost", device_id="dev-1", heartbeat_interval=3600, transport=transport
    )
    assert device.start(block=False) is True
    try:
        assert transport.calls
        assert transport.calls[0][0] == "/api/heartbeat"
        assert transport.calls[0][1]["device_id"] == "dev-1"
    finally:
        device.stop()
    assert transport.close_calls == 1


def test_device_start_returns_false_if_initial_heartbeat_fails() -> None:
    transport = FakeTransport(responses=[TransportError("boom")])
    device = RISELDevice(
        "http://localhost", device_id="dev-1", heartbeat_interval=3600, transport=transport
    )
    assert device.start(block=False) is False


def test_device_report_payload_shape() -> None:
    transport = FakeTransport()
    device = RISELDevice(
        "http://localhost", device_id="dev-1", heartbeat_interval=3600, transport=transport
    )
    device.start(block=False)
    try:
        assert device.report("temperature", {"value": 23.5}, severity="warning") is True
        endpoint, payload = transport.calls[-1]
        assert endpoint == "/api/report"
        assert payload["device_id"] == "dev-1"
        assert payload["event_type"] == "temperature"
        assert payload["data"] == {"value": 23.5}
        assert payload["severity"] == "warning"
        assert payload["timestamp"] > 0
    finally:
        device.stop()


def test_device_report_failure_returns_false() -> None:
    transport = FakeTransport(responses=[{"ok": True}, TransportError("net")])
    device = RISELDevice(
        "http://localhost", device_id="d", heartbeat_interval=3600, transport=transport
    )
    device.start(block=False)
    try:
        assert device.report("x", {}) is False
    finally:
        device.stop()


def test_retry_middleware_drives_resend(monkeypatch: pytest.MonkeyPatch) -> None:
    import rise_l_net.client.middleware as m

    monkeypatch.setattr(m, "sleep", lambda _: None)
    transport = FakeTransport(
        responses=[
            {"ok": True},  # initial heartbeat in start()
            TransportError("first"),
            TransportError("second"),
            {"ok": True},
        ]
    )
    device = RISELDevice(
        "http://localhost",
        device_id="d",
        heartbeat_interval=3600,
        transport=transport,
    )
    device.use(RetryMiddleware(max_retries=3, base_delay=0.0, jitter=0))
    assert device.start(block=False) is True
    try:
        assert device.report("x", {}) is True
        endpoints = [c[0] for c in transport.calls]
        # initial heartbeat + 3 attempts on /api/report
        assert endpoints.count("/api/report") == 3
    finally:
        device.stop()


def test_hooks_are_invoked() -> None:
    transport = FakeTransport()
    ready: list[int] = []
    success: list[Any] = []
    device = RISELDevice(
        "http://localhost",
        device_id="d",
        heartbeat_interval=3600,
        transport=transport,
    )
    device.on_ready(lambda: ready.append(1))
    device.hook("on_heartbeat_success", lambda r: success.append(r))
    device.start(block=False)
    try:
        assert ready == [1]
        assert len(success) >= 1
    finally:
        device.stop()


def test_set_api_key_updates_http_transport_headers() -> None:
    device = RISELDevice("http://localhost", device_id="d", heartbeat_interval=3600)
    device.set_api_key("new-secret")
    from rise_l_net.client.transport import HTTPTransport

    assert isinstance(device._transport, HTTPTransport)
    assert device._transport.headers["X-API-Key"] == "new-secret"


def test_unknown_hook_raises() -> None:
    device = RISELDevice("http://localhost", device_id="d", heartbeat_interval=3600)
    with pytest.raises(ValueError):
        device.hook("on_does_not_exist", lambda: None)
