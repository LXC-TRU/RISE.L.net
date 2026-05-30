from __future__ import annotations

import json
import urllib.error
import urllib.request
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from rise_l_net.server import (
    AuthMiddleware,
    InMemoryStorage,
    RISELServer,
    ValidationMiddleware,
)


def _post(
    url: str, payload: dict[str, Any], headers: dict[str, str] | None = None
) -> tuple[int, dict[str, Any]]:
    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        method="POST",
        headers={"Content-Type": "application/json", **(headers or {})},
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8") or "{}")
    except urllib.error.HTTPError as exc:
        body = exc.read()
        return exc.code, json.loads(body.decode("utf-8") or "{}") if body else {}


@pytest.fixture
def server(free_port: int) -> Iterator[RISELServer]:
    srv = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    with srv:
        yield srv


def test_heartbeat_registers_new_device(server: RISELServer, free_port: int) -> None:
    status, body = _post(
        f"http://127.0.0.1:{free_port}/api/heartbeat",
        {"device_id": "dev-1", "version": "1.2.3"},
    )
    assert status == 200
    assert body["status"] == "ok"
    assert body["registered"] is True
    devices = server.get_devices()
    assert len(devices) == 1
    assert devices[0].device_id == "dev-1"
    assert devices[0].firmware_version == "1.2.3"


def test_heartbeat_then_report(server: RISELServer, free_port: int) -> None:
    base = f"http://127.0.0.1:{free_port}"
    _post(f"{base}/api/heartbeat", {"device_id": "dev"})
    status, body = _post(
        f"{base}/api/report",
        {"device_id": "dev", "event_type": "tick", "data": {"v": 1}},
    )
    assert status == 200
    assert body["message_id"].startswith("msg_")
    events = server.get_events(limit=10)
    assert len(events) == 1
    assert events[0].event_type == "tick"
    assert events[0].data == {"v": 1}


def test_invalid_json_returns_400(server: RISELServer, free_port: int) -> None:
    req = urllib.request.Request(
        f"http://127.0.0.1:{free_port}/api/heartbeat",
        data=b"not-json",
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(req, timeout=5)
        pytest.fail("expected HTTPError")
    except urllib.error.HTTPError as exc:
        assert exc.code == 400


def test_missing_device_id_returns_400(server: RISELServer, free_port: int) -> None:
    status, body = _post(f"http://127.0.0.1:{free_port}/api/heartbeat", {})
    assert status == 400
    assert "device_id" in body["error"]


def test_unknown_route_returns_404(server: RISELServer, free_port: int) -> None:
    status, _ = _post(f"http://127.0.0.1:{free_port}/api/missing", {"device_id": "x"})
    assert status == 404


def test_oversize_body_returns_413(free_port: int) -> None:
    server = RISELServer(
        port=free_port,
        host="127.0.0.1",
        storage=InMemoryStorage(),
        max_body_bytes=64,
    )
    with server:
        status, _ = _post(
            f"http://127.0.0.1:{free_port}/api/heartbeat",
            {"device_id": "dev", "padding": "x" * 500},
        )
        assert status == 413


def test_auth_middleware_rejects_without_key(free_port: int) -> None:
    server = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    server.use(AuthMiddleware(api_key="secret"))
    with server:
        status, _ = _post(f"http://127.0.0.1:{free_port}/api/heartbeat", {"device_id": "dev"})
        assert status == 403
        status, body = _post(
            f"http://127.0.0.1:{free_port}/api/heartbeat",
            {"device_id": "dev"},
            headers={"X-API-Key": "secret"},
        )
        assert status == 200


def test_validation_middleware_rejects_missing_device_id(free_port: int) -> None:
    server = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    server.use(ValidationMiddleware())
    with server:
        status, _ = _post(f"http://127.0.0.1:{free_port}/api/heartbeat", {})
        assert status == 403  # blocked by middleware before parser


def test_custom_route(free_port: int) -> None:
    server = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    server.route("/api/echo", lambda req: {"echoed": req.body})
    with server:
        status, body = _post(
            f"http://127.0.0.1:{free_port}/api/echo",
            {"device_id": "dev", "v": 7},
        )
        assert status == 200
        assert body["echoed"] == {"device_id": "dev", "v": 7}


def test_storage_swap(free_port: int) -> None:
    server = RISELServer(port=free_port, host="127.0.0.1", storage=InMemoryStorage())
    server.storage(InMemoryStorage())
    with server:
        _post(f"http://127.0.0.1:{free_port}/api/heartbeat", {"device_id": "x"})
        assert len(server.get_devices()) == 1


def test_sqlite_persistence_across_restart(tmp_path: Path, free_port: int) -> None:
    db_path = tmp_path / "rl.db"
    server1 = RISELServer(port=free_port, host="127.0.0.1", db_path=str(db_path))
    with server1:
        _post(f"http://127.0.0.1:{free_port}/api/heartbeat", {"device_id": "persist"})
    server2 = RISELServer(port=free_port, host="127.0.0.1", db_path=str(db_path))
    try:
        devices = server2.get_devices()
        assert {d.device_id for d in devices} == {"persist"}
    finally:
        server2._storage.close()
