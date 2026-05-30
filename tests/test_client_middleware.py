from __future__ import annotations

from typing import Any

import pytest

from rise_l_net.client.middleware import (
    CacheMiddleware,
    LoggingMiddleware,
    Middleware,
    RetryMiddleware,
    ThrottleMiddleware,
)
from rise_l_net.client.transport import Transport
from rise_l_net.exceptions import TransportError


class FakeTransport(Transport):
    def __init__(self, responses: list[Any] | None = None) -> None:
        self.responses: list[Any] = responses or []
        self.calls: list[tuple[str, dict[str, Any]]] = []

    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        self.calls.append((endpoint, dict(data)))
        if not self.responses:
            return {"ok": True}
        item = self.responses.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def test_logging_middleware_does_not_modify_payload() -> None:
    mw = LoggingMiddleware()
    assert mw.before_send("/x", {"a": 1}) == {"a": 1}
    assert mw.after_send("/x", {"a": 1}, {"ok": True}) == {"ok": True}


def test_throttle_pauses_between_sends(monkeypatch: pytest.MonkeyPatch) -> None:
    sleeps: list[float] = []
    fake_now = [0.0]

    def fake_monotonic() -> float:
        return fake_now[0]

    def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        fake_now[0] += seconds

    import rise_l_net.client.middleware as m

    monkeypatch.setattr(m, "monotonic", fake_monotonic)
    monkeypatch.setattr(m, "sleep", fake_sleep)

    mw = ThrottleMiddleware(max_requests_per_second=10.0)  # interval = 0.1s
    # First call: clock=0, last_send=0, elapsed=0 < 0.1, so sleeps 0.1.
    mw.before_send("/x", {})
    assert sleeps[-1] == pytest.approx(0.1)
    # Manually advance clock by 0.05 — less than interval, so we sleep again.
    fake_now[0] += 0.05
    mw.before_send("/x", {})
    assert sleeps[-1] == pytest.approx(0.05)


def test_throttle_invalid_rate() -> None:
    with pytest.raises(ValueError):
        ThrottleMiddleware(max_requests_per_second=0)


def test_retry_returns_true_until_max(monkeypatch: pytest.MonkeyPatch) -> None:
    import rise_l_net.client.middleware as m

    monkeypatch.setattr(m, "sleep", lambda _: None)
    mw = RetryMiddleware(max_retries=2, base_delay=0.0, jitter=0)
    err = TransportError("boom")
    assert mw.on_error("/x", {}, err) is True
    assert mw.on_error("/x", {}, err) is True
    assert mw.on_error("/x", {}, err) is False


def test_retry_resets_after_success(monkeypatch: pytest.MonkeyPatch) -> None:
    import rise_l_net.client.middleware as m

    monkeypatch.setattr(m, "sleep", lambda _: None)
    mw = RetryMiddleware(max_retries=2, base_delay=0.0, jitter=0)
    mw.on_error("/x", {}, TransportError("a"))
    mw.after_send("/x", {}, {"ok": True})
    # Counter cleared, so we get max_retries opportunities again.
    assert mw.on_error("/x", {}, TransportError("b")) is True
    assert mw.on_error("/x", {}, TransportError("c")) is True
    assert mw.on_error("/x", {}, TransportError("d")) is False


def test_cache_middleware_persists_failed_sends(tmp_path: Any) -> None:
    cache_path = tmp_path / "cache.json"
    mw = CacheMiddleware(cache_path=str(cache_path), max_entries=5)
    err = TransportError("offline")
    mw.on_error("/api/report", {"x": 1}, err)
    mw.on_error("/api/report", {"x": 2}, err)
    assert len(mw.pending()) == 2
    drained = mw.drain()
    assert drained[0]["data"] == {"x": 1}
    assert mw.pending() == []
    # Persistence: a fresh middleware sees the empty cache after drain.
    fresh = CacheMiddleware(cache_path=str(cache_path), max_entries=5)
    assert fresh.pending() == []


def test_cache_middleware_ignores_non_transport_errors(tmp_path: Any) -> None:
    mw = CacheMiddleware(cache_path=str(tmp_path / "c.json"))
    mw.on_error("/x", {}, RuntimeError("not network"))
    assert mw.pending() == []


def test_middleware_base_class_is_passthrough() -> None:
    mw = Middleware()
    assert mw.before_send("/x", {"a": 1}) == {"a": 1}
    assert mw.after_send("/x", {"a": 1}, {"ok": True}) == {"ok": True}
    assert mw.on_error("/x", {}, TransportError("x")) is False
