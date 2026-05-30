from __future__ import annotations

import pytest

from rise_l_net.server.middleware import (
    AuthMiddleware,
    RateLimitMiddleware,
    Request,
    ValidationMiddleware,
)


def _request(**kwargs: object) -> Request:
    defaults = {
        "path": "/api/heartbeat",
        "headers": {},
        "body": {"device_id": "dev"},
        "device_id": "dev",
        "remote_addr": "127.0.0.1",
    }
    defaults.update(kwargs)
    return Request(**defaults)  # type: ignore[arg-type]


def test_request_header_lookup_is_case_insensitive() -> None:
    req = _request(headers={"X-API-Key": "secret"})
    assert req.header("x-api-key") == "secret"
    assert req.header("X-API-KEY") == "secret"
    assert req.header("missing", "default") == "default"


def test_auth_accepts_valid_key() -> None:
    mw = AuthMiddleware(api_key="secret")
    req = _request(headers={"X-API-Key": "secret"})
    assert mw.before_request(req) is req


def test_auth_rejects_missing_or_wrong_key() -> None:
    mw = AuthMiddleware(api_key="secret")
    assert mw.before_request(_request(headers={})) is None
    assert mw.before_request(_request(headers={"X-API-Key": "wrong"})) is None


def test_auth_rejects_empty_key_in_constructor() -> None:
    with pytest.raises(ValueError):
        AuthMiddleware(api_key="")


def test_validation_requires_device_id() -> None:
    mw = ValidationMiddleware()
    assert mw.before_request(_request(device_id="dev")) is not None
    assert mw.before_request(_request(device_id=None)) is None


def test_rate_limit_blocks_after_quota() -> None:
    mw = RateLimitMiddleware(max_requests_per_minute=2)
    req = _request()
    assert mw.before_request(req) is req
    assert mw.before_request(req) is req
    assert mw.before_request(req) is None


def test_rate_limit_keys_per_device() -> None:
    mw = RateLimitMiddleware(max_requests_per_minute=1)
    a = _request(device_id="a")
    b = _request(device_id="b")
    assert mw.before_request(a) is a
    assert mw.before_request(b) is b
    assert mw.before_request(a) is None
    assert mw.before_request(b) is None


def test_rate_limit_invalid_quota() -> None:
    with pytest.raises(ValueError):
        RateLimitMiddleware(max_requests_per_minute=0)
