from __future__ import annotations

from rise_l_net._compat import MICROPYTHON, get_mac_address, monotonic, now_unix


def test_micropython_flag_is_false_on_cpython() -> None:
    assert MICROPYTHON is False


def test_now_unix_is_int() -> None:
    value = now_unix()
    assert isinstance(value, int)
    assert value > 1_700_000_000  # sanity: post-2023


def test_monotonic_is_monotonic() -> None:
    a = monotonic()
    b = monotonic()
    assert b >= a


def test_get_mac_address_returns_string_or_none() -> None:
    mac = get_mac_address()
    if mac is not None:
        parts = mac.split(":")
        assert len(parts) == 6
        for part in parts:
            assert len(part) == 2
            int(part, 16)
