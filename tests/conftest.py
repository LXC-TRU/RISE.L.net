"""Shared test fixtures."""

from __future__ import annotations

import socket
from collections.abc import Iterator

import pytest


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return int(s.getsockname()[1])


@pytest.fixture
def free_port() -> int:
    return _free_port()


@pytest.fixture
def free_ports() -> Iterator[list[int]]:
    """Pre-allocate two distinct free ports."""
    yield [_free_port(), _free_port()]
