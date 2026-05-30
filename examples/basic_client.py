"""Minimal sync client example.

Run a server first (python examples/basic_server.py) and then::

    python examples/basic_client.py
"""
from __future__ import annotations

import time

from rise_l_net import RISELDevice, configure_logging
from rise_l_net.client import RetryMiddleware


def main() -> None:
    configure_logging("INFO")
    device = RISELDevice(
        "http://127.0.0.1:8080",
        device_id="example-1",
        heartbeat_interval=10,
        metadata={"location": "lab", "device_name": "Example Device"},
    ).use(RetryMiddleware(max_retries=3))

    if not device.start(block=False):
        return
    try:
        for i in range(3):
            device.report("temperature", {"value": 22.0 + i})
            time.sleep(2)
    finally:
        device.stop()


if __name__ == "__main__":
    main()
