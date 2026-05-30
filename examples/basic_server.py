"""Minimal sync server example.

    python examples/basic_server.py
"""
from __future__ import annotations

from rise_l_net import RISELServer, configure_logging


def main() -> None:
    configure_logging("INFO")
    server = RISELServer(port=8080)
    server.on_report(
        lambda device_id, payload: print(
            f"event from {device_id}: {payload['event_type']} -> {payload.get('data')}"
        )
    )
    server.start()


if __name__ == "__main__":
    main()
