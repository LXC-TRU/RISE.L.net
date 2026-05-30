"""Async server example.

    python examples/async_server.py
"""
from __future__ import annotations

import asyncio

from rise_l_net import AsyncRISELServer, configure_logging


async def main() -> None:
    configure_logging("INFO")
    async with AsyncRISELServer(port=8080) as server:

        async def on_report(device_id: str, payload: dict) -> None:
            print(f"async event from {device_id}: {payload['event_type']}")

        server.hook("on_report", on_report)
        await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
