"""Async server example.

    python examples/async_server.py
"""

from __future__ import annotations

import asyncio
from typing import Any

from rise_l_net.server.async_server import AsyncRISELServer

from rise_l_net import configure_logging


async def main() -> None:
    configure_logging("INFO")
    async with AsyncRISELServer(port=8080) as server:

        async def on_report(device_id: str, payload: dict[str, Any]) -> None:
            print(f"async event from {device_id}: {payload['event_type']}")

        server.hook("on_report", on_report)
        await server.wait_closed()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
