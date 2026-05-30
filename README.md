# rise-l-net

Lightweight IoT device management toolkit. Pair a small MicroPython client with a Python server and you have heartbeats, telemetry, and device lifecycle events on tap. Pure stdlib by default; aiohttp/aiosqlite are opt-in for the async track.

- **Homepage**: <https://lyuxc.cn/risel>
- **Source**: <https://github.com/LXC-TRU/RISE.L.net>

- **Two-way library**: ships both a device-side client and a server-side manager.
- **Sync and async**: `RISELDevice`/`RISELServer` for stdlib, `AsyncRISELDevice`/`AsyncRISELServer` for asyncio.
- **MicroPython-friendly**: the sync client runs on ESP32-class boards (no extra deps).
- **Pluggable**: middleware, plugins, custom routes, swappable storage and transport.

## Install

```bash
pip install rise-l-net             # client + sync server (stdlib only)
pip install "rise-l-net[async]"    # adds aiohttp + aiosqlite
pip install "rise-l-net[dev]"      # for contributors
```

> macOS / zsh 用户:`[...]` 是 zsh 的 glob 字符,所以带 extras 的命令必须加引号(或单引号)。bash 不需要。

Python 3.10+ on the server side. The sync client is also designed to import on MicroPython.

## Quick start

### Server (sync)

```python
from rise_l_net import RISELServer

server = RISELServer(port=8080)
server.on_report(lambda device_id, payload: print(device_id, payload["event_type"]))
server.start()
```

### Server (async)

```python
import asyncio
from rise_l_net import AsyncRISELServer

async def main() -> None:
    async with AsyncRISELServer(port=8080) as server:
        await server.wait_closed()

asyncio.run(main())
```

### Client (sync, runs on MicroPython too)

```python
from rise_l_net import RISELDevice

device = RISELDevice(
    "http://server.local:8080",
    wifi_ssid="MyWiFi",
    wifi_password="secret",
)
device.start()
device.report("temperature", {"value": 23.5})
```

### Client (async)

```python
import asyncio
from rise_l_net import AsyncRISELDevice

async def main() -> None:
    async with AsyncRISELDevice("http://server.local:8080") as device:
        await device.report("ping", {"v": 1})
        await device.wait_closed()

asyncio.run(main())
```

## Extending

Both client and server expose three extension points: middleware, plugins (server only), and a swappable storage/transport layer.

```python
from rise_l_net import RISELServer
from rise_l_net.server import AuthMiddleware, RateLimitMiddleware, AlertPlugin

server = (
    RISELServer(port=8080)
    .use(AuthMiddleware(api_key="secret"))
    .use(RateLimitMiddleware(max_requests_per_minute=120))
    .plugin(AlertPlugin({"temperature": {"warning": 30, "critical": 40}}))
)
server.start()
```

```python
from rise_l_net import RISELDevice
from rise_l_net.client import RetryMiddleware, ThrottleMiddleware, CacheMiddleware

device = (
    RISELDevice("http://server:8080")
    .use(RetryMiddleware(max_retries=5))
    .use(ThrottleMiddleware(max_requests_per_second=2))
    .use(CacheMiddleware(cache_path="/data/cache.json"))
)
device.start()
```

## API surface

| Endpoint | Direction | Body |
| --- | --- | --- |
| `POST /api/heartbeat` | client → server | `{device_id, ip, uptime, version, metadata}` |
| `POST /api/report` | client → server | `{device_id, event_type, data, severity, timestamp}` |
| `POST /<custom>` | client → server | Anything you handle in `server.route(...)` |

Authentication is opt-in via `AuthMiddleware`. The middleware uses a constant-time comparison and the header lookup is case-insensitive.

## Logging

Library logs go through the `rise_l_net` logger. To enable a default stderr handler:

```python
from rise_l_net import configure_logging
configure_logging("INFO")
```

If your application already configures `logging`, do not call `configure_logging` and the library will respect your handlers.

## Development

```bash
git clone https://github.com/LXC-TRU/RISE.L.net.git
cd RISE.L.net
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
ruff check src tests
mypy src
pytest
```

## License

MIT — see [LICENSE](LICENSE).
