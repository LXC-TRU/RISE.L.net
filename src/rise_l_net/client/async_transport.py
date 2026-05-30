"""Async client transport built on aiohttp."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .._logging import get_logger
from ..exceptions import TransportError

if TYPE_CHECKING:
    import aiohttp

log = get_logger("client.async_transport")


class AsyncTransport:
    """Abstract async transport."""

    async def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        raise NotImplementedError

    async def connect(self) -> None:
        return None

    async def close(self) -> None:
        return None


class AsyncHTTPTransport(AsyncTransport):
    """aiohttp-backed transport with a reusable client session."""

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        try:
            import aiohttp  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "AsyncHTTPTransport requires aiohttp. Install with: pip install rise-l-net[async]"
            ) from exc
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers: dict[str, str] = dict(headers or {})
        self._session: aiohttp.ClientSession | None = None

    async def connect(self) -> None:
        import aiohttp

        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self.headers)

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        import aiohttp

        if not endpoint.startswith("/"):
            raise ValueError("endpoint must start with '/'")
        if self._session is None or self._session.closed:
            await self.connect()
        assert self._session is not None
        url = self.base_url + endpoint
        try:
            async with self._session.post(url, json=data) as resp:
                payload = await resp.read()
                if resp.status < 200 or resp.status >= 300:
                    raise TransportError(f"HTTP {resp.status}: {payload[:200]!r}")
                if not payload:
                    return {}
                try:
                    decoded = json.loads(payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise TransportError(f"invalid response body: {exc}") from exc
                if not isinstance(decoded, dict):
                    raise TransportError("response was not a JSON object")
                return decoded
        except aiohttp.ClientError as exc:
            raise TransportError(f"network error: {exc}") from exc
