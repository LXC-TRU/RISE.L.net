"""Async client transport built on aiohttp.

This module provides :class:`AsyncHTTPTransport`, the async counterpart of
:class:`~rise_l_net.client.transport.HTTPTransport`.  It uses an
``aiohttp.ClientSession`` that is created once in :meth:`connect` and reused
across all subsequent ``send`` calls, which avoids the overhead of opening a
new TCP connection for every request.

The session is closed in :meth:`close`, which is called by
:class:`~rise_l_net.client.async_device.AsyncRISELDevice` on shutdown.

Requires the ``aiohttp`` package::

    pip install "rise-l-net[async]"
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from .._logging import get_logger
from ..exceptions import TransportError

if TYPE_CHECKING:
    # Only imported for type checking; the runtime import is deferred to the
    # constructor so that the module can be imported without aiohttp installed.
    import aiohttp

log = get_logger("client.async_transport")


class AsyncTransport:
    """Abstract async transport base class.

    Mirrors the sync :class:`~rise_l_net.client.transport.Transport` interface
    but with ``async`` methods so it can be used inside ``asyncio`` coroutines.
    """

    async def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send *data* to *endpoint* and return the parsed JSON response.

        Raises:
            TransportError: On any network, HTTP, or decoding failure.
        """
        raise NotImplementedError

    async def connect(self) -> None:
        """Open persistent resources (e.g. an aiohttp ClientSession).

        Called once before the first ``send``.  Default is a no-op.
        """
        return None

    async def close(self) -> None:
        """Release resources held by the transport.

        Called on device shutdown.  Default is a no-op.
        """
        return None


class AsyncHTTPTransport(AsyncTransport):
    """Async HTTP/HTTPS POST transport backed by ``aiohttp``.

    A single ``aiohttp.ClientSession`` is created in :meth:`connect` and
    reused for all requests.  This enables HTTP keep-alive and avoids the
    overhead of a new TCP handshake per request.

    Args:
        base_url: Server base URL including scheme and port,
                  e.g. ``"http://192.168.1.100:8080"``.
        timeout:  Total request timeout in seconds (passed to
                  ``aiohttp.ClientTimeout``).
        headers:  Extra HTTP headers included in every request.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        # Fail fast at construction time if aiohttp is not installed, rather
        # than raising an ImportError the first time send() is called.
        try:
            import aiohttp  # noqa: F401
        except ImportError as exc:
            raise ImportError(
                "AsyncHTTPTransport requires aiohttp. Install with: pip install rise-l-net[async]"
            ) from exc
        if not base_url:
            raise ValueError("base_url is required")
        # Strip trailing slash so endpoint paths can always start with "/".
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Mutable so that the API key can be updated after construction.
        self.headers: dict[str, str] = dict(headers or {})
        # The session is None until connect() is called.
        self._session: aiohttp.ClientSession | None = None

    async def connect(self) -> None:
        """Create the aiohttp ClientSession.

        Idempotent: if the session already exists and is open, this is a no-op.
        """
        import aiohttp

        # Re-create the session if it was closed (e.g. after a previous stop()).
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            # Pass headers at session level so they are included in every request.
            self._session = aiohttp.ClientSession(timeout=timeout, headers=self.headers)

    async def close(self) -> None:
        """Close the aiohttp ClientSession and release the underlying connections."""
        if self._session is not None and not self._session.closed:
            await self._session.close()
        self._session = None

    async def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST *data* as JSON to ``base_url + endpoint``.

        Lazily calls :meth:`connect` if the session is not yet open, so the
        transport works even if the caller forgets to call ``connect`` first.

        Args:
            endpoint: Server-relative path, e.g. ``"/api/heartbeat"``.
            data:     Payload to serialise as JSON and POST.

        Returns:
            Parsed JSON response body as a dictionary.

        Raises:
            TransportError: On network error, non-2xx HTTP status, or JSON
                            decode failure.
        """
        import aiohttp

        if not endpoint.startswith("/"):
            raise ValueError("endpoint must start with '/'")

        # Ensure the session is open before sending.
        if self._session is None or self._session.closed:
            await self.connect()
        assert self._session is not None  # guaranteed by connect()

        url = self.base_url + endpoint
        try:
            async with self._session.post(url, json=data) as resp:
                # Read the full response body before checking the status so
                # that the connection is left in a clean state.
                payload = await resp.read()

                # Treat any non-2xx status as a transport-level failure.
                if resp.status < 200 or resp.status >= 300:
                    raise TransportError(f"HTTP {resp.status}: {payload[:200]!r}")

                if not payload:
                    return {}  # empty body is valid (e.g. 204 No Content)

                # Decode and parse the JSON response body.
                try:
                    decoded = json.loads(payload.decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    raise TransportError(f"invalid response body: {exc}") from exc

                if not isinstance(decoded, dict):
                    raise TransportError("response was not a JSON object")
                return decoded
        except aiohttp.ClientError as exc:
            # Wrap aiohttp-specific errors in our own TransportError so callers
            # don't need to import aiohttp to handle network failures.
            raise TransportError(f"network error: {exc}") from exc
