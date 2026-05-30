"""Client-side transport layer.

The transport is the only piece that talks to the network. It exists so the
device class can be tested with a fake and so that a future MQTT/WebSocket
transport can drop in without touching anything else.
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from typing import Any

from .._compat import MICROPYTHON
from .._logging import get_logger
from ..exceptions import TransportError

log = get_logger("client.transport")


class Transport(ABC):
    """Abstract transport. Implementations must be reusable across requests."""

    @abstractmethod
    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send `data` to `endpoint` and return the parsed response.

        Raises TransportError on any failure (network, HTTP, decoding).
        """

    def connect(self) -> None:
        """Optional: open persistent resources (sockets, sessions)."""

    def close(self) -> None:
        """Optional: release resources."""


class HTTPTransport(Transport):
    """HTTP/HTTPS POST transport.

    On CPython this uses the stdlib http.client (full HTTPS, chunked, real
    Content-Length parsing). On MicroPython it falls back to a hand-rolled
    socket implementation that reads the response to EOF instead of a fixed
    1024 byte buffer.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.headers: dict[str, str] = dict(headers or {})

    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        if not endpoint.startswith("/"):
            raise ValueError("endpoint must start with '/'")
        url = self.base_url + endpoint
        body = json.dumps(data).encode("utf-8")
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            **self.headers,
        }
        if MICROPYTHON:
            return _send_micropython(url, body, headers, self.timeout)
        return _send_cpython(url, body, headers, self.timeout)


def _send_cpython(url: str, body: bytes, headers: dict[str, str], timeout: float) -> dict[str, Any]:
    import http.client
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise TransportError(f"unsupported scheme: {parsed.scheme!r}")
    host = parsed.hostname or ""
    if not host:
        raise TransportError(f"missing host in url: {url!r}")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        path = f"{path}?{parsed.query}"

    conn_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    conn = conn_cls(host, port, timeout=timeout)
    try:
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            payload = resp.read()
        except OSError as exc:
            raise TransportError(f"network error: {exc}") from exc
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
    finally:
        conn.close()


def _send_micropython(
    url: str, body: bytes, headers: dict[str, str], timeout: float
) -> dict[str, Any]:  # pragma: no cover - exercised on MicroPython only
    import usocket  # type: ignore[import-not-found]

    if url.startswith("https://"):
        rest = url[len("https://") :]
        default_port = 443
        secure = True
    elif url.startswith("http://"):
        rest = url[len("http://") :]
        default_port = 80
        secure = False
    else:
        raise TransportError(f"unsupported scheme in url: {url!r}")

    if "/" in rest:
        host_port, path_part = rest.split("/", 1)
        path = "/" + path_part
    else:
        host_port = rest
        path = "/"
    if ":" in host_port:
        host, port_str = host_port.split(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = default_port

    request = f"POST {path} HTTP/1.1\r\nHost: {host}\r\n".encode()
    for k, v in headers.items():
        request += f"{k}: {v}\r\n".encode()
    request += b"Connection: close\r\n\r\n" + body

    addr = usocket.getaddrinfo(host, port)[0][-1]
    sock = usocket.socket()
    sock.settimeout(timeout)
    try:
        if secure:
            try:
                import ussl  # type: ignore[import-not-found]
            except ImportError as exc:
                raise TransportError("HTTPS requires ussl on MicroPython") from exc
            sock.connect(addr)
            sock = ussl.wrap_socket(sock, server_hostname=host)
        else:
            sock.connect(addr)
        sock.write(request)

        chunks = []
        while True:
            chunk = sock.read(1024)
            if not chunk:
                break
            chunks.append(chunk)
        raw = b"".join(chunks)
    except Exception as exc:
        raise TransportError(f"network error: {exc}") from exc
    finally:
        try:
            sock.close()
        except Exception:
            pass

    try:
        head, _, body_bytes = raw.partition(b"\r\n\r\n")
        status_line = head.split(b"\r\n", 1)[0].decode("ascii", "replace")
        parts = status_line.split(" ", 2)
        if len(parts) < 2:
            raise TransportError(f"invalid HTTP response: {status_line!r}")
        status = int(parts[1])
        if status < 200 or status >= 300:
            raise TransportError(f"HTTP {status}: {body_bytes[:200]!r}")
        if not body_bytes:
            return {}
        decoded = json.loads(body_bytes.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise TransportError("response was not a JSON object")
        return decoded
    except TransportError:
        raise
    except Exception as exc:
        raise TransportError(f"invalid response: {exc}") from exc
