"""Device-side HTTP transport layer.

The transport is the only component that actually touches the network.
Keeping it isolated behind an abstract base class means:

* The device class can be unit-tested with a ``FakeTransport`` that never
  opens a socket.
* Alternative transports (MQTT, WebSocket, CoAP) can be plugged in without
  changing any other code.

CPython path
------------
Uses ``http.client.HTTPConnection`` / ``HTTPSConnection`` from the standard
library.  This gives us proper HTTPS, chunked transfer encoding, and correct
``Content-Length`` handling — none of which the original hand-rolled socket
code supported.

MicroPython path
----------------
``http.client`` is not available on MicroPython, so we fall back to a
hand-rolled ``usocket`` implementation.  The key improvement over the original
code is that we read the response to EOF instead of stopping at 1024 bytes,
which prevented large responses from being parsed correctly.
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
    """Abstract transport base class.

    A transport is responsible for serialising a payload dictionary to bytes,
    sending it to a server endpoint, and returning the parsed response.

    Implementations must be reusable across multiple ``send`` calls.  They
    may optionally maintain a persistent connection (``connect`` / ``close``).
    """

    @abstractmethod
    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """Send *data* to *endpoint* and return the parsed JSON response.

        Args:
            endpoint: Server-relative path, e.g. ``"/api/heartbeat"``.
            data:     Payload to serialise as JSON and POST to the server.

        Returns:
            Parsed JSON response body as a dictionary.

        Raises:
            TransportError: On any network, HTTP, or decoding failure.
        """

    def connect(self) -> None:
        """Open persistent resources (sockets, sessions, connections).

        Called once by the device before the first ``send``.  The default
        implementation is a no-op; override when the transport needs to
        establish a long-lived connection.
        """

    def close(self) -> None:
        """Release resources held by the transport.

        Called by the device on shutdown.  The default implementation is a
        no-op; override when the transport holds open sockets or sessions.
        """


class HTTPTransport(Transport):
    """HTTP/HTTPS POST transport.

    On CPython this uses ``http.client`` from the standard library, which
    supports HTTPS, chunked transfer encoding, and reads the full response
    body regardless of size.

    On MicroPython it falls back to a hand-rolled ``usocket`` implementation
    that reads the response to EOF (fixing the original 1024-byte truncation
    bug) and optionally wraps the socket with ``ussl`` for HTTPS.

    Args:
        base_url: Server base URL including scheme and port,
                  e.g. ``"http://192.168.1.100:8080"``.
        timeout:  Socket / connection timeout in seconds.
        headers:  Extra HTTP headers to include in every request, e.g.
                  ``{"X-API-Key": "secret"}``.
    """

    def __init__(
        self,
        base_url: str,
        timeout: float = 10.0,
        headers: dict[str, str] | None = None,
    ) -> None:
        if not base_url:
            raise ValueError("base_url is required")
        # Strip trailing slash so endpoint paths can always start with "/".
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        # Mutable so that set_api_key() can update the header after construction.
        self.headers: dict[str, str] = dict(headers or {})

    def send(self, endpoint: str, data: dict[str, Any]) -> dict[str, Any]:
        """POST *data* as JSON to ``base_url + endpoint``.

        Dispatches to the CPython or MicroPython implementation based on the
        runtime detected at import time.
        """
        if not endpoint.startswith("/"):
            raise ValueError("endpoint must start with '/'")
        url = self.base_url + endpoint
        # Serialise the payload once; both implementations use the same bytes.
        body = json.dumps(data).encode("utf-8")
        # Build the full header set, merging instance headers with required ones.
        headers = {
            "Content-Type": "application/json",
            "Content-Length": str(len(body)),
            **self.headers,  # instance headers override defaults if they clash
        }
        if MICROPYTHON:
            return _send_micropython(url, body, headers, self.timeout)
        return _send_cpython(url, body, headers, self.timeout)


def _send_cpython(url: str, body: bytes, headers: dict[str, str], timeout: float) -> dict[str, Any]:
    """CPython implementation using the stdlib ``http.client`` module.

    Opens a new connection for each request (``Connection: close`` semantics).
    For high-throughput use cases, consider subclassing ``HTTPTransport`` and
    keeping the connection alive across requests.
    """
    import http.client
    from urllib.parse import urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https"):
        raise TransportError(f"unsupported scheme: {parsed.scheme!r}")
    host = parsed.hostname or ""
    if not host:
        raise TransportError(f"missing host in url: {url!r}")
    # Use the explicit port from the URL, or the scheme default.
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    path = parsed.path or "/"
    if parsed.query:
        # Preserve query string if present (unusual for this API but correct).
        path = f"{path}?{parsed.query}"

    # Choose the right connection class based on the scheme.
    conn_cls = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    conn = conn_cls(host, port, timeout=timeout)
    try:
        try:
            conn.request("POST", path, body=body, headers=headers)
            resp = conn.getresponse()
            # Read the full response body before checking the status code so
            # that the connection is left in a clean state.
            payload = resp.read()
        except OSError as exc:
            raise TransportError(f"network error: {exc}") from exc

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
    finally:
        # Always close the connection to release the socket, even on error.
        conn.close()


def _send_micropython(
    url: str, body: bytes, headers: dict[str, str], timeout: float
) -> dict[str, Any]:  # pragma: no cover — exercised on MicroPython only
    """MicroPython implementation using raw ``usocket``.

    Key improvements over the original code:
    * Reads the response to EOF instead of stopping at 1024 bytes.
    * Parses the HTTP status line properly instead of doing a string search.
    * Supports HTTPS via ``ussl.wrap_socket`` when the scheme is ``https://``.
    """
    import usocket  # type: ignore[import-not-found]

    # Parse the URL manually because MicroPython lacks urllib.parse.
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

    # Split host:port from the path component.
    if "/" in rest:
        host_port, path_part = rest.split("/", 1)
        path = "/" + path_part
    else:
        host_port = rest
        path = "/"

    # Extract the port number if explicitly specified.
    if ":" in host_port:
        host, port_str = host_port.split(":", 1)
        port = int(port_str)
    else:
        host = host_port
        port = default_port

    # Build the raw HTTP/1.1 request bytes.
    request = f"POST {path} HTTP/1.1\r\nHost: {host}\r\n".encode()
    for k, v in headers.items():
        request += f"{k}: {v}\r\n".encode()
    # Connection: close tells the server to close after the response so we
    # know when to stop reading.
    request += b"Connection: close\r\n\r\n" + body

    # Resolve the hostname to an IP address.
    addr = usocket.getaddrinfo(host, port)[0][-1]
    sock = usocket.socket()
    sock.settimeout(timeout)
    try:
        if secure:
            # Wrap the socket with TLS for HTTPS.
            try:
                import ussl  # type: ignore[import-not-found]
            except ImportError as exc:
                raise TransportError("HTTPS requires ussl on MicroPython") from exc
            sock.connect(addr)
            sock = ussl.wrap_socket(sock, server_hostname=host)
        else:
            sock.connect(addr)

        sock.write(request)

        # Read the full response into memory.  The server closes the connection
        # after sending the response (Connection: close), so we read until EOF.
        chunks = []
        while True:
            chunk = sock.read(1024)
            if not chunk:
                break  # EOF — server closed the connection
            chunks.append(chunk)
        raw = b"".join(chunks)
    except Exception as exc:
        raise TransportError(f"network error: {exc}") from exc
    finally:
        # Always close the socket to free the file descriptor.
        try:
            sock.close()
        except Exception:
            pass  # ignore errors during cleanup

    # Parse the HTTP response: split headers from body on the blank line.
    try:
        head, _, body_bytes = raw.partition(b"\r\n\r\n")
        # The first line of the response is the status line, e.g. "HTTP/1.1 200 OK".
        status_line = head.split(b"\r\n", 1)[0].decode("ascii", "replace")
        parts = status_line.split(" ", 2)
        if len(parts) < 2:
            raise TransportError(f"invalid HTTP response: {status_line!r}")
        status = int(parts[1])

        if status < 200 or status >= 300:
            raise TransportError(f"HTTP {status}: {body_bytes[:200]!r}")

        if not body_bytes:
            return {}

        # Parse the JSON response body.
        decoded = json.loads(body_bytes.decode("utf-8"))
        if not isinstance(decoded, dict):
            raise TransportError("response was not a JSON object")
        return decoded
    except TransportError:
        raise  # re-raise our own errors unchanged
    except Exception as exc:
        raise TransportError(f"invalid response: {exc}") from exc
