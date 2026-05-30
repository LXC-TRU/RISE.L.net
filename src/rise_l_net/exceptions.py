"""Exception hierarchy for the rise-l-net library.

All library-specific exceptions inherit from :class:`RISELError` so that
callers can catch the entire family with a single ``except RISELError`` clause,
or catch individual sub-types for finer-grained error handling.

Example::

    from rise_l_net.exceptions import TransportError, AuthError

    try:
        device.report("temperature", {"value": 23.5})
    except TransportError as exc:
        # Network-level failure — safe to retry later.
        cache.store(exc)
    except AuthError:
        # Bad API key — retrying won't help; alert the operator.
        alert("invalid credentials")
"""

from __future__ import annotations


class RISELError(Exception):
    """Base class for all rise-l-net library errors.

    Catching this type will catch every error raised by the library.
    """


class TransportError(RISELError):
    """Raised when a network or protocol failure occurs during send/receive.

    Examples: connection refused, DNS failure, HTTP 5xx, response decode error.
    These errors are generally transient and safe to retry.
    """


class AuthError(RISELError):
    """Raised when authentication or authorisation fails.

    Examples: missing API key, wrong API key, expired token.
    Retrying without fixing the credentials will not help.
    """


class StorageError(RISELError):
    """Raised when the persistence layer encounters an unrecoverable error.

    Examples: SQLite constraint violation, disk full, corrupted database file.
    """


class ValidationError(RISELError):
    """Raised when an inbound payload fails schema or field validation.

    Examples: missing ``device_id`` field, wrong data type, value out of range.
    The server returns HTTP 400 for these errors.
    """


class RateLimitedError(RISELError):
    """Raised when a rate limit is exceeded.

    The caller should back off and retry after a delay.
    """


class ConfigError(RISELError):
    """Raised when the library is configured with invalid or incompatible options.

    Examples: empty ``server_url``, negative ``heartbeat_interval``.
    These errors are programming mistakes and should not be caught in production.
    """
