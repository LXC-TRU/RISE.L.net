"""Exception hierarchy for the rise-l-net library."""

from __future__ import annotations


class RISELError(Exception):
    """Base class for all library errors."""


class TransportError(RISELError):
    """Network or protocol failure when sending or receiving data."""


class AuthError(RISELError):
    """Authentication or authorization failure."""


class StorageError(RISELError):
    """Persistence layer failure."""


class ValidationError(RISELError):
    """Inbound payload failed validation."""


class RateLimitedError(RISELError):
    """Rate limit exceeded."""


class ConfigError(RISELError):
    """Invalid configuration."""
