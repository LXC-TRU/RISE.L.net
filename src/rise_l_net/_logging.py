"""Logging helpers. All library logs go through these loggers."""

from __future__ import annotations

import logging

_ROOT = "rise_l_net"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a namespaced logger. Use module __name__ as `name`."""
    if name is None or name == _ROOT:
        return logging.getLogger(_ROOT)
    if name.startswith(_ROOT + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT}.{name}")


def configure(level: int | str = logging.INFO) -> None:
    """Convenience: attach a stderr handler to the root library logger.

    Applications that already configure logging should not call this.
    """
    logger = logging.getLogger(_ROOT)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(level)
