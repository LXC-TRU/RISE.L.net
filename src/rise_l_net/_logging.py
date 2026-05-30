"""Logging helpers shared across the entire rise-l-net library.

All library-internal log records are emitted through loggers that live under
the ``rise_l_net`` namespace.  Application code can therefore silence or
redirect library logs by configuring the ``rise_l_net`` logger without
touching the root logger.

Typical application setup::

    import logging
    logging.getLogger("rise_l_net").setLevel(logging.WARNING)

Or use the convenience helper::

    from rise_l_net import configure_logging
    configure_logging("INFO")
"""

from __future__ import annotations

import logging

# Root logger name for the entire library.  All child loggers are created as
# ``rise_l_net.<submodule>`` so they inherit this logger's level and handlers.
_ROOT = "rise_l_net"


def get_logger(name: str | None = None) -> logging.Logger:
    """Return a namespaced ``logging.Logger`` for internal library use.

    Args:
        name: Submodule name, e.g. ``"server.storage"``.  If *name* already
              starts with ``"rise_l_net."`` it is used as-is.  Pass ``None``
              to get the root library logger.

    Returns:
        A ``logging.Logger`` whose name is ``"rise_l_net"`` or
        ``"rise_l_net.<name>"``.
    """
    if name is None or name == _ROOT:
        return logging.getLogger(_ROOT)
    # Avoid double-prefixing if the caller already includes the root namespace.
    if name.startswith(_ROOT + "."):
        return logging.getLogger(name)
    return logging.getLogger(f"{_ROOT}.{name}")


def configure(level: int | str = logging.INFO) -> None:
    """Attach a stderr ``StreamHandler`` to the root library logger.

    This is a convenience function for scripts and examples that do not
    configure the standard ``logging`` module themselves.  Applications that
    already call ``logging.basicConfig`` or set up their own handlers should
    **not** call this function — doing so would add a duplicate handler.

    Args:
        level: Logging level for the library logger, e.g. ``logging.DEBUG``
               or the string ``"DEBUG"``.  Defaults to ``INFO``.
    """
    logger = logging.getLogger(_ROOT)
    # Only add a handler if none exist yet, to avoid duplicate log lines when
    # the function is called more than once.
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(
            # Include timestamp, logger name, and level for easy filtering.
            logging.Formatter("%(asctime)s [%(name)s] %(levelname)s: %(message)s")
        )
        logger.addHandler(handler)
    logger.setLevel(level)
