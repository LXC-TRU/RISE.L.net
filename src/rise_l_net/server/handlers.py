"""Pure request-parsing helpers shared between sync and async server implementations.

Keeping these functions in a separate module means:

* The sync :class:`~rise_l_net.server.server.RISELServer` and the async
  :class:`~rise_l_net.server.async_server.AsyncRISELServer` share identical
  validation and response-building logic.
* The helpers are easy to unit-test in isolation without spinning up an HTTP
  server.

None of the functions here perform I/O or have side effects — they only
translate raw dictionaries into typed model objects and build response dicts.
"""

from __future__ import annotations

from typing import Any

from .._compat import now_unix
from ..exceptions import ValidationError
from ..models import Event, Heartbeat


def parse_heartbeat(payload: dict[str, Any]) -> Heartbeat:
    """Validate and deserialise a raw heartbeat payload.

    Args:
        payload: Raw JSON body from the HTTP request.

    Returns:
        A validated :class:`~rise_l_net.models.Heartbeat` instance.

    Raises:
        ValidationError: If *payload* is not a dict or is missing ``device_id``.
    """
    if not isinstance(payload, dict):
        raise ValidationError("heartbeat payload must be a JSON object")
    # device_id is the only required field; all others have sensible defaults.
    if not payload.get("device_id"):
        raise ValidationError("device_id is required")
    return Heartbeat.from_dict(payload)


def parse_event(payload: dict[str, Any]) -> Event:
    """Validate and deserialise a raw event/report payload.

    Args:
        payload: Raw JSON body from the HTTP request.

    Returns:
        A validated :class:`~rise_l_net.models.Event` instance.

    Raises:
        ValidationError: If *payload* is missing ``device_id`` or ``event_type``.
    """
    if not isinstance(payload, dict):
        raise ValidationError("event payload must be a JSON object")
    if not payload.get("device_id"):
        raise ValidationError("device_id is required")
    # event_type is required so the server can route/filter events meaningfully.
    if not payload.get("event_type"):
        raise ValidationError("event_type is required")
    return Event.from_dict(payload)


def heartbeat_response(*, registered: bool) -> dict[str, Any]:
    """Build the standard heartbeat acknowledgement response.

    Args:
        registered: True if this was the device's first heartbeat (new
                    registration), False if it was a subsequent update.

    Returns:
        A dictionary that will be serialised to JSON and returned to the device.
    """
    return {
        "status": "ok",
        # Let the device know whether it was newly registered so it can log
        # or trigger a "first boot" action.
        "registered": registered,
        # Include the server's current time so the device can detect clock skew.
        "server_time": now_unix(),
    }


def event_response(event_id: int) -> dict[str, Any]:
    """Build the standard event acknowledgement response.

    Args:
        event_id: The auto-assigned integer ID of the persisted event.

    Returns:
        A dictionary that will be serialised to JSON and returned to the device.
    """
    return {
        "status": "ok",
        # Return a string message ID so the device can correlate responses
        # with the events it sent (useful for deduplication).
        "message_id": f"msg_{event_id}",
    }
