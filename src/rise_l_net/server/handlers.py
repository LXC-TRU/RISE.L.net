"""Pure helpers shared between sync and async server implementations.

These functions only translate inbound payloads into model objects and
build response dicts. The storage and event-dispatch side effects live
in the server classes themselves.
"""

from __future__ import annotations

from typing import Any

from .._compat import now_unix
from ..exceptions import ValidationError
from ..models import Event, Heartbeat


def parse_heartbeat(payload: dict[str, Any]) -> Heartbeat:
    if not isinstance(payload, dict):
        raise ValidationError("heartbeat payload must be a JSON object")
    if not payload.get("device_id"):
        raise ValidationError("device_id is required")
    return Heartbeat.from_dict(payload)


def parse_event(payload: dict[str, Any]) -> Event:
    if not isinstance(payload, dict):
        raise ValidationError("event payload must be a JSON object")
    if not payload.get("device_id"):
        raise ValidationError("device_id is required")
    if not payload.get("event_type"):
        raise ValidationError("event_type is required")
    return Event.from_dict(payload)


def heartbeat_response(*, registered: bool) -> dict[str, Any]:
    return {
        "status": "ok",
        "registered": registered,
        "server_time": now_unix(),
    }


def event_response(event_id: int) -> dict[str, Any]:
    return {"status": "ok", "message_id": f"msg_{event_id}"}
