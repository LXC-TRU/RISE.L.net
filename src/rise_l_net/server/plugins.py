"""Server-side plugins. Plugins react to lifecycle events; they cannot block requests."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib import error as urlerror
from urllib import request as urlrequest

from .._logging import get_logger
from ..models import Device, Event, Heartbeat

if TYPE_CHECKING:
    from .server import RISELServer

log = get_logger("server.plugins")


class Plugin:
    """Base plugin. Override the relevant hooks."""

    def on_load(self, server: RISELServer) -> None:
        pass

    def on_device_registered(self, device: Device, heartbeat: Heartbeat) -> None:
        pass

    def on_heartbeat(self, device: Device, heartbeat: Heartbeat) -> None:
        pass

    def on_report(self, device_id: str, event: Event) -> None:
        pass

    def on_device_online(self, device_id: str) -> None:
        pass

    def on_device_offline(self, device_id: str) -> None:
        pass


class WebhookPlugin(Plugin):
    """Forwards events to a generic webhook (e.g. Slack/Lark/DingTalk)."""

    def __init__(
        self,
        webhook_url: str,
        events: list[str] | None = None,
        timeout: float = 10.0,
    ) -> None:
        if not webhook_url:
            raise ValueError("webhook_url is required")
        self.webhook_url = webhook_url
        self.events = events or ["on_report"]
        self.timeout = timeout

    def _send(self, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode("utf-8")
        req = urlrequest.Request(
            self.webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urlrequest.urlopen(req, timeout=self.timeout) as resp:
                log.info("webhook delivered status=%s", resp.status)
        except urlerror.URLError as exc:
            log.warning("webhook delivery failed: %s", exc)
        except Exception as exc:
            log.warning("webhook delivery error: %s", exc)

    def on_report(self, device_id: str, event: Event) -> None:
        if "on_report" not in self.events:
            return
        self._send(
            {
                "msg_type": "text",
                "content": {
                    "text": (
                        f"Device report: {device_id}\n"
                        f"Type: {event.event_type}\n"
                        f"Severity: {event.severity}\n"
                        f"Data: {event.data}"
                    )
                },
            }
        )

    def on_device_offline(self, device_id: str) -> None:
        if "on_device_offline" not in self.events:
            return
        self._send({"msg_type": "text", "content": {"text": f"Device offline: {device_id}"}})


class AlertPlugin(Plugin):
    """Threshold-based alerting on report payloads.

    Rule format::

        {
            "temperature": {"warning": 30, "critical": 40},
            "violation_rate": {"warning": 5, "critical": 10},
        }
    """

    def __init__(self, threshold_rules: dict[str, dict[str, float]]) -> None:
        self.rules = threshold_rules

    def on_report(self, device_id: str, event: Event) -> None:
        for key, thresholds in self.rules.items():
            if key not in event.data:
                continue
            try:
                value = float(event.data[key])
            except (TypeError, ValueError):
                continue
            critical = thresholds.get("critical")
            warning = thresholds.get("warning")
            if critical is not None and value >= critical:
                log.error("alert critical device=%s %s=%s", device_id, key, value)
            elif warning is not None and value >= warning:
                log.warning("alert warning device=%s %s=%s", device_id, key, value)
