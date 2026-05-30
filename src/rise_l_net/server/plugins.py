"""Server-side plugins.

Plugins react to device lifecycle events (registration, heartbeat, report,
online/offline transitions) but cannot block or modify requests — that is the
job of middleware.

The plugin system is designed for side-effect integrations such as:

* Forwarding events to external webhooks (Slack, Lark, DingTalk).
* Sending alerts when sensor values exceed thresholds.
* Writing events to a time-series database.
* Sending push notifications on device offline events.

Example::

    server = RISELServer(port=8080)
    server.plugin(WebhookPlugin("https://hooks.slack.com/..."))
    server.plugin(AlertPlugin({"temperature": {"critical": 40}}))
    server.start()
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any
from urllib import error as urlerror
from urllib import request as urlrequest

from .._logging import get_logger
from ..models import Device, Event, Heartbeat

if TYPE_CHECKING:
    # Avoid a circular import at runtime; only needed for type annotations.
    from .server import RISELServer

log = get_logger("server.plugins")


class Plugin:
    """Base plugin class.  All hooks are no-ops by default.

    Subclass this and override only the hooks you need.  Exceptions raised
    inside plugin hooks are caught by the server and logged; they do not
    propagate to the device or crash the server.
    """

    def on_load(self, server: RISELServer) -> None:
        """Called once when the plugin is registered with the server.

        Use this hook to perform one-time setup, e.g. opening a database
        connection or validating configuration.

        Args:
            server: The :class:`~rise_l_net.server.server.RISELServer` instance
                    that the plugin was registered with.
        """

    def on_device_registered(self, device: Device, heartbeat: Heartbeat) -> None:
        """Called when a device sends its first heartbeat (new registration).

        Args:
            device:    The newly created device record.
            heartbeat: The heartbeat payload that triggered the registration.
        """

    def on_heartbeat(self, device: Device, heartbeat: Heartbeat) -> None:
        """Called on every heartbeat, including the initial registration.

        Args:
            device:    The updated device record.
            heartbeat: The heartbeat payload.
        """

    def on_report(self, device_id: str, event: Event) -> None:
        """Called when a device sends a telemetry event.

        Args:
            device_id: ID of the reporting device.
            event:     The persisted event.
        """

    def on_device_online(self, device_id: str) -> None:
        """Called when a previously offline device comes back online.

        Not called on the initial registration — use ``on_device_registered``
        for that.

        Args:
            device_id: ID of the device that came online.
        """

    def on_device_offline(self, device_id: str) -> None:
        """Called when a device is marked offline by the timeout checker.

        Args:
            device_id: ID of the device that went offline.
        """


class WebhookPlugin(Plugin):
    """Forward selected events to a generic HTTP webhook endpoint.

    Compatible with Slack incoming webhooks, Lark/Feishu bots, DingTalk
    robots, and any other service that accepts a JSON POST.

    Uses the stdlib ``urllib`` so that no extra dependencies are required.
    For async servers, consider subclassing and using ``aiohttp`` instead.

    Args:
        webhook_url: Full URL of the webhook endpoint.
        events:      List of event names to forward.  Defaults to
                     ``["on_report"]``.  Valid values: ``"on_report"``,
                     ``"on_device_offline"``, ``"on_device_registered"``,
                     ``"on_device_online"``.
        timeout:     HTTP request timeout in seconds.
    """

    def __init__(
        self,
        webhook_url: str,
        events: list[str] | None = None,
        timeout: float = 10.0,
    ) -> None:
        if not webhook_url:
            raise ValueError("webhook_url is required")
        self.webhook_url = webhook_url
        # Default to forwarding only report events to avoid webhook spam.
        self.events = events or ["on_report"]
        self.timeout = timeout

    def _send(self, payload: dict[str, Any]) -> None:
        """POST *payload* as JSON to the webhook URL.

        Errors are logged at WARNING level and swallowed so that a webhook
        failure does not affect the device or the server's response.
        """
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
            # Network-level failure (DNS, connection refused, timeout).
            log.warning("webhook delivery failed: %s", exc)
        except Exception as exc:
            # Catch-all for unexpected errors (e.g. SSL certificate issues).
            log.warning("webhook delivery error: %s", exc)

    def on_report(self, device_id: str, event: Event) -> None:
        """Forward report events to the webhook if ``"on_report"`` is enabled."""
        if "on_report" not in self.events:
            return
        # Format the message in a way that most webhook bots can display.
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
        """Forward device-offline events to the webhook if enabled."""
        if "on_device_offline" not in self.events:
            return
        self._send({"msg_type": "text", "content": {"text": f"Device offline: {device_id}"}})


class AlertPlugin(Plugin):
    """Emit log alerts when report payload values exceed configured thresholds.

    Threshold rules are defined as a dictionary mapping field names to
    ``{"warning": <value>, "critical": <value>}`` dicts.  Both keys are
    optional — you can define only a warning threshold, only a critical one,
    or both.

    Example::

        AlertPlugin({
            "temperature": {"warning": 30, "critical": 40},
            "error_rate":  {"critical": 0.05},
        })

    When a report arrives, the plugin checks each configured field in
    ``event.data``.  If the value meets or exceeds the critical threshold,
    an ERROR log is emitted.  If it meets or exceeds the warning threshold
    (but not critical), a WARNING log is emitted.

    Args:
        threshold_rules: Mapping of field name → threshold dict.
    """

    def __init__(self, threshold_rules: dict[str, dict[str, float]]) -> None:
        self.rules = threshold_rules

    def on_report(self, device_id: str, event: Event) -> None:
        """Check each configured field in the event data against thresholds."""
        for key, thresholds in self.rules.items():
            # Skip fields that are not present in this event's data.
            if key not in event.data:
                continue
            # Coerce the value to float for comparison; skip non-numeric values.
            try:
                value = float(event.data[key])
            except (TypeError, ValueError):
                continue

            critical = thresholds.get("critical")
            warning = thresholds.get("warning")

            # Check critical first so we don't emit a warning for a critical value.
            if critical is not None and value >= critical:
                log.error("alert critical device=%s %s=%s", device_id, key, value)
            elif warning is not None and value >= warning:
                log.warning("alert warning device=%s %s=%s", device_id, key, value)
