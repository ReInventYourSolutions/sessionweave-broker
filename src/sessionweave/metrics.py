from __future__ import annotations

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)


class Metrics:
    """Prometheus collectors for the relay (spec §5)."""

    __slots__ = (
        "command_cancelled_total",
        "command_completed_total",
        "commands_total",
        "notification_buffer_depth",
        "participants_active",
        "registry",
        "session_ended_total",
        "sessions_active",
    )

    def __init__(self, registry: CollectorRegistry | None = None) -> None:
        self.registry = registry if registry is not None else CollectorRegistry()
        self.sessions_active = Gauge(
            "sessionweave_sessions_active",
            "Number of active sessions",
            registry=self.registry,
        )
        self.participants_active = Gauge(
            "sessionweave_participants_active",
            "Number of participants",
            ["online"],
            registry=self.registry,
        )
        self.commands_total = Counter(
            "sessionweave_commands_total",
            "Commands sent",
            ["dispatch_mode"],
            registry=self.registry,
        )
        self.command_completed_total = Counter(
            "sessionweave_command_completed_total",
            "Commands completed",
            ["reason"],
            registry=self.registry,
        )
        self.command_cancelled_total = Counter(
            "sessionweave_command_cancelled_total",
            "Commands cancelled",
            ["reason"],
            registry=self.registry,
        )
        self.session_ended_total = Counter(
            "sessionweave_session_ended_total",
            "Sessions ended",
            ["reason"],
            registry=self.registry,
        )
        self.notification_buffer_depth = Histogram(
            "sessionweave_notification_buffer_depth",
            "Per-participant notification buffer depth at observation",
            buckets=(0, 1, 5, 10, 50, 100, 500, 1000, 5000),
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


METRICS_CONTENT_TYPE = CONTENT_TYPE_LATEST
