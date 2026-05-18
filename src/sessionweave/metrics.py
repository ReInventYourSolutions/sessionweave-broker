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
        "events_browse_returned_total",
        "events_browse_seconds",
        "events_browse_total",
        "notification_buffer_depth",
        "participants_active",
        "persistence_failures_total",
        "persistence_write_seconds",
        "registry",
        "session_cold_total",
        "session_delete_failed_total",
        "session_ended_total",
        "session_hydrate_seconds",
        "session_hydrate_total",
        "session_persisted_total",
        "session_purge_total",
        "session_stranded_recovered_total",
        "sessions_active",
        "subscriber_dispatch_failed_total",
        "wait_for_event_active",
        "wait_for_event_clamped_total",
        "wait_for_event_duration_seconds",
        "wait_for_event_total",
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
        self.wait_for_event_total = Counter(
            "sessionweave_wait_for_event_total",
            "Outcomes of session_wait_for_event calls",
            ["outcome"],
            registry=self.registry,
        )
        self.wait_for_event_duration_seconds = Histogram(
            "sessionweave_wait_for_event_duration_seconds",
            "Wall-clock duration of session_wait_for_event calls",
            buckets=(0.01, 0.1, 1, 5, 30, 60, 300, 600),
            registry=self.registry,
        )
        self.wait_for_event_active = Gauge(
            "sessionweave_wait_for_event_active",
            "Currently-blocked session_wait_for_event waiters",
            registry=self.registry,
        )
        self.wait_for_event_clamped_total = Counter(
            "sessionweave_wait_for_event_clamped_total",
            "session_wait_for_event calls where the requested timeout was "
            "clamped by the transport-specific cap (MCP) or the global cap (HTTP)",
            ["transport"],
            registry=self.registry,
        )
        self.subscriber_dispatch_failed_total = Counter(
            "sessionweave_subscriber_dispatch_failed_total",
            "Subscriber callbacks that raised during _deliver_or_buffer. "
            "Indicates a slow/broken WS consumer that lost an event; the "
            "event still reached at least one healthy subscriber, otherwise "
            "the buffer fallback path would have run.",
            registry=self.registry,
        )
        self.session_persisted_total = Counter(
            "sessionweave_session_persisted_total",
            "Sessions created with persistence enabled, by mode",
            ["persistence_mode"],
            registry=self.registry,
        )
        self.session_cold_total = Counter(
            "sessionweave_session_cold_total",
            "Hot-to-cold transitions (persisted session evicted from RAM by idle TTL)",
            registry=self.registry,
        )
        self.session_hydrate_total = Counter(
            "sessionweave_session_hydrate_total",
            "Cold-to-hot transitions (persisted session reloaded into RAM)",
            registry=self.registry,
        )
        self.session_hydrate_seconds = Histogram(
            "sessionweave_session_hydrate_seconds",
            "Latency of cold-to-hot hydration",
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5),
            registry=self.registry,
        )
        self.session_purge_total = Counter(
            "sessionweave_session_purge_total",
            "Sessions purged from DB",
            ["reason"],
            registry=self.registry,
        )
        self.session_delete_failed_total = Counter(
            "sessionweave_session_delete_failed_total",
            "session_delete attempts that did not delete",
            ["reason"],
            registry=self.registry,
        )
        self.session_stranded_recovered_total = Counter(
            "sessionweave_session_stranded_recovered_total",
            "Sessions marked ended at startup because they were stranded",
            registry=self.registry,
        )
        self.persistence_write_seconds = Histogram(
            "sessionweave_persistence_write_seconds",
            "Latency of persist_event_atomic (incl. fsync)",
            buckets=(0.0005, 0.001, 0.002, 0.005, 0.01, 0.025, 0.05, 0.1),
            registry=self.registry,
        )
        self.persistence_failures_total = Counter(
            "sessionweave_persistence_failures_total",
            "Persistence operation failures by op",
            ["op"],
            registry=self.registry,
        )
        self.events_browse_total = Counter(
            "sessionweave_events_browse_total",
            "GET /v1/sessions/{id}/events calls, by session_state",
            ["session_state"],
            registry=self.registry,
        )
        self.events_browse_returned_total = Counter(
            "sessionweave_events_browse_returned_total",
            "Total events returned by browse endpoint",
            registry=self.registry,
        )
        self.events_browse_seconds = Histogram(
            "sessionweave_events_browse_seconds",
            "Latency of events browse calls",
            buckets=(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5),
            registry=self.registry,
        )

    def render(self) -> bytes:
        return generate_latest(self.registry)


METRICS_CONTENT_TYPE = CONTENT_TYPE_LATEST
