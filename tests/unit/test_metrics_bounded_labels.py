"""Guard test — every metric exposed by ``Metrics`` must use bounded labels.

Issue #56 raised the concern that Prometheus metrics could regress to
high-cardinality labels (``session_id``, ``participant_id``, ``command_id``
— all UUIDs) and silently DoS the scraper. This test enumerates every
collector and asserts:

  1. The label NAMES are from a hard-coded allowlist of bounded enums.
  2. No metric introduces a label whose values could grow unboundedly.

If a future PR adds a new metric or a new label, this test fails by
default. The author then either (a) adds the new label to
``ALLOWED_LABELS`` after verifying its cardinality is bounded, or (b)
restructures the metric to avoid the high-cardinality label.

The check operates against the live ``Metrics()`` instance — it reads
``prometheus_client.Collector._labelnames`` so it's transport-agnostic
across prometheus_client versions.
"""

from __future__ import annotations

from sessionweave.metrics import Metrics

# Maps metric NAME → set of allowed label NAMES. Empty set means the
# metric must NOT have any labels.
#
# Every label name here is documented to take values from a small,
# bounded enum (no UUIDs, no free-form strings). Auditing rationale per
# label:
#
#   online       : "true" | "false"
#   dispatch_mode: DispatchMode enum (3 values)
#   reason       : enum from session.py (≤ 5 values; depends on metric)
#   outcome      : wait_for_event outcomes (event | timeout | error |
#                  evicted | cancelled)
#   transport    : "mcp" | "http"
#
# To add a metric or label, EXPAND this dict deliberately AND comment
# the cardinality bound. Anything that takes a participant_id /
# session_id / command_id as a label should be reshaped to a counter
# without that label, or to a separate per-session-emitted log line.
ALLOWED_LABELS: dict[str, set[str]] = {
    "sessionweave_sessions_active": set(),
    "sessionweave_participants_active": {"online"},
    "sessionweave_commands_total": {"dispatch_mode"},
    "sessionweave_command_completed_total": {"reason"},
    "sessionweave_command_cancelled_total": {"reason"},
    "sessionweave_session_ended_total": {"reason"},
    "sessionweave_notification_buffer_depth": set(),
    "sessionweave_wait_for_event_total": {"outcome"},
    "sessionweave_wait_for_event_duration_seconds": set(),
    "sessionweave_wait_for_event_active": set(),
    "sessionweave_wait_for_event_clamped_total": {"transport"},
    "sessionweave_subscriber_dispatch_failed_total": set(),
}

# Hard-banned label NAMES. If any metric tries to use one of these the
# test fails immediately — they're the canonical UUID / unbounded
# cardinality cases that justified opening #56.
BANNED_LABELS: set[str] = {
    "session_id",
    "participant_id",
    "command_id",
    "responder_id",
    "sender_id",
    "tracker_id",
    "dispatch_target",
    "target",
    "client_ip",
    "user_agent",
}


def _iter_collectors(metrics: Metrics):
    """Yield (exposed_name, labelnames_tuple) for every collector on
    the ``Metrics`` instance, using the EXTERNALLY-visible metric name
    (counters get the ``_total`` suffix appended by prometheus_client
    on exposition; internally the ``_name`` attribute is stored without
    it, so we re-add it here).
    """
    for attr in Metrics.__slots__:
        if attr == "registry":
            continue
        collector = getattr(metrics, attr)
        name = getattr(collector, "_name", None)
        type_ = getattr(collector, "_type", None)
        labelnames = getattr(collector, "_labelnames", ())
        if name is None:
            continue
        # Counters expose ``<name>_total`` in /metrics output; the
        # internal ``_name`` strips that suffix. Other collector types
        # don't get a suffix.
        if type_ == "counter":
            name = f"{name}_total"
        yield name, tuple(labelnames)


def test_every_metric_is_in_allowlist() -> None:
    metrics = Metrics()
    seen: set[str] = set()
    for name, _labels in _iter_collectors(metrics):
        assert name in ALLOWED_LABELS, (
            f"unexpected metric {name!r} — add it to ALLOWED_LABELS in "
            f"{__file__} with a cardinality justification, or remove "
            "the metric. Unbounded labels are a Prometheus anti-pattern."
        )
        seen.add(name)
    # Every entry in the allowlist must correspond to a real metric.
    # Catches drift where a metric is removed but the allowlist
    # still lists it.
    extra = set(ALLOWED_LABELS) - seen
    assert not extra, f"stale entries in ALLOWED_LABELS: {extra}"


def test_no_metric_uses_a_banned_label() -> None:
    metrics = Metrics()
    violations: list[tuple[str, str]] = []
    for name, labels in _iter_collectors(metrics):
        for label in labels:
            if label in BANNED_LABELS:
                violations.append((name, label))
    assert not violations, (
        "metrics use banned high-cardinality labels (UUIDs / unbounded "
        "values would explode Prometheus scraper memory): "
        f"{violations}. Either remove the label or restructure the "
        "metric — see #56 for the rationale."
    )


def test_every_metric_label_matches_allowlist() -> None:
    """A new label on an existing metric still trips the guard. Catches
    e.g. someone adding ``participant_id`` to ``commands_total`` thinking
    it's bounded because the metric name already passed the basic check.
    """
    metrics = Metrics()
    mismatches: list[tuple[str, set[str], set[str]]] = []
    for name, labels in _iter_collectors(metrics):
        actual = set(labels)
        expected = ALLOWED_LABELS.get(name, set())
        if actual != expected:
            mismatches.append((name, expected, actual))
    assert not mismatches, (
        "metric label set diverges from ALLOWED_LABELS — update both "
        "in lockstep with a cardinality justification:\n"
        + "\n".join(
            f"  {name}: expected={expected!r} actual={actual!r}"
            for name, expected, actual in mismatches
        )
    )
