from prometheus_client import CollectorRegistry, generate_latest

from sessionweave.metrics import Metrics


def _render(registry: CollectorRegistry) -> str:
    return generate_latest(registry).decode("utf-8")


def test_persistence_counters_registered() -> None:
    reg = CollectorRegistry()
    m = Metrics(registry=reg)
    m.session_persisted_total.labels(persistence_mode="ttl").inc()
    m.session_cold_total.inc()
    m.session_hydrate_total.inc()
    m.session_purge_total.labels(reason="host_deleted").inc()
    m.session_delete_failed_total.labels(reason="invalid_token").inc()
    m.session_stranded_recovered_total.inc()
    m.persistence_failures_total.labels(op="save_event").inc()
    m.events_browse_total.labels(session_state="cold").inc()
    m.events_browse_returned_total.inc(42)

    text = _render(reg)
    assert "sessionweave_session_persisted_total" in text
    assert 'persistence_mode="ttl"' in text
    assert "sessionweave_session_cold_total" in text
    assert "sessionweave_session_hydrate_total" in text
    assert 'reason="host_deleted"' in text
    assert 'reason="invalid_token"' in text
    assert "sessionweave_session_stranded_recovered_total" in text
    assert 'op="save_event"' in text
    assert 'session_state="cold"' in text
    assert "sessionweave_events_browse_returned_total" in text


def test_persistence_histograms_registered() -> None:
    reg = CollectorRegistry()
    m = Metrics(registry=reg)
    m.session_hydrate_seconds.observe(0.05)
    m.persistence_write_seconds.observe(0.001)
    m.events_browse_seconds.observe(0.010)

    text = _render(reg)
    assert "sessionweave_session_hydrate_seconds" in text
    assert "sessionweave_persistence_write_seconds" in text
    assert "sessionweave_events_browse_seconds" in text
