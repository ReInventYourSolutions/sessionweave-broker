from __future__ import annotations

from sessionweave.metrics import METRICS_CONTENT_TYPE, Metrics


def test_increment_commands_total() -> None:
    m = Metrics()
    m.commands_total.labels(dispatch_mode="all").inc()
    metric = next(iter(m.commands_total.collect()))
    samples = list(metric.samples)
    assert any(
        s.value == 1 and s.labels.get("dispatch_mode") == "all"
        for s in samples
    )


def test_render_returns_prometheus_text() -> None:
    m = Metrics()
    m.sessions_active.set(3)
    text = m.render().decode()
    assert "sessionweave_sessions_active" in text
    assert "3" in text


def test_content_type_constant_is_set() -> None:
    assert METRICS_CONTENT_TYPE.startswith("text/plain")


def test_separate_instances_have_isolated_registries() -> None:
    a = Metrics()
    b = Metrics()
    a.sessions_active.set(7)
    b.sessions_active.set(2)
    assert b"7" in a.render()
    assert b"2" in b.render()
