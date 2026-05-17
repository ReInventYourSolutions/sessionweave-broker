"""Unit tests for the supervisor notes domain."""
from __future__ import annotations

import time

import pytest

from sessionweave.supervisor import (
    NoteRateLimiter,
    SupervisorNote,
    SupervisorRequestError,
    parse_supervisor_request,
)


def test_parse_minimal_note():
    note = parse_supervisor_request(
        {"kind": "note", "target": "all", "message": "hello"}
    )
    assert isinstance(note, SupervisorNote)
    assert note.kind == "note"
    assert note.target == "all"
    assert note.message == "hello"


def test_parse_halt_with_participant_target():
    note = parse_supervisor_request(
        {"kind": "halt", "target": "p-abc123"}
    )
    assert note.kind == "halt"
    assert note.target == "p-abc123"
    assert note.message is None


def test_parse_invalid_kind_rejected():
    with pytest.raises(SupervisorRequestError) as exc:
        parse_supervisor_request({"kind": "delete", "target": "all"})
    assert "kind" in str(exc.value)


def test_parse_missing_target_rejected():
    with pytest.raises(SupervisorRequestError):
        parse_supervisor_request({"kind": "note", "message": "hi"})


def test_parse_message_too_large_rejected():
    big = "x" * 3000
    with pytest.raises(SupervisorRequestError):
        parse_supervisor_request({"kind": "note", "target": "all", "message": big})


def test_rate_limiter_allows_under_cap(monkeypatch):
    t = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    limiter = NoteRateLimiter(per_minute=30, per_hour=200)
    for _ in range(30):
        assert limiter.try_acquire("sid-1") is True


def test_rate_limiter_rejects_over_per_minute_cap(monkeypatch):
    t = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    limiter = NoteRateLimiter(per_minute=3, per_hour=200)
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is False


def test_rate_limiter_window_slides(monkeypatch):
    t = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    limiter = NoteRateLimiter(per_minute=2, per_hour=200)
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is False
    t[0] += 61
    assert limiter.try_acquire("sid-1") is True


def test_rate_limiter_per_ip_buckets_are_independent(monkeypatch):
    """Distinct source IPs against the same session must NOT share a bucket."""
    t = [1_000.0]
    monkeypatch.setattr(time, "monotonic", lambda: t[0])
    limiter = NoteRateLimiter(per_minute=2, per_hour=200)
    # IP A burns its quota.
    assert limiter.try_acquire("sid-1", "10.0.0.1") is True
    assert limiter.try_acquire("sid-1", "10.0.0.1") is True
    assert limiter.try_acquire("sid-1", "10.0.0.1") is False
    # IP B is unaffected.
    assert limiter.try_acquire("sid-1", "10.0.0.2") is True
    assert limiter.try_acquire("sid-1", "10.0.0.2") is True
    assert limiter.try_acquire("sid-1", "10.0.0.2") is False


def test_rate_limiter_default_ip_is_empty_string():
    """Calls without a source_ip share a single ('sid', '') bucket — legacy compat."""
    limiter = NoteRateLimiter(per_minute=1, per_hour=200)
    assert limiter.try_acquire("sid-1") is True
    assert limiter.try_acquire("sid-1") is False
    # The empty-string bucket isn't shared with an explicit IP:
    assert limiter.try_acquire("sid-1", "10.0.0.1") is True
