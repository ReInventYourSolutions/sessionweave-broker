"""Unit tests for HTTPParticipantHandle and per-request handle lifecycle.

Regression coverage for the v0.13.3 fix that replaced the module-level
``_HTTP_HANDLE`` singleton with a fresh handle per request. The singleton
caused ``ConnectionManager.bind`` collisions when two parallel HTTP
clients were active against the same relay process.
"""
from __future__ import annotations

from sessionweave.connections import ConnectionManager
from sessionweave.http_api import HTTPParticipantHandle


def test_http_handle_send_is_noop() -> None:
    """HTTP handle.send is a no-op — events go to the per-participant
    buffer for inbox drain, not to a live push transport."""
    h = HTTPParticipantHandle()
    # No exception, no state.
    assert h.send("evt", {"x": 1}) is None


def test_http_handle_is_alive_always_true() -> None:
    """HTTP transport has no observable connection — is_alive is True
    until the handle is dropped/unbound at end of request."""
    h = HTTPParticipantHandle()
    assert h.is_alive() is True


def test_two_http_handles_are_distinct_instances() -> None:
    """Per-request instances must not share identity, otherwise
    ConnectionManager.bind would reject the second concurrent request."""
    a = HTTPParticipantHandle()
    b = HTTPParticipantHandle()
    assert a is not b


def test_concurrent_http_handles_bind_to_different_participants() -> None:
    """Two HTTP requests on the same session must be bindable to two
    different participants concurrently — this is the regression that
    motivated the singleton removal in v0.13.3."""
    cm = ConnectionManager()
    a = HTTPParticipantHandle()
    b = HTTPParticipantHandle()
    cm.bind(a, "sess-1", "pid-A")
    cm.bind(b, "sess-1", "pid-B")
    # Each handle is associated with its own participant.
    sa = cm.session_for(a)
    sb = cm.session_for(b)
    assert sa is not None and sa[1] == "pid-A"
    assert sb is not None and sb[1] == "pid-B"
    cm.unbind(a)
    cm.unbind(b)
    # After unbind, lookups return None.
    assert cm.session_for(a) is None
    assert cm.session_for(b) is None


def test_unbind_is_idempotent() -> None:
    """Unbinding a handle that's already unbound must not raise — the
    HTTP request lifecycle uses unbind in a finally block that may run
    after an earlier error has already cleaned up."""
    cm = ConnectionManager()
    h = HTTPParticipantHandle()
    cm.bind(h, "sess-1", "pid-A")
    cm.unbind(h)
    cm.unbind(h)  # Second unbind: no-op, no exception.
