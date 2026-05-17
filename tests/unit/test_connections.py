import pytest

from sessionweave.connections import ConnectionHandle, ConnectionManager


class FakeHandle(ConnectionHandle):
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, object], int | None]] = []
        self.closed = False

    def send(
        self,
        name: str,
        payload: dict[str, object],
        *,
        seq: int | None = None,
    ) -> None:
        # This local FakeHandle stores 3-tuples so the test below can
        # assert on the seq directly; other tests use the shared
        # ``tests/unit/fakes.py`` FakeHandle which keeps 2-tuples for
        # backwards-compat.
        self.sent.append((name, payload, seq))

    def is_alive(self) -> bool:
        return not self.closed


def test_bind_and_lookup() -> None:
    mgr = ConnectionManager()
    h = FakeHandle()
    mgr.bind(h, session_id="s1", participant_id="p1")
    assert mgr.session_for(h) == ("s1", "p1")
    assert mgr.handle_for("s1", "p1") is h


def test_bind_returns_old_handle_when_rebinding() -> None:
    mgr = ConnectionManager()
    h1, h2 = FakeHandle(), FakeHandle()
    mgr.bind(h1, "s1", "p1")
    old = mgr.bind(h2, "s1", "p1")
    assert old is h1
    assert mgr.handle_for("s1", "p1") is h2


def test_unbind_handle() -> None:
    mgr = ConnectionManager()
    h = FakeHandle()
    mgr.bind(h, "s1", "p1")
    mgr.unbind(h)
    assert mgr.session_for(h) is None
    assert mgr.handle_for("s1", "p1") is None


def test_double_bind_to_different_sessions_rejected() -> None:
    mgr = ConnectionManager()
    h = FakeHandle()
    mgr.bind(h, "s1", "p1")
    with pytest.raises(ValueError):
        mgr.bind(h, "s2", "p2")


def test_send_notification_via_handle() -> None:
    mgr = ConnectionManager()
    h = FakeHandle()
    mgr.bind(h, "s1", "p1")
    sent = mgr.send("s1", "p1", "participant.joined", {"x": 1}, seq=5)
    assert sent is True
    assert h.sent == [("participant.joined", {"x": 1}, 5)]


def test_send_returns_false_when_no_handle() -> None:
    mgr = ConnectionManager()
    assert mgr.send("s1", "p1", "x", {}) is False


def test_unbind_unknown_handle_is_noop() -> None:
    """Per-request HTTP handle lifecycle relies on idempotent unbind in
    a finally block. A handle that was never bound (e.g. bind failed)
    must not raise on unbind."""
    mgr = ConnectionManager()
    h = FakeHandle()
    mgr.unbind(h)
    assert mgr.session_for(h) is None


def test_rebind_with_same_pair_returns_old_handle() -> None:
    """Re-binding the same (sid, pid) with a fresh handle is the normal
    reconnect path. The old handle is returned so the caller can detach
    its subscriber / drop notifications."""
    mgr = ConnectionManager()
    h1, h2 = FakeHandle(), FakeHandle()
    mgr.bind(h1, "s1", "p1")
    old = mgr.bind(h2, "s1", "p1")
    assert old is h1
    # Old handle is no longer associated.
    assert mgr.session_for(h1) is None
    assert mgr.session_for(h2) == ("s1", "p1")
