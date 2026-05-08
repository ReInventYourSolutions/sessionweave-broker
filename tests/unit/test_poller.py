from __future__ import annotations

from sessionweave.poller import _is_terminal


def test_terminal_on_session_ended() -> None:
    assert _is_terminal(
        {"name": "session.ended", "payload": {"reason": "idle_ttl"}}, "any-pid"
    )


def test_terminal_on_own_left() -> None:
    own = "abc-123"
    assert _is_terminal(
        {"name": "participant.left", "payload": {"participant_id": own}}, own
    )


def test_not_terminal_on_other_left() -> None:
    own = "abc-123"
    assert not _is_terminal(
        {"name": "participant.left", "payload": {"participant_id": "other"}},
        own,
    )


def test_not_terminal_on_command_received() -> None:
    assert not _is_terminal(
        {"name": "command.received", "payload": {"command_id": "x"}}, "pid"
    )
