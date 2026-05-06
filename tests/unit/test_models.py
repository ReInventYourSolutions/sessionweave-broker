from datetime import UTC, datetime, timedelta

from sessionweave.models import CommandTracker, Participant
from sessionweave.schemas import DispatchMode


def test_participant_to_public_strips_internal_fields() -> None:
    p = Participant.create(role="reviewer", capabilities=["sec"], metadata={"k": "v"})
    p.online = True
    pub = p.to_public()
    assert pub.role == "reviewer"
    assert pub.online is True
    assert pub.metadata == {"k": "v"}


def test_command_tracker_marks_response_once() -> None:
    now = datetime.now(UTC)
    t = CommandTracker(
        command_id="cid",
        sender_id="sid",
        dispatch_mode=DispatchMode.ALL,
        dispatched_to=["a", "b"],
        created_at=now,
        deadline=now + timedelta(seconds=30),
    )
    assert t.record_response("a") is True
    assert t.record_response("a") is False  # already responded
    assert t.received_count == 1
    assert t.is_complete() is False
    assert t.record_response("b") is True
    assert t.is_complete() is True


def test_command_tracker_pending_recipients() -> None:
    now = datetime.now(UTC)
    t = CommandTracker(
        command_id="cid",
        sender_id="sid",
        dispatch_mode=DispatchMode.ALL,
        dispatched_to=["a", "b", "c"],
        created_at=now,
        deadline=now + timedelta(seconds=30),
    )
    t.record_response("a")
    assert set(t.pending_recipients()) == {"b", "c"}
