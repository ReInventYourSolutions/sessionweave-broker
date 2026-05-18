import json
import time
from typing import Any

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import SessionSnapshot
from sessionweave.session import Session


def _snap(**overrides: Any) -> SessionSnapshot:
    defaults: dict[str, Any] = dict(
        session_id="sid",
        config_json=json.dumps({"idle_ttl_seconds": 1800}),
        purpose_json=None,
        creator_token_hash=b"\x00" * 32,
        persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=int(time.time() * 1000),
        last_activity_at=int(time.time() * 1000),
        ended_at=None,
        ended_reason=None,
        relay_epoch_at_create=1,
        participants=[],
        inflight={},
        buffered=[],
        max_event_seq=0,
    )
    defaults.update(overrides)
    return SessionSnapshot(**defaults)


def _build(snap: SessionSnapshot) -> Session:
    return Session.from_snapshot(
        snap, config=SessionConfig(),
        connections=ConnectionManager(),
        store=None,
        metrics=None,
        firehose=None,
        on_deadline_shortened=None,
    )


def test_from_snapshot_reconstructs_metadata() -> None:
    s = _build(_snap())
    assert s.session_id == "sid"
    assert s._persistence_mode == "ttl"
    assert s.purpose is None
    assert s.creator_token_hash == b"\x00" * 32


def test_from_snapshot_reconstructs_participants() -> None:
    s = _build(_snap(participants=[{
        "participant_id": "p1",
        "snapshot": {
            "role": "agent", "objective": None, "capabilities": ["math"],
            "metadata": {}, "delivery_mode": "push",
        },
        "last_seen_seq": 5,
        "joined_at": int(time.time() * 1000),
    }]))
    assert "p1" in s._participants
    p = s._participants["p1"]
    assert p.role == "agent"
    assert "math" in p.capabilities
    assert p.last_acked_seq == 5


def test_from_snapshot_reconstructs_inflight() -> None:
    deadline_ms = (int(time.time()) + 60) * 1000
    s = _build(_snap(inflight={
        "c1": {
            "command_id": "c1",
            "sender_id": "p1",
            "dispatch_mode": "all",
            "dispatched_to": ["p2"],
            "deadline_ms": deadline_ms,
            "received_count": 0,
            "expected_count": 1,
            "responses_received": [],
            "created_at_ms": int(time.time() * 1000),
        }
    }))
    assert "c1" in s._inflight


def test_from_snapshot_reconstructs_buffers_and_byte_budget() -> None:
    payload = {"text": "x" * 200}
    s = _build(_snap(
        participants=[{
            "participant_id": "p1",
            "snapshot": {"role": "agent", "objective": None,
                          "capabilities": [], "metadata": {},
                          "delivery_mode": "push"},
            "last_seen_seq": 0,
            "joined_at": 0,
        }],
        buffered=[
            {"participant_id": "p1", "seq": 1, "kind": "result.received",
             "payload": payload},
            {"participant_id": "p1", "seq": 2, "kind": "supervisor.note",
             "payload": {"message": "hi"}},
        ],
    ))
    assert "p1" in s._buffers
    assert s._buffers["p1"].size() == 2
    # _buffered_bytes equals sum of payload JSON byte lengths
    expected = (
        len(json.dumps(payload, separators=(",", ":")).encode())
        + len(json.dumps({"message": "hi"}, separators=(",", ":")).encode())
    )
    assert s._buffered_bytes == expected


def test_from_snapshot_resumes_event_seq_from_max_event_seq_when_buffered_empty() -> None:
    """Regression for the review finding: a push-only session that went
    cold with all subscribers caught up has snap.buffered == [] but
    its events table can be non-empty. _event_seq must resume from the
    DB max_seq or the next dispatch will collide on the (sid, seq) PK.
    """
    s = _build(_snap(
        participants=[{
            "participant_id": "p1",
            "snapshot": {"role": "agent", "objective": None,
                          "capabilities": [], "metadata": {},
                          "delivery_mode": "push"},
            "last_seen_seq": 7,
            "joined_at": 0,
        }],
        buffered=[],
        max_event_seq=7,
    ))
    assert s._event_seq == 7, (
        f"expected _event_seq=7 (max_event_seq) but got {s._event_seq} — "
        "push-only sessions would otherwise collide with existing event rows"
    )


def test_from_snapshot_event_seq_picks_max_of_buffered_and_max_event_seq() -> None:
    """When both buffered and max_event_seq are populated, take the
    larger one. (max_event_seq should always be >= buffered, but the
    code is defensive.)
    """
    s = _build(_snap(
        participants=[{
            "participant_id": "p1",
            "snapshot": {"role": "agent", "objective": None,
                          "capabilities": [], "metadata": {},
                          "delivery_mode": "push"},
            "last_seen_seq": 3,
            "joined_at": 0,
        }],
        buffered=[
            {"participant_id": "p1", "seq": 3, "kind": "x", "payload": {}},
        ],
        max_event_seq=5,
    ))
    assert s._event_seq == 5
