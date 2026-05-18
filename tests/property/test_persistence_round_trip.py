"""Property tests: persist → load round-trip preserves session state."""
from __future__ import annotations

import time
from pathlib import Path

import pytest
import pytest_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sessionweave.persistence import PersistenceStore, SessionMetadata
from sessionweave.persistence_token import generate_creator_token, hash_creator_token


def _now_ms() -> int:
    return int(time.time() * 1000)


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


# ---------------------------------------------------------------------------
# Deterministic round-trip smoke test
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_round_trip_simple(store: PersistenceStore) -> None:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="r1", config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    await store.upsert_participant(
        "r1", "p1",
        {"role": "agent", "delivery_mode": "push"},
        last_seen_seq=0, joined_at=now,
    )
    await store.persist_event_atomic(
        sid="r1", seq=1, kind="command.received",
        payload={"command_id": "c1", "text": "hi"}, ts=now,
        inflight_upsert=("c1", {"sender_pid": "p1", "deadline_ms": now + 60_000}),
        buffered_pids=("p1",),
    )
    snap = await store.load_session("r1")
    assert snap is not None
    assert snap.session_id == "r1"
    assert len(snap.participants) == 1
    assert snap.participants[0]["participant_id"] == "p1"
    assert "c1" in snap.inflight
    assert len(snap.buffered) == 1
    assert snap.buffered[0]["payload"]["command_id"] == "c1"


@pytest.mark.asyncio
async def test_round_trip_no_participants(store: PersistenceStore) -> None:
    """A session with no participants and no events still round-trips cleanly."""
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    await store.save_session_metadata(SessionMetadata(
        session_id="empty", config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    snap = await store.load_session("empty")
    assert snap is not None
    assert snap.session_id == "empty"
    assert snap.participants == []
    assert snap.inflight == {}
    assert snap.buffered == []


@pytest.mark.asyncio
async def test_round_trip_multiple_events_and_participants(store: PersistenceStore) -> None:
    """Multiple participants, multiple events, correct reconstruction."""
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    sid = "multi"
    await store.save_session_metadata(SessionMetadata(
        session_id=sid, config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    for pid in ("p1", "p2", "p3"):
        await store.upsert_participant(
            sid, pid, {"role": "agent", "delivery_mode": "push"},
            last_seen_seq=0, joined_at=now,
        )
    # 5 events, all buffered for p1
    for i in range(1, 6):
        await store.persist_event_atomic(
            sid=sid, seq=i, kind="result.received",
            payload={"index": i}, ts=now + i,
            buffered_pids=("p1",),
        )
    snap = await store.load_session(sid)
    assert snap is not None
    assert len(snap.participants) == 3
    assert len(snap.buffered) == 5
    # payloads preserved
    for entry in snap.buffered:
        assert "index" in entry["payload"]


# ---------------------------------------------------------------------------
# Hypothesis variant: a small number of events with randomised payloads
# ---------------------------------------------------------------------------

@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(
    num_events=st.integers(min_value=0, max_value=10),
    text=st.text(max_size=20),
)
@pytest.mark.asyncio
async def test_round_trip_hypothesis(
    tmp_path: Path, num_events: int, text: str,
) -> None:
    """For any small sequence of events, load_session returns matching state."""
    store = PersistenceStore(str(tmp_path / f"hyp_{num_events}_{abs(hash(text))}.db"))
    await store.init(pool_size=2)
    try:
        th = hash_creator_token(generate_creator_token())
        now = _now_ms()
        sid = "hyp"
        await store.save_session_metadata(SessionMetadata(
            session_id=sid, config_json="{}", purpose_json=None,
            creator_token_hash=th, persistence_mode="ttl",
            persistence_ttl_seconds=3600,
            created_at=now, last_activity_at=now, relay_epoch_at_create=1,
        ))
        await store.upsert_participant(
            sid, "p1", {"role": "agent", "delivery_mode": "push"},
            last_seen_seq=0, joined_at=now,
        )
        for i in range(1, num_events + 1):
            await store.persist_event_atomic(
                sid=sid, seq=i, kind="result.received",
                payload={"i": i, "t": text[:10]}, ts=now + i,
                buffered_pids=("p1",),
            )
        snap = await store.load_session(sid)
        assert snap is not None
        assert snap.session_id == sid
        assert len(snap.participants) == 1
        assert len(snap.buffered) == num_events
    finally:
        await store.close()
