"""Property test: after hot→cold→hot (Session.from_snapshot) round-trip,
Session._buffered_bytes equals the sum of stored payload JSON byte lengths."""
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from sessionweave.access import generate_token as generate_creator_token
from sessionweave.access import hash_token as hash_creator_token
from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore, SessionMetadata
from sessionweave.session import Session


def _now_ms() -> int:
    return int(time.time() * 1000)


def _payload_bytes(payload: dict[str, Any]) -> int:
    return len(json.dumps(payload, separators=(",", ":")).encode())


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


async def _seed_session_with_buffered(
    store: PersistenceStore, sid: str, n_events: int
) -> None:
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
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
    for i in range(1, n_events + 1):
        await store.persist_event_atomic(
            sid=sid, seq=i, kind="result.received",
            payload={"index": i, "data": "x" * i}, ts=now + i,
            buffered_pids=("p1",),
        )


@pytest.mark.asyncio
async def test_byte_budget_invariant_after_hydrate(store: PersistenceStore) -> None:
    """Session._buffered_bytes after from_snapshot == sum of payload byte lengths."""
    sid = "bb1"
    await _seed_session_with_buffered(store, sid, n_events=5)

    snap = await store.load_session(sid)
    assert snap is not None

    sess = Session.from_snapshot(
        snap,
        config=SessionConfig(),
        connections=ConnectionManager(),
        store=None,
        metrics=None,
        firehose=None,
        on_deadline_shortened=None,
    )

    expected = sum(_payload_bytes(b["payload"]) for b in snap.buffered)
    assert sess._buffered_bytes == expected


@pytest.mark.asyncio
async def test_byte_budget_empty_buffered(store: PersistenceStore) -> None:
    """With no buffered events, _buffered_bytes is 0 after hydration."""
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    sid = "bb_empty"
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
    # Event persisted but NOT buffered
    await store.persist_event_atomic(
        sid=sid, seq=1, kind="x", payload={"v": 1}, ts=now + 1,
    )

    snap = await store.load_session(sid)
    assert snap is not None
    sess = Session.from_snapshot(
        snap,
        config=SessionConfig(),
        connections=ConnectionManager(),
        store=None, metrics=None, firehose=None, on_deadline_shortened=None,
    )
    assert sess._buffered_bytes == 0


@pytest.mark.asyncio
async def test_byte_budget_multiple_participants(store: PersistenceStore) -> None:
    """Byte budget sums across multiple participants correctly."""
    th = hash_creator_token(generate_creator_token())
    now = _now_ms()
    sid = "bb_multi"
    await store.save_session_metadata(SessionMetadata(
        session_id=sid, config_json="{}", purpose_json=None,
        creator_token_hash=th, persistence_mode="ttl",
        persistence_ttl_seconds=3600,
        created_at=now, last_activity_at=now, relay_epoch_at_create=1,
    ))
    for pid in ("p1", "p2"):
        await store.upsert_participant(
            sid, pid, {"role": "agent", "delivery_mode": "push"},
            last_seen_seq=0, joined_at=now,
        )
    for i in range(1, 4):
        await store.persist_event_atomic(
            sid=sid, seq=i, kind="x", payload={"i": i}, ts=now + i,
            buffered_pids=("p1", "p2"),
        )

    snap = await store.load_session(sid)
    assert snap is not None

    sess = Session.from_snapshot(
        snap,
        config=SessionConfig(),
        connections=ConnectionManager(),
        store=None, metrics=None, firehose=None, on_deadline_shortened=None,
    )
    expected = sum(_payload_bytes(b["payload"]) for b in snap.buffered)
    assert sess._buffered_bytes == expected


@settings(
    max_examples=10,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.function_scoped_fixture],
)
@given(n_events=st.integers(min_value=0, max_value=8))
@pytest.mark.asyncio
async def test_byte_budget_hypothesis(tmp_path: Path, n_events: int) -> None:
    """Hypothesis: for any n_events, byte budget matches after hydration."""
    store = PersistenceStore(str(tmp_path / f"bb_h_{n_events}.db"))
    await store.init(pool_size=2)
    try:
        sid = "h"
        th = hash_creator_token(generate_creator_token())
        now = _now_ms()
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
        for i in range(1, n_events + 1):
            await store.persist_event_atomic(
                sid=sid, seq=i, kind="x",
                payload={"i": i, "pad": "y" * (i * 3)}, ts=now + i,
                buffered_pids=("p1",),
            )
        snap = await store.load_session(sid)
        assert snap is not None

        sess = Session.from_snapshot(
            snap,
            config=SessionConfig(),
            connections=ConnectionManager(),
            store=None, metrics=None, firehose=None, on_deadline_shortened=None,
        )
        expected = sum(_payload_bytes(b["payload"]) for b in snap.buffered)
        assert sess._buffered_bytes == expected
    finally:
        await store.close()
