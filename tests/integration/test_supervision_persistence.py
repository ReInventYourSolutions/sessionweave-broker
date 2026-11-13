"""Hydration restores supervision_state and the note registry."""
from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import generate_token, hash_token
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from sessionweave.supervisor import NoteRecord
from tests.unit.fakes import FakeHandle


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _registry(store: PersistenceStore) -> SessionRegistry:
    return SessionRegistry(
        defaults=load_deployment_defaults({}),
        connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )


@pytest.mark.asyncio
async def test_hydrate_restores_paused_state_and_notes(store: PersistenceStore) -> None:
    reg = _registry(store)
    sid, _, _, _, _ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_token(generate_token()),
    )
    sess = await reg.get_or_raise(sid)
    paused = await sess.join(FakeHandle(), role="a", capabilities=[])
    other = await sess.join(FakeHandle(), role="b", capabilities=[])

    # Persist the paused flag the way the snapshot carries it, plus a note event.
    snap = await store.load_session(sid)
    assert snap is not None
    for entry in snap.participants:
        assert entry["snapshot"].get("supervision_state") == "active"
    paused_snap = next(
        e for e in snap.participants if e["participant_id"] == paused.participant_id
    )["snapshot"]
    await store.upsert_participant(
        sid, paused.participant_id, {**paused_snap, "supervision_state": "paused"},
        last_seen_seq=0, joined_at=next(
            e for e in snap.participants if e["participant_id"] == paused.participant_id
        )["joined_at"],
    )
    await store.persist_event_atomic(
        sid=sid, seq=snap.max_event_seq + 1, kind="supervisor.note",
        payload={"note_id": "sn-abc", "kind": "halt", "target": paused.participant_id,
                 "ts_ms": 1234, "message": None},
        ts=1234,
    )

    cold = _registry(store)
    hydrated = await cold.get_or_raise(sid)
    states = {p.participant_id: p.supervision_state for p in hydrated.roster_snapshot()}
    assert states[paused.participant_id] == "paused"
    assert states[other.participant_id] == "active"
    assert hydrated._notes == {
        "sn-abc": NoteRecord("sn-abc", "halt", paused.participant_id, 1234)
    }
