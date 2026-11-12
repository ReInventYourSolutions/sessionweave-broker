"""Regression tests for the ``participants_active`` gauge over the full
hot↔cold persistence lifecycle.

The gauge models participants of in-RAM (hot) sessions, split across the
``online="true"`` / ``online="false"`` label series. For the count to
stay balanced, every path that removes a ``Session`` from RAM must
release that session's participants, and the hydration path that brings
them back must re-account for them.

Two asymmetries existed before this fix:

* hot→cold eviction popped the session without decrementing
  ``participants_active`` for any of its participants, so they stayed
  counted as online **forever** — the relay reported live participants
  for sessions that no longer existed;
* ``Session.from_snapshot`` repopulated ``_participants`` (as offline)
  without incrementing the ``online="false"`` series, so a subsequent
  ``resume`` — which does ``offline.dec(); online.inc()`` — drove the
  offline series negative and invented a phantom online participant.

This mirrors ``test_metrics_sessions_active_balance.py``, which pins the
same invariant for ``sessions_active``.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from prometheus_client import CollectorRegistry

from sessionweave.access import (
    generate_token as generate_creator_token,
)
from sessionweave.access import (
    hash_token as hash_creator_token,
)
from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.metrics import Metrics
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from tests.unit.fakes import FakeHandle


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


def _online(m: Metrics) -> float:
    """Current value of ``participants_active{online="true"}``."""
    return m.participants_active.labels(online="true")._value.get()  # type: ignore[attr-defined,no-any-return,unused-ignore]


def _offline(m: Metrics) -> float:
    """Current value of ``participants_active{online="false"}``."""
    return m.participants_active.labels(online="false")._value.get()  # type: ignore[attr-defined,no-any-return,unused-ignore]


@pytest_asyncio.fixture
async def reg_with_metrics(store: PersistenceStore):
    m = Metrics(registry=CollectorRegistry())
    r = SessionRegistry(
        defaults=_defaults(),
        connections=ConnectionManager(),
        metrics=m,
        firehose=None,
        store=store,
    )
    return r, m


async def _create_persisted(reg: SessionRegistry, **overrides: Any) -> str:
    """Create a session with persistence enabled and a low idle TTL so a
    single ``tick`` past it forces the hot→cold eviction."""
    token_hash = hash_creator_token(generate_creator_token())
    base = {"idle_ttl_seconds": 60}
    base.update(overrides)
    sid, _exp, _cfg, _purp, _state = await reg.create(
        overrides=base,
        purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=token_hash,
    )
    return sid


async def _join(reg: SessionRegistry, sid: str, role: str) -> str:
    sess = await reg.get_or_raise(sid)
    p = await sess.join(FakeHandle(), role=role, capabilities=[])
    return p.participant_id


def _evict(reg: SessionRegistry) -> None:
    """Tick past the idle TTL to force hot→cold eviction."""
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=120))


@pytest.mark.asyncio
async def test_cold_eviction_releases_online_participants(
    reg_with_metrics: Any,
) -> None:
    """The crux. When a persisted session leaves RAM its participants
    must stop being counted — otherwise the relay reports participants
    online for a session that is no longer there."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    await _join(reg, sid, "agent-a")
    await _join(reg, sid, "agent-b")
    assert _online(m) == 2.0, "two joins should +2 the online series"

    _evict(reg)

    assert _online(m) == 0.0, (
        "hot→cold eviction must release the session's participants; "
        "leaving them counted strands them online forever"
    )


@pytest.mark.asyncio
async def test_hydrate_reaccounts_participants_as_offline(
    reg_with_metrics: Any,
) -> None:
    """Cold→hot hydration restores ``_participants`` as offline, so the
    ``online="false"`` series must be incremented to match."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    await _join(reg, sid, "agent-a")
    await _join(reg, sid, "agent-b")
    _evict(reg)

    await reg.get_or_raise(sid)

    assert _offline(m) == 2.0, (
        "hydrate must +1 the offline series per restored participant"
    )
    assert _online(m) == 0.0, "hydrated participants start offline"


@pytest.mark.asyncio
async def test_resume_after_hydrate_does_not_invent_online_participant(
    reg_with_metrics: Any,
) -> None:
    """``resume`` moves a participant offline→online. Without the
    hydrate inc the offline series goes negative and the online series
    reports a participant that was never accounted for."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    pid = await _join(reg, sid, "agent-a")
    _evict(reg)

    sess = await reg.get_or_raise(sid)
    await sess.resume(FakeHandle(), pid)

    assert _online(m) == 1.0, "the resumed participant is online exactly once"
    assert _offline(m) == 0.0, (
        "the offline series must return to zero, not drift negative"
    )


@pytest.mark.asyncio
async def test_join_evict_delete_balances_to_zero(reg_with_metrics: Any) -> None:
    """End-to-end, the sequence the operator actually hit: join two
    agents, let the session go cold, then delete it. Both series must
    net to zero — the deleted session must leave nothing behind."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    await _join(reg, sid, "agent-a")
    await _join(reg, sid, "agent-b")
    _evict(reg)

    await reg.delete(sid, reason="test")

    assert _online(m) == 0.0, "no participant may survive the deleted session"
    assert _offline(m) == 0.0, "full lifecycle must net to zero"


@pytest.mark.asyncio
async def test_join_evict_hydrate_resume_delete_balances_to_zero(
    reg_with_metrics: Any,
) -> None:
    """The long way round: join → cold → hydrate → resume → delete."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    pid = await _join(reg, sid, "agent-a")
    _evict(reg)
    sess = await reg.get_or_raise(sid)
    await sess.resume(FakeHandle(), pid)

    await reg.delete(sid, reason="test")

    assert _online(m) == 0.0
    assert _offline(m) == 0.0


@pytest.mark.asyncio
async def test_ephemeral_join_end_baseline_still_zero(
    reg_with_metrics: Any,
) -> None:
    """Sanity: the ephemeral create→join→end path was never broken and
    must keep balancing to zero after the fix (no regression)."""
    reg, m = reg_with_metrics
    sid, _, _, _, _ = await reg.create({})  # ephemeral, no persistence
    await _join(reg, sid, "agent-a")
    assert _online(m) == 1.0

    await reg.end_session(sid, reason="test")

    assert _online(m) == 0.0
    assert _offline(m) == 0.0
