"""Regression tests for the ``sessions_active`` gauge over the full
hot↔cold persistence lifecycle.

The gauge models in-RAM (hot) sessions. ``+1`` on create, ``-1`` on
hot→cold eviction. For the count to stay balanced, the cold→hot
hydration path **must** ``+1`` again. Before this fix it didn't, so
every persisted session that was evicted and then either rejoined (via
``get_or_raise``) or deleted (via ``delete``, which hydrates internally)
double-decremented the gauge — producing a steady negative drift on
long-running relays with persistence enabled.

These tests pin the lifecycle invariant so the drift cannot
re-introduce itself.
"""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio
from prometheus_client import CollectorRegistry

from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.metrics import Metrics
from sessionweave.persistence import PersistenceStore
from sessionweave.persistence_token import (
    generate_creator_token,
    hash_creator_token,
)
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


def _sessions_active(m: Metrics) -> float:
    """Read the current value of the unlabeled ``sessions_active`` gauge.

    Uses the ``prometheus_client`` internal ``_value.get()`` accessor —
    stable across recent versions and the same trick used elsewhere in
    this repo's tests.
    """
    return m.sessions_active._value.get()  # type: ignore[attr-defined,no-any-return,unused-ignore]


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
    """Helper: create a session with persistence enabled and a low
    idle TTL so we can force eviction with one tick."""
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


@pytest.mark.asyncio
async def test_hydrate_increments_sessions_active(reg_with_metrics: Any) -> None:
    """The crux. ``get_or_raise`` on a cold session must ``+1`` the
    gauge to balance the ``-1`` from eviction."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    assert _sessions_active(m) == 1.0, "create should +1"

    # Force hot→cold eviction by ticking past the idle TTL.
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=120))
    assert _sessions_active(m) == 0.0, "evict should -1 (net 0)"

    # Hydrate cold→hot. This is the fix point.
    await reg.get_or_raise(sid)
    assert _sessions_active(m) == 1.0, (
        "hydrate must +1 sessions_active to balance the evict dec()"
    )


@pytest.mark.asyncio
async def test_create_evict_hydrate_end_balances_to_zero(
    reg_with_metrics: Any,
) -> None:
    """End-to-end: create → evict → hydrate (rejoin) → end must net 0."""
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=120))  # evict
    await reg.get_or_raise(sid)                                # hydrate
    await reg.end_session(sid, reason="test")                  # end (hot dec)
    assert _sessions_active(m) == 0.0, (
        "full lifecycle must net to zero — drift indicates an asymmetry"
    )


@pytest.mark.asyncio
async def test_create_evict_delete_balances_to_zero(
    reg_with_metrics: Any,
) -> None:
    """End-to-end: create → evict → delete must net 0.

    ``delete`` calls ``get_or_raise`` internally, so it hydrates the
    cold session before decrementing. Without the hydrate inc the
    sequence drifts to ``-1``.
    """
    reg, m = reg_with_metrics
    sid = await _create_persisted(reg)
    reg.tick(now=datetime.now(UTC) + timedelta(seconds=120))  # evict to cold
    await reg.delete(sid, reason="test")                      # hydrate + dec
    assert _sessions_active(m) == 0.0, (
        "evict+delete must net to zero — without the hydrate inc this drifts -1"
    )


@pytest.mark.asyncio
async def test_create_end_baseline_still_zero(reg_with_metrics: Any) -> None:
    """Sanity: the simple ephemeral create→end path was never broken and
    must keep balancing to zero after the fix (no regression)."""
    reg, m = reg_with_metrics
    sid, _, _, _, _ = await reg.create({})  # ephemeral, no persistence
    assert _sessions_active(m) == 1.0
    await reg.end_session(sid, reason="test")
    assert _sessions_active(m) == 0.0
