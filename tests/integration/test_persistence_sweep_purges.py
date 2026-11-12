"""Integration test: sweep purges TTL-expired sessions while leaving until_deleted intact.

Setup:
  - SW_MIN_PERSISTENCE_TTL_SECONDS=1   → allow ttl_seconds=1 in tests
  - SW_PERSISTENCE_SWEEP_INTERVAL_SECONDS=10  → minimum interval (fast sweep)
  - SW_PERSISTENCE_UNTIL_DELETED_SAFETY_SECONDS=7776000 (90 days, default)

Creates 2 ttl=1 sessions and 2 until_deleted sessions, then waits >11 s for
a sweep cycle to fire. After the wait:
  - TTL sessions → GET /v1/sessions/{sid} returns 404 (purged from DB).
  - until_deleted sessions → GET /v1/sessions/{sid} returns 200 (still alive).
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.slow
@pytest.mark.asyncio
async def test_sweep_purges_ttl_keeps_until_deleted(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]

    # Use a 1-second TTL; override the min-TTL bound so the relay accepts it.
    # Also use the minimum allowed sweep interval (10 s) to minimise test duration.
    env = {
        "SW_MIN_PERSISTENCE_TTL_SECONDS": "1",
        "SW_PERSISTENCE_SWEEP_INTERVAL_SECONDS": "10",
    }

    async with relay_subprocess(persistence_db_path=db, env_overrides=env) as base:
        async with httpx.AsyncClient(timeout=30.0) as c:
            # ── Create 2 TTL sessions (ttl=1 s) ─────────────────────────────
            ttl_sids: list[str] = []
            tokens: dict[str, str] = {}
            for _ in range(2):
                r = await c.post(
                    f"{base}/v1/sessions",
                    json={"persistence": {"mode": "ttl", "ttl_seconds": 1}},
                )
                assert r.status_code == 201, (
                    f"TTL session create failed: {r.status_code} {r.text}"
                )
                ttl_sids.append(r.json()["session_id"])
                tokens[ttl_sids[-1]] = r.json()["creator_token"]

            # ── Create 2 until_deleted sessions ──────────────────────────────
            ud_sids: list[str] = []
            for _ in range(2):
                r = await c.post(
                    f"{base}/v1/sessions",
                    json={"persistence": {"mode": "until_deleted"}},
                )
                assert r.status_code == 201, (
                    f"until_deleted session create failed: {r.status_code} {r.text}"
                )
                ud_sids.append(r.json()["session_id"])
                tokens[ud_sids[-1]] = r.json()["creator_token"]

            # ── Wait for TTL to expire AND at least one sweep to run ─────────
            # TTL expires after 1 s; sweep interval is 10 s; the first sweep
            # fires at relay startup (before session creation), so the next
            # sweep is at approximately t_startup+10s. We wait 13 s to give
            # a generous margin over the 10-second sweep interval.
            await asyncio.sleep(13)

            # ── TTL sessions must be gone (purged by sweep) ──────────────────
            for sid in ttl_sids:
                r_gone = await c.get(
                    f"{base}/v1/sessions/{sid}", headers=bearer(tokens[sid])
                )
                assert r_gone.status_code == 404, (
                    f"TTL session {sid} should be purged (404) after sweep, "
                    f"got {r_gone.status_code}: {r_gone.text}"
                )

            # ── until_deleted sessions must still be present ─────────────────
            for sid in ud_sids:
                r_alive = await c.get(
                    f"{base}/v1/sessions/{sid}", headers=bearer(tokens[sid])
                )
                assert r_alive.status_code == 200, (
                    f"until_deleted session {sid} should still be alive (200), "
                    f"got {r_alive.status_code}: {r_alive.text}"
                )
                body = r_alive.json()
                assert body.get("session_id") == sid, (
                    f"Unexpected session_id in describe response: {body}"
                )
