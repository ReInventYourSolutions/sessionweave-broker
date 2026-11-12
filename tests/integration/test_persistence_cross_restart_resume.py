"""Integration test: cross-restart event readability.

Iteration 1: start relay → create persisted session → join participant →
             send 3 commands → stop relay.
Iteration 2: start NEW relay with same DB path → GET /v1/sessions/{sid}/events
             → must return ≥ 3 command.received events.

This validates that cold-path history reads work across relay restarts.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_persistence_cross_restart_resume(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]

    # ── Iteration 1: create session + send 3 commands ────────────────────────
    sid: str
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            jt = r.json()["join_token"]
            creator = r.json()["creator_token"]
            # Sender joins
            ra = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "sender"},
            )
            assert ra.status_code == 201, ra.text
            token_a = ra.json()["participant_token"]

            # Receiver joins (needed for dispatch=specific target)
            rb = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "receiver"},
            )
            assert rb.status_code == 201, rb.text
            pid_b = rb.json()["participant_id"]

            # Send 3 commands
            for i in range(3):
                rs = await c.post(
                    f"{base}/v1/sessions/{sid}/commands",
                    headers={"Authorization": f"Bearer {token_a}"},
                    json={
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"seq_hint": i},
                        "timeout_seconds": 30,
                    },
                )
                assert rs.status_code == 201, rs.text

    # relay is now stopped; DB file persists

    # ── Iteration 2: new relay, same DB ─────────────────────────────────────
    async with relay_subprocess(persistence_db_path=db) as base2:
        async with httpx.AsyncClient(timeout=10.0) as c2:
            r_events = await c2.get(
                f"{base2}/v1/sessions/{sid}/events",
                headers=bearer(creator),
                params={"kind": "command.received", "limit": 100},
            )
            assert r_events.status_code == 200, r_events.text
            body = r_events.json()
            events = body.get("events", [])
            cmd_received = [e for e in events if e.get("kind") == "command.received"]
            assert len(cmd_received) >= 3, (
                f"Expected ≥ 3 command.received events after restart,"
                f" got {len(cmd_received)}: {events}"
            )
