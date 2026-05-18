"""Integration test: concurrent reads on a cold session after relay restart.

Scenario:
  1. Start relay → create persisted session → send 3 commands → stop relay.
  2. Start NEW relay with the same DB path (RAM state is cold — nothing loaded).
  3. Fire 20 concurrent GET /v1/sessions/{sid}/events AND
         20 concurrent GET /v1/sessions/{sid} (describe) calls.
  4. Assert every request returns 200 and consistent metadata.

This validates that concurrent cold-path hydration reads don't conflict
(the relay's lock-based "exactly-one-hydrate" invariant holds under load).
"""
from __future__ import annotations

import asyncio

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_concurrent_cold_hydrate(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    sid: str

    # ── Phase 1: create session + send 3 commands, then stop relay ──────────
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]

            # Sender joins
            ra = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                json={"role": "sender"},
            )
            assert ra.status_code == 201, ra.text
            token_a = ra.json()["participant_token"]

            # Receiver joins
            rb = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                json={"role": "receiver"},
            )
            assert rb.status_code == 201, rb.text
            pid_b = rb.json()["participant_id"]

            # Send 3 commands so there are events to read
            for i in range(3):
                rs = await c.post(
                    f"{base}/v1/sessions/{sid}/commands",
                    headers={"Authorization": f"Bearer {token_a}"},
                    json={
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"n": i},
                        "timeout_seconds": 30,
                    },
                )
                assert rs.status_code == 201, rs.text

    # relay stopped; DB on disk, RAM wiped

    # ── Phase 2: new relay (cold) + concurrent reads ─────────────────────────
    async with relay_subprocess(persistence_db_path=db) as base2:
        async with httpx.AsyncClient(timeout=30.0) as c2:

            async def get_events() -> int:
                r = await c2.get(
                    f"{base2}/v1/sessions/{sid}/events",
                    params={"limit": 100},
                )
                return r.status_code

            async def describe_session() -> tuple[int, str]:
                r = await c2.get(f"{base2}/v1/sessions/{sid}")
                return r.status_code, r.json().get("session_id", "")

            # Fire 20 events requests + 20 describe requests concurrently
            results = await asyncio.gather(
                *[get_events() for _ in range(20)],
                *[describe_session() for _ in range(20)],
                return_exceptions=True,
            )

            event_results = results[:20]
            describe_results = results[20:]

            # All events requests must succeed
            for i, res in enumerate(event_results):
                assert not isinstance(res, Exception), (
                    f"events request {i} raised: {res}"
                )
                assert res == 200, (
                    f"events request {i} returned {res}, expected 200"
                )

            # All describe requests must succeed and return the correct session_id
            for i, res in enumerate(describe_results):
                assert not isinstance(res, Exception), (
                    f"describe request {i} raised: {res}"
                )
                assert isinstance(res, tuple)
                status: int = res[0]
                returned_sid: str = res[1]
                assert status == 200, (
                    f"describe request {i} returned {status}, expected 200"
                )
                assert returned_sid == sid, (
                    f"describe request {i} returned session_id={returned_sid!r}, "
                    f"expected {sid!r}"
                )
