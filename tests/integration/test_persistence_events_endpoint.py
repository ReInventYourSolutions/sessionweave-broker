"""Integration test: /v1/sessions/{sid}/events pagination + filters.

Creates a persisted session, pushes 30 command.received events, then exercises:
  - ?limit=10 → 10 events, has_more=true
  - ?after_seq=<cursor>&limit=10 → next page
  - ?order=desc&limit=5 → last 5 in descending order (seq decreasing)
  - ?kind=command.received → only command.received events
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_persistence_events_pagination_and_filters(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            # Create persisted session
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            jt = r.json()["join_token"]
            # Sender joins
            ra = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "sender"},
            )
            assert ra.status_code == 201, ra.text
            token_a = ra.json()["participant_token"]
            headers_a = {"Authorization": f"Bearer {token_a}"}

            # Receiver joins
            rb = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                headers=bearer(jt),
                json={"role": "receiver"},
            )
            assert rb.status_code == 201, rb.text
            pid_b = rb.json()["participant_id"]

            # Push 30 commands
            for i in range(30):
                rs = await c.post(
                    f"{base}/v1/sessions/{sid}/commands",
                    headers=headers_a,
                    json={
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"n": i},
                        "timeout_seconds": 30,
                    },
                )
                assert rs.status_code == 201, rs.text

            # ── Page 1: limit=10 ─────────────────────────────────────────────
            r1 = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(jt),
                params={"limit": 10, "order": "asc"},
            )
            assert r1.status_code == 200, r1.text
            body1 = r1.json()
            assert len(body1["events"]) == 10
            assert body1["has_more"] is True
            assert body1["next_cursor"] is not None
            first_cursor = body1["next_cursor"]

            # Verify ascending order within first page
            seqs_p1 = [e["seq"] for e in body1["events"]]
            assert seqs_p1 == sorted(seqs_p1), "Events not in ascending seq order"

            # ── Page 2: after_seq=cursor&limit=10 ───────────────────────────
            r2 = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(jt),
                params={"after_seq": first_cursor, "limit": 10, "order": "asc"},
            )
            assert r2.status_code == 200, r2.text
            body2 = r2.json()
            seqs_p2 = [e["seq"] for e in body2["events"]]
            assert len(seqs_p2) == 10
            # All seqs on page 2 must be greater than the cursor
            assert all(s > first_cursor for s in seqs_p2), (
                f"Page 2 contains seq ≤ cursor {first_cursor}: {seqs_p2}"
            )

            # ── Descending order: last 5 ─────────────────────────────────────
            r_desc = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(jt),
                params={"order": "desc", "limit": 5},
            )
            assert r_desc.status_code == 200, r_desc.text
            body_desc = r_desc.json()
            seqs_desc = [e["seq"] for e in body_desc["events"]]
            assert len(seqs_desc) == 5
            # Descending: each seq should be ≥ the next
            assert seqs_desc == sorted(seqs_desc, reverse=True), (
                f"Desc events not in descending order: {seqs_desc}"
            )

            # ── Kind filter: command.received ────────────────────────────────
            r_kind = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(jt),
                params={"kind": "command.received", "limit": 100},
            )
            assert r_kind.status_code == 200, r_kind.text
            body_kind = r_kind.json()
            all_kinds = {e["kind"] for e in body_kind["events"]}
            assert all_kinds <= {"command.received"}, (
                f"Unexpected kinds in filtered result: {all_kinds}"
            )
            assert len(body_kind["events"]) == 30, (
                f"Expected 30 command.received events, got {len(body_kind['events'])}"
            )

            # ── Non-persisted session → 409 ──────────────────────────────────
            r_ephem = await c.post(f"{base}/v1/sessions", json={})
            assert r_ephem.status_code == 201
            sid_ephem = r_ephem.json()["session_id"]
            r_no_hist = await c.get(
                f"{base}/v1/sessions/{sid_ephem}/events",
                headers=bearer(r_ephem.json()["creator_token"]),
            )
            assert r_no_hist.status_code == 409, r_no_hist.text
