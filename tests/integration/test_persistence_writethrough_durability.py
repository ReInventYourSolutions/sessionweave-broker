"""Integration test: write-through durability check.

Creates a persisted session, joins 2 participants, sends 5 commands (dispatch=all
with role filter), then queries the SQLite DB file directly to confirm all 5
command.received events were written to the events table.
"""
from __future__ import annotations

import sqlite3

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_persistence_writethrough_durability(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create a persisted session
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]

            # Participant A joins as "sender"
            ra = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                json={"role": "sender"},
            )
            assert ra.status_code == 201, ra.text
            token_a = ra.json()["participant_token"]
            headers_a = {"Authorization": f"Bearer {token_a}"}

            # Participant B joins as "receiver"
            rb = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                json={"role": "receiver"},
            )
            assert rb.status_code == 201, rb.text
            pid_b = rb.json()["participant_id"]

            headers_a = {"Authorization": f"Bearer {token_a}"}

            # Send 5 commands (specific dispatch targeting receiver)
            for i in range(5):
                rs = await c.post(
                    f"{base}/v1/sessions/{sid}/commands",
                    headers=headers_a,
                    json={
                        "dispatch": "specific",
                        "target": {"participant_ids": [pid_b]},
                        "payload": {"idx": i},
                        "timeout_seconds": 30,
                    },
                )
                assert rs.status_code == 201, rs.text

    # After relay exits, inspect the SQLite DB directly
    conn = sqlite3.connect(db)
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM events WHERE session_id = ? AND kind = 'command.received'",
            (sid,),
        )
        count = cur.fetchone()[0]
    finally:
        conn.close()

    assert count == 5, (
        f"Expected 5 command.received events in DB, found {count}"
    )
