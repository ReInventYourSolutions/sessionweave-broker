"""Integration test: hot/cold session DB row presence.

Creates a persisted session while the relay is running (hot path), then
verifies the session row exists in the SQLite DB file. This confirms the
write-through path committed the session metadata before responding 201.
"""
from __future__ import annotations

import sqlite3

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_persistence_db_row_created_on_session_create(tmp_path: object) -> None:
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
            # Session is "hot" (in memory); still verify the DB row is there
            conn = sqlite3.connect(db)
            try:
                cur = conn.execute(
                    "SELECT session_id, persistence_mode FROM sessions WHERE session_id = ?",
                    (sid,),
                )
                row = cur.fetchone()
            finally:
                conn.close()

            assert row is not None, "Session row not found in DB after create"
            assert row[0] == sid
            assert row[1] == "ttl"


@pytest.mark.integration
@pytest.mark.asyncio
async def test_ephemeral_session_not_in_db(tmp_path: object) -> None:
    """Ephemeral (no persistence) sessions must NOT appear in the DB."""
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create ephemeral session (no persistence field)
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            conn = sqlite3.connect(db)
            try:
                cur = conn.execute(
                    "SELECT 1 FROM sessions WHERE session_id = ?",
                    (sid,),
                )
                row = cur.fetchone()
            finally:
                conn.close()

            assert row is None, (
                "Ephemeral session must not be persisted to DB"
            )
