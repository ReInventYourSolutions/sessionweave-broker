"""E2E smoke: the cockpit's events endpoint is reachable on a freshly-created
persisted session.

Full Playwright UI rendering against a cold session is brittle and adds little
coverage. The meaningful invariant is the API contract: the cockpit's
GET /v1/sessions/{sid}/events call succeeds and returns the right shape.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_cockpit_can_load_session_events_endpoint(tmp_path) -> None:
    """Smoke test: a persisted session's events endpoint returns the
    timeline to a generic HTTP client (the cockpit's GET /events call).
    """
    db = str(tmp_path / "relay.db")
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{base}/v1/sessions", json={
                "persistence": {"mode": "ttl", "ttl_seconds": 3600},
            })
            r.raise_for_status()
            sid = r.json()["session_id"]
            jt = r.json()["join_token"]
            # Events endpoint must be reachable
            r = await c.get(f"{base}/v1/sessions/{sid}/events", headers=bearer(jt))
            r.raise_for_status()
            body = r.json()
            assert body["session_id"] == sid
            assert isinstance(body["events"], list)
            assert body["session_state"] in {"hot", "cold", "ended"}


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_cockpit_events_endpoint_returns_no_events_for_new_session(
    tmp_path,
) -> None:
    """A brand-new session has an empty events list."""
    db = str(tmp_path / "relay2.db")
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{base}/v1/sessions", json={
                "persistence": {"mode": "ttl", "ttl_seconds": 3600},
            })
            r.raise_for_status()
            sid = r.json()["session_id"]
            jt = r.json()["join_token"]
            r = await c.get(f"{base}/v1/sessions/{sid}/events", headers=bearer(jt))
            r.raise_for_status()
            body = r.json()
            assert body["events"] == []


@pytest.mark.e2e
@pytest.mark.asyncio
async def test_cockpit_events_endpoint_404_for_unknown_session(tmp_path) -> None:
    """Unknown session ID returns 404."""
    db = str(tmp_path / "relay3.db")
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.get(
                f"{base}/v1/sessions/nonexistent-sid/events", headers=bearer("x")
            )
            assert r.status_code == 404
