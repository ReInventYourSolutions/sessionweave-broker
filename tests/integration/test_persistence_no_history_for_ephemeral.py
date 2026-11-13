"""Integration test: GET /v1/sessions/{sid}/events on an ephemeral session returns 409.

An ephemeral session (created without the `persistence` field) has no event
history. The relay must respond with HTTP 409 and error code `no_history`.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_no_history_for_ephemeral_session(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create an EPHEMERAL session (no persistence field)
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            creator = r.json()["creator_token"]
            # GET /v1/sessions/{sid}/events → must return 409 no_history
            r_events = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(creator)
            )
            assert r_events.status_code == 409, (
                f"Expected 409 for ephemeral session events,"
                f" got {r_events.status_code}: {r_events.text}"
            )
            body = r_events.json()
            # Error body should contain "no_history" in code or nested error object
            error = body.get("error", body)
            if isinstance(error, dict):
                code = error.get("code", "")
            else:
                code = str(error)
            assert "no_history" in code.lower(), (
                f"Expected error code 'no_history', got: {body}"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_no_history_for_ephemeral_session_no_persistence_db(tmp_path: object) -> None:
    """Even without a persistence_db_path set at all, ephemeral sessions return 409."""
    # Spawn relay without persistence enabled
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]
            creator = r.json()["creator_token"]
            r_events = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(creator)
            )
            assert r_events.status_code == 409, (
                f"Expected 409 for ephemeral session events,"
                f" got {r_events.status_code}: {r_events.text}"
            )
            body = r_events.json()
            error = body.get("error", body)
            if isinstance(error, dict):
                code = error.get("code", "")
            else:
                code = str(error)
            assert "no_history" in code.lower(), (
                f"Expected error code 'no_history', got: {body}"
            )
