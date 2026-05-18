"""Integration test: creator_token is one-time only (never re-emitted).

Creates a persisted session and captures the creator_token from the create
response. Then verifies:
  - GET /v1/sessions/{sid} does NOT contain the token string.
  - GET /v1/sessions/{sid}/events does NOT contain the token string.
  - POST /v1/sessions/{sid}/participants (join) does NOT contain the token string.

The creator_token is only disclosed once at creation time and must not
appear in any subsequent response.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_creator_token_not_reemitted(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create persisted session
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            created = r.json()
            sid = created["session_id"]
            token = created.get("creator_token")
            assert token is not None and token != "", "creator_token must be in create response"

            # 1. GET /v1/sessions/{sid} must not expose the token
            r_desc = await c.get(f"{base}/v1/sessions/{sid}")
            assert r_desc.status_code == 200, r_desc.text
            assert token not in r_desc.text, (
                "creator_token leaked in GET /v1/sessions/{sid} response"
            )

            # 2. GET /v1/sessions/{sid}/events must not expose the token
            r_events = await c.get(f"{base}/v1/sessions/{sid}/events")
            # 200 (events page, possibly empty) or 409 (non-persisted would be 409;
            # here it's persisted so we expect 200)
            assert r_events.status_code in (200, 404), r_events.text
            assert token not in r_events.text, (
                "creator_token leaked in GET /v1/sessions/{sid}/events response"
            )

            # 3. POST join must not expose the token
            r_join = await c.post(
                f"{base}/v1/sessions/{sid}/participants",
                json={"role": "worker"},
            )
            assert r_join.status_code == 201, r_join.text
            assert token not in r_join.text, (
                "creator_token leaked in POST /v1/sessions/{sid}/participants response"
            )
