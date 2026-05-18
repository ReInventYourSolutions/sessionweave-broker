"""Integration test: GET /v1/sessions/{sid} includes persistence state.

Verifies:
  - Persisted session (ttl) → describe returns persistence.mode == "ttl".
  - Ephemeral session (no persistence) → describe returns persistence == null or
    the key is absent.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_describe_persisted_session_includes_persistence_block(
    tmp_path: object,
) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create persisted session
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]

            # Describe
            r_desc = await c.get(f"{base}/v1/sessions/{sid}")
            assert r_desc.status_code == 200, r_desc.text
            body = r_desc.json()

            # persistence key must be present
            assert "persistence" in body, (
                f"persistence key missing from describe response: {body}"
            )
            p = body["persistence"]
            assert p is not None, "persistence must not be null for a persisted session"
            assert p.get("mode") == "ttl", (
                f"Expected persistence.mode == 'ttl', got: {p}"
            )


@pytest.mark.integration
@pytest.mark.asyncio
async def test_describe_ephemeral_session_persistence_null(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # Create ephemeral session
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            sid = r.json()["session_id"]

            # Describe
            r_desc = await c.get(f"{base}/v1/sessions/{sid}")
            assert r_desc.status_code == 200, r_desc.text
            body = r_desc.json()

            # persistence must be null/None or absent
            p = body.get("persistence")
            assert p is None, (
                f"Ephemeral session describe returned non-null persistence: {p}"
            )
