"""Integration test: persisted session create → delete e2e.

Verifies:
  - POST /v1/sessions with persistence returns creator_token and persistence.mode.
  - DELETE with wrong token → 403.
  - DELETE with correct creator_token → 200 with deleted_at.
  - GET after deletion → 404.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_persistence_create_delete_e2e(tmp_path: object) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=10.0) as c:
            # 1. Create persisted session
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            created = r.json()
            sid = created["session_id"]
            token = created.get("creator_token")

            # creator_token must be present
            assert token is not None and token != ""

            # persistence block must show mode=ttl
            assert created.get("persistence") is not None
            assert created["persistence"]["mode"] == "ttl"

            # 2. DELETE with wrong token → 403
            r_wrong = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"Authorization": "Bearer wrong-token"},
            )
            assert r_wrong.status_code == 403, r_wrong.text

            # Session still exists
            r_get = await c.get(f"{base}/v1/sessions/{sid}", headers=bearer(token))
            assert r_get.status_code == 200

            # 3. DELETE with correct creator_token → 200 with deleted_at
            r_del = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"Authorization": f"Bearer {token}"},
            )
            assert r_del.status_code == 200, r_del.text
            body = r_del.json()
            assert "deleted_at" in body

            # 4. GET after deletion → 404
            r_gone = await c.get(f"{base}/v1/sessions/{sid}", headers=bearer(token))
            assert r_gone.status_code == 404, r_gone.text
