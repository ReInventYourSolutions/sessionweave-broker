"""Integration test: DELETE /v1/sessions/{sid} ends a session."""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_succeeds_with_sid_bearer():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            resp = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"authorization": f"Bearer {sid}"},
            )
            assert resp.status_code == 204
            gone = await c.get(f"{base}/v1/sessions/{sid}")
            assert gone.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_rejects_missing_auth():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            resp = await c.delete(f"{base}/v1/sessions/{sid}")
            assert resp.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_rejects_wrong_token():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            resp = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"authorization": "Bearer some-other-id"},
            )
            assert resp.status_code == 403
