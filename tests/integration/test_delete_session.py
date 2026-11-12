"""Integration test: DELETE /v1/sessions/{sid} ends a session.

Auth (sub-project #2): Bearer accepts the per-session ``creator_token``
returned at create, OR the relay-wide ``SW_ADMIN_TOKEN``.
The legacy "token == session_id" path was retired in Task 14.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_succeeds_with_creator_token():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            created = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()
            sid = created["session_id"]
            token = created["creator_token"]
            resp = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"authorization": f"Bearer {token}"},
            )
            assert resp.status_code == 200
            assert "deleted_at" in resp.json()
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


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_rejects_legacy_sid_bearer():
    """The pre-Task-14 ``Bearer ${session_id}`` auth path is no longer
    accepted. This test pins the breaking change so a future revert is
    immediately visible."""
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (
                await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
            ).json()["session_id"]
            resp = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"authorization": f"Bearer {sid}"},
            )
            assert resp.status_code == 403
