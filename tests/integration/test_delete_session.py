"""DELETE /v1/sessions/{sid} ends a session.

Allowed: the per-session ``creator_token`` or the relay-wide ``SW_ADMIN_TOKEN``.
Supervisor, participant and join tokens and the session id are refused.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess

ADMIN = "delete-admin-secret-0123"


def _h(token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {token}"}


async def _create(c: httpx.AsyncClient, base: str) -> dict:
    r = await c.post(f"{base}/v1/sessions", json={"purpose": {"kind": "smoke"}})
    assert r.status_code == 201, r.text
    return r.json()


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("who", ["creator_token", "admin"])
async def test_delete_session_succeeds_for_creator_and_admin(who: str):
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            sid = made["session_id"]
            token = ADMIN if who == "admin" else made[who]
            resp = await c.delete(f"{base}/v1/sessions/{sid}", headers=_h(token))
            assert resp.status_code == 200
            assert "deleted_at" in resp.json()
            gone = await c.get(f"{base}/v1/sessions/{sid}", headers=_h(ADMIN))
            assert gone.status_code == 404


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_rejects_missing_auth():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            sid = (await _create(c, base))["session_id"]
            resp = await c.delete(f"{base}/v1/sessions/{sid}")
            assert resp.status_code == 401


@pytest.mark.integration
@pytest.mark.asyncio
async def test_delete_session_refused_for_every_other_credential():
    async with relay_subprocess() as base:
        async with httpx.AsyncClient() as c:
            made = await _create(c, base)
            sid = made["session_id"]
            joined = (
                await c.post(
                    f"{base}/v1/sessions/{sid}/participants",
                    headers=_h(made["join_token"]), json={"role": "w"},
                )
            ).json()
            for bad in ("some-other-id", sid, made["supervisor_token"],
                        made["join_token"], joined["participant_token"]):
                resp = await c.delete(f"{base}/v1/sessions/{sid}", headers=_h(bad))
                assert resp.status_code == 403, (bad, resp.status_code)
            # still alive
            ok = await c.get(f"{base}/v1/sessions/{sid}", headers=_h(made["creator_token"]))
            assert ok.status_code == 200
