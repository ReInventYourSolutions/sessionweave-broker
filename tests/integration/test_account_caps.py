from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from tests.integration.accounts_fixtures import ADMIN, Seeded, seeded_relay
from tests.integration.client import bearer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    caps = {"SW_ACCOUNT_MAX_SESSIONS": "2", "SW_ACCOUNT_MAX_TOKENS": "3"}
    async with seeded_relay(tmp_path, mode="account", env_extra=caps) as s:
        yield s


async def test_session_cap_counts_live_sessions_and_admin_creations(relay: Seeded) -> None:
    async with httpx.AsyncClient(base_url=relay.base) as c:
        r1 = await c.post("/v1/sessions", json={}, headers=bearer(relay.token))
        assert r1.status_code == 201
        # Admin-created session for the account counts too.
        r2 = await c.post(
            "/v1/sessions",
            json={"owner_account_id": relay.acc.account_id},
            headers=bearer(ADMIN),
        )
        assert r2.status_code == 201
        r3 = await c.post("/v1/sessions", json={}, headers=bearer(relay.token))
        assert r3.status_code == 429
        assert r3.json()["error"]["code"] == "account_session_limit"
        assert "2" in r3.json()["error"]["message"]
        r4 = await c.post(
            "/v1/sessions",
            json={"owner_account_id": relay.acc.account_id},
            headers=bearer(ADMIN),
        )
        assert r4.status_code == 429
        # Another account is unaffected.
        r5 = await c.post("/v1/sessions", json={}, headers=bearer(relay.other_token))
        assert r5.status_code == 201
        # Ending a session frees a slot.
        sid = r1.json()["session_id"]
        assert (
            await c.delete(f"/v1/sessions/{sid}", headers=bearer(relay.token))
        ).status_code in (200, 204)
        r6 = await c.post("/v1/sessions", json={}, headers=bearer(relay.token))
        assert r6.status_code == 201


async def test_token_cap_ignores_revoked_and_applies_to_both_routes(relay: Seeded) -> None:
    aid = relay.acc.account_id
    async with httpx.AsyncClient(base_url=relay.base) as c:
        # Seeded: one active token (+ one revoked, which does not count) -> 1/3.
        minted = []
        for i in range(2):
            r = await c.post("/v1/me/tokens", json={"label": f"m{i}"}, headers=bearer(relay.token))
            assert r.status_code == 201, r.text
            minted.append(r.json())
        r = await c.post("/v1/me/tokens", json={"label": "x"}, headers=bearer(relay.token))
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "account_token_limit"
        r = await c.post(f"/v1/accounts/{aid}/tokens", json={"label": "x"}, headers=bearer(ADMIN))
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "account_token_limit"
        # Revoking frees a slot.
        r = await c.delete(
            f"/v1/me/tokens/{minted[0]['token_id']}", headers=bearer(relay.token)
        )
        assert r.status_code == 204
        r = await c.post(f"/v1/accounts/{aid}/tokens", json={"label": "y"}, headers=bearer(ADMIN))
        assert r.status_code == 201
