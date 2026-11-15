"""Accounts REST API, ``/v1/me`` and account-lifecycle effects over HTTP."""
from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.accounts_fixtures import ADMIN, Seeded, seeded_relay
from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with seeded_relay(tmp_path, mode="account") as s:
        yield s


async def _create_session(s: Seeded, token: str) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post("/v1/sessions", json={}, headers=bearer(token))
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def test_rest_happy_path(tmp_path: Path) -> None:
    env = {"SW_ADMIN_TOKEN": ADMIN, "SW_SESSION_CREATION": "account"}
    db = str(tmp_path / "fresh.db")
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            r = await c.post("/v1/accounts", json={"name": "alice"}, headers=bearer(ADMIN))
            assert r.status_code == 201, r.text
            acc = r.json()
            assert acc["name"] == "alice" and acc["status"] == "active"
            aid = acc["account_id"]
            r = await c.get("/v1/accounts", headers=bearer(ADMIN))
            assert [a["account_id"] for a in r.json()["accounts"]] == [aid]
            r = await c.post(
                f"/v1/accounts/{aid}/tokens", json={"label": "laptop"}, headers=bearer(ADMIN)
            )
            assert r.status_code == 201, r.text
            tok = r.json()
            assert tok["token"].startswith("swa_") and tok["label"] == "laptop"
            assert tok["expires_at"] is None
            r = await c.get(f"/v1/accounts/{aid}", headers=bearer(ADMIN))
            assert [t["token_id"] for t in r.json()["tokens"]] == [tok["token_id"]]
            assert "token" not in r.json()["tokens"][0]
            r = await c.get("/v1/me", headers=bearer(tok["token"]))
            assert r.status_code == 200, r.text
            assert r.json()["account_id"] == aid
            made = await c.post("/v1/sessions", json={}, headers=bearer(tok["token"]))
            assert made.status_code == 201, made.text
            r = await c.get("/v1/me/sessions", headers=bearer(tok["token"]))
            assert r.status_code == 200
            assert r.json()["sessions"] == [
                {
                    "session_id": made.json()["session_id"],
                    "suspended": False,
                    "persisted": False,
                    "purpose": None,
                }
            ]


async def test_me_sessions_lists_cold_persisted_once(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(
            "/v1/sessions",
            json={
                "purpose": {"kind": "k"},
                "persistence": {"mode": "ttl", "ttl_seconds": 7200},
            },
            headers=bearer(s.token),
        )
        assert r.status_code == 201, r.text
        sid = r.json()["session_id"]
        r = await c.get("/v1/me/sessions", headers=bearer(s.token))
        entries = r.json()["sessions"]
        assert [e["session_id"] for e in entries] == [sid]
        assert entries[0]["persisted"] is True and entries[0]["purpose"] == {"kind": "k"}
        r = await c.get("/v1/me/sessions", headers=bearer(s.other_token))
        assert r.json()["sessions"] == []


async def test_disable_suspends_and_enable_resumes(relay: Seeded) -> None:
    s = relay
    made = await _create_session(s, s.token)
    sid, creator, jt = made["session_id"], made["creator_token"], made["join_token"]
    aid = s.acc.account_id
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(
            f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "w"}
        )
        assert r.status_code == 201, r.text
        ptoken = r.json()["participant_token"]

        r = await c.patch(f"/v1/accounts/{aid}", json={"status": "disabled"}, headers=bearer(ADMIN))
        assert r.status_code == 200, r.text
        assert r.json()["status"] == "disabled" and r.json()["sessions_affected"] == 1

        r = await c.post(f"/v1/sessions/{sid}/participants/me/inbox/drain", headers=bearer(ptoken))
        halts = [
            m for m in r.json()["messages"]
            if m["name"] == "supervisor.note" and m["payload"].get("kind") == "halt"
        ]
        assert len(halts) == 1
        assert (await c.get("/v1/me", headers=bearer(s.token))).status_code == 401
        r = await c.get(f"/v1/sessions/{sid}", headers=bearer(creator))
        assert r.status_code == 200 and r.json()["suspended"] is True
        r = await c.post(
            f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "w2"}
        )
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "session_suspended"

        r = await c.patch(f"/v1/accounts/{aid}", json={"status": "active"}, headers=bearer(ADMIN))
        assert r.status_code == 200 and r.json()["sessions_affected"] == 1
        assert (await c.get("/v1/me", headers=bearer(s.token))).status_code == 200
        r = await c.post(
            f"/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "w2"}
        )
        assert r.status_code == 201, r.text


async def test_owner_token_refused_after_disable_or_revoke(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(
            f"/v1/accounts/{s.acc.account_id}/tokens", json={"label": "x"}, headers=bearer(ADMIN)
        )
        tok = r.json()
        sid = (await _create_session(s, tok["token"]))["session_id"]
        r = await c.delete(
            f"/v1/accounts/{s.acc.account_id}/tokens/{tok['token_id']}", headers=bearer(ADMIN)
        )
        assert r.status_code == 204
        r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(tok["token"]))
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"

        # The remaining (unrevoked) token stops acting as creator once disabled.
        r = await c.patch(
            f"/v1/accounts/{s.acc.account_id}", json={"status": "disabled"}, headers=bearer(ADMIN)
        )
        assert r.status_code == 200
        r = await c.delete(f"/v1/sessions/{sid}", headers=bearer(s.token))
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"


async def test_delete_account_deletes_sessions(relay: Seeded) -> None:
    s = relay
    sid = (await _create_session(s, s.token))["session_id"]
    creator = bearer(s.token)
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.delete(f"/v1/accounts/{s.acc.account_id}", headers=bearer(ADMIN))
        assert r.status_code == 200, r.text
        assert r.json() == {"deleted_sessions": 1}
        r = await c.get(f"/v1/sessions/{sid}", headers=bearer(ADMIN))
        assert r.status_code == 404
        assert (await c.get("/v1/me", headers=creator)).status_code == 401
        r = await c.delete(f"/v1/accounts/{s.acc.account_id}", headers=bearer(ADMIN))
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "account_not_found"


async def test_self_cannot_touch_others(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.get("/v1/accounts", headers=bearer(s.token))
        assert r.status_code == 403
        assert r.json()["error"]["code"] == "permission_denied"
        r = await c.get("/v1/me", headers=bearer(s.other_token))
        bob_token_id = r.json()["tokens"][0]["token_id"]
        r = await c.delete(f"/v1/me/tokens/{bob_token_id}", headers=bearer(s.token))
        assert r.status_code == 404
        assert r.json()["error"]["code"] == "token_not_found"
        assert (await c.get("/v1/me", headers=bearer(s.other_token))).status_code == 200


async def test_self_token_management(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post("/v1/me/tokens", json={"label": "ci"}, headers=bearer(s.token))
        assert r.status_code == 201, r.text
        new = r.json()
        r = await c.get("/v1/me/tokens", headers=bearer(new["token"]))
        assert new["token_id"] in [t["token_id"] for t in r.json()["tokens"]]
        r = await c.delete(f"/v1/me/tokens/{new['token_id']}", headers=bearer(s.token))
        assert r.status_code == 204
        assert (await c.get("/v1/me", headers=bearer(new["token"]))).status_code == 401


async def test_auth_errors(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.get("/v1/accounts")
        assert r.status_code == 401
        assert r.json() == {
            "error": {"code": "unauthorized", "message": "missing Authorization: Bearer header"}
        }
        r = await c.get("/v1/accounts", headers=bearer("nope"))
        assert r.status_code == 403
        r = await c.get("/v1/me")
        assert r.status_code == 401
        assert r.json()["error"]["code"] == "invalid_account_token"
        r = await c.get("/v1/me", headers=bearer(s.pending_token))
        assert r.status_code == 401


async def test_name_taken_409(relay: Seeded) -> None:
    async with httpx.AsyncClient(base_url=relay.base) as c:
        r = await c.post("/v1/accounts", json={"name": "Alice"}, headers=bearer(ADMIN))
        assert r.status_code == 409
        assert r.json()["error"]["code"] == "account_name_taken"


async def test_expired_token_401(relay: Seeded) -> None:
    s = relay
    past = (datetime.now(UTC) - timedelta(hours=1)).isoformat()
    soon = (datetime.now(UTC) + timedelta(hours=1)).isoformat().replace("+00:00", "Z")
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(
            f"/v1/accounts/{s.acc.account_id}/tokens",
            json={"label": "old", "expires_at": past},
            headers=bearer(ADMIN),
        )
        assert r.status_code == 400
        assert r.json()["error"]["code"] == "invalid_request"
        r = await c.post(
            f"/v1/accounts/{s.acc.account_id}/tokens",
            json={"label": "ok", "expires_at": soon},
            headers=bearer(ADMIN),
        )
        assert r.status_code == 201, r.text
        assert isinstance(r.json()["expires_at"], int)
        assert (await c.get("/v1/me", headers=bearer(r.json()["token"]))).status_code == 200
        r = await c.post(
            f"/v1/accounts/{s.acc.account_id}/tokens",
            json={"label": "bad", "expires_at": "tomorrow"},
            headers=bearer(ADMIN),
        )
        assert r.status_code == 400


async def test_pending_account_and_patch_validation(relay: Seeded) -> None:
    s = relay
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.patch(
            f"/v1/accounts/{s.acc.account_id}", json={"status": "bogus"}, headers=bearer(ADMIN)
        )
        assert r.status_code == 400
        r = await c.patch(
            f"/v1/accounts/{s.acc.account_id}",
            json={"name": "Alice2", "contact": "a@x"},
            headers=bearer(ADMIN),
        )
        assert r.status_code == 200
        assert r.json()["name"] == "Alice2" and r.json()["sessions_affected"] == 0
        r = await c.patch("/v1/accounts/acc_nope", json={"name": "z"}, headers=bearer(ADMIN))
        assert r.status_code == 404


async def test_accounts_unavailable_without_db() -> None:
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": ADMIN}) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            for method, path in (("GET", "/v1/accounts"), ("GET", "/v1/me")):
                r = await c.request(method, path)
                assert r.status_code == 503, path
                assert r.json()["error"]["code"] == "accounts_unavailable"


async def test_mcp_snippet_account_mode(relay: Seeded) -> None:
    async with httpx.AsyncClient(base_url=relay.base) as c:
        snip = (await c.get("/connect.mcp.json")).json()
    assert snip["mcpServers"]["sessionweave"]["headers"] == {
        "Authorization": "Bearer ${SW_ACCOUNT_TOKEN}"
    }
