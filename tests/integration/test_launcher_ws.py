"""Launcher WebSocket connect, auth in both modes, and coordinator-gated discovery."""
from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from tests.integration.accounts_fixtures import ADMIN, seeded_relay
from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DENIED = "only the session's coordinator can request agents — ask the coordinator"


def _ws(base: str) -> str:
    return f"ws://{urlparse(base).netloc}/v1/launchers/connect"


def _prof(pid: str = "p1", domain: str = "backend", **kw: Any) -> dict[str, Any]:
    return {"id": pid, "domain": domain, "role": "analyst", "capabilities": ["sql"],
            "description": "d", "runtime": "claude", "model": "m",
            "access": {"preset": "reader"}, "limits": {}, "max_instances": 2, **kw}


def _hello(profiles: list[Any], **kw: Any) -> str:
    return json.dumps({"type": "hello", "machine": "box", "visibility": "personal",
                       "allowed_accounts": [], "profiles": profiles, **kw})


async def _connect(base: str, token: str | None, hello: str) -> tuple[Any, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    ws = await websockets.connect(_ws(base), additional_headers=headers)
    await ws.send(hello)
    return ws, json.loads(await ws.recv())


async def _closed_code(base: str, token: str | None) -> tuple[int, str]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    async with websockets.connect(_ws(base), additional_headers=headers) as ws:
        try:
            await ws.send(_hello([]))
            await ws.recv()
        except ConnectionClosed:
            pass
        await ws.wait_closed()
        assert ws.close_code is not None
        return ws.close_code, ws.close_reason or ""


async def _session(c: httpx.AsyncClient, cred: str | None = None) -> dict[str, Any]:
    r = await c.post("/v1/sessions", json={}, headers=bearer(cred) if cred else {})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _join(c: httpx.AsyncClient, sid: str, cred: str) -> dict[str, Any]:
    r = await c.post(f"/v1/sessions/{sid}/participants", headers=bearer(cred),
                     json={"role": "coord"})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def test_open_mode_register_discover_disconnect() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        assert (await c.get("/v1/info")).json()["capabilities"]["launchers"] is True
        made = await _session(c)
        sid, creator, jt = made["session_id"], made["creator_token"], made["join_token"]
        coord = await _join(c, sid, creator)
        other = await _join(c, sid, jt)
        profiles = [_prof(), _prof(), _prof("bad", runtime="x")]
        ws, welcome = await _connect(base, None, _hello(profiles))
        async with ws:
            assert welcome["type"] == "welcome" and welcome["launcher_id"].startswith("lnc_")
            assert [r["id"] for r in welcome["rejected"]] == ["p1", "bad"]
            q = {"session_id": sid}
            for cred in (coord["participant_token"], creator):
                r = await c.get("/v1/agents/domains", params=q, headers=bearer(cred))
                assert r.json() == {"domains": ["backend"]}
            r = await c.get("/v1/agents", params={**q, "domain": "backend"},
                            headers=bearer(coord["participant_token"]))
            item = r.json()["agents"][0]
            assert item["launcher_id"] == welcome["launcher_id"] and item["machine"] == "box"
            assert item["free_slots"] == 2 and item["online"] is True
            assert "workdir" not in item and "start_prompt" not in item
            r = await c.get("/v1/agents", params={**q, "domain": "backend", "role": "nope"},
                            headers=bearer(creator))
            assert r.json() == {"agents": []}
            # non-coordinator participant and join token are refused
            for cred in (other["participant_token"], jt):
                r = await c.get("/v1/agents/domains", params=q, headers=bearer(cred))
                assert r.status_code == 403, r.text
            r = await c.get("/v1/agents/domains", params=q,
                            headers=bearer(other["participant_token"]))
            assert r.json()["error"] == {"code": "permission_denied", "message": DENIED}
            r = await c.get("/v1/agents", params=q, headers=bearer(creator))
            assert r.status_code == 400
        # socket closed: launcher gone
        for _ in range(50):
            r = await c.get("/v1/agents/domains", params={"session_id": sid},
                            headers=bearer(creator))
            if r.json() == {"domains": []}:
                break
            await asyncio.sleep(0.1)
        assert r.json() == {"domains": []}


async def test_open_mode_launcher_token() -> None:
    async with relay_subprocess(env_overrides={"SW_LAUNCHER_TOKEN": "lt-secret"}) as base:
        assert (await _closed_code(base, None))[0] == 4401
        assert (await _closed_code(base, "wrong"))[0] == 4401
        ws, welcome = await _connect(base, "lt-secret", _hello([_prof()]))
        async with ws:
            assert welcome["type"] == "welcome"


async def test_bad_hello_closes_4400() -> None:
    async with relay_subprocess() as base:
        ws = await websockets.connect(_ws(base))
        await ws.send(json.dumps({"type": "hello", "machine": "", "profiles": []}))
        with pytest.raises(ConnectionClosed):
            await ws.recv()
        assert ws.close_code == 4400


async def test_account_mode_auth_and_visibility(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s:
        assert (await _closed_code(s.base, None))[0] == 4401
        assert (await _closed_code(s.base, "bogus"))[0] == 4401
        assert (await _closed_code(s.base, s.revoked_token))[0] == 4401
        async with httpx.AsyncClient(base_url=s.base) as c:
            alice_sess = await _session(c, s.token)
            bob_sess = await _session(c, s.other_token)
            ws, w1 = await _connect(s.base, s.token, _hello([_prof("pers")], machine="a"))
            ws2, w2 = await _connect(
                s.base, s.token, _hello([_prof("gen")], visibility="general", machine="b"))
            async with ws, ws2:
                assert w1["type"] == w2["type"] == "welcome"

                async def ids(sess: dict[str, Any], cred: str) -> set[str]:
                    r = await c.get("/v1/agents", params={
                        "session_id": sess["session_id"], "domain": "backend"},
                        headers=bearer(cred))
                    assert r.status_code == 200, r.text
                    return {i["profile_id"] for i in r.json()["agents"]}

                assert await ids(alice_sess, s.token) == {"pers", "gen"}
                assert await ids(bob_sess, s.other_token) == {"gen"}
                # admin acts for the session too
                assert await ids(bob_sess, ADMIN) == {"gen"}


async def test_account_mode_allowed_accounts_by_name(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s:
        async with httpx.AsyncClient(base_url=s.base) as c:
            bob_sess = await _session(c, s.other_token)
            ws, _ = await _connect(s.base, s.token, _hello(
                [_prof()], visibility="general", allowed_accounts=["bob"]))
            ws2, _ = await _connect(s.base, s.token, _hello(
                [_prof("x")], visibility="general", allowed_accounts=["carol"]))
            async with ws, ws2:
                r = await c.get("/v1/agents", params={
                    "session_id": bob_sess["session_id"], "domain": "backend"},
                    headers=bearer(s.other_token))
                assert [i["profile_id"] for i in r.json()["agents"]] == ["p1"]


async def test_account_token_network_not_approved(tmp_path: Path) -> None:
    from sessionweave.accounts import AccountStore
    from sessionweave.persistence import PersistenceStore

    db = str(tmp_path / "net.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("Net")
    token, _ = await accounts.create_token(
        acc.account_id, "n", network_policy="approved_only", approved_networks=["10.99.0.0/16"]
    )
    await store.close()
    async with relay_subprocess(
        env_overrides={"SW_SESSION_CREATION": "account", "SW_ADMIN_TOKEN": ADMIN},
        persistence_db_path=db,
    ) as base:
        code, reason = await _closed_code(base, token)
        assert code == 4403 and reason == "network not approved"
