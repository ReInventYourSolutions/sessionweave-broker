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
from tests.integration.client import bearer, sw_client
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


async def _mint(c: httpx.AsyncClient, account_id: str, **body: Any) -> dict[str, Any]:
    r = await c.post(f"/v1/accounts/{account_id}/tokens", headers=bearer(ADMIN),
                     json={"label": "launcher", **body})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _closed_by_broker(ws: Any) -> tuple[int | None, str]:
    with pytest.raises(ConnectionClosed):
        for _ in range(100):
            await asyncio.wait_for(ws.recv(), 5)
    return ws.close_code, ws.close_reason or ""


async def _launch_pending(c: httpx.AsyncClient, base: str, cred: str, lnc_id: str) -> tuple[
        str, str, str]:
    sess = await _session(c, cred)
    sid = sess["session_id"]
    coord = await _join(c, sid, cred)
    r = await c.post(f"/v1/sessions/{sid}/agents", headers=bearer(coord["participant_token"]),
                     json={"launcher_id": lnc_id, "profile_id": "p1", "task": "t"})
    assert r.status_code == 202, r.text
    return sid, coord["participant_token"], r.json()["launch_id"]


async def _launch_status(c: httpx.AsyncClient, sid: str, cred: str, lid: str) -> str:
    for _ in range(100):
        r = await c.get(f"/v1/sessions/{sid}/agents/{lid}", headers=bearer(cred))
        if r.json()["status"] == "failed":
            break
        await asyncio.sleep(0.05)
    assert r.json()["error"] == "launcher_offline", r.text
    return str(r.json()["status"])


async def test_launcher_dropped_when_token_revoked_or_account_disabled(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        minted = await _mint(c, s.acc.account_id)
        keep = await _mint(c, s.acc.account_id)
        ws, w = await _connect(s.base, minted["token"], _hello([_prof()]))
        ws_keep, _ = await _connect(s.base, keep["token"], _hello([_prof("k")]))
        sid, coord, lid = await _launch_pending(c, s.base, s.token, w["launcher_id"])
        r = await c.delete(f"/v1/accounts/{s.acc.account_id}/tokens/{minted['token_id']}",
                           headers=bearer(ADMIN))
        assert r.status_code in (200, 204), r.text
        code, reason = await _closed_by_broker(ws)
        assert code == 4403 and reason == "account token revoked"
        assert await _launch_status(c, sid, coord, lid) == "failed"
        # the other token's launcher stays
        r = await c.get("/v1/agents", params={"session_id": sid, "domain": "backend"},
                        headers=bearer(coord))
        assert [a["profile_id"] for a in r.json()["agents"]] == ["k"]
        # disabling the account drops it too
        r = await c.patch(f"/v1/accounts/{s.acc.account_id}", headers=bearer(ADMIN),
                          json={"status": "disabled"})
        assert r.status_code == 200, r.text
        code, reason = await _closed_by_broker(ws_keep)
        assert code == 4403 and reason == "account not active"


async def test_launcher_dropped_when_account_deleted(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _connect(s.base, s.other_token, _hello([_prof()]))
        r = await c.delete(f"/v1/accounts/{s.other.account_id}", headers=bearer(ADMIN))
        assert r.status_code in (200, 204), r.text
        code, reason = await _closed_by_broker(ws)
        assert code == 4403 and reason == "account deleted"


async def test_launcher_dropped_when_network_no_longer_approved(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        minted = await _mint(c, s.acc.account_id)
        ws, _ = await _connect(s.base, minted["token"], _hello([_prof()]))
        # a policy that still allows 127.0.0.1 keeps it
        url = f"/v1/accounts/{s.acc.account_id}/tokens/{minted['token_id']}/networks"
        r = await c.put(url, headers=bearer(ADMIN), json={
            "network_policy": "approved_only", "approved_networks": ["127.0.0.0/8"]})
        assert r.status_code == 200, r.text
        await ws.send(json.dumps({"type": "pong"}))
        r = await c.put(url, headers=bearer(ADMIN), json={
            "network_policy": "approved_only", "approved_networks": ["10.99.0.0/16"]})
        assert r.status_code == 200, r.text
        code, reason = await _closed_by_broker(ws)
        assert code == 4403 and reason == "network not approved"


async def test_launcher_caps_per_account(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s:
        conns = [await _connect(s.base, s.token, _hello([_prof()])) for _ in range(5)]
        assert all(w["type"] == "welcome" for _, w in conns)
        code, reason = await _closed_code(s.base, s.token)
        assert code == 4429 and "5 launchers" in reason
        # another account is not affected
        ws, w = await _connect(s.base, s.other_token, _hello([_prof()]))
        assert w["type"] == "welcome"
        for c, _ in [*conns, (ws, None)]:
            await c.close()


async def test_failed_launcher_auth_is_throttled() -> None:
    async with relay_subprocess(env_overrides={"SW_LAUNCHER_TOKEN": "lt-secret"}) as base:
        for _ in range(5):
            assert (await _closed_code(base, "wrong"))[0] == 4401
        code, reason = await _closed_code(base, "wrong")
        assert code == 4429 and reason == "too many failed attempts"
        # a valid token still passes
        ws, welcome = await _connect(base, "lt-secret", _hello([_prof()]))
        async with ws:
            assert welcome["type"] == "welcome"


async def test_launcher_frame_too_large_closes_1009() -> None:
    async with relay_subprocess() as base:
        ws, _ = await _connect(base, None, _hello([_prof()]))
        await ws.send(json.dumps({"type": "pong", "pad": "x" * (256 * 1024)}))
        code, _reason = await _closed_by_broker(ws)
        assert code == 1009


async def test_mcp_agents_discover_end_to_end() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        made = await _session(c)
        sid = made["session_id"]
        ws, _ = await _connect(base, None, _hello([_prof()]))
        async with ws, sw_client(base) as agent, sw_client(base) as other:
            j = await agent.call_tool("session_join", {
                "session_id": sid, "join_token": made["creator_token"], "role": "c"})
            out = await agent.call_tool("agents_discover", {
                "session_id": sid, "domain": "backend",
                "participant_token": j["participant_token"]})
            assert [a["profile_id"] for a in out["agents"]] == ["p1"]
            j2 = await other.call_tool("session_join", {
                "session_id": sid, "join_token": made["join_token"], "role": "w"})
            with pytest.raises(RuntimeError, match="permission_denied"):
                await other.call_tool("agents_discover", {
                    "session_id": sid, "domain": "backend",
                    "participant_token": j2["participant_token"]})


async def test_launcher_hello_too_large_closes_1009() -> None:
    async with relay_subprocess() as base:
        ws = await websockets.connect(_ws(base))
        await ws.send(_hello([], pad="x" * (256 * 1024)))
        code, _reason = await _closed_by_broker(ws)
        assert code == 1009


async def test_launcher_closed_when_account_token_expires(tmp_path: Path) -> None:
    from datetime import UTC, datetime, timedelta

    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        soon = (datetime.now(UTC) + timedelta(seconds=3)).isoformat()
        minted = await _mint(c, s.acc.account_id, expires_at=soon)
        ws, w = await _connect(s.base, minted["token"], _hello([_prof()]))
        assert w["type"] == "welcome" and w["account"] == "Alice"
        code, reason = await _closed_by_broker(ws)
        assert code == 4401 and reason == "token expired"
        sess = await _session(c, s.token)
        coord = await _join(c, sess["session_id"], s.token)
        r = await c.get("/v1/agents", params={"session_id": sess["session_id"],
                                              "domain": "backend"},
                        headers=bearer(coord["participant_token"]))
        assert r.json()["agents"] == []
