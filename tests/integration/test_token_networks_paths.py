"""Token networks are enforced on every account-token path; account events fire."""
from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosed, InvalidStatus

from sessionweave.accounts import AccountStore
from sessionweave.errors import NETWORK_NOT_APPROVED_MESSAGE
from sessionweave.persistence import PersistenceStore
from tests.integration.accounts_fixtures import ADMIN
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

GOOD = "203.0.113.5"
GOOD2 = "203.0.113.6"
BAD = "198.51.100.77"
UA = "tn-test/1.0"


@dataclass
class Rig:
    base: str
    account_id: str
    bound: str  # approved_only token, approved for GOOD and GOOD2
    bound_id: str
    free: str  # policy "any" (Cockpit-like) token of the same account


@contextlib.asynccontextmanager
async def rig(tmp_path: Path) -> AsyncIterator[Rig]:
    db = str(tmp_path / "tn.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("Alice")
    bound, meta = await accounts.create_token(
        acc.account_id, "bound", network_policy="approved_only",
        approved_networks=[GOOD, GOOD2],
    )
    free, _ = await accounts.create_token(acc.account_id, "cockpit")
    await store.close()
    env = {"SW_ADMIN_TOKEN": ADMIN, "SW_SESSION_CREATION": "account"}
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        yield Rig(base, acc.account_id, bound, meta.token_id, free)


def _h(token: str, ip: str) -> dict[str, str]:
    return {**bearer(token), "X-Forwarded-For": ip, "User-Agent": UA}


def _assert_refused(r: httpx.Response) -> None:
    assert r.status_code == 403, r.text
    err = r.json()["error"]
    assert err["code"] == "network_not_approved"
    assert err["message"] == NETWORK_NOT_APPROVED_MESSAGE
    assert err["message"] == (
        "this token is not approved for your network; "
        "the account owner has been asked to approve it"
    )


async def _events(r: Rig, after: int = 0) -> list[dict[str, Any]]:
    async with httpx.AsyncClient(base_url=r.base) as c:
        resp = await c.get(f"/v1/me/events?after={after}&limit=500", headers=bearer(r.free))
    assert resp.status_code == 200, resp.text
    out: list[dict[str, Any]] = resp.json()["events"]
    return out


async def _kind(r: Rig, kind: str) -> list[dict[str, Any]]:
    return [e for e in await _events(r) if e["kind"] == kind]


async def test_rest_create_refused_then_allowed_and_event(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        _assert_refused(await c.post("/v1/sessions", json={}, headers=_h(r.bound, BAD)))
        pend = await _kind(r, "token.network_pending")
        assert [e["data"]["ip"] for e in pend] == [BAD]
        assert (await _kind(r, "session.created")) == []
        ok = await c.post(
            "/v1/sessions", json={"purpose": {"title": "T1"}}, headers=_h(r.bound, GOOD)
        )
        assert ok.status_code == 201, ok.text
        created = (await _kind(r, "session.created"))[0]["data"]
        assert created["session_id"] == ok.json()["session_id"]
        assert created["title"] == "T1"
        assert created["persistence"] == "none"
        assert created["token"] == {"id": r.bound_id, "label": "bound"}
        assert created["ip"] == GOOD and created["client"] == UA
        assert "creator_token" not in str(created) and "swa_" not in str(created)


async def test_me_routes_refused(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        for path in ("/v1/me", "/v1/me/tokens", "/v1/me/sessions", "/v1/me/events"):
            _assert_refused(await c.get(path, headers=_h(r.bound, BAD)))
            assert (await c.get(path, headers=_h(r.bound, GOOD))).status_code == 200
        _assert_refused(
            await c.post("/v1/me/tokens", json={"label": "x"}, headers=_h(r.bound, BAD))
        )


async def test_owner_as_creator_delete_and_join_unaffected(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        made = (await c.post("/v1/sessions", json={}, headers=_h(r.free, GOOD))).json()
        sid = made["session_id"]
        _assert_refused(await c.delete(f"/v1/sessions/{sid}", headers=_h(r.bound, BAD)))
        # Join / supervisor / creator tokens never depend on networks.
        brief = await c.get(
            f"/invite/{sid}", params={"jt": made["join_token"]},
            headers={"X-Forwarded-For": BAD},
        )
        assert brief.status_code == 200, brief.text
        joined = await c.post(
            f"/v1/sessions/{sid}/participants",
            json={"role": "worker"},
            headers=_h(made["join_token"], BAD),
        )
        assert joined.status_code == 201, joined.text
        assert joined.json()["participant_token"]
        sup = await c.get(
            f"/v1/sessions/{sid}/participants", headers=_h(made["creator_token"], BAD)
        )
        assert sup.status_code == 200, sup.text
        assert (await c.delete(f"/v1/sessions/{sid}", headers=_h(r.bound, GOOD))).status_code == 200
        deleted = (await _kind(r, "session.deleted"))[0]["data"]
        assert deleted == {"session_id": sid, "by": "owner"}


async def test_admin_delete_event(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        sid = (await c.post("/v1/sessions", json={}, headers=_h(r.free, BAD))).json()["session_id"]
        assert (await c.delete(f"/v1/sessions/{sid}", headers=bearer(ADMIN))).status_code == 200
        assert (await _kind(r, "session.deleted"))[0]["data"] == {
            "session_id": sid, "by": "admin",
        }


async def test_mcp_create_and_creator_tool(tmp_path: Path) -> None:
    async with rig(tmp_path) as r:
        async with sw_client(r.base, headers=_h(r.bound, BAD)) as c:
            with pytest.raises(RuntimeError) as ei:
                await c.call_tool("session_create", {})
            assert "network_not_approved" in str(ei.value)
            assert NETWORK_NOT_APPROVED_MESSAGE in str(ei.value)
        async with sw_client(r.base, headers=_h(r.free, GOOD)) as c:
            sid = (await c.call_tool("session_create", {}))["session_id"]
        async with sw_client(r.base, headers=_h(r.bound, BAD)) as c:
            with pytest.raises(RuntimeError) as ei2:
                await c.call_tool("session_mint_join_token", {"session_id": sid})
            assert "network_not_approved" in str(ei2.value)
            assert NETWORK_NOT_APPROVED_MESSAGE in str(ei2.value)
        async with sw_client(r.base, headers=_h(r.bound, GOOD)) as c:
            out = await c.call_tool("session_create", {"purpose": {"title": "viaMCP"}})
            assert out["session_id"]
            assert (await c.call_tool("session_mint_join_token", {"session_id": sid}))[
                "join_token"
            ]
        ev = [e for e in await _kind(r, "session.created") if e["data"]["title"] == "viaMCP"]
        assert len(ev) == 1
        data = ev[0]["data"]
        assert data["ip"] == GOOD and data["token"]["label"] == "bound"
        assert UA in data["client"] and "mcp:" in data["client"]


def _ws(r: Rig, sid: str, token: str) -> str:
    return f"{r.base.replace('http://', 'ws://')}/v1/sessions/{sid}/firehose?token={token}"


async def test_firehose_refused_from_unapproved_network(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        sid = (await c.post("/v1/sessions", json={}, headers=_h(r.free, GOOD))).json()["session_id"]
        with pytest.raises(InvalidStatus) as ei:
            async with websockets.connect(
                _ws(r, sid, r.bound), additional_headers={"X-Forwarded-For": BAD}
            ):
                pass
        assert ei.value.response.status_code == 403
        async with websockets.connect(
            _ws(r, sid, r.bound), additional_headers={"X-Forwarded-For": GOOD}
        ) as ws:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ws.recv(), timeout=0.5)


async def test_removing_a_network_closes_that_stream_only(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        sid = (await c.post("/v1/sessions", json={}, headers=_h(r.free, GOOD))).json()["session_id"]
        async with websockets.connect(
            _ws(r, sid, r.bound), additional_headers={"X-Forwarded-For": GOOD2}
        ) as gone, websockets.connect(
            _ws(r, sid, r.bound), additional_headers={"X-Forwarded-For": GOOD}
        ) as kept:
            put = await c.put(
                f"/v1/me/tokens/{r.bound_id}/networks",
                json={"approved_networks": [GOOD]},
                headers=bearer(r.free),
            )
            assert put.status_code == 200, put.text
            assert put.json() == {
                "network_policy": "approved_only", "approved_networks": [f"{GOOD}/32"],
            }
            with pytest.raises(ConnectionClosed) as closed:
                await asyncio.wait_for(gone.recv(), timeout=3.0)
            assert closed.value.rcvd is not None and closed.value.rcvd.code == 4403
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(kept.recv(), timeout=0.5)
        # tightening the policy to a network-less first_use cuts every stream of the token
        async with websockets.connect(
            _ws(r, sid, r.bound), additional_headers={"X-Forwarded-For": GOOD}
        ) as ws:
            put = await c.put(
                f"/v1/me/tokens/{r.bound_id}/networks",
                json={"network_policy": "approved_only", "approved_networks": []},
                headers=bearer(r.free),
            )
            assert put.status_code == 200
            with pytest.raises(ConnectionClosed):
                await asyncio.wait_for(ws.recv(), timeout=3.0)


async def test_networks_put_validation_is_400(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        for body in (
            {"approved_networks": "10.0.0.0/8"},
            {"approved_networks": [5]},
            {"approved_networks": ["nope"]},
            {"network_policy": "bogus"},
            {"network_policy": 3},
        ):
            resp = await c.put(
                f"/v1/me/tokens/{r.bound_id}/networks", json=body, headers=bearer(r.free)
            )
            assert resp.status_code == 400, (body, resp.text)


async def test_token_and_status_events(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        made = await c.post(
            "/v1/me/tokens", json={"label": "mine"}, headers=_h(r.free, "192.0.2.9")
        )
        assert made.status_code == 201
        tid = made.json()["token_id"]
        adm = await c.post(
            f"/v1/accounts/{r.account_id}/tokens", json={"label": "byadmin"},
            headers={**bearer(ADMIN), "X-Forwarded-For": "192.0.2.10"},
        )
        assert adm.status_code == 201
        aid = adm.json()["token_id"]
        mine = _h(r.free, "192.0.2.9")
        assert (await c.delete(f"/v1/me/tokens/{tid}", headers=mine)).status_code == 204
        # Revoking again is idempotent (204) and emits nothing.
        again = await c.delete(f"/v1/me/tokens/{tid}", headers=mine)
        assert again.status_code == 204, again.text
        # Concurrent admin DELETEs of one token both succeed; one event only.
        url = f"/v1/accounts/{r.account_id}/tokens/{aid}"
        both = await asyncio.gather(
            c.delete(url, headers=bearer(ADMIN)), c.delete(url, headers=bearer(ADMIN))
        )
        assert [x.status_code for x in both] == [204, 204]
        for status in ("disabled", "disabled", "active"):
            assert (
                await c.patch(
                    f"/v1/accounts/{r.account_id}", json={"status": status},
                    headers=bearer(ADMIN),
                )
            ).status_code == 200
        evs = [
            e for e in await _events(r)
            if e["kind"] in ("token.created", "token.revoked", "account.status_changed")
        ]
        by_kind: dict[str, list[dict[str, Any]]] = {}
        for e in evs:
            by_kind.setdefault(e["kind"], []).append(e["data"])
        created = by_kind["token.created"]
        assert {"token": {"id": tid, "label": "mine"}, "by": "self", "ip": "192.0.2.9"} in created
        assert {
            "token": {"id": aid, "label": "byadmin"}, "by": "admin", "ip": "192.0.2.10",
        } in created
        revoked = by_kind["token.revoked"]
        assert [d["by"] for d in revoked if d["token"]["id"] == tid] == ["self"]  # not repeated
        assert [d["by"] for d in revoked if d["token"]["id"] == aid] == ["admin"]
        assert by_kind["account.status_changed"] == [
            {"from": "active", "to": "disabled", "by": "admin"},
            {"from": "disabled", "to": "active", "by": "admin"},
        ]
