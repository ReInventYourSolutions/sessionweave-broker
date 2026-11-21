"""REST surface of token networks and approvals: self + admin routes."""
from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pytest

from sessionweave.accounts import AccountStore
from sessionweave.persistence import PersistenceStore
from tests.integration.accounts_fixtures import ADMIN
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

GOOD = "203.0.113.5"
BAD = "198.51.100.77"
UA = "tn-api/1.0"


@dataclass
class Rig:
    base: str
    a_id: str
    a_free: str
    a_bound: str
    a_bound_id: str
    b_id: str
    b_free: str
    b_bound: str
    b_bound_id: str


@contextlib.asynccontextmanager
async def rig(tmp_path: Path) -> AsyncIterator[Rig]:
    db = str(tmp_path / "tna.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    a = await accounts.create("Alice")
    b = await accounts.create("Bob")
    a_free, _ = await accounts.create_token(a.account_id, "cockpit")
    b_free, _ = await accounts.create_token(b.account_id, "cockpit")
    a_bound, am = await accounts.create_token(
        a.account_id, "bound", network_policy="approved_only", approved_networks=[GOOD]
    )
    b_bound, bm = await accounts.create_token(
        b.account_id, "bound", network_policy="approved_only", approved_networks=[GOOD]
    )
    await store.close()
    env = {"SW_ADMIN_TOKEN": ADMIN, "SW_SESSION_CREATION": "account"}
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        yield Rig(
            base, a.account_id, a_free, a_bound, am.token_id,
            b.account_id, b_free, b_bound, bm.token_id,
        )


def _h(token: str, ip: str) -> dict[str, str]:
    return {**bearer(token), "X-Forwarded-For": ip, "User-Agent": UA}


async def _pend(c: httpx.AsyncClient, token: str, ip: str = BAD) -> None:
    r = await c.get("/v1/me", headers=_h(token, ip))
    assert r.status_code == 403, r.text


async def test_create_bodies_accept_network_fields(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        mine = await c.post(
            "/v1/me/tokens",
            json={"label": "x", "network_policy": "first_use",
                  "approved_networks": ["10.0.0.0/8", "2001:db8::1"]},
            headers=bearer(r.a_free),
        )
        assert mine.status_code == 201, mine.text
        tid = mine.json()["token_id"]
        assert mine.json()["network_policy"] == "first_use"
        assert mine.json()["approved_networks"] == ["10.0.0.0/8", "2001:db8::1/128"]
        adm = await c.post(
            f"/v1/accounts/{r.a_id}/tokens",
            json={"label": "y", "network_policy": "approved_only",
                  "approved_networks": ["192.0.2.0/24"]},
            headers=bearer(ADMIN),
        )
        assert adm.status_code == 201, adm.text
        assert adm.json()["network_policy"] == "approved_only"
        listing = (await c.get("/v1/me/tokens", headers=bearer(r.a_free))).json()["tokens"]
        row = next(t for t in listing if t["token_id"] == tid)
        assert row["network_policy"] == "first_use"
        assert row["approved_networks"] == ["10.0.0.0/8", "2001:db8::1/128"]
        assert "last_used_at" in row and "last_used_ip" in row
        admin_view = (await c.get(f"/v1/accounts/{r.a_id}", headers=bearer(ADMIN))).json()
        assert all("network_policy" in t for t in admin_view["tokens"])
        for body in (
            {"label": "z", "network_policy": "bogus"},
            {"label": "z", "network_policy": 3},
            {"label": "z", "approved_networks": "10.0.0.0/8"},
            {"label": "z", "approved_networks": ["nope"]},
        ):
            bad = await c.post("/v1/me/tokens", json=body, headers=bearer(r.a_free))
            assert bad.status_code == 400, (body, bad.text)
            bad = await c.post(
                f"/v1/accounts/{r.a_id}/tokens", json=body, headers=bearer(ADMIN)
            )
            assert bad.status_code == 400, (body, bad.text)


async def test_get_and_put_networks_self_and_admin(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        url = f"/v1/me/tokens/{r.a_bound_id}/networks"
        got = await c.get(url, headers=bearer(r.a_free))
        assert got.status_code == 200
        assert got.json() == {
            "network_policy": "approved_only", "approved_networks": [f"{GOOD}/32"],
        }
        aurl = f"/v1/accounts/{r.a_id}/tokens/{r.a_bound_id}/networks"
        assert (await c.get(aurl, headers=bearer(ADMIN))).json() == got.json()
        put = await c.put(
            aurl, json={"network_policy": "first_use", "approved_networks": []},
            headers=bearer(ADMIN),
        )
        assert put.json() == {"network_policy": "first_use", "approved_networks": []}
        assert (await c.get(url, headers=bearer(r.a_free))).json() == put.json()


async def test_networks_auth_404_and_isolation(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        url = f"/v1/me/tokens/{r.a_bound_id}/networks"
        assert (await c.get(url)).status_code == 401
        aurl = f"/v1/accounts/{r.a_id}/tokens/{r.a_bound_id}/networks"
        assert (await c.get(aurl)).status_code == 401
        assert (await c.get(aurl, headers=bearer(r.a_free))).status_code == 403
        assert (await c.put(aurl, json={}, headers=bearer(r.a_free))).status_code == 403
        # Bob cannot read Alice's token; unknown tokens and accounts are 404
        assert (await c.get(url, headers=bearer(r.b_free))).status_code == 404
        assert (
            await c.get("/v1/me/tokens/nope/networks", headers=bearer(r.a_free))
        ).status_code == 404
        assert (
            await c.get(
                f"/v1/accounts/nope/tokens/{r.a_bound_id}/networks", headers=bearer(ADMIN)
            )
        ).status_code == 404
        assert (
            await c.get(
                f"/v1/accounts/{r.b_id}/tokens/{r.a_bound_id}/networks", headers=bearer(ADMIN)
            )
        ).status_code == 404


async def test_approval_flow_self_ip_scope(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        await _pend(c, r.a_bound)
        listing = await c.get("/v1/me/network-approvals", headers=bearer(r.a_free))
        assert listing.status_code == 200
        rows = listing.json()["approvals"]
        assert len(rows) == 1
        ap = rows[0]
        assert ap["token_id"] == r.a_bound_id and ap["ip"] == BAD and ap["client"] == UA
        assert ap["label"] == "bound"
        done = await c.post(
            f"/v1/me/network-approvals/{ap['approval_id']}/approve",
            json={"scope": "ip"}, headers=bearer(r.a_free),
        )
        assert done.status_code == 200, done.text
        assert done.json() == {
            "network_policy": "approved_only",
            "approved_networks": [f"{GOOD}/32", f"{BAD}/32"],
        }
        assert (await c.get("/v1/me", headers=_h(r.a_bound, BAD))).status_code == 200
        assert (await c.get("/v1/me/network-approvals", headers=bearer(r.a_free))).json() == {
            "approvals": []
        }
        evs = (await c.get("/v1/me/events", headers=bearer(r.a_free))).json()["events"]
        assert "token.network_approved" in [e["kind"] for e in evs]
        ev = next(e for e in evs if e["kind"] == "token.network_approved")
        assert ev["data"]["by"] == "self"


async def test_approval_network_scope_and_deny_admin(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        await _pend(c, r.a_bound)
        base = f"/v1/accounts/{r.a_id}/network-approvals"
        rows = (await c.get(base, headers=bearer(ADMIN))).json()["approvals"]
        aid = rows[0]["approval_id"]
        ok = await c.post(f"{base}/{aid}/approve", json={"scope": "network"},
                          headers=bearer(ADMIN))
        assert ok.status_code == 200, ok.text
        assert "198.51.100.0/24" in ok.json()["approved_networks"]
        await _pend(c, r.a_bound, "192.0.2.50")
        rows = (await c.get(base, headers=bearer(ADMIN))).json()["approvals"]
        aid = rows[0]["approval_id"]
        denied = await c.post(f"{base}/{aid}/deny", headers=bearer(ADMIN))
        assert denied.status_code == 204
        assert (await c.get(base, headers=bearer(ADMIN))).json() == {"approvals": []}
        kinds = [
            e["kind"]
            for e in (await c.get("/v1/me/events", headers=bearer(r.a_free))).json()["events"]
        ]
        assert "token.network_denied" in kinds
        evs = (await c.get("/v1/me/events", headers=bearer(r.a_free))).json()["events"]
        assert next(e for e in evs if e["kind"] == "token.network_denied")["data"]["by"] == "admin"
        # a denied approval is gone
        again = await c.post(f"{base}/{aid}/deny", headers=bearer(ADMIN))
        assert again.status_code == 404


async def test_approval_validation_auth_and_isolation(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        await _pend(c, r.a_bound)
        aid = (await c.get("/v1/me/network-approvals", headers=bearer(r.a_free))).json()[
            "approvals"
        ][0]["approval_id"]
        # auth
        assert (await c.get("/v1/me/network-approvals")).status_code == 401
        assert (
            await c.get(f"/v1/accounts/{r.a_id}/network-approvals", headers=bearer(r.a_free))
        ).status_code == 403
        assert (
            await c.post(f"/v1/me/network-approvals/{aid}/approve", json={"scope": "ip"})
        ).status_code == 401
        # validation
        url = f"/v1/me/network-approvals/{aid}/approve"
        for body in ({}, {"scope": "all"}, {"scope": 3}):
            resp = await c.post(url, json=body, headers=bearer(r.a_free))
            assert resp.status_code == 400, (body, resp.text)
        # Bob can neither see nor act on Alice's approval
        assert (await c.get("/v1/me/network-approvals", headers=bearer(r.b_free))).json() == {
            "approvals": []
        }
        assert (
            await c.post(url, json={"scope": "ip"}, headers=bearer(r.b_free))
        ).status_code == 404
        assert (
            await c.post(f"/v1/me/network-approvals/{aid}/deny", headers=bearer(r.b_free))
        ).status_code == 404
        assert (
            await c.post(
                f"/v1/accounts/{r.b_id}/network-approvals/{aid}/approve",
                json={"scope": "ip"}, headers=bearer(ADMIN),
            )
        ).status_code == 404
        assert (
            await c.get("/v1/accounts/nope/network-approvals", headers=bearer(ADMIN))
        ).status_code == 404
        # still pending for Alice
        assert len(
            (await c.get("/v1/me/network-approvals", headers=bearer(r.a_free))).json()[
                "approvals"
            ]
        ) == 1


async def test_foreign_token_on_others_session_creates_no_approval(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        sid = (
            await c.post("/v1/sessions", json={}, headers=_h(r.a_free, GOOD))
        ).json()["session_id"]
        # Bob's bound token, from an unapproved network, on Alice's session
        resp = await c.delete(f"/v1/sessions/{sid}", headers=_h(r.b_bound, BAD))
        assert resp.status_code == 403
        assert resp.json()["error"]["code"] == "permission_denied"
        assert (await c.get("/v1/me/network-approvals", headers=bearer(r.b_free))).json() == {
            "approvals": []
        }
        evs = (await c.get("/v1/me/events", headers=bearer(r.b_free))).json()["events"]
        assert [e for e in evs if e["kind"] == "token.network_pending"] == []


async def test_events_example_shape(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        await _pend(c, r.a_bound)
        body: dict[str, Any] = (
            await c.get("/v1/me/events", params={"wait": 1}, headers=bearer(r.a_free))
        ).json()
        assert body["events"][0]["kind"] == "token.network_pending"
        assert body["next"] == body["events"][-1]["event_id"]


async def test_invalid_put_bodies_and_admin_approve_body(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        bodies = (
            {"approved_networks": "10.0.0.0/8"},
            {"approved_networks": [5]},
            {"approved_networks": ["nope"]},
            {"network_policy": "bogus"},
            {"network_policy": 3},
        )
        for body in bodies:
            self_put = await c.put(
                f"/v1/me/tokens/{r.a_bound_id}/networks", json=body, headers=bearer(r.a_free)
            )
            assert self_put.status_code == 400, (body, self_put.text)
            adm_put = await c.put(
                f"/v1/accounts/{r.a_id}/tokens/{r.a_bound_id}/networks",
                json=body, headers=bearer(ADMIN),
            )
            assert adm_put.status_code == 400, (body, adm_put.text)
        await _pend(c, r.a_bound)
        base = f"/v1/accounts/{r.a_id}/network-approvals"
        aid = (await c.get(base, headers=bearer(ADMIN))).json()["approvals"][0]["approval_id"]
        for body in ({}, {"scope": "all"}, {"scope": 3}):
            resp = await c.post(f"{base}/{aid}/approve", json=body, headers=bearer(ADMIN))
            assert resp.status_code == 400, (body, resp.text)
        # admin deny with the wrong account id: 404, approval untouched
        wrong = await c.post(
            f"/v1/accounts/{r.b_id}/network-approvals/{aid}/deny", headers=bearer(ADMIN)
        )
        assert wrong.status_code == 404
        assert len((await c.get(base, headers=bearer(ADMIN))).json()["approvals"]) == 1


async def test_mcp_join_with_join_token_from_unapproved_ip(tmp_path: Path) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        made = (await c.post("/v1/sessions", json={}, headers=_h(r.a_free, GOOD))).json()
        async with sw_client(r.base, headers={"X-Forwarded-For": BAD, "User-Agent": UA}) as m:
            out = await m.call_tool(
                "session_join",
                {"session_id": made["session_id"], "join_token": made["join_token"],
                 "role": "w"},
            )
        assert out["participant_id"]


async def test_foreign_revoked_token_records_rejected_on_its_own_account_only(
    tmp_path: Path,
) -> None:
    async with rig(tmp_path) as r, httpx.AsyncClient(base_url=r.base) as c:
        sid = (
            await c.post("/v1/sessions", json={}, headers=_h(r.a_free, GOOD))
        ).json()["session_id"]
        made = await c.post(
            f"/v1/accounts/{r.b_id}/tokens",
            json={"label": "gone", "network_policy": "approved_only"},
            headers=bearer(ADMIN),
        )
        assert made.status_code == 201
        assert (
            await c.delete(
                f"/v1/accounts/{r.b_id}/tokens/{made.json()['token_id']}",
                headers=bearer(ADMIN),
            )
        ).status_code == 204
        resp = await c.delete(f"/v1/sessions/{sid}", headers=_h(made.json()["token"], BAD))
        assert resp.status_code == 403
        bob = (await c.get("/v1/me/events", headers=bearer(r.b_free))).json()["events"]
        rejected = [e for e in bob if e["kind"] == "token.rejected"]
        assert len(rejected) == 1
        assert rejected[0]["data"]["reason"] == "revoked" and rejected[0]["data"]["ip"] == BAD
        assert [e for e in bob if e["kind"].startswith("token.network_")] == []
        assert (await c.get("/v1/me/network-approvals", headers=bearer(r.b_free))).json() == {
            "approvals": []
        }
        alice = (await c.get("/v1/me/events", headers=bearer(r.a_free))).json()["events"]
        assert [e for e in alice if e["kind"] == "token.rejected"] == []
