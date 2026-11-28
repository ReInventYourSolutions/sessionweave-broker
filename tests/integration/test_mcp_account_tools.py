"""Account self-service and admin MCP tools, and their parity with REST.

Each MCP tool runs the same shared operation as its REST route
(``accounts_api``), so for the same inputs both must give the same result or
the same error code. In-process: the auth limiter is reset between cases and
the relay's logs are captured.
"""
from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration.client import bearer
from tests.integration.inproc import InProc, inproc_relay, mcp_outcome, rest_outcome

pytestmark = [pytest.mark.asyncio]

ADMIN = "adm-mcp-account-tools"
WRONG_ADMIN = "not-the-admin-token-1234"


@pytest.fixture
async def relay(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> AsyncIterator[InProc]:
    env = {
        "SW_ADMIN_TOKEN": ADMIN,
        "SW_SESSION_CREATION": "account",
        "SW_PERSISTENCE_DB_PATH": str(tmp_path / "mcp-acc.db"),
    }
    async with inproc_relay(monkeypatch, env) as r:
        yield r


async def _account(r: InProc, name: str, *, status: str = "active") -> tuple[str, str, str]:
    """(account_id, token, token_id)."""
    a = await r.http.post(
        "/v1/accounts", json={"name": name, "status": status}, headers=bearer(ADMIN)
    )
    assert a.status_code == 201, a.text
    aid = a.json()["account_id"]
    t = await r.http.post(
        f"/v1/accounts/{aid}/tokens", json={"label": "seed"}, headers=bearer(ADMIN)
    )
    assert t.status_code == 201, t.text
    return aid, t.json()["token"], t.json()["token_id"]


def _strip_volatile(out: dict[str, Any]) -> dict[str, Any]:
    """A token listing without the fields a call itself updates (last use)."""
    def tok(t: dict[str, Any]) -> dict[str, Any]:
        return {k: v for k, v in t.items() if k not in ("last_used_at", "last_used_ip")}
    if "tokens" in out:
        return {**out, "tokens": [tok(t) for t in out["tokens"]]}
    return out


MINT_KEYS = {"token", "token_id", "label", "created_at", "expires_at",
             "network_policy", "approved_networks"}


# ---------- happy paths ----------


async def test_account_me_and_tokens_match_rest(relay: InProc) -> None:
    r = relay
    aid, token, tid = await _account(r, "alice")
    async with r.mcp() as c:
        me = await c.call_tool("account_me", {"account_token": token})
        toks = await c.call_tool("account_tokens", {"account_token": token})
    rest_me = (await r.http.get("/v1/me", headers=bearer(token))).json()
    rest_toks = (await r.http.get("/v1/me/tokens", headers=bearer(token))).json()
    assert me["account_id"] == aid and [t["token_id"] for t in me["tokens"]] == [tid]
    assert _strip_volatile(me) == _strip_volatile(rest_me)
    assert _strip_volatile(toks) == _strip_volatile(rest_toks)
    assert token not in str(me) + str(toks)


async def test_account_token_from_bearer_header(relay: InProc) -> None:
    aid, token, _ = await _account(relay, "alice")
    async with relay.mcp(headers=bearer(token)) as c:
        me = await c.call_tool("account_me", {})
    assert me["account_id"] == aid


async def test_non_account_bearer_header_is_not_tried_as_account_token(relay: InProc) -> None:
    """A launched agent's MCP requests carry its participant token as Bearer:
    it is never tried as an account token (no auth failure counted)."""
    async with relay.mcp(headers=bearer("participant-token-xyz")) as c:
        code, _ = await mcp_outcome(c, "account_me", {})
    assert code == "invalid_account_token"
    assert not relay.deps.auth_limiter._buckets


async def test_account_token_create_and_revoke(relay: InProc) -> None:
    r = relay
    aid, token, _ = await _account(r, "alice")
    async with r.mcp() as c:
        made = await c.call_tool(
            "account_token_create",
            {"account_token": token, "label": "cli", "network_policy": "first_use",
             "approved_networks": ["10.0.0.0/8"], "expires_at": "2099-01-01T00:00:00Z"},
        )
        assert set(made) == MINT_KEYS
        assert made["token"].startswith("swa_") and made["label"] == "cli"
        assert made["network_policy"] == "first_use"
        assert made["approved_networks"] == ["10.0.0.0/8"]
        plain = await c.call_tool("account_token_create", {"account_token": token, "label": "p"})
        assert (await c.call_tool("account_me", {"account_token": plain["token"]}))[
            "account_id"] == aid
        out = await c.call_tool(
            "account_token_revoke", {"account_token": token, "token_id": plain["token_id"]}
        )
        assert out == {"token_id": plain["token_id"], "revoked": True}
        code, _ = await mcp_outcome(c, "account_me", {"account_token": plain["token"]})
        assert code == "invalid_account_token"


async def test_account_token_create_with_revoke_replaces(relay: InProc) -> None:
    r = relay
    _, token, tid = await _account(r, "alice")
    async with r.mcp() as c:
        code, _ = await mcp_outcome(
            c, "account_token_create",
            {"account_token": token, "label": "n", "revoke_token_id": "nope"},
        )
        assert code == "token_not_found"
        toks = await c.call_tool("account_tokens", {"account_token": token})
        assert len(toks["tokens"]) == 1  # nothing minted
        made = await c.call_tool(
            "account_token_create",
            {"account_token": token, "label": "n", "revoke_token_id": tid},
        )
        assert made["revoked_token_id"] == tid
        code, _ = await mcp_outcome(c, "account_me", {"account_token": token})
        assert code == "invalid_account_token"
        me = await c.call_tool("account_me", {"account_token": made["token"]})
        assert next(t for t in me["tokens"] if t["token_id"] == tid)["revoked_at"] is not None


async def test_admin_tools(relay: InProc) -> None:
    r = relay
    aid, token, tid = await _account(r, "alice")
    async with r.mcp() as c:
        listed = await c.call_tool("admin_accounts", {"admin_token": ADMIN})
        assert listed == (await r.http.get("/v1/accounts", headers=bearer(ADMIN))).json()
        one = await c.call_tool("admin_account", {"admin_token": ADMIN, "account_id": aid})
        rest = (await r.http.get(f"/v1/accounts/{aid}", headers=bearer(ADMIN))).json()
        assert _strip_volatile(one) == _strip_volatile(rest)
        made = await c.call_tool(
            "admin_token_create",
            {"admin_token": ADMIN, "account_id": aid, "label": "repl", "revoke_token_id": tid},
        )
        assert made["revoked_token_id"] == tid and made["token"].startswith("swa_")
        code, _ = await mcp_outcome(c, "account_me", {"account_token": token})
        assert code == "invalid_account_token"
        out = await c.call_tool(
            "admin_token_revoke",
            {"admin_token": ADMIN, "account_id": aid, "token_id": made["token_id"]},
        )
        assert out == {"token_id": made["token_id"], "revoked": True}
    async with r.mcp(headers=bearer(ADMIN)) as c:  # the admin token as Bearer header
        assert (await c.call_tool("admin_accounts", {}))["accounts"]


async def test_admin_mint_and_revoke_are_audited_like_rest(relay: InProc) -> None:
    r = relay
    aid, _, _ = await _account(r, "alice")
    async with r.mcp() as c:
        made = await c.call_tool(
            "admin_token_create", {"admin_token": ADMIN, "account_id": aid, "label": "a"}
        )
        await c.call_tool(
            "admin_token_revoke",
            {"admin_token": ADMIN, "account_id": aid, "token_id": made["token_id"]},
        )
    ev = (await r.http.get(
        "/v1/accounts/events", params={"account_id": aid}, headers=bearer(ADMIN)
    )).json()["events"]
    mine = [e for e in ev if e["data"].get("token", {}).get("id") == made["token_id"]]
    assert [e["kind"] for e in mine] == ["token.created", "token.revoked"]
    assert all(e["data"]["by"] == "admin" and e["data"]["ip"] == "127.0.0.1" for e in mine)


# ---------- REST / MCP parity table ----------


async def _parity_cases(r: InProc) -> list[Any]:
    """(name, rest method, rest path, rest body, rest bearer, (tool, args))."""
    aid, token, tid = await _account(r, "alice")
    other_aid, other_token, other_tid = await _account(r, "bob")
    _, pending_token, _ = await _account(r, "pend", status="pending")
    _, revoked, revoked_tid = await _account(r, "rev")
    await r.http.delete(f"/v1/me/tokens/{revoked_tid}", headers=bearer(revoked))
    long_label = "x" * 65
    cases: list[Any] = [
        ("me ok", "GET", "/v1/me", None, token, ("account_me", {"account_token": token})),
        ("me bogus", "GET", "/v1/me", None, "swa_bogus",
         ("account_me", {"account_token": "swa_bogus"})),
        ("me revoked", "GET", "/v1/me", None, revoked,
         ("account_me", {"account_token": revoked})),
        ("me pending", "GET", "/v1/me", None, pending_token,
         ("account_me", {"account_token": pending_token})),
        ("me admin token", "GET", "/v1/me", None, ADMIN,
         ("account_me", {"account_token": ADMIN})),
        ("tokens ok", "GET", "/v1/me/tokens", None, token,
         ("account_tokens", {"account_token": token})),
        ("create bad expiry", "POST", "/v1/me/tokens", {"label": "a", "expires_at": "nope"},
         token, ("account_token_create",
                 {"account_token": token, "label": "a", "expires_at": "nope"})),
        ("create past expiry", "POST", "/v1/me/tokens",
         {"label": "a", "expires_at": "2000-01-01T00:00:00Z"}, token,
         ("account_token_create", {"account_token": token, "label": "a",
                                   "expires_at": "2000-01-01T00:00:00Z"})),
        ("create bad policy", "POST", "/v1/me/tokens",
         {"label": "a", "network_policy": "bogus"}, token,
         ("account_token_create",
          {"account_token": token, "label": "a", "network_policy": "bogus"})),
        ("create bad network", "POST", "/v1/me/tokens",
         {"label": "a", "approved_networks": ["not-a-net"]}, token,
         ("account_token_create",
          {"account_token": token, "label": "a", "approved_networks": ["not-a-net"]})),
        ("create long label", "POST", "/v1/me/tokens", {"label": long_label}, token,
         ("account_token_create", {"account_token": token, "label": long_label})),
        ("revoke unknown", "DELETE", "/v1/me/tokens/nope", None, token,
         ("account_token_revoke", {"account_token": token, "token_id": "nope"})),
        ("revoke other account's token", "DELETE", f"/v1/me/tokens/{other_tid}", None, token,
         ("account_token_revoke", {"account_token": token, "token_id": other_tid})),
        ("admin list ok", "GET", "/v1/accounts", None, ADMIN,
         ("admin_accounts", {"admin_token": ADMIN})),
        ("admin list wrong token", "GET", "/v1/accounts", None, WRONG_ADMIN,
         ("admin_accounts", {"admin_token": WRONG_ADMIN})),
        ("admin list with account token", "GET", "/v1/accounts", None, other_token,
         ("admin_accounts", {"admin_token": other_token})),
        ("admin get unknown", "GET", "/v1/accounts/acc_nope", None, ADMIN,
         ("admin_account", {"admin_token": ADMIN, "account_id": "acc_nope"})),
        ("admin get wrong token", "GET", f"/v1/accounts/{aid}", None, WRONG_ADMIN,
         ("admin_account", {"admin_token": WRONG_ADMIN, "account_id": aid})),
        ("admin create unknown account", "POST", "/v1/accounts/acc_nope/tokens",
         {"label": "a"}, ADMIN,
         ("admin_token_create", {"admin_token": ADMIN, "account_id": "acc_nope",
                                 "label": "a"})),
        ("admin create bad expiry", "POST", f"/v1/accounts/{aid}/tokens",
         {"label": "a", "expires_at": "nope"}, ADMIN,
         ("admin_token_create", {"admin_token": ADMIN, "account_id": aid, "label": "a",
                                 "expires_at": "nope"})),
        ("admin create wrong token", "POST", f"/v1/accounts/{aid}/tokens", {"label": "a"},
         token, ("admin_token_create", {"admin_token": token, "account_id": aid,
                                        "label": "a"})),
        ("admin revoke unknown account", "DELETE", "/v1/accounts/acc_nope/tokens/x", None,
         ADMIN, ("admin_token_revoke", {"admin_token": ADMIN, "account_id": "acc_nope",
                                        "token_id": "x"})),
        ("admin revoke unknown token", "DELETE", f"/v1/accounts/{aid}/tokens/nope", None,
         ADMIN, ("admin_token_revoke", {"admin_token": ADMIN, "account_id": aid,
                                        "token_id": "nope"})),
        ("admin revoke token of another account", "DELETE",
         f"/v1/accounts/{other_aid}/tokens/{tid}", None, ADMIN,
         ("admin_token_revoke", {"admin_token": ADMIN, "account_id": other_aid,
                                 "token_id": tid})),
    ]
    return cases


async def test_rest_and_mcp_agree(relay: InProc) -> None:
    r = relay
    cases = await _parity_cases(r)
    async with r.mcp() as c:
        for name, method, path, body, cred, (tool, args) in cases:
            r.reset_auth_limiter()
            rest_code, rest_out = rest_outcome(
                await r.http.request(method, path, json=body, headers=bearer(cred))
            )
            r.reset_auth_limiter()
            mcp_code, mcp_out = await mcp_outcome(c, tool, args)
            assert rest_code == mcp_code, (name, rest_out, mcp_out)
            if rest_code is None and method == "GET":
                assert _strip_volatile(mcp_out) == _strip_volatile(rest_out), name
        # a create that succeeds both ways answers the same shape
        rest_code, rest_out = rest_outcome(await r.http.post(
            "/v1/me/tokens", json={"label": "same"}, headers=bearer(cases[0][4])
        ))
        mcp_code, mcp_out = await mcp_outcome(
            c, "account_token_create", {"account_token": cases[0][4], "label": "same"}
        )
        assert rest_code is None and mcp_code is None
        assert set(rest_out) == set(mcp_out) == MINT_KEYS


async def test_failed_admin_attempts_share_the_rest_limiter(relay: InProc) -> None:
    """Wrong admin tokens through MCP and REST count in one per-address
    budget; once throttled both refuse with rate_limited, and the real admin
    token still passes."""
    r = relay
    async with r.mcp() as c:
        for _ in range(3):
            code, _ = await mcp_outcome(c, "admin_accounts", {"admin_token": WRONG_ADMIN})
            assert code == "permission_denied"
        for _ in range(2):
            resp = await r.http.get("/v1/accounts", headers=bearer(WRONG_ADMIN))
            assert resp.status_code == 403
        code, _ = await mcp_outcome(c, "admin_accounts", {"admin_token": WRONG_ADMIN})
        assert code == "rate_limited"
        resp = await r.http.get("/v1/accounts", headers=bearer(WRONG_ADMIN))
        assert resp.json()["error"]["code"] == "rate_limited"
        assert (await c.call_tool("admin_accounts", {"admin_token": ADMIN}))["accounts"] == []


async def test_missing_credentials(relay: InProc) -> None:
    async with relay.mcp() as c:
        code, out = await mcp_outcome(c, "admin_accounts", {})
        assert code == "permission_denied" and "admin_token" in out["text"]
        code, out = await mcp_outcome(c, "account_me", {})
        assert code == "invalid_account_token" and "account_token" in out["text"]
    assert not relay.deps.auth_limiter._buckets  # a missing credential is no failure


async def test_accounts_unavailable_without_a_db(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    async with inproc_relay(monkeypatch, {"SW_ADMIN_TOKEN": ADMIN}) as r, r.mcp() as c:
        for tool, args, path in (
            ("account_me", {"account_token": "swa_x"}, "/v1/me"),
            ("admin_accounts", {"admin_token": ADMIN}, "/v1/accounts"),
        ):
            code, _ = await mcp_outcome(c, tool, args)
            rest_code, _ = rest_outcome(
                await r.http.get(path, headers=bearer(str(next(iter(args.values())))))
            )
            assert code == rest_code == "accounts_unavailable"


async def test_no_credential_reaches_the_logs(
    relay: InProc, caplog: pytest.LogCaptureFixture
) -> None:
    r = relay
    aid, token, _ = await _account(r, "alice")
    caplog.set_level(logging.DEBUG)
    async with r.mcp() as c:
        await mcp_outcome(c, "admin_accounts", {"admin_token": WRONG_ADMIN})
        await mcp_outcome(c, "admin_token_create",
                          {"admin_token": WRONG_ADMIN, "account_id": aid, "label": "x"})
        made = await c.call_tool(
            "admin_token_create", {"admin_token": ADMIN, "account_id": aid, "label": "y"}
        )
        mine = await c.call_tool("account_token_create", {"account_token": token, "label": "z"})
        await mcp_outcome(c, "account_me", {"account_token": "swa_wrong_guess"})
        await c.call_tool("admin_token_revoke", {"admin_token": ADMIN, "account_id": aid,
                                                 "token_id": made["token_id"]})
    # Relay-side records only: the in-process MCP *client* logs its own
    # outgoing requests (arguments included) at DEBUG.
    text = "\n".join(
        rec.getMessage() for rec in caplog.records
        if not rec.name.startswith(("mcp.client", "httpx", "httpcore"))
    )
    assert "Tool 'admin_accounts' failed" in text  # the capture saw the relay's records
    for secret in (ADMIN, WRONG_ADMIN, token, made["token"], mine["token"], "swa_wrong_guess"):
        assert secret not in text
