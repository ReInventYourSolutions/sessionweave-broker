"""Machines and definitions as MCP tools, and their parity with REST.

The tools run the same code as ``/v1/me/launchers...`` (account token) and
``/v1/launchers...`` (open mode): same validation, owner-only authz and
errors. A fake v2 launcher answers over the WebSocket.
"""
from __future__ import annotations

import contextlib
import json
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.accounts_fixtures import ADMIN, Seeded, seeded_relay
from tests.integration.client import Client, bearer, sw_client
from tests.integration.harness import relay_subprocess
from tests.integration.inproc import mcp_outcome, rest_outcome
from tests.integration.test_agent_deploy import (
    KEY,
    _agent,
    _def,
    _eventually,
    _Launcher,
    _offline,
    _recv,
    _status,
)
from tests.integration.test_launcher_browse import _answering, _ok_browse

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

PROCS = [{"pid": 42, "ppid": 1, "user": "app", "name": "nginx",
          "cmdline": "nginx: master process", "started_at": 1700000000}]


async def _answer(sock: Any, msg: dict[str, Any]) -> None:
    if msg["type"] == "browse":
        await _ok_browse([{"name": "logs", "kind": "dir"}], "logs")(sock, msg)
    else:
        await sock.send(json.dumps({
            "type": "processes_result", "request_id": msg["request_id"], "ok": True,
            "processes": PROCS, "next_cursor": 42}))


@contextlib.asynccontextmanager
async def _clients(base: str, headers: dict[str, str] | None = None) -> AsyncIterator[
        tuple[httpx.AsyncClient, Client]]:
    async with httpx.AsyncClient(base_url=base) as c, sw_client(base, headers=headers) as m:
        yield c, m


def _rest_args(token: str | None) -> tuple[str, dict[str, str]]:
    return ("/v1/me/launchers", bearer(token)) if token else ("/v1/launchers", {})


def _mcp_args(token: str | None, **kw: Any) -> dict[str, Any]:
    return {**({"account_token": token} if token else {}), **kw}


async def _ops_match(c: httpx.AsyncClient, m: Client, token: str | None) -> None:
    """Every read and relay operation answers the same through REST and MCP."""
    base, h = _rest_args(token)
    # the deployed definition has settled (applied) before anything is compared
    await _eventually(lambda: _agent(c, token, KEY, "obs"),
                      lambda a: a is not None and a["deploy"]["state"] == "applied")
    pairs: list[tuple[str, str, dict[str, str], str, dict[str, Any]]] = [
        ("list", base, {}, "machines_list", _mcp_args(token)),
        ("get", f"{base}/{KEY}", {}, "machine_get", _mcp_args(token, key=KEY)),
        ("browse default", f"{base}/{KEY}/browse", {},
         "machine_browse", _mcp_args(token, key=KEY)),
        ("browse page", f"{base}/{KEY}/browse",
         {"path": "/srv/app/logs", "cursor": "a", "limit": "5", "hidden": "1"},
         "machine_browse", _mcp_args(token, key=KEY, path="/srv/app/logs", cursor="a",
                                     limit=5, hidden=True)),
        ("processes", f"{base}/{KEY}/processes", {"q": "nginx", "limit": "1000", "cursor": "7"},
         "machine_processes", _mcp_args(token, key=KEY, query="nginx", limit=1000,
                                        cursor="7")),
    ]
    for name, path, params, tool, args in pairs:
        rest_code, rest_out = rest_outcome(await c.get(path, headers=h, params=params))
        mcp_code, mcp_out = await mcp_outcome(m, tool, args)
        assert rest_code is None and mcp_code is None, (name, rest_out, mcp_out)
        assert _no_seen(rest_out) == _no_seen(mcp_out), name


def _no_seen(out: dict[str, Any]) -> dict[str, Any]:
    """Drop the fields that change between two reads (last_seen times)."""
    text = json.dumps(out, sort_keys=True)
    data = json.loads(text)
    for m in data.get("launchers", []):
        m.pop("last_seen", None)
    data.pop("last_seen", None)
    return data  # type: ignore[no-any-return]


# ---------- account mode ----------


async def test_account_mode_tools_match_rest(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, _clients(s.base) as (c, m):
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)  # definitions_index
            made = await m.call_tool("definition_deploy", {
                "account_token": s.token, "key": KEY, "definition_id": "obs",
                "definition": _def()})
            assert made["definition_id"] == "obs" and made["state"] == "sent"
            put = await _recv(ws)
            assert put["type"] == "definition_put" and put["definition"]["revision"] == 1
            await _status(ws, "obs", 1, "applied")
            async with _answering(ws, _answer) as seen:
                await _ops_match(c, m, s.token)
                # the launcher got the same requests from both surfaces
                browse = [x for x in seen if x["type"] == "browse"]
                assert len(browse) == 4
                strip = ("request_id",)
                assert {k: v for k, v in browse[2].items() if k not in strip} == {
                    k: v for k, v in browse[3].items() if k not in strip}
            removed = await m.call_tool("definition_remove", {
                "account_token": s.token, "key": KEY, "definition_id": "obs"})
            assert removed == {"definition_id": "obs", "revision": 2, "state": "removing"}
            code, _ = await mcp_outcome(m, "machine_forget",
                                        {"account_token": s.token, "key": KEY})
            assert code == "launcher_online"
        await _offline(c, s.token, ws)
        out = await m.call_tool("machine_forget", {"account_token": s.token, "key": KEY})
        assert out == {"key": KEY, "forgotten": True}
        assert (await m.call_tool("machines_list", {"account_token": s.token})) == {
            "launchers": []}


async def _parity_table(s: Seeded) -> list[tuple[str, str, str, Any, dict[str, str],
                                                str, dict[str, Any]]]:
    me = "/v1/me/launchers"
    tok, other = s.token, s.other_token
    missing = "lk_" + "0" * 32
    return [
        ("other account's machine", "GET", f"{me}/{KEY}", None, bearer(other),
         "machine_get", {"account_token": other, "key": KEY}),
        ("unknown key", "GET", f"{me}/{missing}", None, bearer(tok),
         "machine_get", {"account_token": tok, "key": missing}),
        ("malformed key", "GET", f"{me}/nope", None, bearer(tok),
         "machine_get", {"account_token": tok, "key": "nope"}),
        ("admin token is not an account token", "GET", me, None, bearer(ADMIN),
         "machines_list", {"account_token": ADMIN}),
        ("revoked token", "GET", me, None, bearer(s.revoked_token),
         "machines_list", {"account_token": s.revoked_token}),
        ("deploy invalid definition", "PUT", f"{me}/{KEY}/definitions/bad",
         {"definition": _def("bad", nope=1)}, bearer(tok),
         "definition_deploy", {"account_token": tok, "key": KEY, "definition_id": "bad",
                               "definition": _def("bad", nope=1)}),
        ("deploy outside ceiling", "PUT", f"{me}/{KEY}/definitions/bad",
         {"definition": _def("bad", workdir="/etc")}, bearer(tok),
         "definition_deploy", {"account_token": tok, "key": KEY, "definition_id": "bad",
                               "definition": _def("bad", workdir="/etc")}),
        ("deploy id mismatch", "PUT", f"{me}/{KEY}/definitions/other",
         {"definition": _def("obs")}, bearer(tok),
         "definition_deploy", {"account_token": tok, "key": KEY, "definition_id": "other",
                               "definition": _def("obs")}),
        ("deploy to other account's machine", "PUT", f"{me}/{KEY}/definitions/obs",
         {"definition": _def()}, bearer(other),
         "definition_deploy", {"account_token": other, "key": KEY, "definition_id": "obs",
                               "definition": _def()}),
        ("remove unknown definition", "DELETE", f"{me}/{KEY}/definitions/nope", None,
         bearer(tok), "definition_remove",
         {"account_token": tok, "key": KEY, "definition_id": "nope"}),
        ("forget online machine", "DELETE", f"{me}/{KEY}", None, bearer(tok),
         "machine_forget", {"account_token": tok, "key": KEY}),
        ("browse relative path", "GET", f"{me}/{KEY}/browse?path=relative", None, bearer(tok),
         "machine_browse", {"account_token": tok, "key": KEY, "path": "relative"}),
        ("browse limit 0", "GET", f"{me}/{KEY}/browse?limit=0", None, bearer(tok),
         "machine_browse", {"account_token": tok, "key": KEY, "limit": 0}),
        ("browse negative limit", "GET", f"{me}/{KEY}/browse?limit=-1", None, bearer(tok),
         "machine_browse", {"account_token": tok, "key": KEY, "limit": -1}),
        ("browse long cursor", "GET", f"{me}/{KEY}/browse?cursor={'c' * 513}", None,
         bearer(tok), "machine_browse", {"account_token": tok, "key": KEY,
                                         "cursor": "c" * 513}),
        ("processes bad cursor", "GET", f"{me}/{KEY}/processes?cursor=abc", None, bearer(tok),
         "machine_processes", {"account_token": tok, "key": KEY, "cursor": "abc"}),
        ("processes long query", "GET", f"{me}/{KEY}/processes?q={'x' * 101}", None,
         bearer(tok), "machine_processes", {"account_token": tok, "key": KEY,
                                            "query": "x" * 101}),
        ("browse other account's machine", "GET", f"{me}/{KEY}/browse", None, bearer(other),
         "machine_browse", {"account_token": other, "key": KEY}),
    ]


async def test_account_mode_errors_match_rest(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, _clients(s.base) as (c, m):
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            async with _answering(ws, _answer) as seen:
                for name, method, path, body, h, tool, args in await _parity_table(s):
                    rest_code, rest_out = rest_outcome(
                        await c.request(method, path, json=body, headers=h))
                    mcp_code, mcp_out = await mcp_outcome(m, tool, args)
                    assert rest_code is not None, (name, rest_out)
                    assert rest_code == mcp_code, (name, rest_out, mcp_out)
                assert seen == []  # validation and authz never reached the launcher


async def test_account_mode_needs_an_account_token(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s:
        # no credential, and a Bearer that is not an account token: refused,
        # never served as open mode
        for headers in (None, bearer(ADMIN)):
            async with sw_client(s.base, headers=headers) as m:
                code, out = await mcp_outcome(m, "machines_list", {})
                assert code == "invalid_account_token", out
                assert "account_token" in out["text"]
        async with sw_client(s.base, headers=bearer(s.token)) as m:  # token as Bearer
            assert await m.call_tool("machines_list", {}) == {"launchers": []}


# ---------- open mode ----------


async def test_open_mode_tools_without_credentials(tmp_path: Path) -> None:
    db = str(tmp_path / "open.db")
    async with relay_subprocess(persistence_db_path=db) as base, _clients(base) as (c, m):
        lnc = _Launcher(base, None)
        ws, _ = await lnc.connect()
        async with ws:
            await _recv(ws)
            made = await m.call_tool("definition_deploy", {
                "key": KEY, "definition_id": "obs", "definition": _def()})
            assert made["state"] == "sent"
            await _recv(ws)
            await _status(ws, "obs", 1, "applied")
            async with _answering(ws, _answer):
                await _ops_match(c, m, None)
            assert (await m.call_tool("definition_remove", {
                "key": KEY, "definition_id": "obs"}))["state"] == "removing"
        await _offline(c, None, ws)
        assert await m.call_tool("machine_forget", {"key": KEY}) == {
            "key": KEY, "forgotten": True}


async def test_open_mode_without_db(tmp_path: Path) -> None:
    async with relay_subprocess() as base, _clients(base) as (c, m):
        ws, _ = await _Launcher(base, None).connect()
        async with ws:
            for method, path, body, tool, args in (
                ("PUT", f"/v1/launchers/{KEY}/definitions/obs", {"definition": _def()},
                 "definition_deploy", {"key": KEY, "definition_id": "obs",
                                       "definition": _def()}),
                ("GET", f"/v1/launchers/{KEY}/browse", None, "machine_browse", {"key": KEY}),
                ("DELETE", f"/v1/launchers/{KEY}", None, "machine_forget", {"key": KEY}),
            ):
                rest_code, _ = rest_outcome(await c.request(method, path, json=body))
                mcp_code, _ = await mcp_outcome(m, tool, args)
                assert rest_code == mcp_code == "definitions_unavailable", tool
            listed = await m.call_tool("machines_list", {})
            assert [x["launcher_key"] for x in listed["launchers"]] == [KEY]
