"""Deploying agent definitions to launchers: the offline queue, re-convergence
on reconnect, ``/v1/me/launchers`` (account mode), ``/v1/launchers`` (open
mode) and the account events."""
from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import Awaitable, Callable
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

KEY = "lk_" + "cd" * 16
V2: dict[str, Any] = {
    "protocol": 2, "launcher_key": KEY, "mode": "local", "ceiling": "/srv/app",
    "capabilities": ["definitions", "browse", "processes"],
    "sandbox": "bwrap", "run_as": False, "platform": "linux",
}


def _ws(base: str) -> str:
    return f"ws://{urlparse(base).netloc}/v1/launchers/connect"


def _prof(pid: str, **kw: Any) -> dict[str, Any]:
    return {"id": pid, "domain": "ops", "role": "watcher", "capabilities": [],
            "description": "d", "runtime": "claude", "model": "m",
            "access": {"preset": "reader"}, "limits": {}, "max_instances": 1, **kw}


def _def(pid: str = "obs", **kw: Any) -> dict[str, Any]:
    return {"id": pid, "domain": "ops", "role": "watcher", "runtime": "claude",
            "kind": "observer", "workdir": "/srv/app", "max_instances": 1, **kw}


def _hello(profiles: list[Any], **kw: Any) -> str:
    return json.dumps({"type": "hello", "machine": "box", "visibility": "personal",
                       "allowed_accounts": [], "profiles": profiles, **kw})


async def _connect(base: str, token: str | None, hello: str) -> tuple[Any, dict[str, Any]]:
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    ws = await websockets.connect(_ws(base), additional_headers=headers)
    await ws.send(hello)
    return ws, json.loads(await ws.recv())


async def _recv(ws: Any, timeout: float = 5.0) -> dict[str, Any]:
    """The next frame that is not a ping."""
    while True:
        msg = json.loads(await asyncio.wait_for(ws.recv(), timeout))
        if msg.get("type") != "ping":
            return msg  # type: ignore[no-any-return]


async def _drain(ws: Any, wait: float = 0.5) -> list[dict[str, Any]]:
    """Every non-ping frame that arrives within ``wait`` seconds."""
    out: list[dict[str, Any]] = []
    while True:
        try:
            out.append(await _recv(ws, wait))
        except TimeoutError:
            return out


async def _status(ws: Any, def_id: str, revision: int, state: str, **kw: Any) -> None:
    await ws.send(json.dumps({"type": "definition_status", "id": def_id, "revision": revision,
                              "state": state, "problems": [], "warnings": [], **kw}))


async def _profiles(ws: Any, *profiles: dict[str, Any]) -> None:
    await ws.send(json.dumps({"type": "profiles", "profiles": list(profiles)}))


def _base(token: str | None) -> str:
    return "/v1/me/launchers" if token else "/v1/launchers"


async def _put(c: httpx.AsyncClient, token: str | None, key: str, def_id: str,
               definition: dict[str, Any]) -> httpx.Response:
    return await c.put(f"{_base(token)}/{key}/definitions/{def_id}",
                       json={"definition": definition}, headers=bearer(token) if token else {})


async def _del(c: httpx.AsyncClient, token: str | None, key: str, def_id: str) -> httpx.Response:
    return await c.delete(f"{_base(token)}/{key}/definitions/{def_id}",
                          headers=bearer(token) if token else {})


async def _machines(c: httpx.AsyncClient, token: str | None) -> list[dict[str, Any]]:
    r = await c.get(_base(token), headers=bearer(token) if token else {})
    assert r.status_code == 200, r.text
    out: list[dict[str, Any]] = r.json()["launchers"]
    return out


async def _agent(c: httpx.AsyncClient, token: str | None, key: str,
                 pid: str) -> dict[str, Any] | None:
    for m in await _machines(c, token):
        if m["launcher_key"] == key:
            for a in m["agents"]:
                if a["profile_id"] == pid:
                    return a  # type: ignore[no-any-return]
    return None


async def _eventually(
    probe: Callable[[], Awaitable[Any]], check: Callable[[Any], bool] = bool,
) -> Any:
    value: Any = None
    for _ in range(100):
        value = await probe()
        if check(value):
            return value
        await asyncio.sleep(0.05)
    raise AssertionError(f"condition not met; last value {value!r}")


async def _my_events(c: httpx.AsyncClient, token: str) -> list[dict[str, Any]]:
    r = await c.get("/v1/me/events", headers=bearer(token))
    assert r.status_code == 200, r.text
    out: list[dict[str, Any]] = r.json()["events"]
    return out


async def _closed_by_broker(ws: Any) -> tuple[int | None, str]:
    with pytest.raises(ConnectionClosed):
        for _ in range(100):
            await asyncio.wait_for(ws.recv(), 5)
    return ws.close_code, ws.close_reason or ""


class _Launcher:
    """A fake v2 launcher that keeps its key secret across reconnects."""

    def __init__(self, base: str, token: str | None, **hello: Any) -> None:
        self.base, self.token, self.hello = base, token, {**V2, **hello}
        self.secret: str | None = None

    async def connect(self, profiles: list[Any] | None = None) -> tuple[Any, dict[str, Any]]:
        extra = {"key_secret": self.secret} if self.secret else {}
        ws, welcome = await _connect(
            self.base, self.token, _hello(profiles or [], **self.hello, **extra))
        assert welcome["type"] == "welcome", welcome
        if welcome.get("key_secret"):
            self.secret = welcome["key_secret"]
        return ws, welcome


async def _offline(c: httpx.AsyncClient, token: str | None, ws: Any) -> None:
    await ws.close()

    async def online() -> bool:
        return any(m["online"] for m in await _machines(c, token) if m["launcher_key"] == KEY)

    await _eventually(online, lambda v: v is False)


# --- account mode -----------------------------------------------------------


async def test_deploy_to_online_launcher_sends_put_and_tracks_status(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            assert await _recv(ws) == {"type": "definitions_index", "definitions": []}
            r = await _put(c, s.token, KEY, "obs", _def())
            assert r.status_code == 200, r.text
            assert r.json() == {"definition_id": "obs", "revision": 1, "state": "sent",
                                "warnings": []}
            put = await _recv(ws)
            assert put["type"] == "definition_put" and put["definition"]["revision"] == 1
            assert put["definition"]["id"] == "obs" and put["definition"]["kind"] == "observer"
            # the broker normalised it (defaults filled)
            assert put["definition"]["approval"] == {"policy": "auto", "patterns": []}
            agent = await _agent(c, s.token, KEY, "obs")
            assert agent is not None and agent["status"] == "not_deployed"
            assert agent["deploy"]["state"] == "sent"
            await _status(ws, "obs", 1, "applied")
            await _profiles(ws, _prof("obs", source="remote", kind="observer"))
            agent = await _eventually(
                lambda: _agent(c, s.token, KEY, "obs"),
                lambda a: a is not None and a["status"] == "idle"
                and a["deploy"]["state"] == "applied")
            assert agent["source"] == "remote" and agent["kind"] == "observer"
            assert agent["deploy"]["applied_revision"] == 1
            assert agent["approval_policy"] == "auto" and agent["running"] == 0
            (m,) = await _machines(c, s.token)
            assert m["online"] is True and m["mode"] == "local" and m["ceiling"] == "/srv/app"
            assert m["launcher_id"] is not None and m["machine"] == "box"
            assert m["capabilities"] == ["browse", "definitions", "processes"]
            r = await c.get(f"/v1/me/launchers/{KEY}", headers=bearer(s.token))
            assert r.status_code == 200, r.text
            (d,) = r.json()["definitions"]
            assert d["definition"]["id"] == "obs" and d["deploy"]["state"] == "applied"
            # a stale status (older revision) is ignored
            await _status(ws, "obs", 0, "rejected", problems=["old"])
            r = await _put(c, s.token, KEY, "obs", _def(description="v2"))
            assert r.json() == {"definition_id": "obs", "revision": 2, "state": "sent",
                                "warnings": []}
            assert (await _recv(ws))["definition"]["description"] == "v2"
            # invalid definitions are refused with the reason
            r = await _put(c, s.token, KEY, "bad", _def("bad", workdir="/etc"))
            assert r.status_code == 400 and r.json()["error"]["code"] == "outside_ceiling"
            r = await _put(c, s.token, KEY, "bad", _def("bad", nope=1))
            assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_request"
            r = await _put(c, s.token, KEY, "other", _def("obs"))
            assert r.status_code == 400 and r.json()["error"]["code"] == "invalid_request"
            r = await _put(c, s.token, "lk_" + "0" * 32, "obs", _def())
            assert r.status_code == 404 and r.json()["error"]["code"] == "launcher_not_found"


async def test_deploy_to_offline_launcher_is_queued_and_sent_on_reconnect(
    tmp_path: Path,
) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        lnc = _Launcher(s.base, s.token)
        ws, _ = await lnc.connect()
        await _offline(c, s.token, ws)
        r = await _put(c, s.token, KEY, "obs", _def())
        assert r.status_code == 200, r.text
        assert r.json()["state"] == "queued"
        agent = await _agent(c, s.token, KEY, "obs")
        assert agent is not None and agent["status"] == "offline"
        assert agent["deploy"]["state"] == "queued" and agent["source"] == "remote"
        ws, _ = await lnc.connect()
        async with ws:
            assert await _recv(ws) == {
                "type": "definitions_index", "definitions": [{"id": "obs", "revision": 1}]}
            put = await _recv(ws)
            assert put["type"] == "definition_put" and put["definition"]["revision"] == 1
            # the state is recorded right after the frame is sent
            await _eventually(
                lambda: _agent(c, s.token, KEY, "obs"),
                lambda a: a is not None and a["deploy"]["state"] == "sent")


async def test_reconnect_reconverges_after_offline_remove_and_redeploy(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        lnc = _Launcher(s.base, s.token)
        ws, _ = await lnc.connect()
        await _recv(ws)  # index
        assert (await _put(c, s.token, KEY, "obs", _def())).json()["state"] == "sent"
        await _recv(ws)  # put
        await _status(ws, "obs", 1, "applied")
        await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                          lambda a: a["deploy"]["state"] == "applied")
        await _offline(c, s.token, ws)
        r = await _del(c, s.token, KEY, "obs")
        assert r.status_code == 200, r.text
        assert r.json() == {"definition_id": "obs", "revision": 2, "state": "removing"}
        r = await _put(c, s.token, KEY, "web", _def("web"))
        assert r.json() == {"definition_id": "web", "revision": 1, "state": "queued",
                            "warnings": []}
        # removing twice: it is already gone for the API
        assert (await _del(c, s.token, KEY, "obs")).status_code == 404
        ws, _ = await lnc.connect()
        async with ws:
            frames = await _drain(ws)
            assert frames[0] == {"type": "definitions_index",
                                 "definitions": [{"id": "web", "revision": 1}]}
            puts = [f["definition"]["id"] for f in frames if f["type"] == "definition_put"]
            assert puts == ["web"]
            deletes = [(f["id"], f["revision"]) for f in frames
                       if f["type"] == "definition_delete"]
            assert deletes == [("obs", 2)]
            await _status(ws, "obs", 2, "removed")
            await _status(ws, "web", 1, "applied")
            await _eventually(lambda: _agent(c, s.token, KEY, "obs"), lambda a: a is None)
            r = await c.get(f"/v1/me/launchers/{KEY}", headers=bearer(s.token))
            assert [d["definition"]["id"] for d in r.json()["definitions"]] == ["web"]
            await _eventually(lambda: _agent(c, s.token, KEY, "web"),
                              lambda a: a["deploy"]["state"] == "applied")
        await _offline(c, s.token, ws)
        ws, _ = await lnc.connect()
        async with ws:
            frames = await _drain(ws)
            assert frames == [{"type": "definitions_index",
                               "definitions": [{"id": "web", "revision": 1}]}]
        # a queued definition never sent is purged at once when removed
        await _offline(c, s.token, ws)
        assert (await _put(c, s.token, KEY, "tmp", _def("tmp"))).json()["state"] == "queued"
        r = await _del(c, s.token, KEY, "tmp")
        assert r.json() == {"definition_id": "tmp", "revision": 2, "state": "removed"}
        assert await _agent(c, s.token, KEY, "tmp") is None


async def test_rejected_definition_not_resent_and_events(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        lnc = _Launcher(s.base, s.token)
        ws, _ = await lnc.connect()
        await _recv(ws)
        await _put(c, s.token, KEY, "obs", _def())
        await _recv(ws)
        await _status(ws, "obs", 1, "rejected", problems=["no \x1bbubblewrap"] + ["p"] * 30,
                      warnings=["w" * 1000])
        agent = await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                                  lambda a: a["deploy"]["state"] == "rejected")
        problems = agent["deploy"]["problems"]
        assert len(problems) == 20 and problems[0] == "no ?bubblewrap"
        assert [len(w) for w in agent["deploy"]["warnings"]] == [300]
        assert agent["status"] == "not_deployed"
        await _offline(c, s.token, ws)
        ws, _ = await lnc.connect()
        async with ws:
            assert await _drain(ws) == [{"type": "definitions_index",
                                         "definitions": [{"id": "obs", "revision": 1}]}]
        events = [e for e in await _my_events(c, s.token) if e["kind"].startswith("definition.")]
        rejected = [e for e in events if e["kind"] == "definition.rejected"]
        assert rejected and rejected[0]["data"]["problems"][0] == "no ?bubblewrap"
        deployed = [e for e in events if e["kind"] == "definition.deployed"]
        assert deployed[0]["data"] == {"launcher_key": KEY, "machine": "box",
                                       "definition_id": "obs", "revision": 1, "queued": False}


async def _launch(c: httpx.AsyncClient, token: str, launcher_id: str, pid: str) -> None:
    r = await c.post("/v1/sessions", json={}, headers=bearer(token))
    assert r.status_code == 201, r.text
    sid = r.json()["session_id"]
    r = await c.post(f"/v1/sessions/{sid}/participants", headers=bearer(token),
                     json={"role": "coord"})
    assert r.status_code == 201, r.text
    r = await c.post(f"/v1/sessions/{sid}/agents",
                     headers=bearer(r.json()["participant_token"]),
                     json={"launcher_id": launcher_id, "profile_id": pid, "task": "t"})
    assert r.status_code == 202, r.text


async def _applied_obs(c: httpx.AsyncClient, s: Any, ws: Any) -> None:
    await _recv(ws)  # index
    await _put(c, s.token, KEY, "obs", _def())
    await _recv(ws)  # put
    await _status(ws, "obs", 1, "applied")
    await _profiles(ws, _prof("obs", source="remote", kind="observer"))
    await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                      lambda a: a is not None and a["status"] == "idle")


async def test_remove_refused_while_in_use(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, welcome = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _applied_obs(c, s, ws)
            await _launch(c, s.token, welcome["launcher_id"], "obs")
            assert (await _recv(ws))["type"] == "launch"
            agent = await _agent(c, s.token, KEY, "obs")
            assert agent is not None and agent["status"] == "in_session"
            assert agent["running"] == 1
            r = await _del(c, s.token, KEY, "obs")
            assert r.status_code == 409 and r.json()["error"]["code"] == "definition_in_use"


async def test_redeploy_in_use_reports_pending_then_applied(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, welcome = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _applied_obs(c, s, ws)
            await _launch(c, s.token, welcome["launcher_id"], "obs")
            assert (await _recv(ws))["type"] == "launch"
            r = await _put(c, s.token, KEY, "obs", _def(description="v2"))
            assert r.json() == {"definition_id": "obs", "revision": 2, "state": "sent",
                                "warnings": []}
            assert (await _recv(ws))["definition"]["revision"] == 2
            await _status(ws, "obs", 2, "pending_in_use")
            agent = await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                                      lambda a: a["deploy"]["state"] == "pending_in_use")
            assert agent["deploy"]["applied_revision"] == 1
            await _status(ws, "obs", 2, "applied")
            agent = await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                                      lambda a: a["deploy"]["state"] == "applied")
            assert agent["deploy"]["applied_revision"] == 2


async def test_only_the_owner_account_can_deploy(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            r = await _put(c, s.other_token, KEY, "obs", _def())
            assert r.json()["error"]["code"] == "launcher_not_found"
            assert (await _put(c, ADMIN, KEY, "obs", _def())).status_code == 401
            assert (await c.get("/v1/me/launchers")).status_code == 401
            assert await _machines(c, s.other_token) == []
            r = await c.get(f"/v1/me/launchers/{KEY}", headers=bearer(s.other_token))
            assert r.status_code == 404
            r = await c.delete(f"/v1/me/launchers/{KEY}", headers=bearer(s.other_token))
            assert r.status_code == 404
            assert (await _del(c, s.other_token, KEY, "obs")).status_code == 404
            # the owner's legacy (v1, keyless) launcher is listed without a key
            ws1, w1 = await _connect(s.base, s.token, _hello([_prof("old")]))
            async with ws1:
                ms = {m["launcher_key"]: m for m in await _machines(c, s.token)}
                assert set(ms) == {KEY, None}
                legacy = ms[None]
                assert legacy["launcher_id"] == w1["launcher_id"] and legacy["mode"] == "config"
                assert [a["profile_id"] for a in legacy["agents"]] == ["old"]
                assert legacy["agents"][0]["deploy"] is None
                assert legacy["agents"][0]["source"] == "local"


async def test_config_mode_launcher_refuses_deploy(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token, mode="config", ceiling=None).connect()
        async with ws:
            r = await _put(c, s.token, KEY, "obs", _def())
            assert r.status_code == 409
            assert r.json()["error"]["code"] == "launcher_capability_missing"
            (m,) = await _machines(c, s.token)
            assert m["mode"] == "config" and m["capabilities"] == []


async def test_id_conflict_with_local_profile(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect([_prof("obs")])
        async with ws:
            r = await _put(c, s.token, KEY, "obs", _def())
            assert r.status_code == 409
            assert r.json()["error"]["code"] == "definition_id_conflict"
            agent = await _agent(c, s.token, KEY, "obs")
            assert agent is not None and agent["source"] == "local"
            assert agent["deploy"] is None and agent["status"] == "idle"


async def test_definition_limit_per_launcher(tmp_path: Path) -> None:
    from sessionweave.definitions import MAX_DEFINITIONS_PER_LAUNCHER

    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        await _offline(c, s.token, ws)
        for i in range(MAX_DEFINITIONS_PER_LAUNCHER):
            assert (await _put(c, s.token, KEY, f"d{i}", _def(f"d{i}"))).status_code == 200
        r = await _put(c, s.token, KEY, "one-more", _def("one-more"))
        assert r.status_code == 409 and r.json()["error"]["code"] == "definition_limit"
        # updating an existing one is still fine
        assert (await _put(c, s.token, KEY, "d0", _def("d0"))).json()["revision"] == 2


async def test_definition_events(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            await _put(c, s.token, KEY, "obs", _def())
            await _recv(ws)
            await _status(ws, "obs", 1, "applied")
            await _eventually(lambda: _agent(c, s.token, KEY, "obs"),
                              lambda a: a["deploy"]["state"] == "applied")
            r = await _del(c, s.token, KEY, "obs")
            assert r.json()["state"] == "removing"
            assert await _recv(ws) == {"type": "definition_delete", "id": "obs", "revision": 2}
            await _status(ws, "obs", 2, "removed")
            await _eventually(lambda: _agent(c, s.token, KEY, "obs"), lambda a: a is None)
        events = await _my_events(c, s.token)
        kinds = [e["kind"] for e in events]
        assert {"definition.deployed", "definition.applied", "definition.removed"} <= set(kinds)
        removed = next(e for e in events if e["kind"] == "definition.removed")
        assert removed["data"] == {"launcher_key": KEY, "machine": "box",
                                   "definition_id": "obs", "revision": 2}
        # bob sees none of them
        assert not [e for e in await _my_events(c, s.other_token)
                    if e["kind"].startswith("definition.")]


async def test_forget_refused_while_online_then_allowed(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        lnc = _Launcher(s.base, s.token)
        ws, _ = await lnc.connect()
        await _put(c, s.token, KEY, "obs", _def())
        r = await c.delete(f"/v1/me/launchers/{KEY}", headers=bearer(s.token))
        assert r.status_code == 409 and r.json()["error"]["code"] == "launcher_online"
        await _offline(c, s.token, ws)
        r = await c.delete(f"/v1/me/launchers/{KEY}", headers=bearer(s.token))
        assert r.status_code == 204, r.text
        assert await _machines(c, s.token) == []
        r = await c.delete(f"/v1/me/launchers/{KEY}", headers=bearer(s.token))
        assert r.status_code == 404
        # the key is free again: a hello without the old secret gets a new one
        lnc.secret = None
        ws, welcome = await lnc.connect()
        async with ws:
            assert isinstance(welcome["key_secret"], str)
            assert await _recv(ws) == {"type": "definitions_index", "definitions": []}


async def test_open_routes_refused_in_account_mode(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        for cred in ({}, bearer(s.token)):
            r = await c.get("/v1/launchers", headers=cred)
            assert r.status_code == 403
            assert r.json()["error"]["code"] == "permission_denied"
            assert "/v1/me/launchers" in r.json()["error"]["message"]
        r = await c.put(f"/v1/launchers/{KEY}/definitions/obs", json={"definition": _def()})
        assert r.status_code == 403


# --- open mode --------------------------------------------------------------


async def test_open_mode_any_client_can_deploy_to_local_launcher(tmp_path: Path) -> None:
    db = str(tmp_path / "open.db")
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        lnc = _Launcher(base, None)
        ws, _ = await lnc.connect()
        other_key = "lk_" + "ef" * 16
        ws2, _ = await _Launcher(base, None, launcher_key=other_key, mode="root",
                                 ceiling="/", machine="srv").connect()
        async with ws, ws2:
            assert await _recv(ws) == {"type": "definitions_index", "definitions": []}
            r = await c.put(f"/v1/launchers/{KEY}/definitions/obs", json={"definition": _def()})
            assert r.status_code == 200 and r.json()["state"] == "sent"
            assert (await _recv(ws))["type"] == "definition_put"
            # every launcher of the broker is listed, whoever started it
            assert sorted(m["launcher_key"] for m in await _machines(c, None)) == sorted(
                [KEY, other_key])
            await _status(ws, "obs", 1, "applied")
            await _eventually(lambda: _agent(c, None, KEY, "obs"),
                              lambda a: a["deploy"]["state"] == "applied")
            r = await c.get(f"/v1/launchers/{KEY}")
            assert r.status_code == 200 and r.json()["definitions"][0]["deploy"]["revision"] == 1
            r = await c.delete(f"/v1/launchers/{KEY}/definitions/obs")
            assert r.json() == {"definition_id": "obs", "revision": 2, "state": "removing"}
            r = await c.delete(f"/v1/launchers/{KEY}")
            assert r.status_code == 409 and r.json()["error"]["code"] == "launcher_online"
        await _offline(c, None, ws)
        assert (await c.delete(f"/v1/launchers/{KEY}")).status_code == 204
    # the definitions survive a broker restart (queued for an offline machine)
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        r = await c.put(f"/v1/launchers/{other_key}/definitions/web",
                        json={"definition": _def("web", workdir="/var/www")})
        assert r.json()["state"] == "queued"
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        assert [m["launcher_key"] for m in await _machines(c, None)] == [other_key]
        agent = await _agent(c, None, other_key, "web")
        assert agent is not None and agent["deploy"]["state"] == "queued"


async def test_open_mode_config_launcher_and_no_db(tmp_path: Path) -> None:
    db = str(tmp_path / "open.db")
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        assert (await c.get("/v1/info")).json()["capabilities"]["agent_definitions"] is True
        ws, _ = await _Launcher(base, None, mode="config", ceiling=None).connect()
        async with ws:
            r = await c.put(f"/v1/launchers/{KEY}/definitions/obs", json={"definition": _def()})
            assert r.status_code == 409
            assert r.json()["error"]["code"] == "launcher_capability_missing"
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        caps = (await c.get("/v1/info")).json()["capabilities"]
        assert caps["agent_definitions"] is False and caps["open_deploy"] is False
        ws, _ = await _Launcher(base, None).connect([_prof("p1")])
        async with ws:
            r = await c.put(f"/v1/launchers/{KEY}/definitions/obs", json={"definition": _def()})
            assert r.status_code == 503
            assert r.json()["error"]["code"] == "definitions_unavailable"
            assert (await c.delete(f"/v1/launchers/{KEY}/definitions/obs")).status_code == 503
            assert (await c.delete(f"/v1/launchers/{KEY}")).status_code == 503
            # live machines are listed read-only (no capability without a DB)
            (m,) = await _machines(c, None)
            assert m["launcher_key"] == KEY and m["online"] is True
            assert m["capabilities"] == [] and m["last_seen"] is None
            assert [a["profile_id"] for a in m["agents"]] == ["p1"]
            r = await c.get(f"/v1/launchers/{KEY}")
            assert r.status_code == 200 and r.json()["definitions"] == []
            assert (await c.get(f"/v1/launchers/{'lk_' + '1' * 32}")).status_code == 404


async def test_open_mode_with_launcher_token_still_gates_launchers(tmp_path: Path) -> None:
    db = str(tmp_path / "open.db")
    env = {"SW_LAUNCHER_TOKEN": "lt-secret"}
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        ws = await websockets.connect(_ws(base))
        with contextlib.suppress(ConnectionClosed):
            await ws.send(_hello([], **V2))
        code, _reason = await _closed_by_broker(ws)
        assert code == 4401
        ws, _ = await _Launcher(base, "lt-secret").connect()
        async with ws:
            await _recv(ws)
            r = await c.put(f"/v1/launchers/{KEY}/definitions/obs", json={"definition": _def()})
            assert r.status_code == 200 and r.json()["state"] == "sent"
