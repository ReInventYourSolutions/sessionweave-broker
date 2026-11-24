"""Launching agents through launchers: orders, credentials, results, lifecycle."""
from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import httpx
import pytest
import websockets

from tests.integration.accounts_fixtures import ADMIN, seeded_relay
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

DENIED = "only the session's coordinator can request agents — ask the coordinator"


def _prof(pid: str = "p1", domain: str = "backend", role: str = "analyst",
          max_instances: int = 2, **kw: Any) -> dict[str, Any]:
    return {"id": pid, "domain": domain, "role": role, "capabilities": ["sql"],
            "description": "d", "runtime": "claude", "model": "m",
            "access": {"preset": "reader"}, "limits": {}, "max_instances": max_instances,
            **kw}


class FakeLauncher:
    """A launcher client: registers, then records every non-ping frame."""

    def __init__(self, ws: Any, welcome: dict[str, Any]) -> None:
        self.ws = ws
        self.welcome = welcome
        self.launcher_id: str = welcome["launcher_id"]
        self.last_credential = ""
        self.inbox: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._reader = asyncio.create_task(self._read())

    async def _read(self) -> None:
        with contextlib.suppress(Exception):
            async for raw in self.ws:
                msg = json.loads(raw)
                if msg.get("type") != "ping":
                    await self.inbox.put(msg)

    async def expect(self, kind: str, timeout: float = 5.0) -> dict[str, Any]:
        while True:
            msg = await asyncio.wait_for(self.inbox.get(), timeout)
            if msg.get("type") == kind:
                return msg

    async def send(self, msg: dict[str, Any]) -> None:
        await self.ws.send(json.dumps(msg))

    async def close(self) -> None:
        await self.ws.close()
        self._reader.cancel()
        with contextlib.suppress(BaseException):
            await self._reader


@contextlib.asynccontextmanager
async def launcher(
    base: str, profiles: list[dict[str, Any]], token: str | None = None, **hello: Any
) -> AsyncIterator[FakeLauncher]:
    url = f"ws://{urlparse(base).netloc}/v1/launchers/connect"
    headers = {"Authorization": f"Bearer {token}"} if token else {}
    ws = await websockets.connect(url, additional_headers=headers)
    await ws.send(json.dumps({"type": "hello", "machine": hello.pop("machine", "box"),
                              "visibility": hello.pop("visibility", "personal"),
                              "allowed_accounts": [], "profiles": profiles, **hello}))
    welcome = json.loads(await ws.recv())
    assert welcome["type"] == "welcome", welcome
    fl = FakeLauncher(ws, welcome)
    try:
        yield fl
    finally:
        await fl.close()


async def _session(c: httpx.AsyncClient, cred: str | None = None, **body: Any) -> dict[str, Any]:
    r = await c.post("/v1/sessions", json=body, headers=bearer(cred) if cred else {})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _join(c: httpx.AsyncClient, sid: str, cred: str, **body: Any) -> httpx.Response:
    return await c.post(f"/v1/sessions/{sid}/participants", headers=bearer(cred),
                        json={"role": "w", **body})


async def _joined(c: httpx.AsyncClient, sid: str, cred: str, **body: Any) -> dict[str, Any]:
    r = await _join(c, sid, cred, **body)
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _launch(c: httpx.AsyncClient, sid: str, cred: str, **body: Any) -> httpx.Response:
    return await c.post(f"/v1/sessions/{sid}/agents", headers=bearer(cred),
                        json={"task": "analyse the backend", **body})


async def _status(c: httpx.AsyncClient, sid: str, cred: str, lid: str) -> dict[str, Any]:
    r = await c.get(f"/v1/sessions/{sid}/agents/{lid}", headers=bearer(cred))
    assert r.status_code == 200, r.text
    out: dict[str, Any] = r.json()
    return out


async def _until(check: Callable[[], Awaitable[bool]], timeout: float = 5.0) -> None:
    for _ in range(int(timeout / 0.05)):
        if await check():
            return
        await asyncio.sleep(0.05)
    raise AssertionError("condition not met in time")


async def _drain(c: httpx.AsyncClient, sid: str, ptoken: str) -> list[dict[str, Any]]:
    r = await c.post(f"/v1/sessions/{sid}/participants/me/inbox/drain",
                     headers=bearer(ptoken), json={})
    assert r.status_code == 200, r.text
    items: list[dict[str, Any]] = r.json()["messages"]
    return items


class _Setup:
    def __init__(self, c: httpx.AsyncClient, made: dict[str, Any], coord: dict[str, Any]):
        self.c = c
        self.sid: str = made["session_id"]
        self.creator: str = made["creator_token"]
        self.jt: str = made["join_token"]
        self.supervisor: str = made["supervisor_token"]
        self.coord_token: str = coord["participant_token"]
        self.coord_pid: str = coord["participant_id"]


async def _setup(c: httpx.AsyncClient, **config: Any) -> _Setup:
    body: dict[str, Any] = {"config": config} if config else {}
    made = await _session(c, None, **body)
    coord = await _joined(c, made["session_id"], made["creator_token"], role="coord")
    assert coord["coordinator_id"] == coord["participant_id"]
    return _Setup(c, made, coord)


async def test_launch_by_profile_join_bound_and_single_use() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.coord_token,
                              launcher_id=fl.launcher_id, profile_id="p1")
            assert r.status_code == 202, r.text
            lid = r.json()["launch_id"]
            assert r.json() == {"launch_id": lid, "status": "pending",
                                "launcher_id": fl.launcher_id, "machine": "box",
                                "owner": None, "own": True}
            assert lid.startswith("lch_") and len(lid) == 20
            order = await fl.expect("launch")
            assert order["launch_id"] == lid and order["session_id"] == s.sid
            assert order["profile_id"] == "p1" and order["task"] == "analyse the backend"
            # no SW_PUBLIC_BASE_URL: the launcher uses the URL it connected to
            assert "broker_url" not in order
            cred = order["join_credential"]
            assert isinstance(cred, str) and len(cred) > 20
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st == {"launch_id": lid, "status": "pending", "profile_id": "p1",
                          "launcher_id": fl.launcher_id, "machine": "box",
                          "owner": None, "own": True}
            assert cred not in json.dumps(st)
            await fl.send({"type": "launch_result", "launch_id": lid, "status": "started"})
            await _until(lambda: _is_status(c, s, lid, "started"))
            # one slot is reserved
            r = await c.get("/v1/agents", params={"session_id": s.sid, "domain": "backend"},
                            headers=bearer(s.coord_token))
            assert r.json()["agents"][0]["free_slots"] == 1
            # the bound role/capabilities win over the body (force-and-ignore)
            agent = await _joined(c, s.sid, cred, role="admin", capabilities=["root"],
                                  metadata={"launched_by": "me", "note": "hi"})
            roster = (await c.get(f"/v1/sessions/{s.sid}/participants",
                                  headers=bearer(s.creator))).json()["roster"]
            me = next(p for p in roster if p["participant_id"] == agent["participant_id"])
            assert me["role"] == "analyst" and me["capabilities"] == ["sql"]
            assert me["metadata"]["launch_id"] == lid
            assert me["metadata"]["launcher_id"] == fl.launcher_id
            assert me["metadata"]["profile_id"] == "p1"
            assert me["metadata"]["launched_by"] == s.coord_pid
            assert me["metadata"]["note"] == "hi"
            assert agent["coordinator_id"] == s.coord_pid
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["status"] == "joined" and st["participant_id"] == agent["participant_id"]
            # single use
            r = await _join(c, s.sid, cred)
            assert r.status_code == 403, r.text
            # the agent cannot rewrite its launch metadata
            r = await c.patch(f"/v1/sessions/{s.sid}/participants/me",
                              headers=bearer(agent["participant_token"]),
                              json={"metadata": {"launched_by": "x", "k": 1}})
            assert r.status_code == 200, r.text
            md = r.json()["participant"]["metadata"]
            assert md["launched_by"] == s.coord_pid and md["launch_id"] == lid and md["k"] == 1
            # nor its bound role or capabilities
            for change in ({"role": "admin"}, {"capabilities": ["root"]}):
                r = await c.patch(f"/v1/sessions/{s.sid}/participants/me",
                                  headers=bearer(agent["participant_token"]), json=change)
                assert r.status_code == 403, r.text


async def _is_status(c: httpx.AsyncClient, s: _Setup, lid: str, want: str) -> bool:
    return bool((await _status(c, s.sid, s.coord_token, lid))["status"] == want)


async def test_launch_credential_bound_to_its_session() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        other = await _session(c)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.creator, launcher_id=fl.launcher_id, profile_id="p1")
            assert r.status_code == 202, r.text
            cred = (await fl.expect("launch"))["join_credential"]
            r = await _join(c, other["session_id"], cred)
            assert r.status_code == 403, r.text
            # still usable in its own session; launched by the creator, no launched_by
            agent = await _joined(c, s.sid, cred)
            roster = (await c.get(f"/v1/sessions/{s.sid}/participants",
                                  headers=bearer(s.creator))).json()["roster"]
            me = next(p for p in roster if p["participant_id"] == agent["participant_id"])
            assert me["metadata"]["launched_by"] is None


async def test_launch_by_domain_role_slots_and_errors() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        other = await _joined(c, s.sid, s.jt)
        async with launcher(base, [_prof(max_instances=1)], machine="a") as a, \
                launcher(base, [_prof(max_instances=1)], machine="b") as b:
            first = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert first.status_code == 202, first.text
            second = await _launch(c, s.sid, s.coord_token, domain="Backend", role="ANALYST")
            assert second.status_code == 202, second.text
            got = {(await a.expect("launch"))["launch_id"], (await b.expect("launch"))["launch_id"]}
            assert got == {first.json()["launch_id"], second.json()["launch_id"]}
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 409 and r.json()["error"]["code"] == "no_free_slot"
            r = await _launch(c, s.sid, s.coord_token, launcher_id=a.launcher_id,
                              profile_id="p1")
            assert r.status_code == 409 and r.json()["error"]["code"] == "no_free_slot"
            r = await _launch(c, s.sid, s.coord_token, domain="frontend", role="analyst")
            assert r.status_code == 404 and r.json()["error"]["code"] == "profile_not_found"
            r = await _launch(c, s.sid, s.coord_token, launcher_id="lnc_nope", profile_id="p1")
            assert r.status_code == 404 and r.json()["error"]["code"] == "profile_not_found"
            # body validation
            for body in ({"domain": "backend"}, {"launcher_id": a.launcher_id},
                         {"domain": "backend", "role": "analyst", "task": "x" * 16385},
                         {"domain": "backend", "role": "analyst", "task": ""},
                         {"domain": "backend", "role": "analyst", "task": 3}):
                r = await c.post(f"/v1/sessions/{s.sid}/agents",
                                 headers=bearer(s.coord_token), json=body)
                assert r.status_code == 400, (body, r.text)
            # only the coordinator (or creator/admin)
            for cred in (other["participant_token"], s.supervisor, s.jt):
                r = await _launch(c, s.sid, cred, domain="backend", role="analyst")
                assert r.status_code == 403, r.text
            r = await _launch(c, s.sid, other["participant_token"],
                              domain="backend", role="analyst")
            assert r.json()["error"] == {"code": "permission_denied", "message": DENIED}
            r = await c.get(f"/v1/sessions/{s.sid}/agents/{first.json()['launch_id']}",
                            headers=bearer(other["participant_token"]))
            assert r.status_code == 403
            r = await c.get(f"/v1/sessions/{s.sid}/agents/lch_0000000000000000",
                            headers=bearer(s.creator))
            assert r.status_code == 404 and r.json()["error"]["code"] == "launch_not_found"


async def test_launch_cap_and_failed_result_releases() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c, max_launched_agents=1)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            lid = r.json()["launch_id"]
            cred = (await fl.expect("launch"))["join_credential"]
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 409 and r.json()["error"]["code"] == "launch_limit"
            await fl.send({"type": "launch_result", "launch_id": lid, "status": "failed",
                           "error": "runtime not found"})
            await _until(lambda: _is_status(c, s, lid, "failed"))
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["error"] == "runtime not found"
            r = await _join(c, s.sid, cred)
            assert r.status_code == 403  # credential invalidated
            items = await _drain(c, s.sid, s.coord_token)
            failed = [i for i in items if i["name"] == "agent.launch_failed"]
            assert failed and failed[0]["payload"] == {"launch_id": lid,
                                                       "error": "runtime not found"}
            # cap and slot released
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 202, r.text


async def test_launcher_offline_mid_launch_fails() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c, max_launched_agents=1)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            lid = r.json()["launch_id"]
            cred = (await fl.expect("launch"))["join_credential"]
        await _until(lambda: _is_status(c, s, lid, "failed"))
        assert (await _status(c, s.sid, s.coord_token, lid))["error"] == "launcher_offline"
        assert (await _join(c, s.sid, cred)).status_code == 403
        async with launcher(base, [_prof()]):
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 202, r.text  # cap released


async def test_join_timeout_fails_and_invalidates() -> None:
    env = {"SW_LAUNCH_JOIN_TIMEOUT_S": "1"}
    async with relay_subprocess(env_overrides=env) as base, \
            httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            lid = r.json()["launch_id"]
            cred = (await fl.expect("launch"))["join_credential"]
            stop = await fl.expect("stop", timeout=5)
            assert stop == {"type": "stop", "launch_id": lid, "reason": "join_timeout"}
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["status"] == "failed" and st["error"] == "join_timeout"
            assert (await _join(c, s.sid, cred)).status_code == 403


async def _launched_agent(
    c: httpx.AsyncClient, s: _Setup, fl: FakeLauncher, cred_owner: str | None = None
) -> tuple[str, dict[str, Any]]:
    r = await _launch(c, s.sid, cred_owner or s.coord_token,
                      launcher_id=fl.launcher_id, profile_id="p1")
    assert r.status_code == 202, r.text
    lid = r.json()["launch_id"]
    cred = (await fl.expect("launch"))["join_credential"]
    fl.last_credential = cred
    await fl.send({"type": "launch_result", "launch_id": lid, "status": "started"})
    agent = await _joined(c, s.sid, cred)
    return lid, agent


async def test_coordinator_removes_only_its_launched_agents() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        plain = await _joined(c, s.sid, s.jt)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            lid2, by_creator = await _launched_agent(c, s, fl, cred_owner=s.creator)
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{plain['participant_id']}",
                               headers=bearer(s.coord_token))
            assert r.status_code == 403, r.text
            # another participant cannot remove it either
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                               headers=bearer(plain["participant_token"]))
            assert r.status_code == 403
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                               headers=bearer(s.coord_token))
            assert r.status_code == 200, r.text
            assert r.json()["removed"] == [agent["participant_id"]]
            stop = await fl.expect("stop")
            assert stop == {"type": "stop", "launch_id": lid, "reason": "removed"}
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["status"] == "stopped" and st["participant_id"] == agent["participant_id"]
            assert st["reason"] == "removed"
            assert r.json()["join_tokens_invalidated"] == 0
            await _joined(c, s.sid, s.jt)  # the invite link still works
            # an agent the creator launched (launched_by null) is removable too
            r = await c.delete(
                f"/v1/sessions/{s.sid}/participants/{by_creator['participant_id']}",
                headers=bearer(s.coord_token))
            assert r.status_code == 200, r.text
            assert (await fl.expect("stop"))["launch_id"] == lid2
            # the creator still removes anyone
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{plain['participant_id']}",
                               headers=bearer(s.creator))
            assert r.status_code == 200


async def test_new_coordinator_removes_predecessors_agents() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        nxt = await _joined(c, s.sid, s.jt, role="coord2")
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            r = await c.put(f"/v1/sessions/{s.sid}/coordinator", headers=bearer(s.creator),
                            json={"participant_id": nxt["participant_id"]})
            assert r.status_code == 200, r.text
            # the old coordinator lost the right
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                               headers=bearer(s.coord_token))
            assert r.status_code == 403
            # the new one may remove a launched agent, never a plain participant
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{s.coord_pid}",
                               headers=bearer(nxt["participant_token"]))
            assert r.status_code == 403
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                               headers=bearer(nxt["participant_token"]))
            assert r.status_code == 200, r.text
            assert await fl.expect("stop") == {"type": "stop", "launch_id": lid,
                                               "reason": "removed"}


async def test_order_names_coordinator_and_session_owner(tmp_path: Path) -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            assert fl.welcome["account"] is None  # open mode
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.json()["owner"] is None and r.json()["own"] is True
            order = await fl.expect("launch")
            assert order["coordinator_id"] == s.coord_pid
            assert order["session_owner"] is None
            # launched by the creator: the session's coordinator gets the result
            r = await _launch(c, s.sid, s.creator, domain="backend", role="analyst")
            assert (await fl.expect("launch"))["coordinator_id"] == s.coord_pid
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.other_token)  # Bob's session
        coord = await _joined(c, made["session_id"], sd.other_token)
        s = _Setup(c, {**made, "creator_token": sd.other_token}, coord)
        async with launcher(sd.base, [_prof()], token=sd.token, machine="bob-box",
                            visibility="general") as foreign, \
                launcher(sd.base, [_prof()], token=sd.other_token, machine="own") as own:
            assert foreign.welcome["account"] == "Alice"
            assert own.welcome["account"] == "Bob"
            r = await c.get("/v1/agents", params={"session_id": s.sid, "domain": "backend"},
                            headers=bearer(s.coord_token))
            items = [(a["machine"], a["owner"], a["own"]) for a in r.json()["agents"]]
            assert items == [("own", "Bob", True), ("bob-box", "Alice", False)]
            r = await _launch(c, s.sid, s.coord_token, launcher_id=foreign.launcher_id,
                              profile_id="p1")
            assert r.status_code == 202, r.text
            assert r.json()["owner"] == "Alice" and r.json()["own"] is False
            order = await foreign.expect("launch")
            assert order["session_owner"] == "Bob"
            assert order["coordinator_id"] == s.coord_pid
            st = await _status(c, s.sid, s.coord_token, r.json()["launch_id"])
            assert st["owner"] == "Alice" and st["own"] is False


async def test_launch_into_suspended_session_is_refused(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.token)  # Alice's session
        coord = await _joined(c, made["session_id"], sd.token)
        sid = made["session_id"]
        async with launcher(sd.base, [_prof()], token=sd.other_token,
                            visibility="general") as fl:
            r = await c.patch(f"/v1/accounts/{sd.acc.account_id}", headers=bearer(ADMIN),
                              json={"status": "disabled"})
            assert r.status_code == 200, r.text
            r = await _launch(c, sid, coord["participant_token"],
                              launcher_id=fl.launcher_id, profile_id="p1")
            assert r.status_code == 409, r.text
            assert r.json()["error"]["code"] == "session_suspended"
            with pytest.raises(asyncio.TimeoutError):
                await fl.expect("launch", timeout=0.5)


async def test_launcher_stop_removes_participant_with_reason() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            await _drain(c, s.sid, s.coord_token)
            await fl.send({"type": "stopped", "launch_id": lid, "reason": "max_idle_s"})
            await _until(lambda: _is_status(c, s, lid, "stopped"))

            async def gone() -> bool:
                roster = (await c.get(f"/v1/sessions/{s.sid}/participants",
                                      headers=bearer(s.creator))).json()["roster"]
                return agent["participant_id"] not in {p["participant_id"] for p in roster}

            await _until(gone)
            await _joined(c, s.sid, s.jt)  # the invite link still works

            async def has_event() -> bool:
                items = await _drain(c, s.sid, s.coord_token)
                has_event.items.extend(items)  # type: ignore[attr-defined]
                return any(i["name"] == "agent.stopped" for i in has_event.items)  # type: ignore[attr-defined]

            has_event.items = []  # type: ignore[attr-defined]
            await _until(has_event)
            ev = next(i for i in has_event.items  # type: ignore[attr-defined]
                      if i["name"] == "agent.stopped")
            assert ev["payload"]["participant_id"] == agent["participant_id"]
            assert ev["payload"]["reason"] == "max_idle_s"
            # no stop is echoed back to the launcher for its own stop
            with pytest.raises(asyncio.TimeoutError):
                await fl.expect("stop", timeout=0.5)
            # a launcher cannot stop another launcher's launch
            async with launcher(base, [_prof("p2")]) as fl2:
                lid3, _agent3 = await _launched_agent(c, s, fl)
                await fl2.send({"type": "stopped", "launch_id": lid3, "reason": "x"})
                await asyncio.sleep(0.3)
                assert (await _status(c, s.sid, s.coord_token, lid3))["status"] == "joined"


async def test_session_delete_stops_launched_agents() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, _agent = await _launched_agent(c, s, fl)
            r = await _launch(c, s.sid, s.coord_token, launcher_id=fl.launcher_id,
                              profile_id="p1")
            pending = r.json()["launch_id"]
            await fl.expect("launch")
            r = await c.delete(f"/v1/sessions/{s.sid}", headers=bearer(s.creator))
            assert r.status_code == 200, r.text
            stops = {(m := await fl.expect("stop"))["launch_id"]: m["reason"]
                     for _ in range(2)}
            assert stops == {lid: "session_ended", pending: "session_ended"}
            # both slots are free again
            made = await _session(c)
            coord = await _joined(c, made["session_id"], made["creator_token"])
            r = await c.get("/v1/agents", params={"session_id": made["session_id"],
                                                  "domain": "backend"},
                            headers=bearer(coord["participant_token"]))
            assert r.json()["agents"][0]["free_slots"] == 2


async def test_removed_coordinator_and_supervisor_denied() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]):
            q = {"session_id": s.sid, "domain": "backend"}
            r = await c.get("/v1/agents", params=q, headers=bearer(s.supervisor))
            assert r.status_code == 403
            r = await _launch(c, s.sid, s.supervisor, domain="backend", role="analyst")
            assert r.status_code == 403
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{s.coord_pid}",
                               headers=bearer(s.creator))
            assert r.status_code == 200
            r = await c.get("/v1/agents", params=q, headers=bearer(s.coord_token))
            assert r.status_code == 403
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 403


async def test_mcp_discover_launch_and_status() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        made = await _session(c)
        sid = made["session_id"]
        async with launcher(base, [_prof()]) as fl, sw_client(base) as agent:
            joined = await agent.call_tool(
                "session_join", {"session_id": sid, "join_token": made["creator_token"],
                                 "role": "coord"})
            tok = joined["participant_token"]
            out = await agent.call_tool("agents_discover",
                                        {"session_id": sid, "participant_token": tok})
            assert out == {"domains": ["backend"]}
            out = await agent.call_tool("agents_discover", {
                "session_id": sid, "domain": "backend", "participant_token": tok})
            assert out["agents"][0]["profile_id"] == "p1"
            out = await agent.call_tool("agents_launch", {
                "session_id": sid, "domain": "backend", "role": "analyst",
                "task": "look", "participant_token": tok})
            assert out["status"] == "pending"
            lid = out["launch_id"]
            order = await fl.expect("launch")
            assert order["launch_id"] == lid and order["task"] == "look"
            st = await agent.call_tool("agents_launch_status", {
                "session_id": sid, "launch_id": lid, "participant_token": tok})
            assert st["status"] == "pending" and st["launcher_id"] == fl.launcher_id


async def test_account_events_on_launch_and_stop(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.token)
        sid = made["session_id"]
        coord = await _joined(c, sid, sd.token)
        assert coord["coordinator_id"] == coord["participant_id"]
        s = _Setup(c, {**made, "creator_token": sd.token}, coord)
        async with launcher(sd.base, [_prof()], token=sd.token) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            r = await c.delete(f"/v1/sessions/{sid}/participants/{agent['participant_id']}",
                               headers=bearer(coord["participant_token"]))
            assert r.status_code == 200, r.text
            await fl.expect("stop")

            async def events() -> list[dict[str, Any]]:
                r = await c.get("/v1/me/events", headers=bearer(sd.token))
                assert r.status_code == 200, r.text
                return [e for e in r.json()["events"] if e["kind"].startswith("agent.")]

            async def both() -> bool:
                return {e["kind"] for e in await events()} == {"agent.launched", "agent.stopped"}

            await _until(both)
            evs = {e["kind"]: e["data"] for e in await events()}
            assert len(await events()) == 2  # same owner: recorded once
            assert evs["agent.launched"]["session_id"] == sid
            assert evs["agent.launched"]["launch_id"] == lid
            assert evs["agent.launched"]["profile_id"] == "p1"
            assert evs["agent.launched"]["launcher_id"] == fl.launcher_id
            assert evs["agent.stopped"]["reason"] == "removed"
            assert evs["agent.stopped"]["status"] == "stopped"
            dumped = json.dumps(await events())
            assert "join_credential" not in dumped and fl.last_credential not in dumped


async def test_agent_leaving_by_itself_stops_with_left() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/me",
                               headers=bearer(agent["participant_token"]))
            assert r.status_code == 204, r.text
            assert await fl.expect("stop") == {"type": "stop", "launch_id": lid,
                                               "reason": "left"}


async def test_broker_url_only_from_configuration() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            r = await c.post(f"/v1/sessions/{s.sid}/agents",
                             headers={**bearer(s.coord_token), "Host": "evil.example"},
                             json={"domain": "backend", "role": "analyst", "task": "t"})
            assert r.status_code == 202, r.text
            assert "broker_url" not in await fl.expect("launch")
    env = {"SW_PUBLIC_BASE_URL": "https://pub.example/"}
    async with relay_subprocess(env_overrides=env) as base, \
            httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            r = await c.post(f"/v1/sessions/{s.sid}/agents",
                             headers={**bearer(s.coord_token), "Host": "evil.example",
                                      "X-Forwarded-Host": "evil.example"},
                             json={"domain": "backend", "role": "analyst", "task": "t"})
            assert r.status_code == 202, r.text
            assert (await fl.expect("launch"))["broker_url"] == "https://pub.example"


async def _free_slots(c: httpx.AsyncClient, s: _Setup) -> int:
    r = await c.get("/v1/agents", params={"session_id": s.sid, "domain": "backend"},
                    headers=bearer(s.coord_token))
    return int(r.json()["agents"][0]["free_slots"])


async def test_launcher_reconnect_reattaches_running_agents() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            r = await _launch(c, s.sid, s.coord_token, launcher_id=fl.launcher_id,
                              profile_id="p1")
            gone_lid = r.json()["launch_id"]
            await fl.expect("launch")
        # the pending launch failed; the joined one waits for its launcher
        await _until(lambda: _is_status(c, s, gone_lid, "failed"))
        assert (await _status(c, s.sid, s.coord_token, lid))["status"] == "joined"
        secret = fl.welcome["resume_secret"]
        # without the resume secret nothing is re-attached
        async with launcher(base, [_prof()], running=[lid]) as nosecret, \
                launcher(base, [_prof()], running=[lid], resume_secret="wrong") as wrong:
            for x in (nosecret, wrong):
                assert await x.expect("stop") == {"type": "stop", "launch_id": lid,
                                                  "reason": "unknown_launch"}
        async with launcher(base, [_prof()], running=[lid, gone_lid, "lch_ffffffffffffffff"],
                            resume_secret=secret) as fl2:
            assert fl2.welcome["resume_secret"] != secret
            stops = {(m := await fl2.expect("stop"))["launch_id"]: m["reason"]
                     for _ in range(2)}
            assert stops == {gone_lid: "launcher_offline",
                             "lch_ffffffffffffffff": "unknown_launch"}
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["status"] == "joined" and st["launcher_id"] == fl2.launcher_id
            assert await _free_slots(c, s) == 1  # the re-attached agent holds one
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                               headers=bearer(s.coord_token))
            assert r.status_code == 200, r.text
            assert await fl2.expect("stop") == {"type": "stop", "launch_id": lid,
                                                "reason": "removed"}
            assert await _free_slots(c, s) == 2


async def test_launcher_lost_after_grace_removes_agent() -> None:
    env = {"SW_LAUNCHER_RECONNECT_GRACE_S": "1"}
    async with relay_subprocess(env_overrides=env) as base, \
            httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            await _drain(c, s.sid, s.coord_token)
        await _until(lambda: _is_status(c, s, lid, "stopped"))
        assert (await _status(c, s.sid, s.coord_token, lid))["reason"] == "launcher_lost"

        async def gone() -> bool:
            roster = (await c.get(f"/v1/sessions/{s.sid}/participants",
                                  headers=bearer(s.creator))).json()["roster"]
            return agent["participant_id"] not in {p["participant_id"] for p in roster}

        await _until(gone)
        items = await _drain(c, s.sid, s.coord_token)
        ev = next(i for i in items if i["name"] == "agent.stopped")
        assert ev["payload"]["reason"] == "launcher_lost"
        # a late reconnect is told to stop it
        async with launcher(base, [_prof()], running=[lid],
                            resume_secret=fl.welcome["resume_secret"]) as fl2:
            assert await fl2.expect("stop") == {"type": "stop", "launch_id": lid,
                                                "reason": "launcher_lost"}


async def test_reconnect_cannot_claim_another_owners_launch(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.token)
        coord = await _joined(c, made["session_id"], sd.token)
        s = _Setup(c, {**made, "creator_token": sd.token}, coord)
        async with launcher(sd.base, [_prof()], token=sd.token) as fl:
            lid, _agent = await _launched_agent(c, s, fl)
        secret = fl.welcome["resume_secret"]
        # even with the (leaked) secret, another account cannot claim it
        async with launcher(sd.base, [_prof()], token=sd.other_token, running=[lid],
                            resume_secret=secret) as bob:
            assert await bob.expect("stop") == {"type": "stop", "launch_id": lid,
                                                "reason": "unknown_launch"}
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["status"] == "joined" and st["launcher_id"] == fl.launcher_id
        async with launcher(sd.base, [_prof()], token=sd.token, running=[lid],
                            resume_secret=secret, machine="renamed") as again:
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["launcher_id"] == again.launcher_id


async def test_visibility_and_owner_only_auto_pick(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.other_token)  # Bob's session
        coord = await _joined(c, made["session_id"], sd.other_token)
        s = _Setup(c, {**made, "creator_token": sd.other_token}, coord)
        async with launcher(sd.base, [_prof()], token=sd.token, machine="alice-p") as pers, \
                launcher(sd.base, [_prof()], token=sd.token, machine="alice-c",
                         visibility="general", allowed_accounts=["carol"]) as carol_only:
            for lnc in (pers, carol_only):
                r = await _launch(c, s.sid, s.coord_token, launcher_id=lnc.launcher_id,
                                  profile_id="p1")
                assert r.status_code == 404, r.text
                assert r.json()["error"]["code"] == "profile_not_found"
        async with launcher(sd.base, [_prof(max_instances=1)], token=sd.other_token,
                            machine="bob") as own, \
                launcher(sd.base, [_prof()], token=sd.token, machine="alice-g",
                         visibility="general") as foreign:
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 202 and r.json()["launcher_id"] == own.launcher_id
            assert r.json()["machine"] == "bob"
            # own launcher full: a foreign general launcher is not auto-picked
            r = await _launch(c, s.sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 409 and r.json()["error"]["code"] == "no_free_slot"
            r = await _launch(c, s.sid, s.coord_token, launcher_id=foreign.launcher_id,
                              profile_id="p1")
            assert r.status_code == 202, r.text
            assert r.json()["machine"] == "alice-g"
            lid = r.json()["launch_id"]
            assert (await _status(c, s.sid, s.coord_token, lid))["machine"] == "alice-g"


async def test_launch_after_launcher_token_revoked_is_refused(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        r = await c.post(f"/v1/accounts/{sd.acc.account_id}/tokens",
                         headers=bearer(ADMIN), json={"label": "l"})
        minted = r.json()
        made = await _session(c, sd.token)
        coord = await _joined(c, made["session_id"], sd.token)
        s = _Setup(c, {**made, "creator_token": sd.token}, coord)
        async with launcher(sd.base, [_prof()], token=minted["token"]) as fl:
            r = await c.delete(f"/v1/accounts/{sd.acc.account_id}/tokens/{minted['token_id']}",
                               headers=bearer(ADMIN))
            assert r.status_code in (200, 204)
            r = await _launch(c, s.sid, s.coord_token, launcher_id=fl.launcher_id,
                              profile_id="p1")
            assert r.status_code == 404, r.text


async def test_cold_session_expiry_stops_launched_agents(tmp_path: Path) -> None:
    """A persisted session purged by the expiry sweep while cold still stops
    its launched agents and releases their slots."""
    from sessionweave.access import Principal, generate_token, hash_token
    from sessionweave.config import load_deployment_defaults
    from sessionweave.connections import ConnectionManager
    from sessionweave.persistence import PersistenceStore
    from sessionweave.registry import SessionRegistry
    from sessionweave.schemas import PersistenceConfig
    from sessionweave.tools import ToolDeps
    from tests.unit.fakes import FakeHandle

    sent: list[dict[str, Any]] = []

    class Conn:
        async def send_json(self, data: Any) -> None:
            sent.append(data)

    store = PersistenceStore(str(tmp_path / "cold.db"))
    await store.init(pool_size=2)
    try:
        reg = SessionRegistry(defaults=load_deployment_defaults({}),
                              connections=ConnectionManager(), store=store)
        deps = ToolDeps(reg)
        sid, _, _, _, _ = await reg.create(
            overrides={}, purpose=None,
            persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
            creator_token_hash=hash_token(generate_token()),
        )
        sess = await reg.get_or_raise(sid)
        coord = await sess.join(FakeHandle(), role="c", as_coordinator=True)
        lnc_id, _ = deps.launchers.register(Conn(), None, {
            "machine": "m", "profiles": [_prof()]})
        out = await deps.launches.launch(
            sess, Principal("participant", coord.participant_id),
            {"launcher_id": lnc_id, "profile_id": "p1", "task": "t"},
            broker_url=None, owner_name=None)
        lid = out["launch_id"]
        reg._sessions.pop(sid)  # cold
        purged = await reg.sweep_expired_once(now_ms=10**15, safety_seconds=0)
        assert purged == 1
        await asyncio.sleep(0.05)
        assert {"type": "stop", "launch_id": lid, "reason": "session_ended"} in sent
        launch = deps.launches.get(lid)
        assert launch is not None and launch.status == "stopped"
        assert deps.launchers._launchers[lnc_id].running["p1"] == 0
    finally:
        await store.close()


async def test_halt_messages_name_who_stopped_the_agent(tmp_path: Path) -> None:
    db = str(tmp_path / "halt.db")
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        made = await _session(c, None, persistence={"mode": "ttl", "ttl_seconds": 3600})
        coord = await _joined(c, made["session_id"], made["creator_token"])
        s = _Setup(c, made, coord)
        async with launcher(base, [_prof()]) as fl:
            _lid, a1 = await _launched_agent(c, s, fl)
            lid2, a2 = await _launched_agent(c, s, fl)
            r = await c.delete(f"/v1/sessions/{s.sid}/participants/{a1['participant_id']}",
                               headers=bearer(s.coord_token))
            assert r.status_code == 200, r.text
            await fl.send({"type": "stopped", "launch_id": lid2, "reason": "max_cost_usd"})
            await _until(lambda: _is_status(c, s, lid2, "stopped"))

            async def halts() -> dict[str, str]:
                r = await c.get(f"/v1/sessions/{s.sid}/events",
                                params={"kind": "supervisor.note"}, headers=bearer(s.creator))
                assert r.status_code == 200, r.text
                return {e["payload"]["target"]: e["payload"]["message"]
                        for e in r.json()["events"] if e["payload"].get("kind") == "halt"}

            async def both() -> bool:
                return len(await halts()) == 2

            await _until(both)
            got = await halts()
            assert got[a1["participant_id"]] == "removed by the coordinator"
            assert got[a2["participant_id"]] == "stopped by its launcher: max_cost_usd"


async def test_quick_reconnect_takes_over_half_open_launcher() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            lid, agent = await _launched_agent(c, s, fl)
            secret = fl.welcome["resume_secret"]
            # the old socket is still open (half-open from the launcher's view)
            async with launcher(base, [_prof()], running=[lid], resume_secret=secret) as fl2:
                with pytest.raises(asyncio.TimeoutError):
                    await fl2.expect("stop", timeout=0.5)
                await asyncio.wait_for(fl.ws.wait_closed(), 5)
                assert fl.ws.close_code == 4409
                st = await _status(c, s.sid, s.coord_token, lid)
                assert st["status"] == "joined" and st["launcher_id"] == fl2.launcher_id
                assert await _free_slots(c, s) == 1
                # the old secret no longer claims it
                async with launcher(base, [_prof()], running=[lid],
                                    resume_secret=secret) as fl3:
                    assert (await fl3.expect("stop"))["reason"] == "unknown_launch"
                r = await c.delete(
                    f"/v1/sessions/{s.sid}/participants/{agent['participant_id']}",
                    headers=bearer(s.coord_token))
                assert r.status_code == 200, r.text
                assert (await fl2.expect("stop"))["launch_id"] == lid


async def test_launcher_dropping_during_join_arms_lost_timer() -> None:
    from sessionweave.access import Principal
    from sessionweave.config import load_deployment_defaults
    from sessionweave.connections import ConnectionManager
    from sessionweave.registry import SessionRegistry
    from sessionweave.tools import ToolDeps
    from tests.unit.fakes import FakeHandle

    class Conn:
        async def send_json(self, data: Any) -> None:
            self.order = data

    reg = SessionRegistry(defaults=load_deployment_defaults({}),
                          connections=ConnectionManager())
    deps = ToolDeps(reg)
    deps.launches.reconnect_grace_s = 0.1
    sid, _, _, _, _ = await reg.create(overrides={}, purpose=None)
    sess = await reg.get_or_raise(sid)
    coord = await sess.join(FakeHandle(), role="c", as_coordinator=True)
    conn = Conn()
    lnc_id, _ = deps.launchers.register(conn, None, {"machine": "m", "profiles": [_prof()]})
    out = await deps.launches.launch(
        sess, Principal("participant", coord.participant_id),
        {"launcher_id": lnc_id, "profile_id": "p1", "task": "t"},
        broker_url=None, owner_name=None)
    lch = deps.launches.get(out["launch_id"])
    assert lch is not None and lch.token_id is not None
    _lch, role, caps = deps.launches.begin_join(sess, lch.token_id)
    deps.launchers.unregister(lnc_id)  # the launcher drops mid-join
    assert lch.status == "pending"
    p = await sess.join(FakeHandle(), role=role, capabilities=caps,
                        metadata=deps.launches.join_metadata(lch))
    deps.launches.joined(sess, lch, p.participant_id)
    assert lch.status == "joined" and lch.lost_timer is not None
    for _ in range(50):
        if lch.status == "stopped" and p.participant_id not in sess._participants:
            break
        await asyncio.sleep(0.05)
    assert lch.status == "stopped" and lch.reason == "launcher_lost"
    assert p.participant_id not in sess._participants


async def test_failed_result_message_and_runtime_failed_stop() -> None:
    """A launcher's preflight refusal carries a message; a runtime that fails
    later stops the agent with runtime_failed."""
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        s = await _setup(c)
        async with launcher(base, [_prof()]) as fl:
            r = await _launch(c, s.sid, s.coord_token, launcher_id=fl.launcher_id,
                              profile_id="p1")
            lid = r.json()["launch_id"]
            await fl.expect("launch")
            await fl.send({"type": "launch_result", "launch_id": lid, "status": "failed",
                           "error": "runtime_unavailable",
                           "message": "opencode 2.x or later is required\n" + "x" * 400})
            await _until(lambda: _is_status(c, s, lid, "failed"))
            st = await _status(c, s.sid, s.coord_token, lid)
            assert st["error"] == "runtime_unavailable"
            assert st["message"].startswith("opencode 2.x or later is required?x")
            assert len(st["message"]) == 300
            items = await _drain(c, s.sid, s.coord_token)
            failed = [i for i in items if i["name"] == "agent.launch_failed"]
            assert failed[0]["payload"] == {"launch_id": lid, "error": "runtime_unavailable",
                                            "message": st["message"]}

            lid2, _agent = await _launched_agent(c, s, fl)
            assert "message" not in await _status(c, s.sid, s.coord_token, lid2)
            await fl.send({"type": "stopped", "launch_id": lid2, "reason": "runtime_failed"})
            await _until(lambda: _is_status(c, s, lid2, "stopped"))
            assert (await _status(c, s.sid, s.coord_token, lid2))["reason"] == "runtime_failed"

            async def stopped_event() -> bool:
                items = await _drain(c, s.sid, s.coord_token)
                return any(i["name"] == "agent.stopped"
                           and i["payload"]["reason"] == "runtime_failed" for i in items)

            await _until(stopped_event)


async def test_failed_launch_account_event_has_status_and_message(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, sd.token)
        coord = await _joined(c, made["session_id"], sd.token)
        s = _Setup(c, {**made, "creator_token": sd.token}, coord)
        async with launcher(sd.base, [_prof()], token=sd.token) as fl:
            r = await _launch(c, s.sid, s.coord_token, launcher_id=fl.launcher_id,
                              profile_id="p1")
            lid = r.json()["launch_id"]
            await fl.expect("launch")
            await fl.send({"type": "launch_result", "launch_id": lid, "status": "failed",
                           "error": "runtime_unavailable", "message": "cline missing"})

            async def stopped() -> list[dict[str, Any]]:
                r = await c.get("/v1/me/events", headers=bearer(sd.token))
                return [e["data"] for e in r.json()["events"] if e["kind"] == "agent.stopped"]

            async def has() -> bool:
                return bool(await stopped())

            await _until(has)
            [data] = await stopped()
            assert data["status"] == "failed" and data["reason"] == "runtime_unavailable"
            assert data["message"] == "cline missing" and data["launch_id"] == lid


async def test_session_going_cold_stops_launched_agents(tmp_path: Path) -> None:
    """A persisted session evicted from RAM by the idle TTL (cold, resumable)
    stops its launched agents with session_cold; an ephemeral one that idles
    out ends, and its agents stop with session_ended."""
    from datetime import UTC, datetime, timedelta

    from sessionweave.access import Principal, generate_token, hash_token
    from sessionweave.config import load_deployment_defaults
    from sessionweave.connections import ConnectionManager
    from sessionweave.persistence import PersistenceStore
    from sessionweave.registry import SessionRegistry
    from sessionweave.schemas import PersistenceConfig
    from sessionweave.tools import ToolDeps
    from tests.unit.fakes import FakeHandle

    sent: list[dict[str, Any]] = []

    class Conn:
        async def send_json(self, data: Any) -> None:
            sent.append(data)

    store = PersistenceStore(str(tmp_path / "cold.db"))
    await store.init(pool_size=2)
    try:
        reg = SessionRegistry(defaults=load_deployment_defaults({}),
                              connections=ConnectionManager(), store=store)
        deps = ToolDeps(reg)
        lnc_id, _ = deps.launchers.register(Conn(), None, {
            "machine": "m", "profiles": [_prof()]})
        lids: dict[str, str] = {}
        for kind, persistence in (
            ("persisted", PersistenceConfig(mode="ttl", ttl_seconds=86400)),
            ("ephemeral", None),
        ):
            sid, _, _, _, _ = await reg.create(
                overrides={}, purpose=None, persistence=persistence,
                creator_token_hash=hash_token(generate_token()),
            )
            sess = await reg.get_or_raise(sid)
            coord = await sess.join(FakeHandle(), role="c", as_coordinator=True)
            out = await deps.launches.launch(
                sess, Principal("participant", coord.participant_id),
                {"launcher_id": lnc_id, "profile_id": "p1", "task": "t"},
                broker_url=None, owner_name=None)
            lids[kind] = out["launch_id"]
        events: list[tuple[str, str, dict[str, Any]]] = []
        deps.launches._account_event = (  # type: ignore[method-assign]
            lambda lch, kind, **extra: events.append((lch.launch_id, kind, extra)))
        reg.tick(now=datetime.now(UTC) + timedelta(days=2))
        await asyncio.sleep(0.05)
        assert (lids["persisted"], "agent.stopped", {"reason": "session_cold"}) in events
        assert (lids["ephemeral"], "agent.stopped", {"reason": "session_ended"}) in events
        assert {"type": "stop", "launch_id": lids["persisted"],
                "reason": "session_cold"} in sent
        assert {"type": "stop", "launch_id": lids["ephemeral"],
                "reason": "session_ended"} in sent
        cold = deps.launches.get(lids["persisted"])
        assert cold is not None and cold.status == "stopped"
        assert cold.reason == "session_cold"
        assert deps.launchers._launchers[lnc_id].running["p1"] == 0
    finally:
        await store.close()


async def test_ownerless_session_sees_only_profiles_open_to_everyone(
    tmp_path: Path,
) -> None:
    """Account mode, a session created with the admin token and no owner:
    general profiles whose allowed_accounts is empty (everyone) are visible
    and launchable by id; personal ones and lists naming accounts are not."""
    async with seeded_relay(tmp_path, mode="account") as sd, \
            httpx.AsyncClient(base_url=sd.base) as c:
        made = await _session(c, ADMIN)
        sid = made["session_id"]
        coord = await _joined(c, sid, made["creator_token"])
        s = _Setup(c, made, coord)
        async with launcher(sd.base, [_prof("pers")], token=sd.token,
                            machine="personal") as pers, \
                launcher(sd.base, [_prof("open"), _prof("bobonly", allowed_accounts=["bob"]),
                                   _prof("own-personal", visibility="personal")],
                         token=sd.token, machine="general", visibility="general") as gen, \
                launcher(sd.base, [_prof("carol"), _prof("override", allowed_accounts=[])],
                         token=sd.other_token, machine="carol", visibility="general",
                         allowed_accounts=["carol"]) as carol:
            r = await c.get("/v1/agents", params={"session_id": sid, "domain": "backend"},
                            headers=bearer(s.coord_token))
            assert r.status_code == 200, r.text
            seen = {(i["machine"], i["profile_id"]) for i in r.json()["agents"]}
            assert seen == {("general", "open"), ("carol", "override")}
            r = await c.get("/v1/agents/domains", params={"session_id": sid},
                            headers=bearer(s.coord_token))
            assert r.json()["domains"] == ["backend"]
            r = await _launch(c, sid, s.coord_token, launcher_id=gen.launcher_id,
                              profile_id="open")
            assert r.status_code == 202, r.text
            order = await gen.expect("launch")
            assert order["session_owner"] is None
            for lnc, pid in ((pers, "pers"), (gen, "bobonly"), (gen, "own-personal"),
                             (carol, "carol")):
                r = await _launch(c, sid, s.coord_token, launcher_id=lnc.launcher_id,
                                  profile_id=pid)
                assert r.status_code == 404, (pid, r.text)
            # no own launcher: a domain + role launch auto-picks nothing
            r = await _launch(c, sid, s.coord_token, domain="backend", role="analyst")
            assert r.status_code == 404, r.text
