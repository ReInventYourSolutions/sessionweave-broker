"""Remote browse and process listing relayed to a launcher: owner-only in
account mode (``/v1/me/launchers/{key}/browse|processes``), any client of an
open-mode broker with a persistence DB (``/v1/launchers/...``). Results are
relayed, never stored."""
from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from pathlib import Path
from typing import Any

import httpx
import pytest

from tests.integration.accounts_fixtures import ADMIN, seeded_relay
from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess
from tests.integration.test_agent_deploy import KEY, _Launcher, _offline, _recv

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

Answer = Callable[[Any, dict[str, Any]], Awaitable[None]]


@contextlib.asynccontextmanager
async def _answering(ws: Any, answer: Answer) -> AsyncIterator[list[dict[str, Any]]]:
    """Run a fake launcher loop: every browse/processes request is recorded
    and handed to ``answer``."""
    seen: list[dict[str, Any]] = []

    async def loop() -> None:
        while True:
            msg = await _recv(ws, 30)
            if msg["type"] in ("browse", "processes"):
                seen.append(msg)
                await answer(ws, msg)

    task = asyncio.create_task(loop())
    try:
        yield seen
    finally:
        task.cancel()
        with contextlib.suppress(BaseException):
            await task


def _ok_browse(entries: list[dict[str, Any]], next_cursor: str | None = None) -> Answer:
    async def answer(ws: Any, msg: dict[str, Any]) -> None:
        await ws.send(json.dumps({
            "type": "browse_result", "request_id": msg["request_id"], "ok": True,
            "path": msg["path"], "entries": entries, "next_cursor": next_cursor}))
    return answer


def _fail(error: str, kind: str = "browse_result") -> Answer:
    async def answer(ws: Any, msg: dict[str, Any]) -> None:
        await ws.send(json.dumps({"type": kind, "request_id": msg["request_id"],
                                  "ok": False, "error": error}))
    return answer


async def _silent(ws: Any, msg: dict[str, Any]) -> None:
    return None


def _code(r: httpx.Response) -> str:
    return str(r.json()["error"]["code"])


async def test_browse_relayed_to_owner_launcher(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)  # definitions_index
            async with _answering(ws, _ok_browse([{"name": "logs", "kind": "dir"}], "logs")) \
                    as seen:
                url = f"/v1/me/launchers/{KEY}/browse"
                r = await c.get(url, headers=bearer(s.token))  # path defaults to ceiling
                assert r.status_code == 200, r.text
                assert r.json() == {"path": "/srv/app",
                                    "entries": [{"name": "logs", "kind": "dir"}],
                                    "next_cursor": "logs"}
                assert seen[0]["path"] == "/srv/app" and seen[0]["limit"] == 100
                assert seen[0]["cursor"] is None and seen[0]["hidden"] is False
                r = await c.get(url, headers=bearer(s.token), params={
                    "path": "/srv/app/logs", "cursor": "a.log", "limit": "5", "hidden": "1"})
                assert r.status_code == 200, r.text
                assert {k: seen[1][k] for k in ("path", "cursor", "limit", "hidden")} == {
                    "path": "/srv/app/logs", "cursor": "a.log", "limit": 5, "hidden": True}
                # query validation never reaches the launcher
                for bad in ({"path": "relative"}, {"path": "/" + "x" * 4096},
                            {"path": "/a\x00b"}, {"limit": "0"}, {"limit": "x"},
                            {"limit": "1_000"}, {"limit": " 5"}, {"limit": "+5"},
                            {"limit": "5 "}, {"limit": "-1"}, {"limit": "\u0665"},
                            {"cursor": "c" * 513}, {"hidden": "yes"}):
                    r = await c.get(url, headers=bearer(s.token), params=bad)
                    assert r.status_code == 400 and _code(r) == "invalid_request", bad
                assert len(seen) == 2


async def test_browse_reply_is_validated(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            url = f"/v1/me/launchers/{KEY}/browse"
            for entries in ([{"name": "a/b", "kind": "dir"}], [{"name": "", "kind": "dir"}],
                            [{"name": "x" * 256, "kind": "file"}],
                            [{"name": "a", "kind": "socket"}],
                            [{"name": ".", "kind": "dir"}], [{"name": "..", "kind": "dir"}],
                            [{"name": "a", "kind": "file", "size": -1}],
                            [{"name": str(i), "kind": "file"} for i in range(3)]):
                async with _answering(ws, _ok_browse(entries)):
                    r = await c.get(url, headers=bearer(s.token), params={"limit": "2"})
                    assert r.status_code == 502 and _code(r) == "launcher_bad_reply", entries
            # optional fields pass, unknown entry keys are dropped
            entry = {"name": "a.log", "kind": "file", "size": 12, "mtime": 1.5,
                     "inside": True, "extra": "x"}
            async with _answering(ws, _ok_browse([entry])):
                r = await c.get(url, headers=bearer(s.token))
                assert r.status_code == 200, r.text
                assert r.json()["entries"] == [{"name": "a.log", "kind": "file", "size": 12,
                                                "mtime": 1.5, "inside": True}]


async def test_browse_errors_map(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            url = f"/v1/me/launchers/{KEY}/browse"
            for error, status, code in (
                ("outside_ceiling", 400, "outside_ceiling"),
                ("not_found", 404, "path_not_found"),
                ("not_a_directory", 400, "invalid_request"),
                ("bad_request", 400, "invalid_request"),
                ("permission_denied", 403, "permission_denied"),
                ("exploded", 502, "launcher_bad_reply"),
            ):
                async with _answering(ws, _fail(error)):
                    r = await c.get(url, headers=bearer(s.token), params={"path": "/etc"})
                    assert (r.status_code, _code(r)) == (status, code), error


async def test_browse_timeout_and_offline(tmp_path: Path) -> None:
    env = {"SW_LAUNCHER_RELAY_TIMEOUT_S": "0.2"}
    async with seeded_relay(tmp_path, mode="account", env_extra=env) as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        url = f"/v1/me/launchers/{KEY}/browse"
        async with ws:
            await _recv(ws)
            async with _answering(ws, _silent):
                r = await c.get(url, headers=bearer(s.token))
                assert r.status_code == 504 and _code(r) == "launcher_timeout"
        await _offline(c, s.token, ws)
        r = await c.get(url, headers=bearer(s.token))
        assert r.status_code == 409 and _code(r) == "launcher_offline"
        r = await c.get(f"/v1/me/launchers/{KEY}/processes", headers=bearer(s.token))
        assert r.status_code == 409 and _code(r) == "launcher_offline"


async def test_browse_result_over_frame_limit_is_refused(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            # 300 entries of ~1 KiB each: about 300 KiB, over the 256 KiB frame limit
            big = [{"name": f"{i:03d}" + "x" * 1000, "kind": "file"} for i in range(300)]

            async def answer(sock: Any, msg: dict[str, Any]) -> None:
                await sock.send(json.dumps({
                    "type": "browse_result", "request_id": msg["request_id"], "ok": True,
                    "path": msg["path"], "entries": big, "next_cursor": None}))

            async with _answering(ws, answer):
                r = await c.get(f"/v1/me/launchers/{KEY}/browse", headers=bearer(s.token),
                                params={"limit": "300"})
                assert r.status_code == 409 and _code(r) == "launcher_offline"
            await asyncio.wait_for(ws.wait_closed(), 5)
            assert ws.close_code == 1009


async def test_processes_relayed_and_limit_capped(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        procs = [{"pid": 42, "ppid": 1, "user": "app", "name": "nginx",
                  "cmdline": "nginx: master process", "started_at": 1700000000}]

        async def answer(sock: Any, msg: dict[str, Any]) -> None:
            await sock.send(json.dumps({
                "type": "processes_result", "request_id": msg["request_id"], "ok": True,
                "processes": procs, "next_cursor": 42}))

        async with ws:
            await _recv(ws)
            async with _answering(ws, answer) as seen:
                url = f"/v1/me/launchers/{KEY}/processes"
                r = await c.get(url, headers=bearer(s.token),
                                params={"limit": "1000", "q": "nginx", "cursor": "7"})
                assert r.status_code == 200, r.text
                assert r.json() == {"processes": procs, "next_cursor": "42"}
                assert seen[0]["type"] == "processes"
                assert {k: seen[0][k] for k in ("limit", "query", "cursor")} == {
                    "limit": 200, "query": "nginx", "cursor": 7}
                for bad in ({"q": "x" * 101}, {"cursor": "abc"}, {"cursor": "-1"},
                            {"limit": "1_000"}, {"limit": "+5"}):
                    r = await c.get(url, headers=bearer(s.token), params=bad)
                    assert r.status_code == 400 and _code(r) == "invalid_request", bad
                assert len(seen) == 1
            # command lines are at most 400 characters (the Runner's CMDLINE_MAX)
            for cmdline, status in (("c" * 400, 200), ("c" * 401, 502)):
                procs[0]["cmdline"] = cmdline
                async with _answering(ws, answer):
                    r = await c.get(f"/v1/me/launchers/{KEY}/processes",
                                    headers=bearer(s.token))
                    assert r.status_code == status, (len(cmdline), r.text)
            # a browse_result does not answer a processes request
            async with _answering(ws, _fail("not_found", kind="processes_result")):
                r = await c.get(f"/v1/me/launchers/{KEY}/processes", headers=bearer(s.token))
                assert r.status_code == 404 and _code(r) == "path_not_found"


async def test_browse_owner_only(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token).connect()
        async with ws:
            await _recv(ws)
            async with _answering(ws, _ok_browse([])) as seen:
                for path in ("browse", "processes"):
                    url = f"/v1/me/launchers/{KEY}/{path}"
                    assert (await c.get(url, headers=bearer(s.other_token))).status_code == 404
                    assert (await c.get(url, headers=bearer(ADMIN))).status_code == 401
                    assert (await c.get(url)).status_code == 401
                    # the open-mode route is refused in account mode
                    r = await c.get(f"/v1/launchers/{KEY}/{path}")
                    assert r.status_code == 403 and _code(r) == "permission_denied"
                r = await c.get("/v1/me/launchers/lk_nope/browse", headers=bearer(s.token))
                assert r.status_code == 404
                assert seen == []


async def test_browse_needs_capability(tmp_path: Path) -> None:
    async with seeded_relay(tmp_path, mode="account") as s, \
            httpx.AsyncClient(base_url=s.base) as c:
        ws, _ = await _Launcher(s.base, s.token, capabilities=["definitions"]).connect()
        async with ws:
            for path in ("browse", "processes"):
                r = await c.get(f"/v1/me/launchers/{KEY}/{path}", headers=bearer(s.token))
                assert r.status_code == 409 and _code(r) == "launcher_capability_missing"


async def test_open_mode_browse_and_processes_without_credentials(tmp_path: Path) -> None:
    db = str(tmp_path / "open.db")
    async with relay_subprocess(persistence_db_path=db) as base, \
            httpx.AsyncClient(base_url=base) as c:
        ws, _ = await _Launcher(base, None, mode="root", ceiling="/").connect()

        async def answer(sock: Any, msg: dict[str, Any]) -> None:
            if msg["type"] == "browse":
                await _ok_browse([{"name": "etc", "kind": "dir"}])(sock, msg)
            else:
                await sock.send(json.dumps({
                    "type": "processes_result", "request_id": msg["request_id"], "ok": True,
                    "processes": [], "next_cursor": None}))

        async with ws:
            await _recv(ws)
            async with _answering(ws, answer) as seen:
                r = await c.get(f"/v1/launchers/{KEY}/browse")
                assert r.status_code == 200, r.text
                assert r.json() == {"path": "/", "entries": [{"name": "etc", "kind": "dir"}],
                                    "next_cursor": None}
                r = await c.get(f"/v1/launchers/{KEY}/processes")
                assert r.json() == {"processes": [], "next_cursor": None}
                assert [m["type"] for m in seen] == ["browse", "processes"]


async def test_open_mode_without_db_browse_unavailable() -> None:
    async with relay_subprocess() as base, httpx.AsyncClient(base_url=base) as c:
        ws, _ = await _Launcher(base, None).connect()
        async with ws:
            for path in ("browse", "processes"):
                r = await c.get(f"/v1/launchers/{KEY}/{path}")
                assert r.status_code == 503 and _code(r) == "definitions_unavailable"
