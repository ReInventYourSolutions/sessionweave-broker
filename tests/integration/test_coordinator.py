"""Session coordinator: who may discover/launch agents; set, clear, persist."""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest

from sessionweave.config import (
    ConfigBounds,
    SessionConfig,
    apply_overrides,
    load_deployment_defaults,
)
from sessionweave.errors import ToolError
from tests.integration.accounts_fixtures import Seeded, seeded_relay
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration]


async def _create(c: httpx.AsyncClient, **body: Any) -> dict[str, Any]:
    r = await c.post("/v1/sessions", json=body)
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _join(c: httpx.AsyncClient, sid: str, cred: str, role: str = "w") -> dict[str, Any]:
    r = await c.post(f"/v1/sessions/{sid}/participants", headers=bearer(cred), json={"role": role})
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _coord(c: httpx.AsyncClient, sid: str, cred: str) -> str | None:
    r = await c.get(f"/v1/sessions/{sid}", headers=bearer(cred))
    assert r.status_code == 200, r.text
    got = r.json()["coordinator_id"]
    r2 = await c.get(f"/v1/sessions/{sid}/participants", headers=bearer(cred))
    assert r2.json()["coordinator_id"] == got
    assert got is None or isinstance(got, str)
    return got


async def _put(
    c: httpx.AsyncClient, sid: str, cred: str, pid: str | None
) -> httpx.Response:
    return await c.put(
        f"/v1/sessions/{sid}/coordinator", headers=bearer(cred), json={"participant_id": pid}
    )


@pytest.mark.asyncio
async def test_creator_join_becomes_coordinator_once() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
            sid, creator, jt = made["session_id"], made["creator_token"], made["join_token"]
            assert await _coord(c, sid, creator) is None
            joiner = await _join(c, sid, jt)
            assert await _coord(c, sid, creator) is None  # join token never coordinates
            first = await _join(c, sid, creator, "coord")
            assert await _coord(c, sid, creator) == first["participant_id"]
            await _join(c, sid, creator, "coord2")  # does not replace
            assert await _coord(c, sid, creator) == first["participant_id"]
            assert joiner["participant_id"] != first["participant_id"]


@pytest.mark.asyncio
async def test_set_clear_and_permissions() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
            sid, creator, jt = made["session_id"], made["creator_token"], made["join_token"]
            a = await _join(c, sid, jt, "a")
            b = await _join(c, sid, jt, "b")
            r = await _put(c, sid, creator, a["participant_id"])
            assert r.status_code == 200, r.text
            assert r.json() == {"coordinator_id": a["participant_id"]}
            assert await _coord(c, sid, creator) == a["participant_id"]
            r = await _put(c, sid, creator, b["participant_id"])
            assert await _coord(c, sid, creator) == b["participant_id"]
            # non-creators refused
            for cred in (a["participant_token"], jt, made["supervisor_token"]):
                r = await _put(c, sid, cred, a["participant_id"])
                assert r.status_code == 403, (cred, r.text)
            assert await _coord(c, sid, creator) == b["participant_id"]
            # unknown participant
            r = await _put(c, sid, creator, "p-nope")
            assert r.status_code == 404, r.text
            assert r.json()["error"]["code"] == "participant_not_found"
            # clear
            r = await _put(c, sid, creator, None)
            assert r.status_code == 200
            assert r.json() == {"coordinator_id": None}
            assert await _coord(c, sid, creator) is None


@pytest.mark.asyncio
async def test_leave_and_removal_clear_without_promotion() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
            sid, creator, jt = made["session_id"], made["creator_token"], made["join_token"]
            coord = await _join(c, sid, creator, "coord")
            await _join(c, sid, jt, "other")
            assert await _coord(c, sid, creator) == coord["participant_id"]
            r = await c.delete(
                f"/v1/sessions/{sid}/participants/me", headers=bearer(coord["participant_token"])
            )
            assert r.status_code in (200, 204), r.text
            assert await _coord(c, sid, creator) is None
            # removal
            made2 = await _create(c)
            sid2, creator2 = made2["session_id"], made2["creator_token"]
            coord2 = await _join(c, sid2, creator2, "coord")
            r = await c.delete(
                f"/v1/sessions/{sid2}/participants/{coord2['participant_id']}",
                headers=bearer(creator2),
            )
            assert r.status_code == 200, r.text
            assert await _coord(c, sid2, creator2) is None


@pytest.mark.asyncio
async def test_persisted_coordinator_survives_restart(tmp_path: Path) -> None:
    db = str(tmp_path / "coord.db")
    persistence = {"mode": "ttl", "ttl_seconds": 3600}
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c, persistence=persistence)
            sid, creator = made["session_id"], made["creator_token"]
            coord = await _join(c, sid, creator, "coord")
    async with relay_subprocess(persistence_db_path=db) as base2:
        async with httpx.AsyncClient(base_url=base2) as c:
            assert await _coord(c, sid, creator) == coord["participant_id"]
            r = await _put(c, sid, creator, None)
            assert r.status_code == 200
    async with relay_subprocess(persistence_db_path=db) as base3:
        async with httpx.AsyncClient(base_url=base3) as c:
            assert await _coord(c, sid, creator) is None


@pytest.mark.asyncio
async def test_mcp_tool_set_coordinator() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
        sid, creator = made["session_id"], made["creator_token"]
        async with sw_client(base) as agent, sw_client(base) as manager:
            joined = await agent.call_tool(
                "session_join",
                {"session_id": sid, "join_token": made["join_token"], "role": "w"},
            )
            pid = joined["participant_id"]
            with pytest.raises(RuntimeError, match="permission_denied"):
                await manager.call_tool(
                    "session_set_coordinator",
                    {"session_id": sid, "participant_id": pid,
                     "creator_token": made["supervisor_token"]},
                )
            out = await manager.call_tool(
                "session_set_coordinator",
                {"session_id": sid, "participant_id": pid, "creator_token": creator},
            )
            assert out == {"coordinator_id": pid}
            out = await manager.call_tool(
                "session_set_coordinator",
                {"session_id": sid, "participant_id": None, "creator_token": creator},
            )
            assert out == {"coordinator_id": None}


@pytest.fixture
async def acct(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with seeded_relay(tmp_path, mode="account") as s:
        yield s


@pytest.mark.asyncio
async def test_owner_account_token_join_is_coordinator(acct: Seeded) -> None:
    s = acct
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post("/v1/sessions", json={}, headers=bearer(s.token))
        made = r.json()
        sid = made["session_id"]
        owner = await _join(c, sid, s.token, "coord")
        assert await _coord(c, sid, s.token) == owner["participant_id"]


def test_max_launched_agents_config() -> None:
    d = load_deployment_defaults({})
    assert d.defaults.max_launched_agents == 10
    assert load_deployment_defaults(
        {"SW_DEFAULT_MAX_LAUNCHED_AGENTS": "3"}
    ).defaults.max_launched_agents == 3
    b, base = ConfigBounds.builtin(), SessionConfig()
    assert apply_overrides(base, b, {"max_launched_agents": 0}).max_launched_agents == 0
    assert apply_overrides(base, b, {"max_launched_agents": 100}).max_launched_agents == 100
    for bad in (-1, 101):
        with pytest.raises(ToolError):
            apply_overrides(base, b, {"max_launched_agents": bad})
