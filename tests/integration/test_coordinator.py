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


@pytest.mark.asyncio
async def test_evicted_coordinator_is_cleared_on_disk_and_not_clobbered(tmp_path: Path) -> None:
    import asyncio

    from sessionweave.access import generate_token, hash_token
    from sessionweave.connections import ConnectionManager
    from sessionweave.persistence import PersistenceStore
    from sessionweave.registry import SessionRegistry
    from sessionweave.schemas import PersistenceConfig
    from tests.unit.fakes import FakeHandle

    store = PersistenceStore(str(tmp_path / "evict.db"))
    await store.init(pool_size=2)
    try:
        reg = SessionRegistry(
            defaults=load_deployment_defaults({}), connections=ConnectionManager(),
            metrics=None, firehose=None, store=store,
        )
        sid, _, _, _, _ = await reg.create(
            overrides={}, purpose=None,
            persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
            creator_token_hash=hash_token(generate_token()),
        )
        sess = await reg.get_or_raise(sid)
        c1 = await sess.join(FakeHandle(), role="c", as_coordinator=True)
        other = await sess.join(FakeHandle(), role="o")
        snap = await store.load_session(sid)
        assert snap is not None and snap.coordinator_id == c1.participant_id
        sess._evict(c1.participant_id, "offline_grace")
        await asyncio.sleep(0.3)
        assert sess.coordinator_id is None
        snap = await store.load_session(sid)
        assert snap is not None and snap.coordinator_id is None
        c2 = await sess.join(FakeHandle(), role="c", as_coordinator=True)
        assert sess.coordinator_id == c2.participant_id
        await sess.leave(other.participant_id)
        snap = await store.load_session(sid)
        assert snap is not None and snap.coordinator_id == c2.participant_id
        assert sess.coordinator_id == c2.participant_id
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_eviction_persist_does_not_clobber_a_newer_coordinator(tmp_path: Path) -> None:
    """The eviction write re-checks the coordinator when it runs: a coordinator
    named after the eviction (before the write task ran) is not overwritten."""
    import asyncio

    from sessionweave.access import generate_token, hash_token
    from sessionweave.connections import ConnectionManager
    from sessionweave.persistence import PersistenceStore
    from sessionweave.registry import SessionRegistry
    from sessionweave.schemas import PersistenceConfig
    from tests.unit.fakes import FakeHandle

    store = PersistenceStore(str(tmp_path / "evict2.db"))
    await store.init(pool_size=2)
    try:
        reg = SessionRegistry(
            defaults=load_deployment_defaults({}), connections=ConnectionManager(),
            metrics=None, firehose=None, store=store,
        )
        sid, _, _, _, _ = await reg.create(
            overrides={}, purpose=None,
            persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
            creator_token_hash=hash_token(generate_token()),
        )
        sess = await reg.get_or_raise(sid)
        c1 = await sess.join(FakeHandle(), role="c", as_coordinator=True)
        other = await sess.join(FakeHandle(), role="o")
        sess._evict(c1.participant_id, "offline_grace")
        # A new coordinator is named before the eviction's write task runs.
        sess.coordinator_id = other.participant_id
        await store.update_session_coordinator(sid, other.participant_id)
        await asyncio.sleep(0.3)
        snap = await store.load_session(sid)
        assert snap is not None and snap.coordinator_id == other.participant_id
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_admin_and_supervisor_join_never_coordinate_and_resume_reports_it() -> None:
    admin = "admin-token-for-coordinator-tests"
    async with relay_subprocess(env_overrides={"SW_ADMIN_TOKEN": admin}) as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
            sid, creator = made["session_id"], made["creator_token"]
            r = await c.post(
                f"/v1/sessions/{sid}/participants",
                headers=bearer(made["supervisor_token"]), json={"role": "s"},
            )
            assert r.status_code == 403, r.text
            await _join(c, sid, admin, "admin")
            assert await _coord(c, sid, creator) is None
        async with sw_client(base) as agent:
            joined = await agent.call_tool(
                "session_join", {"session_id": sid, "join_token": creator, "role": "c"}
            )
            assert joined["coordinator_id"] == joined["participant_id"]
        async with sw_client(base) as again:
            resumed = await again.call_tool(
                "session_resume",
                {"session_id": sid, "participant_token": joined["participant_token"]},
            )
            assert resumed["coordinator_id"] == joined["participant_id"]


@pytest.mark.asyncio
async def test_concurrent_creator_joins_yield_one_coordinator() -> None:
    import asyncio

    async with relay_subprocess() as base:
        async with httpx.AsyncClient(base_url=base) as c:
            made = await _create(c)
            sid, creator = made["session_id"], made["creator_token"]
            outs = await asyncio.gather(*(_join(c, sid, creator, f"c{i}") for i in range(5)))
            got = await _coord(c, sid, creator)
            assert got in {o["participant_id"] for o in outs}
            # Exactly one join response names its own participant as coordinator.
            own = [o for o in outs if o["coordinator_id"] == o["participant_id"]]
            assert len(own) == 1 and own[0]["participant_id"] == got
            r = await c.get(f"/v1/sessions/{sid}/participants", headers=bearer(creator))
            assert len(r.json()["roster"]) == 5
