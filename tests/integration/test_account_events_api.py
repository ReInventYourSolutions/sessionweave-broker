"""``/v1/me/events`` and ``/v1/accounts/events`` over HTTP."""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from pathlib import Path

import httpx
import pytest

from sessionweave.account_events import EventLog
from sessionweave.accounts import AccountStore
from sessionweave.persistence import PersistenceStore
from tests.integration.accounts_fixtures import ADMIN
from tests.integration.client import bearer
from tests.integration.harness import relay_subprocess

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def relay(tmp_path: Path) -> AsyncIterator[tuple[str, str, str, str, str]]:
    db = str(tmp_path / "ev.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    a = await accounts.create("Alice")
    b = await accounts.create("Bob")
    ta, _ = await accounts.create_token(a.account_id, "t")
    log = EventLog(store)
    await log.emit(a.account_id, "token.created", {"n": 1})
    await log.emit(b.account_id, "token.created", {"n": 2})
    await log.emit(a.account_id, "token.revoked", {"n": 3})
    await store.close()
    env = {"SW_ADMIN_TOKEN": ADMIN, "SW_SESSION_CREATION": "account"}
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        yield base, a.account_id, b.account_id, ta, db


async def test_self_sees_only_own(relay) -> None:
    base, aid, _, ta, _ = relay
    async with httpx.AsyncClient(base_url=base) as c:
        r = await c.get("/v1/me/events", headers=bearer(ta))
        assert r.status_code == 200, r.text
        body = r.json()
        assert [e["data"]["n"] for e in body["events"]] == [1, 3]
        assert {e["account_id"] for e in body["events"]} == {aid}
        assert body["next"] == body["events"][-1]["event_id"] and body["truncated"] is False
        # account_id param is ignored on the self route
        r = await c.get("/v1/me/events", params={"account_id": "x"}, headers=bearer(ta))
        assert len(r.json()["events"]) == 2
        r = await c.get("/v1/me/events", params={"after": body["next"]}, headers=bearer(ta))
        assert r.json()["events"] == []
        assert (await c.get("/v1/me/events")).status_code == 401


async def test_admin_sees_all_and_filters(relay) -> None:
    base, _, bid, ta, _ = relay
    async with httpx.AsyncClient(base_url=base) as c:
        r = await c.get("/v1/accounts/events", headers=bearer(ADMIN))
        assert [e["data"]["n"] for e in r.json()["events"]] == [1, 2, 3]
        r = await c.get("/v1/accounts/events", params={"account_id": bid}, headers=bearer(ADMIN))
        assert [e["data"]["n"] for e in r.json()["events"]] == [2]
        r = await c.get("/v1/accounts/events", headers=bearer(ta))
        assert r.status_code == 403
        r = await c.get("/v1/accounts/events")
        assert r.status_code == 401


async def test_bad_params(relay) -> None:
    base, _, _, ta, _ = relay
    async with httpx.AsyncClient(base_url=base) as c:
        for params in (
            {"limit": 501}, {"limit": 0}, {"limit": "x"}, {"after": -1},
            {"wait": 61}, {"wait": -1}, {"after": "nan"},
        ):
            r = await c.get("/v1/me/events", params=params, headers=bearer(ta))
            assert r.status_code == 400, (params, r.text)
        r = await c.get("/v1/me/events", params={"limit": 500}, headers=bearer(ta))
        assert r.status_code == 200
        r = await c.get("/v1/me/events", params={"limit": 1}, headers=bearer(ta))
        assert len(r.json()["events"]) == 1


async def test_long_poll_times_out_empty(relay) -> None:
    base, _, _, ta, _ = relay
    async with httpx.AsyncClient(base_url=base, timeout=15) as c:
        last = (await c.get("/v1/me/events", headers=bearer(ta))).json()["next"]
        t0 = time.monotonic()
        r = await c.get("/v1/me/events", params={"after": last, "wait": 1}, headers=bearer(ta))
        assert r.status_code == 200
        assert r.json() == {"events": [], "next": last, "truncated": False}
        assert time.monotonic() - t0 >= 0.9


async def test_long_poll_released_early_by_emit(tmp_path: Path) -> None:
    """In-process ASGI: an emit in the same loop releases a long poll well before ``wait``."""
    from types import SimpleNamespace

    from starlette.applications import Starlette

    from sessionweave import accounts_api

    store = PersistenceStore(str(tmp_path / "lp.db"))
    await store.init(pool_size=2)
    log = EventLog(store)
    deps = SimpleNamespace(
        accounts=object(), events=log, admin_token="adm",
        auth_limiter=SimpleNamespace(
            is_throttled=lambda *a: False, record_failure=lambda *a: None
        ),
    )
    app = Starlette(routes=accounts_api.routes())
    app.state.tool_deps = deps
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
        t0 = time.monotonic()
        poll = asyncio.create_task(
            c.get("/v1/accounts/events", params={"wait": 30}, headers=bearer("adm"))
        )
        await asyncio.sleep(0.3)
        await log.emit("acc_x", "token.created", {"n": 1})
        r = await asyncio.wait_for(poll, 5)
    assert r.status_code == 200
    assert [e["data"]["n"] for e in r.json()["events"]] == [1]
    assert time.monotonic() - t0 < 5
    await store.close()
