"""Live firehose streams opened with an account token end when it stops working."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import pytest
import websockets
from websockets.exceptions import ConnectionClosed

from tests.integration.accounts_fixtures import ADMIN, Seeded, seeded_relay
from tests.integration.client import bearer

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest.fixture
async def relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with seeded_relay(tmp_path, mode="account") as s:
        yield s


async def _new_token(s: Seeded) -> dict[str, Any]:
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post(
            f"/v1/accounts/{s.acc.account_id}/tokens", json={"label": "x"}, headers=bearer(ADMIN)
        )
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


async def _session(s: Seeded, token: str) -> str:
    async with httpx.AsyncClient(base_url=s.base) as c:
        r = await c.post("/v1/sessions", json={}, headers=bearer(token))
    assert r.status_code == 201, r.text
    return str(r.json()["session_id"])


async def _assert_cut(ws: Any) -> None:
    with pytest.raises(ConnectionClosed) as closed:
        await asyncio.wait_for(ws.recv(), timeout=3.0)
    assert closed.value.rcvd is not None
    assert closed.value.rcvd.code == 4403


def _url(s: Seeded, sid: str, token: str) -> str:
    return f"{s.base.replace('http://', 'ws://')}/v1/sessions/{sid}/firehose?token={token}"


@pytest.mark.parametrize("route", ["admin", "me"])
async def test_token_revoke_closes_its_stream_only(relay: Seeded, route: str) -> None:
    s = relay
    tok = await _new_token(s)
    sid = await _session(s, s.token)
    async with websockets.connect(_url(s, sid, tok["token"])) as cut, \
            websockets.connect(_url(s, sid, s.token)) as kept:
        async with httpx.AsyncClient(base_url=s.base) as c:
            if route == "admin":
                r = await c.delete(
                    f"/v1/accounts/{s.acc.account_id}/tokens/{tok['token_id']}",
                    headers=bearer(ADMIN),
                )
            else:
                r = await c.delete(f"/v1/me/tokens/{tok['token_id']}", headers=bearer(s.token))
        assert r.status_code == 204, r.text
        await _assert_cut(cut)
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(kept.recv(), timeout=0.5)


async def test_disable_closes_account_streams(relay: Seeded) -> None:
    s = relay
    sid = await _session(s, s.token)
    async with websockets.connect(_url(s, sid, s.token)) as ws:
        async with httpx.AsyncClient(base_url=s.base) as c:
            r = await c.patch(
                f"/v1/accounts/{s.acc.account_id}",
                json={"status": "disabled"}, headers=bearer(ADMIN),
            )
        assert r.status_code == 200, r.text
        await _assert_cut(ws)


async def test_delete_closes_account_streams(relay: Seeded) -> None:
    s = relay
    sid = await _session(s, s.token)
    async with websockets.connect(_url(s, sid, s.token)) as ws:
        async with httpx.AsyncClient(base_url=s.base) as c:
            r = await c.delete(f"/v1/accounts/{s.acc.account_id}", headers=bearer(ADMIN))
        assert r.status_code == 200, r.text
        await _assert_cut(ws)


async def test_recheck_after_subscribe_closes_revoked_token() -> None:
    """A token revoked between resolve and subscribe is cut at once."""
    from types import SimpleNamespace

    from sessionweave import events_ws

    acc = SimpleNamespace(account_id="acc_1")

    class _Accounts:
        result: Any = None

        async def match_token_with_id(self, token: str, now_ms: int) -> Any:
            return self.result

    accounts = _Accounts()
    hot = SimpleNamespace(auth=SimpleNamespace(owner_account_id="acc_1"))
    deps = SimpleNamespace(accounts=accounts, registry=SimpleNamespace(get=lambda sid: hot))
    accounts.result = (acc, "tok_1")
    assert await events_ws._account_token_still_valid(deps, "s", "t", "acc_1") is True
    accounts.result = None  # revoked
    assert await events_ws._account_token_still_valid(deps, "s", "t", "acc_1") is False
    accounts.result = (SimpleNamespace(account_id="acc_2"), "tok_9")
    assert await events_ws._account_token_still_valid(deps, "s", "t", "acc_1") is False
