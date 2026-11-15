from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

import httpx
import pytest

from sessionweave.accounts import Account, AccountStore
from sessionweave.persistence import PersistenceStore
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess

ADMIN = "adm-token-xyz"

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@dataclass
class Seeded:
    base: str
    acc: Account
    other: Account
    token: str
    pending_token: str


@contextlib.asynccontextmanager
async def _relay(tmp_path: Path, *, mode: str | None) -> AsyncIterator[Seeded]:
    db = str(tmp_path / f"acc-{mode}.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("Alice")
    other = await accounts.create("Bob")
    pending = await accounts.create("Pending", status="pending")
    token, _ = await accounts.create_token(acc.account_id, "t")
    ptoken, _ = await accounts.create_token(pending.account_id, "p")
    await store.close()
    env = {"SW_ADMIN_TOKEN": ADMIN}
    if mode is not None:
        env["SW_SESSION_CREATION"] = mode
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        yield Seeded(base, acc, other, token, ptoken)


@pytest.fixture
async def account_relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with _relay(tmp_path, mode="account") as s:
        yield s


@pytest.fixture
async def open_relay(tmp_path: Path) -> AsyncIterator[Seeded]:
    async with _relay(tmp_path, mode=None) as s:
        yield s


async def _post(base: str, headers: dict[str, str] | None = None, **body: object) -> httpx.Response:
    async with httpx.AsyncClient(base_url=base) as c:
        return await c.post("/v1/sessions", json=body, headers=headers)


async def test_info_reports_mode(account_relay: Seeded, open_relay: Seeded) -> None:
    async with httpx.AsyncClient() as c:
        r = await c.get(f"{account_relay.base}/v1/info")
        assert r.json()["session_creation"] == "account"
        r = await c.get(f"{open_relay.base}/v1/info")
        assert r.json()["session_creation"] == "open"


async def test_account_mode_requires_token(account_relay: Seeded) -> None:
    r = await _post(account_relay.base)
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "account_token_required"
    assert "Authorization: Bearer" in r.json()["error"]["message"]


async def test_account_mode_valid_token_owns_session(account_relay: Seeded) -> None:
    r = await _post(account_relay.base, bearer(account_relay.token))
    assert r.status_code == 201, r.text
    assert r.json()["owner_account_id"] == account_relay.acc.account_id


async def test_invalid_token_rejected_in_open_mode(open_relay: Seeded) -> None:
    r = await _post(open_relay.base, bearer("garbage"))
    assert r.status_code == 401
    assert r.json()["error"]["code"] == "invalid_account_token"
    r = await _post(open_relay.base)
    assert r.status_code == 201
    assert r.json()["owner_account_id"] is None


async def test_admin_creates_ownerless_or_for_account(account_relay: Seeded) -> None:
    s = account_relay
    r = await _post(s.base, bearer(ADMIN))
    assert r.status_code == 201 and r.json()["owner_account_id"] is None
    r = await _post(s.base, bearer(ADMIN), owner_account_id=s.other.account_id)
    assert r.status_code == 201 and r.json()["owner_account_id"] == s.other.account_id
    r = await _post(s.base, bearer(ADMIN), owner_account_id="acc_nope")
    assert r.status_code == 404
    r = await _post(s.base, bearer(s.token), owner_account_id=s.other.account_id)
    assert r.status_code == 403


async def test_pending_account_token_refused(account_relay: Seeded) -> None:
    r = await _post(account_relay.base, bearer(account_relay.pending_token))
    assert r.status_code == 401


async def test_mcp_create_uses_header(account_relay: Seeded) -> None:
    s = account_relay
    async with sw_client(s.base, headers=bearer(s.token)) as c:
        out = await c.call_tool("session_create", {})
        assert out["owner_account_id"] == s.acc.account_id
    async with sw_client(s.base) as c:
        out = await c.call_tool("session_create", {"account_token": s.token})
        assert out["owner_account_id"] == s.acc.account_id
    async with sw_client(s.base) as c:
        with pytest.raises(RuntimeError, match="account_token_required"):
            await c.call_tool("session_create", {})


async def test_failed_account_tokens_throttled(account_relay: Seeded) -> None:
    s = account_relay
    for _ in range(5):
        r = await _post(s.base, bearer("bad"))
        assert r.status_code == 401
    r = await _post(s.base, bearer("bad"))
    assert r.status_code == 429
    r = await _post(s.base, bearer(s.token))
    assert r.status_code == 201
