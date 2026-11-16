"""Shared helpers: a relay subprocess whose DB is pre-seeded with accounts."""
from __future__ import annotations

import contextlib
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path

from sessionweave.accounts import Account, AccountStore
from sessionweave.persistence import PersistenceStore
from tests.integration.harness import relay_subprocess

ADMIN = "adm-token-xyz"


@dataclass
class Seeded:
    base: str
    acc: Account
    other: Account
    token: str
    pending_token: str
    other_token: str
    revoked_token: str


@contextlib.asynccontextmanager
async def seeded_relay(
    tmp_path: Path, *, mode: str | None, env_extra: dict[str, str] | None = None
) -> AsyncIterator[Seeded]:
    db = str(tmp_path / f"acc-{mode}.db")
    store = PersistenceStore(db)
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("Alice")
    other = await accounts.create("Bob")
    pending = await accounts.create("Pending", status="pending")
    token, _ = await accounts.create_token(acc.account_id, "t")
    other_token, _ = await accounts.create_token(other.account_id, "t")
    ptoken, _ = await accounts.create_token(pending.account_id, "p")
    revoked, rec = await accounts.create_token(acc.account_id, "r")
    await accounts.revoke_token(acc.account_id, rec.token_id)
    await store.close()
    env = {"SW_ADMIN_TOKEN": ADMIN, **(env_extra or {})}
    if mode is not None:
        env["SW_SESSION_CREATION"] = mode
    async with relay_subprocess(env_overrides=env, persistence_db_path=db) as base:
        yield Seeded(base, acc, other, token, ptoken, other_token, revoked)
