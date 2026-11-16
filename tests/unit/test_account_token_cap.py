from __future__ import annotations

import time
from pathlib import Path

import pytest

from sessionweave.accounts import AccountStore
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore


@pytest.mark.asyncio
async def test_expired_and_revoked_tokens_do_not_count(tmp_path: Path) -> None:
    store = PersistenceStore(str(tmp_path / "t.db"))
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("alice")
    past = int(time.time() * 1000) - 1000
    # An already-expired token cannot be minted via the API, so age one directly.
    _, old = await accounts.create_token(acc.account_id, "old")
    conn = store._write_conn
    assert conn is not None
    await conn.execute(
        "UPDATE account_tokens SET expires_at = ? WHERE token_id = ?", (past, old.token_id)
    )
    _, rev = await accounts.create_token(acc.account_id, "rev")
    await accounts.revoke_token(acc.account_id, rev.token_id)
    await accounts.create_token(acc.account_id, "a", max_tokens=2)
    await accounts.create_token(acc.account_id, "b", max_tokens=2)
    with pytest.raises(ToolError) as ei:
        await accounts.create_token(acc.account_id, "c", max_tokens=2)
    assert ei.value.code is ErrorCode.ACCOUNT_TOKEN_LIMIT
    await store.close()
