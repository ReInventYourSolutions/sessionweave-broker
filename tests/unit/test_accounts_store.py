import time
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.accounts import AccountStore
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest.fixture
def accounts(store: PersistenceStore) -> AccountStore:
    return AccountStore(store)


def _now() -> int:
    return int(time.time() * 1000)


async def test_create_and_get_account(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    assert acc.account_id.startswith("acc_")
    assert len(acc.account_id) == len("acc_") + 16
    assert acc.status == "active"
    assert acc.created_by == "admin"
    assert await accounts.get(acc.account_id) == acc
    assert await accounts.list_accounts() == [acc]


async def test_name_unique_case_insensitive(accounts: AccountStore) -> None:
    await accounts.create("Alice")
    with pytest.raises(ToolError) as ei:
        await accounts.create("alice")
    assert ei.value.code == ErrorCode.ACCOUNT_NAME_TAKEN


async def test_get_unknown_account(accounts: AccountStore) -> None:
    with pytest.raises(ToolError) as ei:
        await accounts.get("acc_missing")
    assert ei.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_update_returns_before_and_after(accounts: AccountStore) -> None:
    a = await accounts.create("Alice", status="pending")
    b = await accounts.create("Bob")
    before, after = await accounts.update(
        a.account_id, status="active", name="Alicia", contact="a@x.io"
    )
    assert before == a
    assert (after.status, after.name, after.contact) == ("active", "Alicia", "a@x.io")
    same, same2 = await accounts.update(a.account_id)
    assert same == same2 == after
    with pytest.raises(ToolError) as ei:
        await accounts.update(a.account_id, name="BOB")
    assert ei.value.code == ErrorCode.ACCOUNT_NAME_TAKEN
    with pytest.raises(ToolError) as ei:
        await accounts.update("acc_missing", status="active")
    assert ei.value.code == ErrorCode.ACCOUNT_NOT_FOUND
    with pytest.raises(ToolError) as ei:
        await accounts.update(b.account_id, status="bogus")
    assert ei.value.code == ErrorCode.INVALID_REQUEST


async def test_token_shown_once_and_matches(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    tok, meta = await accounts.create_token(acc.account_id, "cli")
    assert tok.startswith("swa_")
    assert len(meta.token_id) == 16
    assert await accounts.match_token(tok, _now()) == acc
    pub = meta.to_public()
    assert "token" not in pub
    assert not any("hash" in k for k in pub)
    assert await accounts.list_tokens(acc.account_id) == [meta]
    assert await accounts.match_token("swa_unknown", _now()) is None


async def test_token_label_and_account_validation(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    for bad in ("", "x" * 65):
        with pytest.raises(ToolError) as ei:
            await accounts.create_token(acc.account_id, bad)
        assert ei.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(ToolError) as ei:
        await accounts.create_token("acc_missing", "cli")
    assert ei.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_token_refused_when_revoked_expired_or_inactive(
    accounts: AccountStore,
) -> None:
    acc = await accounts.create("Alice")
    tok, meta = await accounts.create_token(acc.account_id, "cli")
    revoked = await accounts.revoke_token(acc.account_id, meta.token_id)
    assert revoked.revoked_at is not None
    again = await accounts.revoke_token(acc.account_id, meta.token_id)
    assert again.revoked_at == revoked.revoked_at
    assert await accounts.match_token(tok, _now()) is None
    with pytest.raises(ToolError) as ei:
        await accounts.revoke_token(acc.account_id, "nope")
    assert ei.value.code == ErrorCode.TOKEN_NOT_FOUND

    tok2, _ = await accounts.create_token(acc.account_id, "old", expires_at_ms=_now() - 1)
    assert await accounts.match_token(tok2, _now()) is None

    tok3, _ = await accounts.create_token(acc.account_id, "ok")
    for status in ("pending", "disabled"):
        await accounts.update(acc.account_id, status=status)
        assert await accounts.match_token(tok3, _now()) is None
    await accounts.update(acc.account_id, status="active")
    assert (await accounts.match_token(tok3, _now())) is not None


async def test_delete_cascades_tokens(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    tok, _ = await accounts.create_token(acc.account_id, "cli")
    await accounts.delete(acc.account_id)
    assert await accounts.match_token(tok, _now()) is None
    with pytest.raises(ToolError) as ei:
        await accounts.get(acc.account_id)
    assert ei.value.code == ErrorCode.ACCOUNT_NOT_FOUND


async def test_migration_adds_session_columns(store: PersistenceStore) -> None:
    async with store._read_pool[0].execute("PRAGMA table_info(sessions)") as cur:
        cols = {row[1] async for row in cur}
    assert {"owner_account_id", "suspended"} <= cols


async def test_revoke_token_changed_reports_first_revoke_only(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    _, meta = await accounts.create_token(acc.account_id, "cli")
    first, changed1 = await accounts.revoke_token_changed(acc.account_id, meta.token_id)
    second, changed2 = await accounts.revoke_token_changed(acc.account_id, meta.token_id)
    assert (changed1, changed2) == (True, False)
    assert first.revoked_at == second.revoked_at is not None


async def test_token_usable_rechecks_without_side_effects(accounts: AccountStore) -> None:
    acc = await accounts.create("Alice")
    _plain, tok = await accounts.create_token(acc.account_id, "launcher")
    now = _now()
    assert await accounts.token_usable(acc.account_id, tok.token_id, now) is True
    assert await accounts.token_usable(acc.account_id, tok.token_id, now, "10.0.0.1") is True
    # bound to a network: other addresses are refused
    await accounts.set_token_networks(acc.account_id, tok.token_id,
                                      network_policy="approved_only",
                                      approved_networks=["10.0.0.0/24"])
    assert await accounts.token_usable(acc.account_id, tok.token_id, now, "10.0.0.7") is True
    assert await accounts.token_usable(acc.account_id, tok.token_id, now, "10.9.0.7") is False
    # expired
    _p2, short = await accounts.create_token(acc.account_id, "short", expires_at_ms=now + 1000)
    assert await accounts.token_usable(acc.account_id, short.token_id, now) is True
    assert await accounts.token_usable(acc.account_id, short.token_id, now + 1000) is False
    # revoked
    await accounts.revoke_token(acc.account_id, tok.token_id)
    assert await accounts.token_usable(acc.account_id, tok.token_id, now) is False
    # account not active; unknown token or account
    await accounts.update(acc.account_id, status="disabled")
    assert await accounts.token_usable(acc.account_id, short.token_id, now) is False
    assert await accounts.token_usable(acc.account_id, "tok_missing", now) is False
    assert await accounts.token_usable("acc_missing", short.token_id, now) is False
    # read-only: no last-use recorded
    assert (await accounts.get_token(acc.account_id, short.token_id)).last_used_at is None
