"""MachineStore: the persisted machine descriptor of each v2 launcher."""
from __future__ import annotations

from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import pytest
import pytest_asyncio

from sessionweave.accounts import AccountStore
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.launchers import (
    MAX_MACHINES_OPEN,
    MAX_MACHINES_PER_ACCOUNT,
    OPEN_OWNER,
    LauncherKeyInUseError,
    parse_hello_v2,
)
from sessionweave.machines import Machine, MachineLimitError, MachineStore, StoredDefinition
from sessionweave.persistence import PersistenceStore

KEY = "lk_" + "ab" * 16
V2: dict[str, Any] = {
    "protocol": 2, "launcher_key": KEY, "mode": "local", "ceiling": "/srv/app",
    "capabilities": ["definitions", "browse", "processes", "teleport"],
    "sandbox": "bwrap", "run_as": False, "platform": "linux",
}


@pytest_asyncio.fixture
async def store(tmp_path: Path) -> AsyncIterator[PersistenceStore]:
    s = PersistenceStore(str(tmp_path / "m.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _key(i: int) -> str:
    return f"lk_{i:032x}"


async def test_upsert_list_forget(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    m, secret = await ms.claim_machine(
        "acc_1", "box", parse_hello_v2(V2, deploy_allowed=True), 1000)
    assert secret is not None
    assert (m.first_seen, m.last_seen, m.mode) == (1000, 1000, "local")
    assert m.capabilities == ["browse", "definitions", "processes"]
    m = await ms.upsert_machine(
        "acc_1", "box2", parse_hello_v2(V2, deploy_allowed=True), 2000, key_secret=secret)
    assert (m.first_seen, m.last_seen, m.machine) == (1000, 2000, "box2")
    assert [x.launcher_key for x in await ms.list_machines("acc_1")] == [KEY]
    assert await ms.list_machines("acc_2") == []
    await ms.forget_machine("acc_1", KEY)
    with pytest.raises(ToolError) as e:
        await ms.get_machine("acc_1", KEY)
    assert e.value.code is ErrorCode.LAUNCHER_NOT_FOUND


async def test_get_machine_round_trips_every_field(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    p = {"name": "bwrap", "state": "missing", "version": None, "required": True,
         "needed_by": ["--local"], "command": "sudo apt-get install bubblewrap"}
    hello = parse_hello_v2({**V2, "prereqs": [p]}, deploy_allowed=True)
    await ms.upsert_machine("acc_1", "box", hello, 5)
    got = await ms.get_machine("acc_1", KEY)
    assert got == Machine(
        account_id="acc_1", launcher_key=KEY, machine="box", mode="local",
        ceiling="/srv/app", capabilities=["browse", "definitions", "processes"],
        sandbox="bwrap", run_as=False, platform="linux", prereqs=[p],
        first_seen=5, last_seen=5,
    )


async def test_upsert_without_key_is_refused(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    with pytest.raises(ValueError):
        await ms.upsert_machine("acc_1", "box", parse_hello_v2({}, deploy_allowed=True), 1)


async def test_set_prereqs(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    await ms.upsert_machine("acc_1", "box", parse_hello_v2(V2, deploy_allowed=True), 1)
    p = [{"name": "cline", "state": "installed", "version": "3.0.75", "required": False,
          "needed_by": ["cline"], "command": None}]
    await ms.set_prereqs("acc_1", KEY, p)
    assert (await ms.get_machine("acc_1", KEY)).prereqs == p
    # unknown machine: no-op
    await ms.set_prereqs("acc_1", _key(9), p)


async def test_machine_limit(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    for i in range(MAX_MACHINES_PER_ACCOUNT):
        await ms.upsert_machine(
            "acc_1", "m",
            parse_hello_v2({**V2, "launcher_key": _key(i)}, deploy_allowed=True), 1)
    with pytest.raises(MachineLimitError):
        await ms.upsert_machine(
            "acc_1", "m",
            parse_hello_v2({**V2, "launcher_key": "lk_" + "f" * 32}, deploy_allowed=True), 1)
    # an existing key is still checked (not counted); another account is not affected
    with pytest.raises(LauncherKeyInUseError):
        await ms.upsert_machine(
            "acc_1", "m2",
            parse_hello_v2({**V2, "launcher_key": _key(0)}, deploy_allowed=True), 2)
    await ms.upsert_machine("acc_2", "m", parse_hello_v2(V2, deploy_allowed=True), 1)


async def test_open_owner_limit_is_higher(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    for i in range(MAX_MACHINES_OPEN):
        await ms.upsert_machine(
            OPEN_OWNER, "m",
            parse_hello_v2({**V2, "launcher_key": _key(i)}, deploy_allowed=True), 1)
    with pytest.raises(MachineLimitError):
        await ms.upsert_machine(
            OPEN_OWNER, "m",
            parse_hello_v2({**V2, "launcher_key": "lk_" + "f" * 32}, deploy_allowed=True), 1)
    assert len(await ms.list_machines(OPEN_OWNER)) == MAX_MACHINES_OPEN


async def _definition_rows(store: PersistenceStore, account_id: str) -> int:
    async with store._read_pool[0].execute(
        "SELECT COUNT(*) FROM agent_definitions WHERE account_id = ?", (account_id,)
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    return int(row[0])


async def _add_definition(store: PersistenceStore, account_id: str, key: str) -> None:
    assert store._write_conn is not None
    await store._write_conn.execute(
        "INSERT INTO agent_definitions (account_id, launcher_key, def_id, revision,"
        " definition_json, state, applied_revision, updated_at)"
        " VALUES (?, ?, 'd1', 1, '{}', 'queued', NULL, 1)",
        (account_id, key),
    )


async def test_forget_machine_and_account_drop_definitions(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    hello = parse_hello_v2(V2, deploy_allowed=True)
    await ms.upsert_machine("acc_1", "box", hello, 1)
    await ms.upsert_machine("acc_2", "box", hello, 1)
    await _add_definition(store, "acc_1", KEY)
    await _add_definition(store, "acc_2", KEY)
    await ms.forget_machine("acc_1", KEY)
    assert await _definition_rows(store, "acc_1") == 0
    assert await _definition_rows(store, "acc_2") == 1
    await ms.forget_account("acc_2")
    assert await ms.list_machines("acc_2") == []
    assert await _definition_rows(store, "acc_2") == 0


async def test_account_delete_drops_machines_and_definitions(store: PersistenceStore) -> None:
    accounts = AccountStore(store)
    acc = await accounts.create("Alice")
    ms = MachineStore(store)
    await ms.upsert_machine(acc.account_id, "box", parse_hello_v2(V2, deploy_allowed=True), 1)
    await _add_definition(store, acc.account_id, KEY)
    await accounts.delete(acc.account_id)
    assert await ms.list_machines(acc.account_id) == []
    assert await _definition_rows(store, acc.account_id) == 0


async def _secret_row(store: PersistenceStore, account_id: str) -> tuple[Any, Any]:
    async with store._read_pool[0].execute(
        "SELECT key_secret_hash, token_id FROM launcher_machines WHERE account_id = ?",
        (account_id,),
    ) as cur:
        row = await cur.fetchone()
    assert row is not None
    return row[0], row[1]


async def test_key_secret_issued_once_and_required(store: PersistenceStore) -> None:
    from sessionweave.access import hash_token

    ms = MachineStore(store)
    hello = parse_hello_v2(V2, deploy_allowed=True)
    _m, secret = await ms.claim_machine("acc_1", "box", hello, 1, token_id="tok_1")
    assert secret is not None and secret.startswith("lks_")
    stored, token_id = await _secret_row(store, "acc_1")
    assert stored == hash_token(secret) and token_id == "tok_1"  # only the hash is kept
    # the right secret refreshes without a new one; the token that registered it stays
    m, again = await ms.claim_machine(
        "acc_1", "box2", hello, 2, key_secret=secret, token_id="tok_2")
    assert again is None and m.machine == "box2"
    assert await _secret_row(store, "acc_1") == (hash_token(secret), "tok_1")
    # without or with a wrong secret: refused, row untouched
    for bad in (None, "lks_wrong", secret + "x"):
        with pytest.raises(LauncherKeyInUseError):
            await ms.claim_machine("acc_1", "evil", hello, 3, key_secret=bad)
    m = await ms.get_machine("acc_1", KEY)
    assert (m.machine, m.last_seen) == ("box2", 2)
    # the key is per owner: another owner registering it gets its own secret
    _m, other = await ms.claim_machine(OPEN_OWNER, "box", hello, 1)
    assert other is not None and other != secret
    # reset path: forgetting the machine lets the key be registered afresh
    await ms.forget_machine("acc_1", KEY)
    _m, fresh = await ms.claim_machine("acc_1", "box", hello, 4)
    assert fresh is not None and fresh != secret


async def test_prune_open_machines(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    day = 86_400_000
    now = 100 * day
    for i, seen in enumerate((now - 40 * day, now - 40 * day, now - 5 * day)):
        await ms.claim_machine(
            OPEN_OWNER, "m",
            parse_hello_v2({**V2, "launcher_key": _key(i)}, deploy_allowed=True), seen)
        await _add_definition(store, OPEN_OWNER, _key(i))
    await ms.claim_machine("acc_1", "m", parse_hello_v2(V2, deploy_allowed=True), 0)
    # key 1 is online: kept whatever its last_seen
    assert await ms.prune_open(now, 30, online_keys={_key(1)}) == 1
    assert [m.launcher_key for m in await ms.list_machines(OPEN_OWNER)] == [_key(1), _key(2)]
    assert await _definition_rows(store, OPEN_OWNER) == 2
    assert len(await ms.list_machines("acc_1")) == 1  # account rows never pruned


async def test_touch_updates_last_seen(store: PersistenceStore) -> None:
    ms = MachineStore(store)
    await ms.claim_machine("acc_1", "m", parse_hello_v2(V2, deploy_allowed=True), 1)
    await ms.touch("acc_1", KEY, 50)
    assert (await ms.get_machine("acc_1", KEY)).last_seen == 50


async def _with_machine(store: PersistenceStore) -> MachineStore:
    ms = MachineStore(store)
    await ms.claim_machine("acc_1", "box", parse_hello_v2(V2, deploy_allowed=True), 0)
    return ms


async def test_definition_revisions_and_status(store: PersistenceStore) -> None:
    ms = await _with_machine(store)
    a = await ms.put_definition("acc_1", KEY, {"id": "obs"}, "sent", 1)
    b = await ms.put_definition("acc_1", KEY, {"id": "obs"}, "sent", 2)
    assert (a.revision, b.revision) == (1, 2)
    assert await ms.set_status("acc_1", KEY, "obs", 1, "applied", [], [], 3) is None   # stale
    s = await ms.set_status("acc_1", KEY, "obs", 2, "applied", [], ["w"], 3)
    assert s is not None and (s.state, s.applied_revision, s.warnings) == ("applied", 2, ["w"])
    d = await ms.mark_deleted("acc_1", KEY, "obs", 4)
    assert (d.revision, d.state, d.deleted) == (3, "removing", True)
    assert await ms.list_definitions("acc_1", KEY) == []
    await ms.purge_definition("acc_1", KEY, "obs")
    assert await ms.list_definitions("acc_1", KEY, include_deleted=True) == []


async def test_definition_rows_fields_and_redeploy_after_delete(store: PersistenceStore) -> None:
    ms = await _with_machine(store)
    a = await ms.put_definition("acc_1", KEY, {"id": "obs", "kind": "observer"}, "queued", 10)
    assert a == StoredDefinition("obs", 1, {"id": "obs", "kind": "observer"}, "queued", None,
                                 [], [], False, 10)
    assert a.deploy() == {"revision": 1, "applied_revision": None, "state": "queued",
                          "problems": [], "warnings": []}
    r = await ms.set_status("acc_1", KEY, "obs", 1, "rejected", ["bad"], [], 11)
    assert r is not None
    assert (r.state, r.problems, r.applied_revision) == ("rejected", ["bad"], None)
    # A later revision keeps the last applied revision and clears problems.
    await ms.set_status("acc_1", KEY, "obs", 1, "applied", [], [], 12)
    b = await ms.put_definition("acc_1", KEY, {"id": "obs", "v": 2}, "sent", 13)
    assert (b.revision, b.applied_revision, b.problems, b.state) == (2, 1, [], "sent")
    assert await ms.get_definition("acc_1", KEY, "obs") == b
    await ms.mark_deleted("acc_1", KEY, "obs", 14)
    assert [x.def_id for x in await ms.list_definitions("acc_1", KEY, include_deleted=True)] \
        == ["obs"]
    # Redeploying a definition being removed continues its revisions and clears ``deleted``.
    c = await ms.put_definition("acc_1", KEY, {"id": "obs"}, "sent", 15)
    assert (c.revision, c.deleted) == (4, False)
    assert [x.def_id for x in await ms.list_definitions("acc_1", KEY)] == ["obs"]


async def test_definitions_scoped_by_owner_and_key(store: PersistenceStore) -> None:
    ms = await _with_machine(store)
    await ms.put_definition("acc_1", KEY, {"id": "b"}, "sent", 1)
    await ms.put_definition("acc_1", KEY, {"id": "a"}, "sent", 2)
    assert [x.def_id for x in await ms.list_definitions("acc_1", KEY)] == ["a", "b"]
    assert await ms.list_definitions("acc_2", KEY) == []
    assert await ms.list_definitions("acc_1", _key(9)) == []
    for fn in (
        ms.get_definition("acc_2", KEY, "a"),
        ms.mark_deleted("acc_1", KEY, "nope", 3),
    ):
        with pytest.raises(ToolError) as e:
            await fn
        assert e.value.code is ErrorCode.DEFINITION_NOT_FOUND
    assert await ms.set_status("acc_1", KEY, "nope", 1, "applied", [], [], 3) is None
    await ms.purge_definition("acc_1", KEY, "nope")  # idempotent
    # Forgetting the machine drops its definitions.
    await ms.forget_machine("acc_1", KEY)
    assert await ms.list_definitions("acc_1", KEY, include_deleted=True) == []
