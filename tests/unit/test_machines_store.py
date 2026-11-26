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
    parse_hello_v2,
)
from sessionweave.machines import Machine, MachineLimitError, MachineStore
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
    m = await ms.upsert_machine("acc_1", "box", parse_hello_v2(V2, deploy_allowed=True), 1000)
    assert (m.first_seen, m.last_seen, m.mode) == (1000, 1000, "local")
    assert m.capabilities == ["browse", "definitions", "processes"]
    m = await ms.upsert_machine("acc_1", "box2", parse_hello_v2(V2, deploy_allowed=True), 2000)
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
    # an existing key is still updated; another account is not affected
    await ms.upsert_machine(
        "acc_1", "m2", parse_hello_v2({**V2, "launcher_key": _key(0)}, deploy_allowed=True), 2)
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
