"""DeployManager: per-machine locking against a concurrent forget."""
from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace

import pytest

from sessionweave.deploy import DeployManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.launchers import OPEN_OWNER, LauncherRegistry, parse_hello_v2
from sessionweave.machines import MachineStore
from sessionweave.persistence import PersistenceStore

KEY = "lk_" + "ab" * 16
_DEF = {"id": "obs", "domain": "ops", "role": "watcher", "runtime": "claude",
        "kind": "observer", "workdir": "/srv/app", "max_instances": 1}


async def _setup(tmp_path: Path) -> tuple[PersistenceStore, MachineStore, DeployManager]:
    store = PersistenceStore(str(tmp_path / "d.db"))
    await store.init(pool_size=2)
    machines = MachineStore(store)
    hello = parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "local",
                            "ceiling": "/srv/app", "capabilities": ["definitions"]},
                           deploy_allowed=True)
    await machines.claim_machine(OPEN_OWNER, "box", hello, int(time.time() * 1000))
    deps = SimpleNamespace(events=None)
    return store, machines, DeployManager(LauncherRegistry(), machines, deps)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_deploy_waiting_on_a_forget_leaves_no_orphan(tmp_path: Path) -> None:
    store, machines, dm = await _setup(tmp_path)
    try:
        async with dm._machine_lock(OPEN_OWNER, KEY):
            deploy = asyncio.create_task(dm.deploy(OPEN_OWNER, KEY, "obs", dict(_DEF)))
            remove = asyncio.create_task(dm.remove(OPEN_OWNER, KEY, "obs"))
            await asyncio.sleep(0.05)
            assert not deploy.done() and not remove.done()
            await machines.forget_machine(OPEN_OWNER, KEY)
        for task in (deploy, remove):
            with pytest.raises(ToolError) as exc:
                await task
            assert exc.value.code is ErrorCode.LAUNCHER_NOT_FOUND
        assert await machines.list_definitions(OPEN_OWNER, KEY, include_deleted=True) == []
        assert dm._locks == {}
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_forget_keeps_one_lock_for_its_waiters(tmp_path: Path) -> None:
    store, machines, dm = await _setup(tmp_path)
    try:
        order: list[str] = []
        async with dm._machine_lock(OPEN_OWNER, KEY):
            forget = asyncio.create_task(dm.forget(OPEN_OWNER, KEY))
            deploy = asyncio.create_task(dm.deploy(OPEN_OWNER, KEY, "obs", dict(_DEF)))
            await asyncio.sleep(0.05)
            order.append("held")
        await forget
        with pytest.raises(ToolError):
            await deploy
        # after forget, a new caller and the waiter never ran under two locks:
        # the entry lives while anyone uses it and goes when nobody does
        assert dm._locks == {} and order == ["held"]
        assert await machines.list_definitions(OPEN_OWNER, KEY, include_deleted=True) == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_locks_are_dropped_when_unused(tmp_path: Path) -> None:
    store, _machines, dm = await _setup(tmp_path)
    try:
        out = await dm.deploy(OPEN_OWNER, KEY, "obs", dict(_DEF))
        assert out["state"] == "queued"
        await dm.remove(OPEN_OWNER, KEY, "obs")
        assert dm._locks == {}
    finally:
        await store.close()


def test_describe_exposes_identity_flags_and_profile_warnings() -> None:
    """Per agent: observer_as_root (bool), run_as (str|null) from the stored
    definition, and the warnings the launcher published with the profile."""
    from sessionweave.launchers import validate_profile
    from sessionweave.machines import Machine, StoredDefinition

    machine = Machine(OPEN_OWNER, KEY, "box", "root", "/", ["definitions"], "bwrap", True,
                      "linux", [], 0, 0)

    def row(def_id: str, **kw: object) -> StoredDefinition:
        return StoredDefinition(def_id, 1, {**_DEF, "id": def_id, **kw}, "queued", None,
                                [], [], False, 0)

    rows = [row("rootobs", observer_as_root=True, run_as=None),
            row("asdeploy", kind="operator", observer_as_root=False, run_as="deploy")]
    prof = validate_profile({"id": "local1", "domain": "ops", "role": "w", "runtime": "claude",
                             "access": {"preset": "reader"}, "max_instances": 1,
                             "warnings": ["shared runtime state"]})
    lnc = SimpleNamespace(
        launcher_id="lnc_1", machine="box", profiles={"local1": prof}, running={},
        v2=parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "root", "ceiling": "/",
                           "capabilities": ["definitions"], "run_as": True},
                          deploy_allowed=True))
    for live in (lnc, None):
        out = DeployManager._describe(live, machine, rows)  # type: ignore[arg-type]
        agents = {a["profile_id"]: a for a in out["agents"]}
        assert agents["rootobs"]["observer_as_root"] is True
        assert agents["rootobs"]["run_as"] is None
        assert agents["asdeploy"]["observer_as_root"] is False
        assert agents["asdeploy"]["run_as"] == "deploy"
        if live is not None:
            assert agents["local1"]["observer_as_root"] is False
            assert agents["local1"]["run_as"] is None
            assert agents["local1"]["warnings"] == ["shared runtime state"]
        assert agents["rootobs"]["warnings"] == []
