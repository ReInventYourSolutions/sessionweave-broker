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


class _FastLauncherConn:
    """Answers every definition_put at once, while the broker is still inside
    the send: the status is handled concurrently with the rest of the put."""

    def __init__(self, state: str) -> None:
        self.state = state
        self.dm: DeployManager | None = None
        self.launcher_id = ""
        self.tasks: list[asyncio.Task[None]] = []

    async def send_json(self, frame: dict[str, object]) -> None:
        if frame.get("type") != "definition_put" or self.dm is None:
            return
        d = frame["definition"]
        assert isinstance(d, dict)
        task = asyncio.create_task(self.dm.on_status(self.launcher_id, {
            "id": d["id"], "revision": d["revision"], "state": self.state,
            "problems": ["p"] if self.state == "rejected" else [], "warnings": []}))
        self.tasks.append(task)
        # Let the status go as far as it can (to the end, unless it must wait
        # for the machine lock the broker holds while it sends).
        await asyncio.wait({task}, timeout=0.3)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["applied", "rejected", "pending_in_use"])
async def test_fast_status_is_never_overwritten_by_sent(tmp_path: Path, state: str) -> None:
    """A launcher status that races the put (on reconnect and on a live deploy)
    is recorded after the broker's own ``sent``: the state never moves back."""
    store, machines, dm = await _setup(tmp_path)
    try:
        assert (await dm.deploy(OPEN_OWNER, KEY, "obs", dict(_DEF)))["state"] == "queued"
        conn = _FastLauncherConn(state)
        v2 = parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "local",
                             "ceiling": "/srv/app", "capabilities": ["definitions"]},
                            deploy_allowed=True)
        lid, _ = dm._launchers.register(
            conn, None, {"machine": "box", "profiles": []}, v2=v2)
        conn.dm, conn.launcher_id = dm, lid
        # reconnect: queued -> put (status races) -> sent
        await dm.on_connected(dm._launchers.get(lid))  # type: ignore[arg-type]
        await asyncio.gather(*conn.tasks)
        row = await machines.get_definition(OPEN_OWNER, KEY, "obs")
        assert (row.revision, row.state) == (1, state)
        # live deploy: stored as sent -> put (status races)
        out = await dm.deploy(OPEN_OWNER, KEY, "obs", {**_DEF, "description": "v2"})
        await asyncio.gather(*conn.tasks)
        row = await machines.get_definition(OPEN_OWNER, KEY, "obs")
        assert (out["revision"], row.revision, row.state) == (2, 2, state)
    finally:
        await store.close()
