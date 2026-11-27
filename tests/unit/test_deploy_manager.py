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


# --- final review fixes ------------------------------------------------------------------

class _SinkConn:
    def __init__(self) -> None:
        self.frames: list[dict[str, object]] = []

    async def send_json(self, frame: dict[str, object]) -> None:
        self.frames.append(frame)


def _big(def_id: str) -> dict[str, object]:
    """A definition whose published profile is about 30 KB (all fields at their caps)."""
    return {**_DEF, "id": def_id, "description": "é" * 1000,
            "capabilities": [f"{i:02d}" + "c" * 62 for i in range(32)],
            "allowed_accounts": [f"{i:03d}" + "a" * 197 for i in range(100)]}


def _local_profile(pid: str, size: int) -> dict[str, object]:
    return {"id": pid, "domain": "ops", "role": "w", "runtime": "claude", "model": "",
            "access": {"preset": "reader"}, "limits": {}, "max_instances": 1,
            "source": "local", "description": "", "capabilities": [],
            "warnings": [f"w{i:03d}" + "x" * 296 for i in range(min(size // 300, 20))]}


@pytest.mark.asyncio
async def test_deploy_refused_over_the_published_profiles_budget(tmp_path: Path) -> None:
    """I1: a deploy that would make the machine publish more than the launcher's
    192 KiB budget is refused (409 definitions_budget) and nothing is stored; an
    update of a stored definition replaces its own share."""
    from sessionweave.definitions import (
        PUBLISHED_PROFILES_BUDGET,
        json_size,
        profiles_frame_size,
        published_profile,
        validate_definition,
    )
    from sessionweave.machines import Machine

    store, machines, dm = await _setup(tmp_path)
    try:
        machine = Machine(OPEN_OWNER, KEY, "box", "local", "/srv/app", ["definitions"],
                          "bwrap", False, "linux", [], 0, 0)
        one = json_size(published_profile(validate_definition(_big("d0"), machine), []))
        fit = 0
        while profiles_frame_size([one] * (fit + 1)) <= PUBLISHED_PROFILES_BUDGET:
            fit += 1
        assert 5 <= fit < 9
        for i in range(fit):
            await dm.deploy(OPEN_OWNER, KEY, f"d{i}", _big(f"d{i}"))
        with pytest.raises(ToolError) as exc:
            await dm.deploy(OPEN_OWNER, KEY, "extra", _big("extra"))
        assert exc.value.code is ErrorCode.DEFINITIONS_BUDGET
        assert "budget exceeded" in exc.value.message and "of the 192 KiB" in exc.value.message
        assert len(await machines.list_definitions(OPEN_OWNER, KEY)) == fit
        # an update of a stored one counts once; a small new one still fits
        out = await dm.deploy(OPEN_OWNER, KEY, "d0", {**_big("d0"), "description": "x"})
        assert out["revision"] == 2
        await dm.deploy(OPEN_OWNER, KEY, "small", {**_DEF, "id": "small"})
        # removing one makes room again
        await dm.remove(OPEN_OWNER, KEY, "d1")
        await dm.deploy(OPEN_OWNER, KEY, "extra", _big("extra"))
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_budget_counts_the_launchers_published_profiles(tmp_path: Path) -> None:
    """Online, the launcher's TOML profiles count at the size it sent them."""
    store, _machines, dm = await _setup(tmp_path)
    try:
        v2 = parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "local",
                             "ceiling": "/srv/app", "capabilities": ["definitions"]},
                            deploy_allowed=True)
        locals_ = [_local_profile(f"t{i}", 6000) for i in range(30)]  # ~180 KB
        lid, rejected = dm._launchers.register(
            _SinkConn(), None, {"machine": "box", "profiles": locals_}, v2=v2)
        assert rejected == []
        lnc = dm._launchers.get(lid)
        assert lnc is not None and sum(lnc.profile_sizes.values()) > 170_000
        with pytest.raises(ToolError) as exc:
            await dm.deploy(OPEN_OWNER, KEY, "d0", _big("d0"))
        assert exc.value.code is ErrorCode.DEFINITIONS_BUDGET
        # offline, the TOML profiles are unknown: the launcher's own check decides
        dm._launchers.unregister(lid)
        assert (await dm.deploy(OPEN_OWNER, KEY, "d0", _big("d0")))["state"] == "queued"
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_removals_done_when_the_launcher_comes_back_without_definitions(
    tmp_path: Path,
) -> None:
    """M5: a launcher online without the definitions capability (config mode)
    holds no definitions: pending removals are purged when it connects, and a
    removal asked while it is connected is done at once."""
    store, machines, dm = await _setup(tmp_path)
    try:
        for d in ("a", "b"):
            await dm.deploy(OPEN_OWNER, KEY, d, {**_DEF, "id": d})
            await machines.set_status(OPEN_OWNER, KEY, d, 1, "applied", [], [], 1)
        assert (await dm.remove(OPEN_OWNER, KEY, "a"))["state"] == "removing"
        v2 = parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "config"},
                            deploy_allowed=True)
        conn = _SinkConn()
        lid, _ = dm._launchers.register(conn, None, {"machine": "box", "profiles": []}, v2=v2)
        lnc = dm._launchers.get(lid)
        assert lnc is not None
        await dm.on_connected(lnc)
        assert conn.frames == []  # nothing is sent to a config-mode launcher
        rows = await machines.list_definitions(OPEN_OWNER, KEY, include_deleted=True)
        assert [r.def_id for r in rows] == ["b"]
        out = await dm.remove(OPEN_OWNER, KEY, "b")
        assert out["state"] == "removed"
        assert await machines.list_definitions(OPEN_OWNER, KEY, include_deleted=True) == []
        assert conn.frames == []
    finally:
        await store.close()


@pytest.mark.asyncio
async def test_repeated_status_is_recorded_and_announced_once(tmp_path: Path) -> None:
    """M7: a launcher repeating the same status writes nothing and emits no
    second definition.applied (or rejected) event."""
    store, machines, dm = await _setup(tmp_path)
    events: list[tuple[str, str, int]] = []

    async def record(owner: str, kind: str, machine: str, key: str, def_id: str,
                     revision: int, **kw: object) -> None:
        events.append((kind, def_id, revision))

    dm._event = record  # type: ignore[method-assign]
    try:
        v2 = parse_hello_v2({"protocol": 2, "launcher_key": KEY, "mode": "local",
                             "ceiling": "/srv/app", "capabilities": ["definitions"]},
                            deploy_allowed=True)
        lid, _ = dm._launchers.register(_SinkConn(), None, {"machine": "box", "profiles": []},
                                        v2=v2)
        await dm.deploy(OPEN_OWNER, KEY, "obs", dict(_DEF))
        for _ in range(3):
            await dm.on_status(lid, {"id": "obs", "revision": 1, "state": "applied",
                                     "warnings": ["w"]})
        row = await machines.get_definition(OPEN_OWNER, KEY, "obs")
        updated = row.updated_at
        await dm.on_status(lid, {"id": "obs", "revision": 1, "state": "applied",
                                 "warnings": ["w"]})
        assert (await machines.get_definition(OPEN_OWNER, KEY, "obs")).updated_at == updated
        # changed warnings are recorded (no new event: still applied at rev 1)...
        await dm.on_status(lid, {"id": "obs", "revision": 1, "state": "applied",
                                 "warnings": ["w2"]})
        assert (await machines.get_definition(OPEN_OWNER, KEY, "obs")).warnings == ["w2"]
        await dm.deploy(OPEN_OWNER, KEY, "obs", {**_DEF, "description": "v2"})
        for _ in range(2):
            await dm.on_status(lid, {"id": "obs", "revision": 2, "state": "rejected",
                                     "problems": ["p"]})
        assert events == [("definition.deployed", "obs", 1), ("definition.applied", "obs", 1),
                          ("definition.deployed", "obs", 2), ("definition.rejected", "obs", 2)]
    finally:
        await store.close()
