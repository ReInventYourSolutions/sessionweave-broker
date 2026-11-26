"""Deploying agent definitions to launchers over their socket.

The broker keeps the desired state of every machine (``MachineStore``:
definitions with a revision and the launcher's last status) and converges the
launcher to it:

* ``deploy`` validates and stores a definition, then sends ``definition_put``
  when the launcher is online (state ``sent``), else it waits (``queued``);
* ``remove`` marks it deleted and sends ``definition_delete``; the row goes
  when the launcher answers ``removed`` (a definition the launcher never got
  goes at once);
* ``on_connected`` (every v2 launcher with the ``definitions`` capability,
  right after ``welcome``) sends ``definitions_index`` (the ids and revisions
  it must hold: anything else it removes), a ``definition_delete`` for every
  removal not yet confirmed, and a ``definition_put`` for every definition
  not applied at its revision (except those it rejected at that revision);
* ``on_status`` records the launcher's ``definition_status`` frames.

Every operation on one machine runs under that machine's lock, so frames
reach a launcher in the order the store changed. ``owner`` is the owner
account id, or ``OPEN_OWNER`` in open mode: account events are emitted only
for an account; open-mode operations are logged at INFO with the client
address instead. Nothing a launcher produces is stored here.
"""
from __future__ import annotations

import asyncio
import logging
import time
from typing import TYPE_CHECKING, Any

from sessionweave.definitions import (
    MAX_DEFINITIONS_PER_LAUNCHER,
    DefinitionError,
    validate_definition,
)
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.launchers import _ID_RE, OPEN_OWNER, _is_int, owner_key
from sessionweave.launches import _short_text

if TYPE_CHECKING:
    from sessionweave.launchers import LauncherRegistry, _Launcher
    from sessionweave.machines import Machine, MachineStore, StoredDefinition
    from sessionweave.tools import ToolDeps

log = logging.getLogger(__name__)

# What a launcher may report about a definition.
STATUS_STATES = frozenset({"applied", "pending_in_use", "rejected", "removed"})
MAX_STATUS_TEXTS = 20
MAX_STATUS_TEXT = 300
_CAPABILITY = "definitions"


def _texts(raw: Any) -> list[str]:
    """At most 20 printable strings of at most 300 chars from a launcher list."""
    if not isinstance(raw, list):
        return []
    out = [_short_text(v, "", MAX_STATUS_TEXT) for v in raw[:MAX_STATUS_TEXTS]]
    return [t for t in out if t]


def _now_ms() -> int:
    return int(time.time() * 1000)


class DeployManager:
    def __init__(
        self, launchers: LauncherRegistry, store: MachineStore | None, deps: ToolDeps
    ) -> None:
        self._launchers = launchers
        # None on an open-mode broker without a persistence DB: machines are
        # listed (live only) and nothing can be deployed.
        self.store = store
        self._deps = deps
        self._locks: dict[tuple[str, str], asyncio.Lock] = {}
        # Launchers that got their index: only these get puts/deletes directly
        # (a launcher between ``welcome`` and its index gets them with it).
        self._ready: set[str] = set()

    # --- helpers -----------------------------------------------------------

    def require_store(self) -> MachineStore:
        if self.store is None:
            raise ToolError(
                ErrorCode.DEFINITIONS_UNAVAILABLE,
                "this broker keeps no agent definitions (it has no persistence DB)",
            )
        return self.store

    def _lock(self, owner: str, key: str) -> asyncio.Lock:
        return self._locks.setdefault((owner, key), asyncio.Lock())

    def _ready_launcher(self, owner: str, key: str) -> _Launcher | None:
        lnc = self._launchers.by_key(owner, key)
        if lnc is None or lnc.launcher_id not in self._ready:
            return None
        return lnc

    @staticmethod
    async def _send(lnc: _Launcher, frame: dict[str, Any]) -> None:
        """Send a frame; a dead socket is left to the reconnect (the store
        keeps the desired state and ``on_connected`` sends it again)."""
        try:
            await lnc.conn.send_json(frame)
        except Exception:
            log.info("launcher %s: could not send %s", lnc.launcher_id, frame.get("type"))

    async def _event(
        self, owner: str, kind: str, machine: str, key: str, def_id: str, revision: int,
        *, client: str = "", **extra: Any,
    ) -> None:
        if owner == OPEN_OWNER:
            log.info("%s %s on %s (%s) rev %d%s", kind, def_id, key, machine, revision,
                     f" from {client}" if client else "")
            return
        from sessionweave.tools import emit_account_event

        await emit_account_event(self._deps, owner, kind, {
            "launcher_key": key, "machine": machine, "definition_id": def_id,
            "revision": revision, **extra,
        })

    @staticmethod
    def _put_frame(row: StoredDefinition) -> dict[str, Any]:
        return {"type": "definition_put",
                "definition": {**row.definition, "revision": row.revision}}

    @staticmethod
    def _delete_frame(row: StoredDefinition) -> dict[str, Any]:
        return {"type": "definition_delete", "id": row.def_id, "revision": row.revision}

    # --- launcher side -----------------------------------------------------

    async def on_connected(self, lnc: _Launcher) -> None:
        """Converge a (re)connected launcher to the stored desired state."""
        key = lnc.v2.launcher_key
        if self.store is None or key is None or _CAPABILITY not in lnc.v2.capabilities:
            return
        owner = owner_key(lnc)
        async with self._lock(owner, key):
            rows = await self.store.list_definitions(owner, key, include_deleted=True)
            live = [r for r in rows if not r.deleted]
            await lnc.conn.send_json({
                "type": "definitions_index",
                "definitions": [{"id": r.def_id, "revision": r.revision} for r in live],
            })
            for r in rows:
                if r.deleted:
                    await lnc.conn.send_json(self._delete_frame(r))
            now = _now_ms()
            for r in live:
                if r.applied_revision == r.revision:
                    continue
                if r.state == "rejected":
                    continue  # rejected at this revision: a new put is needed
                await lnc.conn.send_json(self._put_frame(r))
                if r.state == "queued":
                    await self.store.set_status(
                        owner, key, r.def_id, r.revision, "sent", [], [], now)
            self._ready.add(lnc.launcher_id)

    def on_disconnected(self, launcher_id: str) -> None:
        self._ready.discard(launcher_id)

    async def on_status(self, launcher_id: str, body: dict[str, Any]) -> None:
        """A ``definition_status`` frame (invalid or stale ones are ignored)."""
        lnc = self._launchers.get(launcher_id)
        key = None if lnc is None else lnc.v2.launcher_key
        if lnc is None or key is None or self.store is None:
            return
        def_id, revision, state = body.get("id"), body.get("revision"), body.get("state")
        if (
            not isinstance(def_id, str) or not _ID_RE.fullmatch(def_id)
            or not _is_int(revision) or state not in STATUS_STATES
        ):
            log.info("launcher %s: definition_status ignored (invalid)", launcher_id)
            return
        assert isinstance(revision, int)
        owner = owner_key(lnc)
        problems, warnings = _texts(body.get("problems")), _texts(body.get("warnings"))
        async with self._lock(owner, key):
            try:
                row = await self.store.get_definition(owner, key, def_id)
            except ToolError:
                return
            if row.deleted:
                # A removal: confirmed whatever the revision (the launcher only
                # learnt it from the index); any other report is stale.
                if state == "removed":
                    await self.store.purge_definition(owner, key, def_id)
                    await self._event(owner, "definition.removed", lnc.machine, key,
                                      def_id, row.revision)
                return
            if state == "removed":
                return  # a live definition is not removed by a launcher report
            updated = await self.store.set_status(
                owner, key, def_id, revision, state, problems, warnings, _now_ms())
            if updated is None:
                return
            if state == "applied":
                await self._event(owner, "definition.applied", lnc.machine, key,
                                  def_id, revision)
            elif state == "rejected":
                await self._event(owner, "definition.rejected", lnc.machine, key,
                                  def_id, revision, problems=problems)

    # --- API side ----------------------------------------------------------

    async def _machine(self, owner: str, key: str) -> Machine:
        return await self.require_store().get_machine(owner, key)

    async def deploy(
        self, owner: str, launcher_key: str, def_id: str, raw: Any, *, client: str = "",
    ) -> dict[str, Any]:
        """Validate and store ``raw`` as definition ``def_id`` of the machine,
        then send it (online) or queue it (offline)."""
        store = self.require_store()
        machine = await self._machine(owner, launcher_key)
        if _CAPABILITY not in machine.capabilities:
            raise ToolError(
                ErrorCode.LAUNCHER_CAPABILITY_MISSING,
                "this machine's launcher accepts no remote definitions (start it with"
                " --local <folder> or --root, with a current Runner)",
            )
        if isinstance(raw, dict) and "id" not in raw:
            raw = {**raw, "id": def_id}
        if isinstance(raw, dict) and raw.get("id") != def_id:
            raise ToolError(ErrorCode.INVALID_REQUEST, "definition.id must match the URL")
        try:
            definition = validate_definition(raw, machine)
        except DefinitionError as exc:
            raise ToolError(exc.code, str(exc)) from exc
        async with self._lock(owner, launcher_key):
            lnc = self._launchers.by_key(owner, launcher_key)
            prof = None if lnc is None else lnc.profiles.get(def_id)
            if prof is not None and prof["source"] == "local":
                raise ToolError(
                    ErrorCode.DEFINITION_ID_CONFLICT,
                    f"the launcher has a local profile {def_id!r}; choose another id",
                )
            rows = await store.list_definitions(owner, launcher_key)
            if def_id not in {r.def_id for r in rows} and (
                len(rows) >= MAX_DEFINITIONS_PER_LAUNCHER
            ):
                raise ToolError(
                    ErrorCode.DEFINITION_LIMIT,
                    f"at most {MAX_DEFINITIONS_PER_LAUNCHER} definitions per machine",
                )
            online = lnc is not None
            row = await store.put_definition(
                owner, launcher_key, definition, "sent" if online else "queued", _now_ms())
            ready = self._ready_launcher(owner, launcher_key)
            if ready is not None:
                await self._send(ready, self._put_frame(row))
            await self._event(owner, "definition.deployed", machine.machine, launcher_key,
                              def_id, row.revision, client=client, queued=not online)
        return {"definition_id": def_id, "revision": row.revision, "state": row.state}

    async def remove(
        self, owner: str, launcher_key: str, def_id: str, *, client: str = "",
    ) -> dict[str, Any]:
        """Ask the launcher to remove a definition (refused while in use)."""
        store = self.require_store()
        machine = await self._machine(owner, launcher_key)
        async with self._lock(owner, launcher_key):
            row = await store.get_definition(owner, launcher_key, def_id)
            if row.deleted:
                raise ToolError(ErrorCode.DEFINITION_NOT_FOUND, "no such definition")
            lnc = self._launchers.by_key(owner, launcher_key)
            if lnc is not None and lnc.running.get(def_id, 0) > 0:
                raise ToolError(
                    ErrorCode.DEFINITION_IN_USE,
                    "the agent is running in a session; remove it after the session ends",
                )
            never_held = row.applied_revision is None and row.state in ("queued", "rejected")
            gone = await store.mark_deleted(owner, launcher_key, def_id, _now_ms())
            if never_held:
                # The launcher never held it: nothing to wait for.
                await store.purge_definition(owner, launcher_key, def_id)
                await self._event(owner, "definition.removed", machine.machine,
                                  launcher_key, def_id, gone.revision, client=client)
                return {"definition_id": def_id, "revision": gone.revision, "state": "removed"}
            ready = self._ready_launcher(owner, launcher_key)
            if ready is not None:
                await self._send(ready, self._delete_frame(gone))
            if owner == OPEN_OWNER:
                log.info("definition %s removal asked on %s from %s", def_id, launcher_key,
                         client)
        return {"definition_id": def_id, "revision": gone.revision, "state": gone.state}

    async def forget(self, owner: str, launcher_key: str, *, client: str = "") -> None:
        """Forget an offline machine: its descriptor, key secret and
        definitions (the reset for a lost ``key_secret``)."""
        store = self.require_store()
        await self._machine(owner, launcher_key)
        async with self._lock(owner, launcher_key):
            if self._launchers.by_key(owner, launcher_key) is not None:
                raise ToolError(
                    ErrorCode.LAUNCHER_ONLINE,
                    "the machine's launcher is connected; stop it before forgetting it",
                )
            await store.forget_machine(owner, launcher_key)
        self._locks.pop((owner, launcher_key), None)
        log.info("machine %s forgotten%s", launcher_key,
                 f" from {client}" if owner == OPEN_OWNER and client else "")

    async def machines(self, owner: str) -> list[dict[str, Any]]:
        """Every machine of ``owner``: stored ones (online or not), then live
        launchers without a stored row (keyless v1 launchers, or no DB)."""
        out: list[dict[str, Any]] = []
        seen: set[str] = set()
        if self.store is not None:
            for m in await self.store.list_machines(owner):
                seen.add(m.launcher_key)
                rows = await self.store.list_definitions(
                    owner, m.launcher_key, include_deleted=True)
                out.append(self._describe(
                    self._launchers.by_key(owner, m.launcher_key), m, rows))
        live_keys: set[str] = set()
        for lnc in self._launchers.for_account(owner):
            key = lnc.v2.launcher_key
            if key is not None:
                if key in seen or key in live_keys:
                    continue
                live_keys.add(key)
                lnc = self._launchers.by_key(owner, key) or lnc
            out.append(self._describe(lnc, None, []))
        return out

    async def machine_detail(self, owner: str, launcher_key: str) -> dict[str, Any]:
        """One machine with its stored definitions (full content)."""
        stored: Machine | None = None
        rows: list[StoredDefinition] = []
        if self.store is not None:
            try:
                stored = await self.store.get_machine(owner, launcher_key)
            except ToolError:
                stored = None
            else:
                rows = await self.store.list_definitions(
                    owner, launcher_key, include_deleted=True)
        lnc = self._launchers.by_key(owner, launcher_key)
        if stored is None and lnc is None:
            raise ToolError(ErrorCode.LAUNCHER_NOT_FOUND, "no such machine")
        return {
            **self._describe(lnc, stored, rows),
            "definitions": [{"definition": r.definition, "deploy": r.deploy()} for r in rows],
        }

    @staticmethod
    def _describe(
        lnc: _Launcher | None, stored: Machine | None, rows: list[StoredDefinition],
    ) -> dict[str, Any]:
        """The API's Machine object (live data first, else the stored row)."""
        online = lnc is not None
        if lnc is not None:
            v2 = lnc.v2
            desc: dict[str, Any] = {
                "launcher_key": v2.launcher_key, "launcher_id": lnc.launcher_id,
                "machine": lnc.machine, "online": True, "mode": v2.mode,
                "ceiling": v2.ceiling, "sandbox": v2.sandbox, "run_as": v2.run_as,
                "platform": v2.platform, "capabilities": sorted(v2.capabilities),
                "prereqs": [dict(p) for p in v2.prereqs],
                "last_seen": None if stored is None else stored.last_seen,
            }
        else:
            assert stored is not None
            desc = {
                "launcher_key": stored.launcher_key, "launcher_id": None,
                "machine": stored.machine, "online": False, "mode": stored.mode,
                "ceiling": stored.ceiling, "sandbox": stored.sandbox,
                "run_as": stored.run_as, "platform": stored.platform,
                "capabilities": sorted(stored.capabilities),
                "prereqs": list(stored.prereqs), "last_seen": stored.last_seen,
            }
        profiles = {} if lnc is None else lnc.profiles
        defs = {r.def_id: r for r in rows}
        agents: list[dict[str, Any]] = []
        for pid in [*profiles, *(d for d in defs if d not in profiles)]:
            prof, row = profiles.get(pid), defs.get(pid)
            body = row.definition if row is not None else {}
            running = 0 if lnc is None else lnc.running.get(pid, 0)
            if not online:
                status = "offline"
            elif running > 0:
                status = "in_session"
            elif prof is None or (row is not None and row.applied_revision is None):
                status = "not_deployed"
            else:
                status = "idle"
            src = prof or body
            agents.append({
                "profile_id": pid,
                "source": prof["source"] if prof is not None else "remote",
                "kind": prof["kind"] if prof is not None and prof["kind"] else body.get("kind"),
                "domain": src.get("domain"), "role": src.get("role"),
                "runtime": src.get("runtime"), "model": src.get("model"),
                "max_instances": src.get("max_instances"),
                "running": running, "status": status,
                "approval_policy": (body.get("approval") or {}).get("policy"),
                "deploy": None if row is None else row.deploy(),
            })
        desc["agents"] = agents
        return desc
