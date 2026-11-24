"""Launches: agents a session's coordinator asks a launcher to start.

A launch reserves one slot of a launcher profile and counts towards the
session's ``max_launched_agents`` while it is ``pending``, ``started`` or
``joined``. The broker mints a single-use launch credential bound to the
session and the profile's role/capabilities, and sends it to the launcher
inside the launch order: it is never logged or returned anywhere else.

Statuses: ``pending`` (order sent) -> ``started`` (launcher reported the agent
running) -> ``joined`` (the agent joined with the credential); ``failed``
(launcher offline, start failure, join timeout) and ``stopped`` (removed,
session ended, or stopped by the launcher) are final. Records live in RAM;
a final one is kept for an hour so its result stays readable.
"""
from __future__ import annotations

import asyncio
import hmac
import logging
import os
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from sessionweave.access import Principal, hash_token, is_coordinator
from sessionweave.errors import ErrorCode, ToolError

if TYPE_CHECKING:
    from sessionweave.launchers import LauncherRegistry
    from sessionweave.session import Session
    from sessionweave.tools import ToolDeps

log = logging.getLogger(__name__)

JOIN_TIMEOUT_S = 120.0
RECONNECT_GRACE_S = 300.0
_MAX_RUNNING_IDS = 1000
TASK_MAX_BYTES = 16 * 1024
_KEEP_FINAL_S = 3600.0
_ACTIVE = frozenset({"pending", "started", "joined"})
_TEXT_RE = re.compile(r"[^\x20-\x7e]")


def join_timeout_from_env() -> float:
    """SW_LAUNCH_JOIN_TIMEOUT_S (seconds, 1..3600), default 120."""
    raw = os.environ.get("SW_LAUNCH_JOIN_TIMEOUT_S", "").strip()
    try:
        value = float(raw) if raw else JOIN_TIMEOUT_S
    except ValueError:
        value = JOIN_TIMEOUT_S
    return min(max(value, 1.0), 3600.0)


def reconnect_grace_from_env() -> float:
    """SW_LAUNCHER_RECONNECT_GRACE_S (seconds, 1..86400), default 300."""
    raw = os.environ.get("SW_LAUNCHER_RECONNECT_GRACE_S", "").strip()
    try:
        value = float(raw) if raw else RECONNECT_GRACE_S
    except ValueError:
        value = RECONNECT_GRACE_S
    return min(max(value, 1.0), 86400.0)


def _short_text(raw: Any, default: str, limit: int = 200) -> str:
    """A printable, bounded string from a launcher-supplied value."""
    if not isinstance(raw, str) or not raw.strip():
        return default
    return _TEXT_RE.sub("?", raw.strip())[:limit]


@dataclass
class Launch:
    launch_id: str
    session_id: str
    launcher_id: str
    machine: str
    profile_id: str
    # Identity of the launcher holding the agent, to re-associate it on reconnect.
    launcher_owner: str | None
    launcher_token_id: str | None
    # sha256 of the launcher's resume secret (never the secret itself).
    resume_hash: bytes
    launched_by: str | None
    accounts: tuple[str, ...]  # launcher owner + session owner (deduplicated)
    token_id: str | None  # the launch credential, until joined or invalidated
    # The launcher owner's account name (None in open mode) and whether it is
    # the session owner's own launcher.
    owner: str | None = None
    own: bool = True
    status: str = "pending"
    participant_id: str | None = None
    error: str | None = None
    reason: str | None = None
    slot_held: bool = True
    joining: bool = False
    ended_at: float | None = None
    timer: asyncio.TimerHandle | None = field(default=None, repr=False)
    # Armed while a joined agent's launcher is disconnected.
    lost_timer: asyncio.TimerHandle | None = field(default=None, repr=False)

    def public(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "launch_id": self.launch_id,
            "status": self.status,
            "profile_id": self.profile_id,
            "launcher_id": self.launcher_id,
            "machine": self.machine,
            "owner": self.owner,
            "own": self.own,
        }
        if self.participant_id is not None:
            out["participant_id"] = self.participant_id
        if self.error is not None:
            out["error"] = self.error
        if self.reason is not None:
            out["reason"] = self.reason
        return out

    def event_data(self, **extra: Any) -> dict[str, Any]:
        return {
            "session_id": self.session_id,
            "launch_id": self.launch_id,
            "profile_id": self.profile_id,
            "launcher_id": self.launcher_id,
            "machine": self.machine,
            **extra,
        }


def _parse_body(body: dict[str, Any]) -> tuple[str, dict[str, str]]:
    task = body.get("task")
    if not isinstance(task, str) or not task.strip():
        raise ToolError(ErrorCode.INVALID_REQUEST, "task must be a non-empty string")
    if len(task.encode("utf-8", "surrogatepass")) > TASK_MAX_BYTES:
        raise ToolError(ErrorCode.INVALID_REQUEST, "task must be at most 16 KiB")
    sel: dict[str, str] = {}
    for key in ("launcher_id", "profile_id", "domain", "role"):
        v = body.get(key)
        if v is None or v == "":
            continue
        if not isinstance(v, str) or len(v) > 200:
            raise ToolError(ErrorCode.INVALID_REQUEST, f"{key} must be a string")
        sel[key] = v
    explicit = "launcher_id" in sel and "profile_id" in sel
    by_role = "domain" in sel and "role" in sel
    if explicit == by_role:
        raise ToolError(
            ErrorCode.INVALID_REQUEST,
            "give either launcher_id and profile_id, or domain and role",
        )
    return task, sel


class LaunchManager:
    """Launch records plus the hooks sessions and launchers call."""

    def __init__(self, launchers: LauncherRegistry, deps: ToolDeps) -> None:
        self._launchers = launchers
        self._deps = deps
        self._launches: dict[str, Launch] = {}
        self._tasks: set[asyncio.Task[None]] = set()
        self.join_timeout_s = join_timeout_from_env()
        self.reconnect_grace_s = reconnect_grace_from_env()

    # --- queries ---

    def get(self, launch_id: str) -> Launch | None:
        return self._launches.get(launch_id)

    def active_count(self, session_id: str) -> int:
        return sum(
            1 for lch in self._launches.values()
            if lch.session_id == session_id and lch.status in _ACTIVE
        )

    def status(self, session_id: str, launch_id: str) -> dict[str, Any]:
        lch = self._launches.get(launch_id)
        if lch is None or lch.session_id != session_id:
            raise ToolError(ErrorCode.LAUNCH_NOT_FOUND, "no such launch in this session")
        return lch.public()

    # --- launch ---

    async def launch(
        self,
        sess: Session,
        principal: Principal,
        body: dict[str, Any],
        *,
        broker_url: str | None,
        owner_name: str | None,
    ) -> dict[str, Any]:
        """Reserve a slot, mint the credential and send the order (the caller
        has checked the coordinator right)."""
        task, sel = _parse_body(body)
        self._prune()
        sid = sess.session_id
        # Synchronous from here to the reservation: no concurrent launch can
        # take the same slot or overshoot the session cap.
        if self._deps.registry.get(sid) is not sess:
            raise ToolError(ErrorCode.SESSION_NOT_FOUND, "session not found")
        # The agent could not join a suspended session: refuse up front.
        sess._ensure_not_suspended()
        if self.active_count(sid) >= sess.config.max_launched_agents:
            raise ToolError(
                ErrorCode.LAUNCH_LIMIT,
                f"this session already has {sess.config.max_launched_agents} launched "
                "agents (max_launched_agents); remove one first",
            )
        lnc, prof = self._launchers.select(
            sess, owner_name,
            launcher_id=sel.get("launcher_id"), profile_id=sel.get("profile_id"),
            domain=sel.get("domain"), role=sel.get("role"),
        )
        lnc.running[prof["id"]] = lnc.running.get(prof["id"], 0) + 1
        launch_id = "lch_" + secrets.token_hex(8)
        token, issued = sess.auth.mint_launch(
            int(time.time() * 1000), int(self.join_timeout_s * 1000),
            launch_id=launch_id, role=prof["role"], capabilities=prof["capabilities"],
        )
        accounts = tuple(dict.fromkeys(
            a for a in (lnc.owner_account_id, sess.auth.owner_account_id) if a is not None
        ))
        lch = Launch(
            launch_id=launch_id, session_id=sid, launcher_id=lnc.launcher_id,
            machine=lnc.machine, profile_id=prof["id"],
            launcher_owner=lnc.owner_account_id, launcher_token_id=lnc.token_id,
            resume_hash=lnc.resume_hash,
            launched_by=principal.participant_id if is_coordinator(sess, principal) else None,
            accounts=accounts, token_id=issued.token_id,
            **self._launchers.owner_fields(lnc.owner_account_id, lnc.owner_name, sess),
        )
        self._launches[launch_id] = lch
        lch.timer = asyncio.get_running_loop().call_later(
            self.join_timeout_s, self._on_timeout, launch_id
        )
        self._account_event(lch, "agent.launched")
        order: dict[str, Any] = {
            "type": "launch",
            "launch_id": launch_id,
            "session_id": sid,
            "profile_id": prof["id"],
            "task": task,
            "join_credential": token,
            # Who to report the result to: the coordinator that asked, else the
            # session's coordinator (None when there is none).
            "coordinator_id": lch.launched_by or sess.coordinator_id,
            # The session owner's account name (None in open mode), so the
            # launcher can double-check its visibility rules.
            "session_owner": owner_name,
        }
        # Only the operator-configured public URL: never one derived from
        # request headers (a spoofed Host must not redirect the credential).
        # Without it the launcher uses the URL it connected to.
        if broker_url:
            order["broker_url"] = broker_url
        try:
            await lnc.conn.send_json(order)
        except asyncio.CancelledError:
            if lch.status in _ACTIVE:
                self._fail(lch, "cancelled")  # schedules its notifications
            raise
        except Exception:
            self._fail(lch, "launcher_offline")
        return {
            "launch_id": launch_id, "status": lch.status,
            "launcher_id": lch.launcher_id, "machine": lch.machine,
            "owner": lch.owner, "own": lch.own,
        }

    # --- join (called by session_join with a launch credential) ---

    def begin_join(self, sess: Session, token_id: str) -> tuple[Launch, str, list[str]]:
        """Consume the credential; return the launch and the bound role and
        capabilities. Raises PERMISSION_DENIED when it cannot be used."""
        issued = sess.auth.consume_launch(token_id, int(time.time() * 1000))
        lch = self._launches.get(issued.launch_id or "") if issued is not None else None
        if (
            issued is None or lch is None or lch.session_id != sess.session_id
            or lch.status not in ("pending", "started") or lch.joining
        ):
            raise ToolError(
                ErrorCode.PERMISSION_DENIED, "launch credential already used or expired"
            )
        lch.joining = True
        return lch, issued.role or "", list(issued.capabilities)

    def abort_join(self, sess: Session, lch: Launch) -> None:
        lch.joining = False
        if lch.token_id is not None:
            sess.auth.unconsume_launch(lch.token_id)

    def joined(self, sess: Session, lch: Launch, participant_id: str) -> None:
        lch.joining = False
        lch.participant_id = participant_id
        lch.status = "joined"
        self._cancel_timer(lch)
        self._drop_credential(lch, sess)
        if self._launchers.get_conn(lch.launcher_id) is None:
            # Its launcher dropped while the agent was joining.
            lch.slot_held = False
            lch.lost_timer = asyncio.get_running_loop().call_later(
                self.reconnect_grace_s, self._on_lost, lch.launch_id
            )

    @staticmethod
    def join_metadata(lch: Launch) -> dict[str, Any]:
        return {
            "launch_id": lch.launch_id,
            "launcher_id": lch.launcher_id,
            "launcher_machine": lch.machine,
            "profile_id": lch.profile_id,
            "launched_by": lch.launched_by,
        }

    # --- launcher frames ---

    def _own(self, launcher_id: str, msg: dict[str, Any]) -> Launch | None:
        lid = msg.get("launch_id")
        lch = self._launches.get(lid) if isinstance(lid, str) else None
        if lch is None or lch.launcher_id != launcher_id:
            return None
        return lch

    def on_result(self, launcher_id: str, msg: dict[str, Any]) -> None:
        lch = self._own(launcher_id, msg)
        if lch is None:
            return
        if msg.get("status") == "started" and lch.status == "pending":
            lch.status = "started"
        elif msg.get("status") == "failed" and lch.status in ("pending", "started"):
            if not lch.joining:
                self._fail(lch, _short_text(msg.get("error"), "start_failed"))

    def on_stopped(self, launcher_id: str, msg: dict[str, Any]) -> None:
        lch = self._own(launcher_id, msg)
        if lch is None or lch.status not in _ACTIVE:
            return
        reason = _short_text(msg.get("reason"), "stopped", 64)
        if lch.status != "joined":
            if not lch.joining:
                self._fail(lch, reason)
            return
        self._finish(lch, "stopped", reason=reason)
        self._account_event(lch, "agent.stopped", reason=reason)
        self._spawn(self._remove_stopped(lch, reason, f"stopped by its launcher: {reason}"))

    async def _remove_stopped(self, lch: Launch, reason: str, halt_message: str) -> None:
        try:
            sess = await self._deps.registry.get_or_raise(lch.session_id)
        except ToolError:
            return
        pid = lch.participant_id
        assert pid is not None
        try:
            await sess.remove_participant(pid, halt_message=halt_message)
        except ToolError:
            pass  # already gone
        await sess.emit_event(
            "agent.stopped",
            {"participant_id": pid, "launch_id": lch.launch_id, "reason": reason},
        )

    def launcher_gone(self, launcher_id: str) -> None:
        """The launcher's socket closed: launches not yet joined fail."""
        for lch in list(self._launches.values()):
            if lch.launcher_id != launcher_id:
                continue
            lch.slot_held = False  # the launcher and its slot count are gone
            if lch.status in ("pending", "started") and not lch.joining:
                self._fail(lch, "launcher_offline")
            elif lch.status == "joined" and lch.lost_timer is None:
                # It may reconnect and report the agent still running.
                lch.lost_timer = asyncio.get_running_loop().call_later(
                    self.reconnect_grace_s, self._on_lost, lch.launch_id
                )

    def _on_lost(self, launch_id: str) -> None:
        lch = self._launches.get(launch_id)
        if lch is None:
            return
        lch.lost_timer = None
        if lch.status != "joined" or self._launchers.get_conn(lch.launcher_id) is not None:
            return
        self._finish(lch, "stopped", reason="launcher_lost")
        self._account_event(lch, "agent.stopped", reason="launcher_lost")
        self._spawn(self._remove_stopped(
            lch, "launcher_lost", "stopped: its launcher disconnected"
        ))

    def reattach(
        self, launcher_id: str, owner: str | None, token_id: str | None, machine: str,
        running: list[str], resume_secret: str | None,
    ) -> list[dict[str, Any]]:
        """Re-associate the joined launches a reconnecting launcher reports
        still running; return the stop orders for the ids it may not keep.

        A launch is claimed only with the resume secret its launcher got in
        ``welcome`` (and, in account mode, by the same owner account). If the
        old connection is still registered (a half-open socket), it is
        replaced: closed and unregistered after its launches move over.
        """
        new_hash = self._launchers.resume_hash(launcher_id)
        presented = hash_token(resume_secret) if resume_secret else None
        stops: list[dict[str, Any]] = []
        replaced: set[str] = set()
        for lid in running:
            lch = self._launches.get(lid)
            if (
                lch is None
                or presented is None
                or lch.launcher_owner != owner
                or not hmac.compare_digest(presented, lch.resume_hash)
            ):
                stops.append({"type": "stop", "launch_id": lid, "reason": "unknown_launch"})
                continue
            if lch.status != "joined":
                reason = lch.reason or lch.error or "unknown_launch"
                stops.append({"type": "stop", "launch_id": lid, "reason": reason})
                continue
            if lch.launcher_id == launcher_id:
                continue
            if self._launchers.get_conn(lch.launcher_id) is not None:
                replaced.add(lch.launcher_id)  # the same launcher, reconnected early
            if lch.lost_timer is not None:
                lch.lost_timer.cancel()
                lch.lost_timer = None
            lch.launcher_id = launcher_id
            lch.launcher_token_id = token_id
            lch.machine = machine
            if new_hash is not None:
                lch.resume_hash = new_hash
            lch.slot_held = self._launchers.hold_slot(launcher_id, lch.profile_id)
        for old in replaced:
            conn = self._launchers.get_conn(old)
            self._launchers.unregister(old)
            close = getattr(conn, "close", None)
            if close is not None:
                self._spawn(close(code=4409, reason="replaced by a newer connection"))
        return stops

    # --- session hooks (sync, session lock held) ---

    def participant_gone(self, session_id: str, participant_id: str, reason: str) -> None:
        """``reason``: removed, left (it left by itself) or evicted (offline)."""
        for lch in self._launches.values():
            if (
                lch.session_id == session_id
                and lch.participant_id == participant_id
                and lch.status == "joined"
            ):
                self._finish(lch, "stopped", reason=reason)
                self._send_stop(lch, reason)
                self._account_event(lch, "agent.stopped", reason=reason)

    def session_ended(self, session_id: str, reason: str) -> None:
        # Records stay (final) until pruned, so a reconnecting launcher that
        # still reports one is told session_ended.
        for lch in list(self._launches.values()):
            if lch.session_id == session_id and lch.status in _ACTIVE:
                self._finish(lch, "stopped", reason="session_ended")
                self._send_stop(lch, "session_ended")
                self._account_event(lch, "agent.stopped", reason="session_ended")

    # --- internals ---

    def _on_timeout(self, launch_id: str) -> None:
        lch = self._launches.get(launch_id)
        if lch is None or lch.status not in ("pending", "started"):
            return
        if lch.joining:  # a join is in progress: look again shortly
            lch.timer = asyncio.get_running_loop().call_later(5.0, self._on_timeout, launch_id)
            return
        self._fail(lch, "join_timeout")
        self._send_stop(lch, "join_timeout")

    def _fail(self, lch: Launch, error: str) -> None:
        """Mark a not-yet-joined launch failed and tell the coordinator."""
        self._finish(lch, "failed", error=error)
        self._account_event(lch, "agent.stopped", reason=error)
        self._spawn(self._notify_failed(lch))

    async def _notify_failed(self, lch: Launch) -> None:
        sess = self._deps.registry.get(lch.session_id)
        if sess is None:
            return
        recipients = {p for p in (lch.launched_by, sess.coordinator_id) if p is not None}
        if not recipients:
            return
        try:
            await sess.emit_event(
                "agent.launch_failed", {"launch_id": lch.launch_id, "error": lch.error},
                recipients,
            )
        except Exception:
            log.exception("could not deliver agent.launch_failed for %s", lch.launch_id)

    def _finish(
        self, lch: Launch, status: str, *, error: str | None = None, reason: str | None = None
    ) -> None:
        lch.status = status
        lch.error = error
        lch.reason = reason
        lch.ended_at = time.monotonic()
        self._cancel_timer(lch)
        self._release_slot(lch)
        sess = self._deps.registry.get(lch.session_id)
        if sess is not None:
            self._drop_credential(lch, sess)
        else:
            lch.token_id = None  # the session left RAM; its credentials went with it

    def _release_slot(self, lch: Launch) -> None:
        if not lch.slot_held:
            return
        lch.slot_held = False
        self._launchers.release_slot(lch.launcher_id, lch.profile_id)

    @staticmethod
    def _cancel_timer(lch: Launch) -> None:
        for name in ("timer", "lost_timer"):
            handle = getattr(lch, name)
            if handle is not None:
                handle.cancel()
                setattr(lch, name, None)

    @staticmethod
    def _drop_credential(lch: Launch, sess: Session) -> None:
        if lch.token_id is not None:
            sess.auth.invalidate_launch(lch.token_id)
            lch.token_id = None

    def _send_stop(self, lch: Launch, reason: str) -> None:
        conn = self._launchers.get_conn(lch.launcher_id)
        if conn is None:
            return
        msg = {"type": "stop", "launch_id": lch.launch_id, "reason": reason}

        async def _send() -> None:
            try:
                await conn.send_json(msg)
            except Exception:
                log.info("could not send stop for %s: launcher gone", lch.launch_id)

        self._spawn(_send())

    def _account_event(self, lch: Launch, kind: str, **extra: Any) -> None:
        if not lch.accounts or self._deps.events is None:
            return
        from sessionweave.tools import emit_account_event

        data = lch.event_data(**extra)

        async def _emit() -> None:
            for account_id in lch.accounts:
                await emit_account_event(self._deps, account_id, kind, data)

        self._spawn(_emit())

    def _spawn(self, coro: Any) -> None:
        """Run ``coro`` in the background; its failure is logged, never lost."""

        async def _guarded() -> None:
            try:
                await coro
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("launch background task failed")

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:  # pragma: no cover - no running loop
            coro.close()
            return
        task = loop.create_task(_guarded())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    def _prune(self) -> None:
        cutoff = time.monotonic() - _KEEP_FINAL_S
        for lid in [
            lid for lid, lch in self._launches.items()
            if lch.ended_at is not None and lch.ended_at < cutoff
        ]:
            del self._launches[lid]
