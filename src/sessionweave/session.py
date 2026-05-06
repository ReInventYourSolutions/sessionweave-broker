from __future__ import annotations

import asyncio
import json
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionHandle, ConnectionManager
from sessionweave.dispatcher import RoundRobinPicker, decide_dispatch
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.models import CommandTracker, Participant
from sessionweave.notifications import BufferOverflow, NotificationBuffer
from sessionweave.schemas import (
    DispatchMode,
    ParticipantPublic,
    SessionSendOutput,
    Target,
)

_UNSET: Any = object()


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


class Session:
    """Single-session state machine. All public methods acquire `self.lock`
    so per-session operations are linearizable.

    Tasks 10-13 add membership, command, resume, cancellation methods.
    """

    __slots__ = (
        "_buffers",
        "_connections",
        "_created_at",
        "_inflight",
        "_last_activity_at",
        "_participants",
        "_picker",
        "config",
        "lock",
        "session_id",
    )

    def __init__(
        self,
        session_id: str,
        config: SessionConfig,
        connections: ConnectionManager,
    ) -> None:
        now = _utcnow()
        self.session_id = session_id
        self.config = config
        self._participants: dict[str, Participant] = {}
        self._inflight: dict[str, CommandTracker] = {}
        self._buffers: dict[str, NotificationBuffer] = {}
        self._picker = RoundRobinPicker()
        self._last_activity_at: datetime = now
        self._created_at: datetime = now
        self.lock = asyncio.Lock()
        self._connections = connections

    # --- read-only helpers ---

    def expires_at(self, *, at: datetime | None = None) -> datetime:
        return self._last_activity_at + timedelta(seconds=self.config.idle_ttl_seconds)

    def is_idle_expired(self, *, at: datetime | None = None) -> bool:
        now = at or _utcnow()
        return now >= self.expires_at()

    def roster_snapshot(self) -> list[ParticipantPublic]:
        return [p.to_public() for p in self._participants.values()]

    def expires_at_iso(self) -> str:
        return _isoformat(self.expires_at())

    # --- activity tracking ---

    def bump_activity(self, *, at: datetime | None = None) -> None:
        self._last_activity_at = at or _utcnow()

    # --- membership operations ---

    async def join(
        self,
        handle: ConnectionHandle,
        *,
        role: str | None,
        objective: str | None = None,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> Participant:
        async with self.lock:
            if len(self._participants) >= self.config.max_participants:
                raise ToolError(ErrorCode.SESSION_FULL, "max participants reached")
            p = Participant.create(
                role=role,
                objective=objective,
                capabilities=capabilities or [],
                metadata=metadata or {},
            )
            p.online = True
            p.connection_ref = handle
            self._connections.bind(handle, self.session_id, p.participant_id)
            self._participants[p.participant_id] = p
            self.bump_activity()
            self._notify_others(
                exclude_pid=p.participant_id,
                name="participant.joined",
                payload={"participant": p.to_public().model_dump()},
            )
            return p

    async def leave(self, participant_id: str) -> None:
        async with self.lock:
            p = self._participants.pop(participant_id, None)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            handle = p.connection_ref
            if handle is not None:
                # handle is a ConnectionHandle (set by join/resume)
                self._connections.unbind(handle)  # type: ignore[arg-type]
            self._buffers.pop(participant_id, None)
            self.bump_activity()
            self._cancel_inflight_for_sender(participant_id, reason="sender_gone")
            self._notify_others(
                exclude_pid=participant_id,
                name="participant.left",
                payload={"participant_id": participant_id, "reason": "left"},
            )

    async def update_self(
        self,
        participant_id: str,
        *,
        role: Any = _UNSET,
        objective: Any = _UNSET,
        capabilities: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> ParticipantPublic:
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            if role is not _UNSET:
                p.role = role
            if objective is not _UNSET:
                p.objective = objective
            if capabilities is not None:
                p.capabilities = list(capabilities)
            if metadata is not None:
                p.metadata = dict(metadata)
            self.bump_activity()
            self._notify_others(
                exclude_pid=participant_id,
                name="participant.updated",
                payload={"participant": p.to_public().model_dump()},
            )
            return p.to_public()

    # --- command lifecycle ---

    async def send(
        self,
        *,
        sender_id: str,
        dispatch: DispatchMode,
        target: Target,
        payload: dict[str, Any],
        timeout_seconds: int | None = None,
    ) -> SessionSendOutput:
        async with self.lock:
            if sender_id not in self._participants:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "sender not in session")
            self._enforce_payload_size(payload)
            decision = decide_dispatch(
                roster=self._participants,
                sender_id=sender_id,
                dispatch=dispatch,
                target=target,
                picker=self._picker,
            )
            ttl = self._clamp_timeout(timeout_seconds)
            now = _utcnow()
            command_id = str(uuid.uuid4())
            tracker = CommandTracker(
                command_id=command_id,
                sender_id=sender_id,
                dispatch_mode=dispatch,
                dispatched_to=list(decision.recipients),
                created_at=now,
                deadline=now + timedelta(seconds=ttl),
            )
            self._inflight[command_id] = tracker
            self.bump_activity()
            for recipient_id in decision.recipients:
                self._deliver_or_buffer(
                    recipient_id,
                    "command.received",
                    {
                        "command_id": command_id,
                        "sender_id": sender_id,
                        "dispatch_mode": dispatch.value,
                        "dispatched_to": list(decision.recipients),
                        "payload": payload,
                        "expires_at": _isoformat(tracker.deadline),
                    },
                )
            # If no recipients (e.g., any with no online match), close immediately
            if not decision.recipients:
                self._deliver_or_buffer(
                    sender_id,
                    "command.completed",
                    {
                        "command_id": command_id,
                        "received_count": 0,
                        "expected_count": 0,
                        "reason": "all_responded",
                    },
                )
                self._inflight.pop(command_id, None)
            return SessionSendOutput(command_id=command_id, delivered_to=list(decision.recipients))

    async def respond(
        self,
        *,
        responder_id: str,
        command_id: str,
        status: str,
        payload: dict[str, Any],
    ) -> None:
        async with self.lock:
            tracker = self._inflight.get(command_id)
            if tracker is None or tracker.is_expired():
                raise ToolError(ErrorCode.COMMAND_NOT_FOUND, "command unknown or expired")
            if responder_id not in tracker.dispatched_to:
                raise ToolError(ErrorCode.NOT_A_RECIPIENT, "not a recipient of this command")
            if responder_id in tracker.responses_received:
                raise ToolError(ErrorCode.ALREADY_RESPONDED, "already responded")
            tracker.responses_received.add(responder_id)
            self.bump_activity()
            self._deliver_or_buffer(
                tracker.sender_id,
                "result.received",
                {
                    "command_id": command_id,
                    "responder_id": responder_id,
                    "status": status,
                    "payload": payload,
                },
            )
            if tracker.is_complete():
                self._deliver_or_buffer(
                    tracker.sender_id,
                    "command.completed",
                    {
                        "command_id": command_id,
                        "received_count": tracker.received_count,
                        "expected_count": tracker.expected_count,
                        "reason": "all_responded",
                    },
                )
                # Keep the tracker in _inflight until TTL so that a late
                # duplicate respond raises ALREADY_RESPONDED rather than
                # COMMAND_NOT_FOUND.  TTL-based eviction is handled in Task 13.

    def _enforce_payload_size(self, payload: dict[str, Any]) -> None:
        size = len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
        if size > self.config.max_payload_bytes:
            raise ToolError(
                ErrorCode.PAYLOAD_TOO_LARGE,
                f"payload {size} > max {self.config.max_payload_bytes}",
            )

    def _clamp_timeout(self, requested: int | None) -> int:
        if requested is None:
            return self.config.default_command_timeout_seconds
        if requested < 1:
            return 1
        if requested > self.config.max_command_timeout_seconds:
            return self.config.max_command_timeout_seconds
        return requested

    def _deliver_or_buffer(self, pid: str, name: str, payload: dict[str, Any]) -> None:
        p = self._participants.get(pid)
        if p is None:
            return
        if p.online:
            self._connections.send(self.session_id, pid, name, payload)
            return
        try:
            buf = self._buffer_for(pid)
            buf.enqueue({"_name": name, "_payload": payload})
        except BufferOverflow:
            # Cap exceeded — escalate to eviction. Implemented in Task 13.
            self._enqueue_eviction(pid, reason="offline_timeout")

    def _buffer_for(self, pid: str) -> NotificationBuffer:
        buf = self._buffers.get(pid)
        if buf is None:
            buf = NotificationBuffer(self.config.max_pending_notifications_per_participant)
            self._buffers[pid] = buf
        return buf

    def _enqueue_eviction(self, pid: str, reason: str) -> None:
        # Called by _deliver_or_buffer when buffer cap is exceeded.
        self._evict(pid, reason=reason)

    def _cancel_inflight_for_sender(self, sender_id: str, reason: str) -> None:
        to_close: list[str] = []
        for cid, t in self._inflight.items():
            if t.sender_id == sender_id:
                for pending_pid in t.pending_recipients():
                    self._deliver_or_buffer(
                        pending_pid,
                        "command.cancelled",
                        {"command_id": cid, "reason": reason},
                    )
                to_close.append(cid)
        for cid in to_close:
            self._inflight.pop(cid, None)

    def _evict(self, pid: str, reason: str) -> None:
        p = self._participants.pop(pid, None)
        if p is None:
            return
        self._buffers.pop(pid, None)
        self._cancel_inflight_for_sender(pid, reason="sender_gone")
        self._notify_others(
            exclude_pid=pid,
            name="participant.left",
            payload={"participant_id": pid, "reason": reason},
        )

    def tick(self, *, now: datetime | None = None) -> None:
        """Periodic housekeeping: close expired trackers and evict
        offline-past-grace participants. Called by the registry's loop or
        directly in tests. Synchronous; assumes single-threaded scheduling.
        Must NOT be called while a tool call holds `self.lock` mid-await.
        """
        moment = now or _utcnow()
        # 1. Expired trackers
        expired_cids = [cid for cid, t in self._inflight.items() if t.is_expired(moment)]
        for cid in expired_cids:
            t = self._inflight.pop(cid)
            self._deliver_or_buffer(
                t.sender_id,
                "command.completed",
                {
                    "command_id": cid,
                    "received_count": t.received_count,
                    "expected_count": t.expected_count,
                    "reason": "ttl_expired",
                },
            )
            for pending_pid in t.pending_recipients():
                self._deliver_or_buffer(
                    pending_pid,
                    "command.cancelled",
                    {"command_id": cid, "reason": "ttl_expired"},
                )
        # 2. Offline-grace eviction
        evicted: list[str] = []
        for pid, p in self._participants.items():
            if p.online:
                continue
            if (moment - p.last_online_at).total_seconds() >= self.config.offline_grace_seconds:
                evicted.append(pid)
        for pid in evicted:
            self._evict(pid, reason="offline_timeout")

    def broadcast_session_ended(self, *, reason: str) -> None:
        for pid, p in list(self._participants.items()):
            if p.online:
                self._connections.send(
                    self.session_id, pid, "session.ended", {"reason": reason}
                )
                handle = p.connection_ref
                if handle is not None:
                    self._connections.unbind(handle)  # type: ignore[arg-type]
            self._buffers.pop(pid, None)
        self._participants.clear()
        self._inflight.clear()

    def handle_disconnect(self, handle: ConnectionHandle) -> None:
        """Mark a participant offline and notify others.

        Called synchronously from MCP transport callbacks (within the asyncio
        event loop). Does NOT acquire ``self.lock`` because it is invoked only
        when no Session coroutine is mid-await (cooperative multitasking
        guarantee). If callers cannot guarantee this, make this ``async`` and
        acquire the lock instead.
        """
        binding = self._connections.session_for(handle)
        if binding is None or binding[0] != self.session_id:
            return
        pid = binding[1]
        p = self._participants.get(pid)
        if p is None:
            return
        p.online = False
        p.last_online_at = _utcnow()
        p.connection_ref = None
        self._connections.unbind(handle)
        self._notify_others(
            exclude_pid=pid,
            name="participant.updated",
            payload={"participant": p.to_public().model_dump()},
        )

    async def resume(self, handle: ConnectionHandle, participant_id: str) -> None:
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                raise ToolError(ErrorCode.PARTICIPANT_NOT_FOUND, "no such participant in session")
            was_offline = not p.online
            self._connections.bind(handle, self.session_id, participant_id)
            p.online = True
            p.connection_ref = handle
            p.last_online_at = _utcnow()
            self.bump_activity()
            buf = self._buffers.pop(participant_id, None)
            if buf is not None:
                for item in buf.drain():
                    self._connections.send(
                        self.session_id, participant_id, item["_name"], item["_payload"]
                    )
            if was_offline:
                self._notify_others(
                    exclude_pid=participant_id,
                    name="participant.updated",
                    payload={"participant": p.to_public().model_dump()},
                )

    def _notify_others(
        self,
        *,
        exclude_pid: str,
        name: str,
        payload: dict[str, Any],
    ) -> None:
        for pid in list(self._participants.keys()):
            if pid == exclude_pid:
                continue
            self._deliver_or_buffer(pid, name, payload)
