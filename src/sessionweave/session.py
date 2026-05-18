from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from sessionweave.connections import ConnectionHandle
    from sessionweave.persistence import PersistenceStore, SessionSnapshot

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionHandle, ConnectionManager
from sessionweave.dispatcher import RoundRobinPicker, decide_dispatch
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.firehose import FirehoseRegistry
from sessionweave.metrics import Metrics
from sessionweave.models import CommandTracker, Participant
from sessionweave.notifications import BufferOverflow, NotificationBuffer
from sessionweave.schemas import (
    DispatchMode,
    ParticipantPublic,
    SessionSendOutput,
    Target,
)

_UNSET: Any = object()
MAX_PURPOSE_BYTES = 16 * 1024  # 16 KiB JSON cap for the session purpose blob
_MAX_PURPOSE_BYTES = MAX_PURPOSE_BYTES  # internal alias used in this module


def _utcnow() -> datetime:
    return datetime.now(UTC)


def _isoformat(dt: datetime) -> str:
    return dt.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _payload_size_bytes(payload: dict[str, Any]) -> int:
    """Byte size of a buffer item's ``payload`` field — same metric
    used by ``Session._enforce_payload_size`` at ingress. Keeping
    ingress and bookkeeping consistent means a payload that fits
    ``max_payload_bytes`` contributes exactly that many bytes to
    ``Session._buffered_bytes``.
    """
    return len(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def _passes_ack_filter(item_seq: int, ack_watermark: int) -> bool:
    """True if a buffered item with ``item_seq`` should be delivered
    given the participant's effective ``ack_watermark``.

    Items with ``seq == 0`` are legacy / unsequenced (e.g. handed back
    via ``requeue_to_buffer`` from a pre-seq transport adapter) and
    always pass through — they predate the dedup protocol and a client
    has no way to acknowledge them. Sequenced items are delivered iff
    ``item_seq > ack_watermark``.
    """
    if item_seq <= 0:
        return True
    return item_seq > ack_watermark


@dataclass(slots=True)
class WaitForEventResult:
    """Return value of Session.wait_for_event."""

    messages: list[dict[str, Any]]
    timed_out: bool
    effective_timeout_seconds: float = 0.0


class Session:
    """Single-session state machine. All public methods acquire `self.lock`
    so per-session operations are linearizable.

    Tasks 10-13 add membership, command, resume, cancellation methods.
    """

    __slots__ = (
        "_active_waits",
        "_buffered_bytes",
        "_buffers",
        "_connections",
        "_created_at",
        "_event_seq",
        "_firehose",
        "_firehose_tasks",
        "_inflight",
        "_last_activity_at",
        "_metrics",
        "_on_deadline_shortened",
        "_participants",
        "_persistence_mode",
        "_picker",
        "_store",
        "_subscribers",
        "_wait_in_flight",
        "config",
        "creator_token_hash",
        "lock",
        "purpose",
        "session_id",
    )

    def __init__(
        self,
        session_id: str,
        config: SessionConfig,
        connections: ConnectionManager,
        metrics: Metrics | None = None,
        purpose: dict[str, Any] | None = None,
        on_deadline_shortened: Callable[[Session], None] | None = None,
        firehose: FirehoseRegistry | None = None,
        store: PersistenceStore | None = None,
        persistence_mode: str | None = None,
    ) -> None:
        now = _utcnow()
        self.session_id = session_id
        self.config = config
        self._participants: dict[str, Participant] = {}
        self._inflight: dict[str, CommandTracker] = {}
        self._buffers: dict[str, NotificationBuffer] = {}
        # Running total of buffered notification bytes across ALL
        # participants in this session. Used to enforce
        # ``config.max_buffer_bytes_per_session``. We size each buffered
        # event by its JSON-encoded ``payload`` byte length (the same
        # measure ``_enforce_payload_size`` checks at ingress, so the
        # accounting is consistent end-to-end).
        self._buffered_bytes: int = 0
        self._wait_in_flight: set[str] = set()
        self._active_waits: int = 0
        # Per-participant push subscribers (e.g. WebSocket events handlers).
        # A subscriber is a sync callback ``(name: str, payload: dict) ->
        # None`` invoked from inside ``_deliver_or_buffer`` while ``self.lock``
        # is held — callbacks must not block on the lock or do heavy work.
        # Typical implementation: ``queue.put_nowait`` on an asyncio queue.
        self._subscribers: dict[
            str, list[Callable[[str, dict[str, Any], int], None]]
        ] = {}
        self._picker = RoundRobinPicker()
        self._last_activity_at: datetime = now
        self._created_at: datetime = now
        self.lock = asyncio.Lock()
        self._connections = connections
        self._metrics = metrics
        self.purpose: dict[str, Any] | None = purpose
        # Notify the registry whenever an internal event shortens
        # ``next_deadline`` so its min-heap doesn't sit on a stale,
        # later deadline. The lazy-invalidation path already absorbs
        # later-than-stored deadlines (we re-push), but a SHORTER
        # actual deadline never fires unless the registry sees it.
        self._on_deadline_shortened = on_deadline_shortened
        self._firehose = firehose
        self._firehose_tasks: set[asyncio.Task[None]] = set()
        self._store = store
        self._persistence_mode = persistence_mode
        self._event_seq: int = 0
        self.creator_token_hash: bytes | None = None

    # --- hydration ---

    @classmethod
    def from_snapshot(
        cls,
        snap: SessionSnapshot,
        *,
        config: SessionConfig,
        connections: ConnectionManager,
        store: PersistenceStore | None,
        metrics: Metrics | None,
        firehose: FirehoseRegistry | None,
        on_deadline_shortened: Callable[[Session], None] | None,
    ) -> Session:
        import json as _json
        from datetime import UTC, datetime

        from sessionweave.models import CommandTracker, Participant
        from sessionweave.notifications import NotificationBuffer

        purpose = _json.loads(snap.purpose_json) if snap.purpose_json else None
        sess = cls(
            session_id=snap.session_id,
            config=config,
            connections=connections,
            metrics=metrics,
            purpose=purpose,
            on_deadline_shortened=on_deadline_shortened,
            firehose=firehose,
            store=store,
            persistence_mode=snap.persistence_mode,
        )
        sess.creator_token_hash = snap.creator_token_hash
        # Participants (only active — left_at NULL)
        for p in snap.participants:
            s = p["snapshot"]
            joined_at = datetime.fromtimestamp(p["joined_at"] / 1000, tz=UTC)
            sess._participants[p["participant_id"]] = Participant(
                participant_id=p["participant_id"],
                role=s.get("role"),
                objective=s.get("objective"),
                capabilities=list(s.get("capabilities", ())),
                metadata=dict(s.get("metadata", {})),
                joined_at=joined_at,
                online=False,
                delivery_mode=s.get("delivery_mode", "push"),
                next_seq=p.get("last_seen_seq", 0),
                last_acked_seq=p.get("last_seen_seq", 0),
            )
        # Inflight
        for cid, tracker_d in snap.inflight.items():
            sess._inflight[cid] = CommandTracker.from_snapshot(tracker_d)
        # Buffers + byte accounting
        cap = sess.config.max_pending_notifications_per_participant
        for entry in snap.buffered:
            pid = entry["participant_id"]
            buf = sess._buffers.setdefault(pid, NotificationBuffer(cap=cap))
            payload_bytes = len(
                _json.dumps(entry["payload"], separators=(",", ":")).encode()
            )
            buf.enqueue({
                "_name": entry["kind"],
                "_payload": entry["payload"],
                "_seq": entry["seq"],
                "_size": payload_bytes,
                "_ts": 0,
            })
            sess._buffered_bytes += payload_bytes
        # Restore last_activity_at and per-session event seq
        sess._last_activity_at = datetime.fromtimestamp(
            snap.last_activity_at / 1000, tz=UTC)
        # Resume ``_event_seq`` so the next dispatched event picks up
        # right after the highest seq already on disk. ``snap.max_event_seq``
        # is sourced from MAX(seq) on the `events` table and is correct
        # even when ``snap.buffered`` is empty — which is the common case
        # for push-only sessions whose subscribers all stayed caught up
        # before going cold. Falling back to ``max(buffered.seq)`` would
        # leave ``_event_seq`` at 0 here and cause the next dispatch to
        # collide with an existing (session_id, seq) PK on `events`.
        sess._event_seq = max(
            snap.max_event_seq,
            max((entry["seq"] for entry in snap.buffered), default=0),
        )
        return sess

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

    def next_deadline(self) -> datetime:
        """Soonest moment when ``tick`` could need to do work for this
        session. The registry uses this to drive a min-heap so it can
        skip sessions whose next event is far in the future, instead of
        scanning every entry every tick.

        Includes:
          * idle TTL expiry
          * any inflight command tracker's TTL deadline
          * the offline-grace deadline of any offline participant

        The returned datetime is always timezone-aware (UTC).
        """
        candidates: list[datetime] = [self.expires_at()]
        for tracker in self._inflight.values():
            candidates.append(tracker.deadline)
        grace = timedelta(seconds=self.config.offline_grace_seconds)
        for p in self._participants.values():
            if not p.online and p.last_online_at is not None:
                candidates.append(p.last_online_at + grace)
        return min(candidates)

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
        delivery_mode: str = "push",
    ) -> Participant:
        async with self.lock:
            if len(self._participants) >= self.config.max_participants:
                raise ToolError(ErrorCode.SESSION_FULL, "max participants reached")
            p = Participant.create(
                role=role,
                objective=objective,
                capabilities=capabilities or [],
                metadata=metadata or {},
                delivery_mode=delivery_mode,
            )
            p.online = True
            p.connection_ref = handle
            self._connections.bind(handle, self.session_id, p.participant_id)
            self._participants[p.participant_id] = p
            self.bump_activity()
            if self._metrics is not None:
                self._metrics.participants_active.labels(online="true").inc()
            if self._store is not None:
                snapshot = {
                    "role": p.role,
                    "objective": p.objective,
                    "capabilities": list(p.capabilities),
                    "metadata": dict(p.metadata),
                    "delivery_mode": p.delivery_mode,
                    "online": p.online,
                }
                joined_at_ms = int(p.joined_at.timestamp() * 1000)
                await self._store.upsert_participant(
                    self.session_id, p.participant_id, snapshot,
                    last_seen_seq=0, joined_at=joined_at_ms,
                )
            joined_payload = {"participant": p.to_public().model_dump()}
            others = [pid for pid in self._participants if pid != p.participant_id]
            await self._dispatch_event(
                kind="participant.joined",
                payload=joined_payload,
                recipients=others,
            )
            self._mirror_firehose("participant.joined", joined_payload)
            return p

    async def leave(
        self,
        participant_id: str,
        *,
        feedback: dict[str, Any] | None = None,
    ) -> None:
        """Remove a participant from the session.

        ``feedback`` is an optional structured exit report — the
        skill's recommended convention is ``{"summary": "...",
        "issues": [...], "rating": 1..5}`` but the relay only enforces
        a 4 KiB JSON cap. If supplied, it is propagated on the
        ``participant.left`` event so other participants (and any
        observers driving the host) see how the session went from this
        client's perspective. Sub-project #2 (persistence) will
        additionally archive feedback for post-mortem analysis; in #1
        it lives only in the in-memory event stream.
        """
        if feedback is not None:
            self._enforce_feedback_size(feedback)
        async with self.lock:
            p = self._participants.pop(participant_id, None)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            handle = p.connection_ref
            if handle is not None:
                # handle is a ConnectionHandle (set by join/resume)
                self._connections.unbind(handle)  # type: ignore[arg-type]
            self._discard_buffer(self._buffers.pop(participant_id, None))
            self.bump_activity()
            if self._metrics is not None:
                online_label = "true" if p.online else "false"
                self._metrics.participants_active.labels(online=online_label).dec()
            self._cancel_inflight_for_sender(participant_id, reason="sender_gone")
            # Wake any wait_for_event coroutine so it can raise PARTICIPANT_EVICTED.
            subs = self._subscribers.pop(participant_id, None)
            if subs:
                for cb in list(subs):
                    try:
                        cb("__evicted__", {}, 0)
                    except Exception:
                        pass
            payload: dict[str, Any] = {"participant_id": participant_id, "reason": "left"}
            if feedback is not None:
                payload["feedback"] = feedback
            if self._store is not None:
                left_at_ms = int(_utcnow().timestamp() * 1000)
                await self._store.soft_delete_participant(
                    self.session_id, participant_id, left_at=left_at_ms
                )
            others = list(self._participants.keys())
            await self._dispatch_event(
                kind="participant.left",
                payload=payload,
                recipients=others,
            )
            self._mirror_firehose("participant.left", payload)

    @staticmethod
    def _enforce_feedback_size(feedback: dict[str, Any]) -> None:
        size = len(json.dumps(feedback, separators=(",", ":")).encode("utf-8"))
        if size > 4 * 1024:
            raise ToolError(
                ErrorCode.PAYLOAD_TOO_LARGE,
                f"feedback {size} bytes exceeds 4 KiB cap",
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
            if self._store is not None:
                snapshot = {
                    "role": p.role,
                    "objective": p.objective,
                    "capabilities": list(p.capabilities),
                    "metadata": dict(p.metadata),
                    "delivery_mode": p.delivery_mode,
                    "online": p.online,
                }
                joined_at_ms = int(p.joined_at.timestamp() * 1000)
                await self._store.upsert_participant(
                    self.session_id, participant_id, snapshot,
                    last_seen_seq=p.last_acked_seq, joined_at=joined_at_ms,
                )
            updated_payload = {"participant": p.to_public().model_dump()}
            others = [pid for pid in self._participants if pid != participant_id]
            await self._dispatch_event(
                kind="participant.updated",
                payload=updated_payload,
                recipients=others,
            )
            self._mirror_firehose("participant.updated", updated_payload)
            return p.to_public()

    async def update_purpose(
        self,
        requester_id: str,
        new_purpose: dict[str, Any] | None,
    ) -> dict[str, Any] | None:
        """Replace ``self.purpose`` and notify the rest of the session.

        Any participant can update the purpose. Cap mirrors session_create
        (16 KiB JSON). On success emits ``session.purpose_updated`` to
        every other participant with the new payload.
        """
        async with self.lock:
            if requester_id not in self._participants:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            if new_purpose is not None:
                size = len(
                    json.dumps(new_purpose, separators=(",", ":")).encode("utf-8")
                )
                if size > _MAX_PURPOSE_BYTES:
                    raise ToolError(
                        ErrorCode.INVALID_CONFIG,
                        f"purpose {size} bytes exceeds cap of {_MAX_PURPOSE_BYTES}",
                    )
            self.purpose = new_purpose
            self.bump_activity()
            if self._store is not None:
                purpose_json = json.dumps(new_purpose) if new_purpose is not None else None
                await self._store.update_session_purpose(self.session_id, purpose_json)
            purpose_payload = {"purpose": new_purpose}
            others = [pid for pid in self._participants if pid != requester_id]
            await self._dispatch_event(
                kind="session.purpose_updated",
                payload=purpose_payload,
                recipients=others,
            )
            self._mirror_firehose("session.purpose_updated", purpose_payload)
            return new_purpose

    async def check_inbox(
        self,
        participant_id: str,
        *,
        last_acked_seq: int | None = None,
    ) -> list[dict[str, Any]]:
        """Drain and return any buffered notifications for ``participant_id``.

        Used by ``delivery_mode=pull`` clients (and any client that wants to
        flush the offline buffer mid-session). Returns each entry as
        ``{"name": <event>, "payload": {...}, "seq": <int>}``.

        ``last_acked_seq`` (optional): if provided, only buffered events
        with ``seq > last_acked_seq`` are returned. Merged with the
        participant's authoritative ``last_acked_seq`` watermark via
        ``max(...)`` so a prior ACK on the WS channel is not regressed
        by a weaker caller-supplied value, and clients that ACK
        promptly can omit the parameter entirely.
        """
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            self.bump_activity()
            effective_ack = max(last_acked_seq or 0, p.last_acked_seq)
            buf = self._buffers.pop(participant_id, None)
            if buf is None:
                return []
            drained = self._account_drained(
                self._filter_stale_buffer_items(buf.drain())
            )
            return [
                {
                    "name": item["_name"],
                    "payload": item["_payload"],
                    "seq": item.get("_seq", 0),
                }
                for item in drained
                if _passes_ack_filter(item.get("_seq", 0), effective_ack)
            ]

    async def wait_for_event(
        self,
        participant_id: str,
        names: list[str] | None,
        timeout_seconds: float,
    ) -> WaitForEventResult:
        """Block until a matching event reaches the caller's inbox.

        Drains the buffer atomically; non-matching events are
        re-buffered. Pull-mode only; one wait in flight per
        participant. See spec 2026-05-16-session-wait-for-event.
        """
        match_set = frozenset(names) if names else None

        def is_match(evt_name: str) -> bool:
            return match_set is None or evt_name in match_set

        # Bounded queue between the synchronous subscriber callback
        # and this coroutine. Overflow falls through to the buffer
        # via attach_subscriber's existing fallback contract.
        queue: asyncio.Queue[tuple[str, dict[str, Any], int]] = asyncio.Queue(
            maxsize=64
        )

        def on_event(name: str, payload: dict[str, Any], seq: int) -> None:
            try:
                queue.put_nowait((name, payload, seq))
            except asyncio.QueueFull:
                # Let attach_subscriber's contract route to the buffer.
                raise

        # Phase 1: validate under lock and snapshot config-derived
        # values. The lock is released before calling attach/detach
        # because those acquire it themselves.
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                raise ToolError(
                    ErrorCode.NOT_IN_SESSION, "participant not in session"
                )
            if p.delivery_mode != "pull":
                raise ToolError(
                    ErrorCode.INVALID_DELIVERY_MODE,
                    "wait_for_event requires delivery_mode=pull",
                )
            if participant_id in self._wait_in_flight:
                raise ToolError(
                    ErrorCode.WAIT_ALREADY_IN_FLIGHT,
                    "another wait_for_event is already in flight",
                )
            if self._active_waits >= self.config.max_concurrent_waits_total:
                raise ToolError(
                    ErrorCode.TOO_MANY_WAITS,
                    f"per-session concurrent-wait cap "
                    f"({self.config.max_concurrent_waits_total}) reached",
                )
            self._active_waits += 1
            self._wait_in_flight.add(participant_id)
            self.bump_activity()
            # Effective timeout is the minimum of three terms:
            #  1. caller-requested value
            #  2. max_wait_seconds (relay config cap)
            #  3. remaining session TTL — never block past the moment the
            #     session would idle-expire, since the wait itself doesn't
            #     count as session activity beyond the bump_activity above
            #     and stale wait_for_event calls would just race the eviction.
            remaining_ttl = max(
                0.0, (self.expires_at() - _utcnow()).total_seconds()
            )
            effective_timeout = min(
                float(timeout_seconds),
                float(self.config.max_wait_seconds),
                remaining_ttl,
            )

        loop = asyncio.get_running_loop()
        deadline = loop.time() + effective_timeout

        if self._metrics is not None:
            self._metrics.wait_for_event_active.inc()
        start_time = loop.time()
        outcome = "error"
        try:
            subscriber_attached = False
            try:
                # Phase 2: attach subscriber. attach_subscriber atomically
                # drains the current buffer and registers the callback —
                # no event arriving between the drain and the registration
                # can be lost.
                backlog = await self.attach_subscriber(participant_id, on_event)
                subscriber_attached = True

                matching: list[dict[str, Any]] = []
                non_matching: list[dict[str, Any]] = []
                for entry in backlog:
                    (matching if is_match(entry["name"]) else non_matching).append(entry)

                # Re-buffer non-matching backlog events so a later
                # check_inbox / wait_for_event still sees them.
                # Use requeue_to_buffer (not _deliver_or_buffer) to bypass
                # the subscriber — calling _deliver_or_buffer here would
                # route the event back through on_event → queue, causing
                # an infinite spin loop on non-matching events.
                if non_matching:
                    await self.requeue_to_buffer(
                        participant_id,
                        [
                            (entry["name"], entry["payload"], entry.get("seq", 0))
                            for entry in non_matching
                        ],
                    )

                if matching:
                    outcome = "event"
                    return WaitForEventResult(
                        messages=matching,
                        timed_out=False,
                        effective_timeout_seconds=effective_timeout,
                    )

                # Phase 3: block until queue delivers an event or budget expires.
                def check_sentinel(name: str) -> None:
                    if name == "__evicted__":
                        raise ToolError(
                            ErrorCode.PARTICIPANT_EVICTED,
                            "participant left during wait",
                        )

                while True:
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        outcome = "timeout"
                        return WaitForEventResult(
                            messages=[],
                            timed_out=True,
                            effective_timeout_seconds=effective_timeout,
                        )
                    try:
                        name, payload, seq = await asyncio.wait_for(
                            queue.get(), timeout=remaining
                        )
                    except TimeoutError:
                        outcome = "timeout"
                        return WaitForEventResult(
                            messages=[],
                            timed_out=True,
                            effective_timeout_seconds=effective_timeout,
                        )
                    check_sentinel(name)

                    if is_match(name):
                        matching.append({"name": name, "payload": payload, "seq": seq})
                    else:
                        await self.requeue_to_buffer(
                            participant_id, [(name, payload, seq)]
                        )

                    while True:
                        try:
                            name, payload, seq = queue.get_nowait()
                        except asyncio.QueueEmpty:
                            break
                        check_sentinel(name)
                        if is_match(name):
                            matching.append({"name": name, "payload": payload, "seq": seq})
                        else:
                            await self.requeue_to_buffer(
                                participant_id, [(name, payload, seq)]
                            )

                    if matching:
                        outcome = "event"
                        return WaitForEventResult(
                        messages=matching,
                        timed_out=False,
                        effective_timeout_seconds=effective_timeout,
                    )
                    # else: spurious wake on non-matching event(s), keep
                    # waiting on the same wall-clock budget.
            except ToolError as exc:
                if exc.code is ErrorCode.PARTICIPANT_EVICTED:
                    outcome = "evicted"
                else:
                    outcome = "error"
                raise
            except asyncio.CancelledError:
                outcome = "cancelled"
                raise
            finally:
                if subscriber_attached:
                    await self.detach_subscriber(participant_id, on_event)
        finally:
            if self._metrics is not None:
                duration = loop.time() - start_time
                self._metrics.wait_for_event_total.labels(outcome=outcome).inc()
                self._metrics.wait_for_event_duration_seconds.observe(duration)
                self._metrics.wait_for_event_active.dec()
            async with self.lock:
                self._wait_in_flight.discard(participant_id)
                self._active_waits = max(0, self._active_waits - 1)

    async def attach_subscriber(
        self,
        participant_id: str,
        callback: Callable[[str, dict[str, Any], int], None],
        *,
        last_acked_seq: int | None = None,
    ) -> list[dict[str, Any]]:
        """Atomically drain pending buffered notifications and register a
        live event callback. Returns the drained backlog as a list of
        ``{"name": ..., "payload": ...}`` entries — the caller forwards
        these over its transport before streaming subsequent live events.

        The callback is invoked synchronously from inside
        ``_deliver_or_buffer`` while ``self.lock`` is held. It must not
        block on the lock or do significant work; the typical pattern is
        ``queue.put_nowait`` on an ``asyncio.Queue``. If the callback
        raises (e.g. queue is full), the event falls through to the
        regular buffer so it isn't silently dropped.

        Side effect on ``online``: a long-lived event subscriber (e.g. the
        per-participant ``/events`` WebSocket) is by itself enough to
        consider the participant reachable, even when no traditional
        transport handle is bound. We flip ``online=True`` here when the
        participant is currently offline, broadcast
        ``participant.updated``, and rely on ``detach_subscriber`` to
        flip back when no subscribers + no alive handle remain. Without
        this, a WS-only client would be evicted by ``offline_timeout``
        after ``offline_grace_seconds`` even while actively receiving.
        """
        async with self.lock:
            if participant_id not in self._participants:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "participant not in session")
            p_for_ack = self._participants[participant_id]
            effective_ack = max(last_acked_seq or 0, p_for_ack.last_acked_seq)
            buf = self._buffers.pop(participant_id, None)
            if buf is not None:
                drained = self._account_drained(
                    self._filter_stale_buffer_items(buf.drain())
                )
            else:
                drained = []
            backlog: list[dict[str, Any]] = [
                {
                    "name": item["_name"],
                    "payload": item["_payload"],
                    "seq": item.get("_seq", 0),
                }
                for item in drained
                if _passes_ack_filter(item.get("_seq", 0), effective_ack)
            ]
            self._subscribers.setdefault(participant_id, []).append(callback)
            p = self._participants[participant_id]
            if not p.online:
                p.online = True
                p.last_online_at = _utcnow()
                if self._metrics is not None:
                    self._metrics.participants_active.labels(online="false").dec()
                    self._metrics.participants_active.labels(online="true").inc()
                self._notify_others(
                    exclude_pid=participant_id,
                    name="participant.updated",
                    payload={"participant": p.to_public().model_dump()},
                )
        return backlog

    async def detach_subscriber(
        self,
        participant_id: str,
        callback: Callable[[str, dict[str, Any], int], None],
    ) -> None:
        """Remove a previously-attached subscriber. Idempotent.

        Holds the session lock while removing the callback so any
        concurrent ``_deliver_or_buffer`` either sees the subscriber
        and routes the event to it (and the caller is expected to
        drain whatever queue the callback writes to via
        ``requeue_to_buffer``), or sees it gone and routes to the
        buffer. There is no in-between state where an event can vanish.

        Side effect on ``online``: mirrors ``attach_subscriber``. If
        removing this callback empties the subscriber list AND no live
        connection handle remains, the participant transitions back to
        offline (so the existing eviction loop can sweep them after the
        offline-grace window). If a regular MCP/REST handle is still
        bound, ``online`` is left alone — that transport's own lifecycle
        decides when to flip it.
        """
        async with self.lock:
            subs = self._subscribers.get(participant_id)
            if not subs:
                return
            try:
                subs.remove(callback)
            except ValueError:
                return
            if not subs:
                self._subscribers.pop(participant_id, None)
                p = self._participants.get(participant_id)
                if p is not None and p.online:
                    handle = p.connection_ref
                    # ``connection_ref`` is typed ``object | None`` because
                    # the dataclass keeps the handle opaque to avoid a
                    # circular import with connections.py; in practice it is
                    # always a ConnectionHandle, hence the cast.
                    handle_alive = bool(
                        handle is not None
                        and cast("ConnectionHandle", handle).is_alive()
                    )
                    if not handle_alive:
                        p.online = False
                        p.last_online_at = _utcnow()
                        if self._metrics is not None:
                            self._metrics.participants_active.labels(online="true").dec()
                            self._metrics.participants_active.labels(online="false").inc()
                        self._notify_others(
                            exclude_pid=participant_id,
                            name="participant.updated",
                            payload={"participant": p.to_public().model_dump()},
                        )

    async def requeue_to_buffer(
        self,
        participant_id: str,
        items: list[tuple[str, dict[str, Any], int]],
    ) -> None:
        """Push events that a subscriber had popped (or had handed to
        it) but couldn't actually deliver back onto the per-participant
        buffer, so a REST ``inbox/drain`` (or a fresh subscriber) can
        still recover them.

        Each item is ``(name, payload, seq)`` — the seq is the per-
        participant monotonic sequence number that was assigned when
        the event was originally fanned out from
        ``_deliver_or_buffer``. Preserving it here means a reconnecting
        client that has ACKed up to N can still skip duplicates after a
        flaky disconnect.

        Typical caller: a WebSocket handler that detached because the
        peer closed; anything left in its asyncio.Queue at that point
        was either popped-but-not-sent or arrived while detach was
        winning the session lock from a concurrent broadcast. Items
        are pushed in the order given (matching arrival order).
        """
        if not items:
            return
        async with self.lock:
            if participant_id not in self._participants:
                return
            buf = self._buffer_for(participant_id)
            for name, payload, seq in items:
                size = _payload_size_bytes(payload)
                try:
                    buf.enqueue(
                        {
                            "_name": name,
                            "_payload": payload,
                            "_seq": seq,
                            "_size": size,
                            "_ts": time.monotonic_ns(),
                        }
                    )
                except BufferOverflow:
                    # Buffer cap exceeded — the participant is clearly
                    # past their grace; let the regular eviction path
                    # catch them on the next tick.
                    self._enqueue_eviction(participant_id, reason="offline_timeout")
                    break
                else:
                    self._buffered_bytes += size
            self._enforce_session_byte_budget()

    # --- command lifecycle ---

    async def send(
        self,
        *,
        sender_id: str,
        dispatch: DispatchMode,
        target: Target,
        payload: dict[str, Any],
        timeout_seconds: int | None = None,
        command_id: str | None = None,
    ) -> SessionSendOutput:
        async with self.lock:
            if sender_id not in self._participants:
                raise ToolError(ErrorCode.NOT_IN_SESSION, "sender not in session")
            # Idempotent retry: a caller-supplied command_id that already
            # exists in this session's inflight set returns the original
            # dispatch's response (same delivered_to). No new event, no
            # new tracker. Closes the SDK retry-loop gap.
            if command_id is not None and command_id in self._inflight:
                existing = self._inflight[command_id]
                return SessionSendOutput(
                    command_id=command_id,
                    delivered_to=list(existing.dispatched_to),
                )
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
            if command_id is None:
                command_id = str(uuid.uuid4())
            tracker = CommandTracker(
                command_id=command_id,
                sender_id=sender_id,
                dispatch_mode=dispatch,
                dispatched_to=list(decision.recipients),
                created_at=now,
                deadline=now + timedelta(seconds=ttl),
            )
            # Register inflight FIRST so the registry's next_deadline
            # calculation (triggered by _on_deadline_shortened) reflects
            # the new tracker's TTL. If persist fails below, we roll back.
            self._inflight[command_id] = tracker
            # The new tracker's deadline is almost certainly sooner
            # than the session's prior next_deadline (which was
            # idle_ttl). Tell the registry so its min-heap fires the
            # cancellation tick on time instead of waiting for idle TTL.
            if self._on_deadline_shortened is not None:
                self._on_deadline_shortened(self)
            self.bump_activity()
            if self._metrics is not None:
                self._metrics.commands_total.labels(
                    dispatch_mode=dispatch.value
                ).inc()
            command_received_payload = {
                "command_id": command_id,
                "sender_id": sender_id,
                "dispatch_mode": dispatch.value,
                "dispatched_to": list(decision.recipients),
                "payload": payload,
                "expires_at": _isoformat(tracker.deadline),
            }
            if decision.recipients:
                try:
                    await self._dispatch_event(
                        kind="command.received",
                        payload=command_received_payload,
                        recipients=list(decision.recipients),
                        inflight_upsert=(command_id, tracker.to_snapshot()),
                    )
                except Exception:
                    # Persist failure → rollback inflight; observers won't
                    # see the event (DB ordering invariant), so no orphan.
                    self._inflight.pop(command_id, None)
                    raise
                self._mirror_firehose("command.received", command_received_payload)
            # If no recipients (e.g., any with no online match), close immediately
            if not decision.recipients:
                command_completed_payload = {
                    "command_id": command_id,
                    "received_count": 0,
                    "expected_count": 0,
                    "reason": "all_responded",
                }
                self._deliver_or_buffer(sender_id, "command.completed", command_completed_payload)
                self._mirror_firehose("command.completed", command_completed_payload)
                self._inflight.pop(command_id, None)
                if self._metrics is not None:
                    self._metrics.command_completed_total.labels(
                        reason="all_responded"
                    ).inc()
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
            self._enforce_payload_size(payload)
            tracker.responses_received.add(responder_id)
            self.bump_activity()
            result_payload = {
                "command_id": command_id,
                "responder_id": responder_id,
                "status": status,
                "payload": payload,
            }
            is_complete = tracker.is_complete()
            await self._dispatch_event(
                kind="result.received",
                payload=result_payload,
                recipients=[tracker.sender_id],
                inflight_complete=command_id if is_complete else None,
            )
            self._mirror_firehose("result.received", result_payload)
            if is_complete:
                completed_payload = {
                    "command_id": command_id,
                    "received_count": tracker.received_count,
                    "expected_count": tracker.expected_count,
                    "reason": "all_responded",
                }
                await self._dispatch_event(
                    kind="command.completed",
                    payload=completed_payload,
                    recipients=[tracker.sender_id],
                )
                self._mirror_firehose("command.completed", completed_payload)
                if self._metrics is not None:
                    self._metrics.command_completed_total.labels(
                        reason="all_responded"
                    ).inc()
                # Keep the tracker in _inflight until TTL so that a late
                # duplicate respond raises ALREADY_RESPONDED rather than
                # COMMAND_NOT_FOUND.  TTL-based eviction is handled in Task 13.

    def _is_command_stale(self, command_id: str | None) -> bool:
        """A buffered ``command.received`` is stale if its command is no
        longer dispatchable from the relay's perspective — either evicted
        from ``_inflight`` (responded fully / cancelled / cleaned up) or
        still present but already past its TTL deadline.

        Used to filter buffered events at drain time so a participant
        that resumes after a long offline window doesn't get handed a
        ``command.received`` it can't respond to anyway.
        """
        if command_id is None:
            return False
        tracker = self._inflight.get(command_id)
        if tracker is None:
            return True
        return tracker.is_expired()

    def _filter_stale_buffer_items(
        self, items: Iterable[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """Drop buffer entries representing ``command.received`` for an
        already-expired or gone command. Other event types pass through.

        Operates on raw buffer items (``{"_name": ..., "_payload": ...}``)
        so it can be plugged in at any drain site before the caller
        re-shapes the entries for transport.
        """
        out: list[dict[str, Any]] = []
        for item in items:
            if item.get("_name") == "command.received":
                payload = item.get("_payload") or {}
                if self._is_command_stale(payload.get("command_id")):
                    continue
            out.append(item)
        return out

    async def record_ack(self, participant_id: str, seq: int) -> None:
        """Advance the participant's authoritative ``last_acked_seq``
        and eagerly trim the per-participant buffer of events with
        ``_seq <= seq``.

        Idempotent and monotonic: callers can pass a non-increasing
        ``seq`` (e.g. an old WS connection retrying an ACK) and the
        watermark will not regress. No-op if the participant is gone.
        """
        if seq <= 0:
            return
        async with self.lock:
            p = self._participants.get(participant_id)
            if p is None:
                return
            if seq <= p.last_acked_seq:
                # Already at or past this watermark; nothing to do.
                return
            p.last_acked_seq = seq
            buf = self._buffers.get(participant_id)
            if buf is not None:
                # ``popleft_while`` is amortized O(1) per ACK because
                # ``_seq`` is monotonic and buffer items are in arrival
                # order — once we see an item with seq > ack we know
                # every later item also has seq > ack, so we stop.
                # ``pop_where`` did a full O(n) scan per ACK; under
                # reconnect bursts (1000s of ACK frames) that was
                # O(n^2) total and could stall the event loop. See #66.
                removed = buf.popleft_while(
                    lambda item: item.get("_seq", 0) <= seq
                )
                for item in removed:
                    self._buffered_bytes -= item.get("_size", 0)
                if self._buffered_bytes < 0:
                    self._buffered_bytes = 0

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

    def _mirror_firehose(self, name: str, payload: dict[str, Any]) -> None:
        """Schedule a non-blocking firehose publish for one logical event.

        Called once per logical event (not once per recipient). Uses
        ``asyncio.create_task`` so the firehose publish never blocks or
        stalls the participant delivery path even if subscribers are slow.
        A reference to each task is held in ``_firehose_tasks`` until it
        finishes, so the task is not garbage-collected mid-flight.

        Ordering invariant: every caller MUST run ``_deliver_or_buffer`` for
        all recipients BEFORE invoking ``_mirror_firehose``. Receiver inboxes
        are populated synchronously inside the session lock, so by the time
        the firehose task is scheduled the inbox state already reflects the
        event. Firehose subscribers (cockpits, dashboards) therefore never
        observe an event before the receiver could have observed it.
        """
        if self._firehose is not None:
            task = asyncio.create_task(
                self._firehose.publish(self.session_id, {"name": name, "payload": payload})
            )
            self._firehose_tasks.add(task)
            task.add_done_callback(self._firehose_tasks.discard)

    async def _dispatch_event(
        self,
        *,
        kind: str,
        payload: dict[str, Any],
        recipients: list[str],
        inflight_upsert: tuple[str, dict[str, Any]] | None = None,
        inflight_complete: str | None = None,
    ) -> None:
        """Single durability + delivery primitive used by mutators with a store.

        Caller must hold ``self.lock``. If a store is attached we persist
        the event (and any inflight/buffered metadata) BEFORE applying
        in-RAM effects, so an observer of the event implies durability.
        """
        if self._store is not None:
            buffered_pids: list[str] = []
            for pid in recipients:
                p = self._participants.get(pid)
                if p is None:
                    continue
                if p.delivery_mode == "pull":
                    buffered_pids.append(pid)
                    continue
                if self._subscribers.get(pid):
                    continue  # live subscriber
                if p.online:
                    handle = self._connections.handle_for(self.session_id, pid)
                    if handle is not None and handle.is_alive():
                        continue  # live push delivery
                buffered_pids.append(pid)

            self._event_seq += 1
            ts_ms = int(time.time() * 1000)
            await self._store.persist_event_atomic(
                sid=self.session_id, seq=self._event_seq,
                kind=kind, payload=payload, ts=ts_ms,
                inflight_upsert=inflight_upsert,
                inflight_complete=inflight_complete,
                buffered_pids=tuple(buffered_pids),
            )

        for pid in recipients:
            self._deliver_or_buffer(pid, kind, payload)

    def _deliver_or_buffer(self, pid: str, name: str, payload: dict[str, Any]) -> None:
        p = self._participants.get(pid)
        if p is None:
            return
        # Tag this event with a per-participant monotonic seq BEFORE choosing
        # a delivery path so that the buffer, the live WS subscriber, and any
        # future delivery path observe the same ordering. The seq lets a
        # reconnecting client tell the relay "I already consumed up to N" via
        # ``last_acked_seq`` and skip duplicates on replay.
        p.next_seq += 1
        seq = p.next_seq
        # ``pull`` clients always go to the buffer regardless of online status —
        # they drain it via session_check_inbox. We skip the push path entirely
        # so the same notification isn't delivered twice.
        if p.online and p.delivery_mode == "push":
            handle = self._connections.handle_for(self.session_id, pid)
            if handle is not None and handle.is_alive():
                # ``seq`` rides along on the wire envelope so MCP clients
                # can dedup on reconnect the same way WS clients do.
                self._connections.send(
                    self.session_id, pid, name, payload, seq=seq
                )
                return
            # Handle vanished or transport is dead: transition to offline
            # before falling through to the buffer/subscribers path.
            self._mark_offline(pid)
        # WebSocket-style subscribers replace the buffer when present so we
        # don't double-deliver. attach_subscriber drains the buffer atomically
        # at attach-time, and detach_subscriber resumes buffering for events
        # that arrive after disconnect. Every subscriber attached at delivery
        # time receives a delivery attempt — a slow/broken consumer that
        # raises does NOT starve healthy peers because we iterate the full
        # tuple snapshot. The buffer is only used when ALL subscribers failed,
        # so an event reaches at least one consumer (live or buffered).
        subs = self._subscribers.get(pid)
        if subs:
            delivered = False
            failures = 0
            for cb in tuple(subs):
                try:
                    cb(name, payload, seq)
                    delivered = True
                except Exception:
                    failures += 1
            if delivered:
                if failures and self._metrics is not None:
                    self._metrics.subscriber_dispatch_failed_total.inc(failures)
                return
        try:
            buf = self._buffer_for(pid)
            size = _payload_size_bytes(payload)
            buf.enqueue(
                {
                    "_name": name,
                    "_payload": payload,
                    "_seq": seq,
                    "_size": size,
                    "_ts": time.monotonic_ns(),
                }
            )
        except BufferOverflow:
            # Per-participant count cap exceeded — escalate to per-
            # participant eviction (existing semantics: drop the offender
            # from the session). Distinct from the session-wide byte
            # budget enforced below.
            self._enqueue_eviction(pid, reason="offline_timeout")
            return
        else:
            self._buffered_bytes += size
        # Session-wide byte budget — evicts oldest items across all
        # participants until ``_buffered_bytes`` is under the cap and
        # emits a single ``buffer_evicted`` firehose event so observers
        # see the back-pressure.
        self._enforce_session_byte_budget()

    def _mark_offline(self, pid: str) -> None:
        p = self._participants.get(pid)
        if p is None or not p.online:
            return
        p.online = False
        p.last_online_at = _utcnow()
        if p.connection_ref is not None:
            self._connections.unbind(p.connection_ref)  # type: ignore[arg-type]
        p.connection_ref = None
        if self._metrics is not None:
            self._metrics.participants_active.labels(online="true").dec()
            self._metrics.participants_active.labels(online="false").inc()
        # Going offline introduces an offline-grace deadline; if grace
        # is sooner than the existing next_deadline, the registry's
        # heap entry needs refreshing.
        if self._on_deadline_shortened is not None:
            self._on_deadline_shortened(self)
        self._notify_others(
            exclude_pid=pid,
            name="participant.updated",
            payload={"participant": p.to_public().model_dump()},
        )

    def _account_drained(self, items: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        """Materialise an iterable of buffer items and subtract their
        ``_size`` from ``self._buffered_bytes``. Returns the same list
        so the caller can iterate it once more for its own purposes.

        Use this at every site that ``drain()``s a per-participant
        buffer; without it the session-wide byte accounting drifts.
        """
        out = list(items)
        for item in out:
            self._buffered_bytes -= item.get("_size", 0)
        # Floor at zero — accounting drift would otherwise compound
        # and falsely trip ``_enforce_session_byte_budget``.
        if self._buffered_bytes < 0:
            self._buffered_bytes = 0
        return out

    def _discard_buffer(self, buf: NotificationBuffer | None) -> None:
        """Throw away a buffer's contents while keeping
        ``_buffered_bytes`` in sync. Used by participant eviction
        paths where the buffer's owner is gone and the items have no
        consumer.
        """
        if buf is None:
            return
        for item in buf.drain():
            self._buffered_bytes -= item.get("_size", 0)
        if self._buffered_bytes < 0:
            self._buffered_bytes = 0

    def _enforce_session_byte_budget(self) -> None:
        """Drop the oldest buffered items across the session until
        ``_buffered_bytes <= config.max_buffer_bytes_per_session``.
        Emits a single ``buffer_evicted`` event on the firehose with the
        eviction batch summary so observers can detect back-pressure.

        Item age is the ``_ts`` field set at enqueue time
        (``time.monotonic_ns()``). For each eviction pass, the buffer
        with the oldest head item gives up its head; we repeat until
        the budget is met or every buffer is empty.

        Runs under ``self.lock`` (every caller is already inside the
        session lock).
        """
        cap = self.config.max_buffer_bytes_per_session
        if self._buffered_bytes <= cap:
            return
        evicted_count = 0
        freed_bytes = 0
        while self._buffered_bytes > cap:
            # Find the buffer with the oldest head item.
            oldest_pid: str | None = None
            oldest_ts: int = 0
            for pid, buf in self._buffers.items():
                head = buf.head()
                if head is None:
                    continue
                ts = head.get("_ts", 0)
                if oldest_pid is None or ts < oldest_ts:
                    oldest_pid = pid
                    oldest_ts = ts
            if oldest_pid is None:
                # All buffers empty yet ``_buffered_bytes > cap`` — that's
                # an accounting bug; clamp and bail rather than spin.
                self._buffered_bytes = 0
                break
            head = self._buffers[oldest_pid].popleft()
            size = head.get("_size", 0)
            self._buffered_bytes -= size
            freed_bytes += size
            evicted_count += 1
        if evicted_count > 0:
            self._mirror_firehose(
                "buffer_evicted",
                {
                    "reason": "session_byte_cap",
                    "evicted_count": evicted_count,
                    "freed_bytes": freed_bytes,
                    "buffered_bytes": self._buffered_bytes,
                    "buffer_byte_cap": cap,
                },
            )

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
                cancelled_payload = {"command_id": cid, "reason": reason}
                for pending_pid in t.pending_recipients():
                    self._deliver_or_buffer(pending_pid, "command.cancelled", cancelled_payload)
                self._mirror_firehose("command.cancelled", cancelled_payload)
                # When the sender leaves with pre-existing responses, the
                # corresponding ``result.received`` events were buffered for
                # a participant that is now gone — they would be lost
                # silently when the buffer is evicted. Emit a parallel
                # ``command.completed`` carrying the list of responders
                # whose results have been orphaned. Delivery semantics:
                #
                # 1. Firehose (for observers / cockpits / diagnostic tools)
                # 2. Each responder's own inbox — closes the observability
                #    gap surfaced by the 2026-05-17 joint-review session:
                #    a responder that already answered can correlate the
                #    eventual cancellation with the fact that its result
                #    was orphaned, without having to subscribe to the
                #    firehose. Pending recipients already get
                #    ``command.cancelled`` above; they don't need a second
                #    event.
                # 3. NOT delivered to the sender — its inbox is about to
                #    be torn down by ``_evict``.
                if reason == "sender_gone":
                    completed_payload = {
                        "command_id": cid,
                        "received_count": t.received_count,
                        "expected_count": t.expected_count,
                        "reason": "sender_gone",
                        "orphaned_results": sorted(t.responses_received),
                    }
                    for responder_id in t.responses_received:
                        self._deliver_or_buffer(
                            responder_id, "command.completed", completed_payload
                        )
                    self._mirror_firehose("command.completed", completed_payload)
                    if self._metrics is not None:
                        self._metrics.command_completed_total.labels(
                            reason="sender_gone"
                        ).inc()
                to_close.append(cid)
        for cid in to_close:
            self._inflight.pop(cid, None)
            if self._metrics is not None:
                self._metrics.command_cancelled_total.labels(reason=reason).inc()

    def _evict(self, pid: str, reason: str) -> None:
        p = self._participants.pop(pid, None)
        if p is None:
            return
        self._discard_buffer(self._buffers.pop(pid, None))
        if self._metrics is not None:
            online_label = "true" if p.online else "false"
            self._metrics.participants_active.labels(online=online_label).dec()
        self._cancel_inflight_for_sender(pid, reason="sender_gone")
        self._notify_others(
            exclude_pid=pid,
            name="participant.left",
            payload={"participant_id": pid, "reason": reason},
        )

    def detect_disconnects(self) -> bool:
        """Cheap presence sweep: mark offline any participant whose
        connection handle has died (silent client crash, transport
        closed without explicit leave). Called by the registry on every
        cycle so transitions to ``offline`` happen promptly even when
        the session has no near-term heap deadline.

        Returns ``True`` if any participant transitioned to offline —
        the caller (registry) should re-push the session onto the heap
        because ``next_deadline`` may have shortened to the new
        ``offline_grace`` deadline.
        """
        changed = False
        for pid, p in list(self._participants.items()):
            if not p.online or p.connection_ref is None:
                continue
            handle: ConnectionHandle = p.connection_ref  # type: ignore[assignment]
            if not handle.is_alive():
                self._mark_offline(pid)
                changed = True
        return changed

    def tick(self, *, now: datetime | None = None) -> None:
        """Periodic housekeeping: close expired trackers and evict
        offline-past-grace participants. Called by the registry's loop
        when this session's heap deadline fires. Synchronous; assumes
        single-threaded scheduling. Must NOT be called while a tool call
        holds ``self.lock`` mid-await.

        # Task 9 note: tick() is sync; persistence is best-effort via
        # background task (asyncio.create_task). Timeout-emitted events
        # are observable from RAM immediately; the store write is
        # eventual. Acceptable edge-case: if the relay crashes within
        # milliseconds of the timeout the cancellation event may be
        # absent from the cold snapshot — the timeline will still show
        # the command as inflight (it was removed from RAM here) so the
        # resuming relay will treat it as timed-out anyway.
        """
        moment = now or _utcnow()
        # 0. Detect handles that closed without an explicit leave/disconnect.
        #    The registry already runs detect_disconnects() each cycle, so
        #    this is mostly a belt-and-braces check for callers (and tests)
        #    that invoke ``tick`` directly.
        self.detect_disconnects()
        # 1. Expired trackers. Two sub-cases:
        #    (a) tracker incomplete (waiting on responses) → emit
        #        command.completed reason=ttl_expired + command.cancelled
        #        for each pending recipient
        #    (b) tracker already complete (all_responded, but kept in
        #        _inflight as ALREADY_RESPONDED guard) → silent eviction,
        #        no observable event. A second command.completed at TTL
        #        is a UX bug: the sender already received
        #        reason=all_responded earlier; emitting another one with
        #        reason=ttl_expired confuses cockpits and SDK retry loops.
        expired_cids = [cid for cid, t in self._inflight.items() if t.is_expired(moment)]
        for cid in expired_cids:
            t = self._inflight.pop(cid)
            if t.is_complete():
                # Silent cleanup; the original all_responded was already
                # broadcast in respond().
                continue
            completed_payload = {
                "command_id": cid,
                "received_count": t.received_count,
                "expected_count": t.expected_count,
                "reason": "ttl_expired",
            }
            self._deliver_or_buffer(t.sender_id, "command.completed", completed_payload)
            self._mirror_firehose("command.completed", completed_payload)
            if self._metrics is not None:
                self._metrics.command_completed_total.labels(reason="ttl_expired").inc()
            pending = t.pending_recipients()
            cancelled_payload = {"command_id": cid, "reason": "ttl_expired"}
            for pending_pid in pending:
                self._deliver_or_buffer(pending_pid, "command.cancelled", cancelled_payload)
                if self._metrics is not None:
                    self._metrics.command_cancelled_total.labels(reason="ttl_expired").inc()
            if pending:
                self._mirror_firehose("command.cancelled", cancelled_payload)
            # Best-effort persistence for tick()-emitted events (store write is
            # eventual since tick() is sync; see Task 9 note above).
            if self._store is not None:
                ts_ms = int(time.time() * 1000)
                self._event_seq += 1
                seq_completed = self._event_seq
                task_completed = asyncio.create_task(
                    self._store.persist_event_atomic(
                        sid=self.session_id, seq=seq_completed,
                        kind="command.completed", payload=completed_payload,
                        ts=ts_ms, inflight_complete=cid,
                        buffered_pids=(),
                    )
                )
                self._firehose_tasks.add(task_completed)
                task_completed.add_done_callback(self._firehose_tasks.discard)
                if pending:
                    self._event_seq += 1
                    seq_cancelled = self._event_seq
                    task_cancelled = asyncio.create_task(
                        self._store.persist_event_atomic(
                            sid=self.session_id, seq=seq_cancelled,
                            kind="command.cancelled", payload=cancelled_payload,
                            ts=ts_ms,
                            buffered_pids=tuple(pending),
                        )
                    )
                    self._firehose_tasks.add(task_cancelled)
                    task_cancelled.add_done_callback(self._firehose_tasks.discard)
        # 2. Offline-grace eviction
        evicted: list[str] = []
        for pid, p in self._participants.items():
            if p.online:
                continue
            if (moment - p.last_online_at).total_seconds() >= self.config.offline_grace_seconds:
                evicted.append(pid)
        for pid in evicted:
            self._evict(pid, reason="offline_timeout")

    def broadcast_to_subscribers(self, name: str, payload: dict[str, Any]) -> None:
        """Best-effort fan-out to ALL active subscribers. Used for control-plane
        notifications like ``session.cold`` that should NOT be buffered.
        """
        for pid, subs in self._subscribers.items():
            p = self._participants.get(pid)
            if p is None:
                continue
            # Use a synthetic seq (0) — this is a control event, not part
            # of the per-participant resume seq stream.
            for cb in tuple(subs):
                try:
                    cb(name, payload, 0)
                except Exception:
                    pass

    def close_all_subscribers(self) -> None:
        """Drop all subscriber callbacks. WS handlers should observe their
        socket close shortly after; new attaches will work normally.
        """
        self._subscribers.clear()

    def cancel_all_wait_for_event(self) -> None:
        """Best-effort: clear any pending wait_for_event state so callers
        return promptly with timed_out=True or similar. Called during
        explicit session deletion."""
        self._wait_in_flight.clear()
        self._active_waits = 0

    def broadcast_session_ended(self, *, reason: str) -> None:
        # Wake any wait_for_event coroutines so they raise PARTICIPANT_EVICTED
        # rather than silently timing out on their full budget while the session
        # disappears under them. Must fire BEFORE _participants is cleared so the
        # sentinel handler's membership check sees the right state.
        for _pid, subs in list(self._subscribers.items()):
            for cb in list(subs):
                try:
                    cb("__evicted__", {}, 0)
                except Exception:
                    pass
        self._subscribers.clear()
        for pid, p in list(self._participants.items()):
            if p.online:
                self._connections.send(
                    self.session_id, pid, "session.ended", {"reason": reason}
                )
                handle = p.connection_ref
                if handle is not None:
                    self._connections.unbind(handle)  # type: ignore[arg-type]
            if self._metrics is not None:
                online_label = "true" if p.online else "false"
                self._metrics.participants_active.labels(online=online_label).dec()
            self._discard_buffer(self._buffers.pop(pid, None))
        self._participants.clear()
        self._inflight.clear()
        if self._metrics is not None:
            self._metrics.session_ended_total.labels(reason=reason).inc()
        self._mirror_firehose("session.ended", {"reason": reason})

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
        if self._metrics is not None:
            self._metrics.participants_active.labels(online="true").dec()
            self._metrics.participants_active.labels(online="false").inc()
        self._notify_others(
            exclude_pid=pid,
            name="participant.updated",
            payload={"participant": p.to_public().model_dump()},
        )

    async def resume(
        self,
        handle: ConnectionHandle,
        participant_id: str,
        *,
        last_acked_seq: int | None = None,
    ) -> None:
        """Re-attach a participant after a disconnect.

        ``last_acked_seq`` (optional): highest seq the caller has
        already consumed in memory before its prior transport died.
        On push-mode replay the buffer is filtered so events with
        ``seq <= effective_last_acked_seq`` are skipped. The effective
        value is ``max(last_acked_seq or 0,
        participant.last_acked_seq)`` — a stronger watermark from a
        prior ACK is never regressed by a weaker caller-supplied
        value.
        """
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
            if was_offline and self._metrics is not None:
                self._metrics.participants_active.labels(online="false").dec()
                self._metrics.participants_active.labels(online="true").inc()
            # Resolve the effective ACK watermark. Caller-supplied value
            # is merged with the participant's prior watermark; the
            # latter is also advanced so subsequent drains see the same
            # state.
            effective_ack: int = 0
            if last_acked_seq is not None and last_acked_seq > effective_ack:
                effective_ack = last_acked_seq
            if p.last_acked_seq > effective_ack:
                effective_ack = p.last_acked_seq
            if effective_ack > p.last_acked_seq:
                p.last_acked_seq = effective_ack
            # Pull-mode participants drain their own buffer via
            # session_check_inbox; flushing here would push events through
            # the transport and lose them (their model only sees synchronous
            # tool results, not server logs).
            if p.delivery_mode != "pull":
                buf = self._buffers.pop(participant_id, None)
                if buf is not None:
                    for item in self._account_drained(
                        self._filter_stale_buffer_items(buf.drain())
                    ):
                        if not _passes_ack_filter(
                            item.get("_seq", 0), effective_ack
                        ):
                            continue
                        item_seq = item.get("_seq", 0)
                        self._connections.send(
                            self.session_id,
                            participant_id,
                            item["_name"],
                            item["_payload"],
                            seq=item_seq if item_seq > 0 else None,
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
        self._mirror_firehose(name, payload)
