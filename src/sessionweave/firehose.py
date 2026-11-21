"""Per-session firehose subscriber registry.

A firehose subscriber is NOT a participant — it does not appear in the
roster, has no participant_id, occupies no participant slot. It is a
pure observer that receives every event published to its session.

Used by:
  - The hosted cockpit observatory (one subscription per browser tab).
  - The local hub's archive subscriber (one per owned session).
"""
from __future__ import annotations

import asyncio
import os
from collections.abc import Callable
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

_DEFAULT_PER_SESSION_CAP = int(os.environ.get("SW_MAX_FIREHOSE_PER_SESSION", "16"))
_DEFAULT_QUEUE_CAPACITY = int(os.environ.get("SW_FIREHOSE_QUEUE_CAPACITY", "256"))
_LAG_EVENT_NAME = "firehose.lagged"


@dataclass
class FirehoseSubscriber:
    id: str
    session_id: str
    queue: asyncio.Queue[dict[str, Any]]
    # The supervisor token that authenticated this stream (None for creator
    # or admin streams). Revoking that token closes the stream.
    token_id: str | None = None
    # The owner account (and its token) that authenticated this stream, if any.
    account_id: str | None = None
    account_token_id: str | None = None
    # Client address the stream was opened from ("" when unknown).
    source_ip: str = ""
    # Set when the stream must end (e.g. its token was revoked); the reason
    # is sent as the WebSocket close reason.
    closed: asyncio.Event = field(default_factory=asyncio.Event)
    close_reason: str = ""
    _lagged: bool = field(default=False)


class FirehoseRegistry:
    def __init__(
        self,
        per_session_cap: int = _DEFAULT_PER_SESSION_CAP,
        queue_capacity: int = _DEFAULT_QUEUE_CAPACITY,
    ) -> None:
        self._cap = per_session_cap
        self._queue_capacity = queue_capacity
        self._subs: dict[str, list[FirehoseSubscriber]] = {}
        self._lock = asyncio.Lock()

    async def subscribe(
        self,
        session_id: str,
        token_id: str | None = None,
        account_id: str | None = None,
        account_token_id: str | None = None,
        source_ip: str = "",
    ) -> FirehoseSubscriber:
        async with self._lock:
            bucket = self._subs.setdefault(session_id, [])
            if len(bucket) >= self._cap:
                raise RuntimeError(f"firehose_cap reached for session {session_id}")
            sub = FirehoseSubscriber(
                id=f"fh-{uuid4().hex[:10]}",
                session_id=session_id,
                queue=asyncio.Queue(maxsize=self._queue_capacity),
                token_id=token_id,
                account_id=account_id,
                account_token_id=account_token_id,
                source_ip=source_ip,
            )
            bucket.append(sub)
            return sub

    async def unsubscribe(self, sub: FirehoseSubscriber) -> None:
        async with self._lock:
            bucket = self._subs.get(sub.session_id, [])
            if sub in bucket:
                bucket.remove(sub)
            if not bucket and sub.session_id in self._subs:
                del self._subs[sub.session_id]

    async def close_for(
        self, session_id: str, token_id: str, reason: str = "supervisor token revoked"
    ) -> int:
        """Signal every stream of ``session_id`` opened with ``token_id`` to close.

        Returns how many subscribers were signalled. The WebSocket handler
        closes the socket and unsubscribes.
        """
        async with self._lock:
            hits = [s for s in self._subs.get(session_id, ()) if s.token_id == token_id]
        for sub in hits:
            sub.close_reason = reason
            sub.closed.set()
        return len(hits)

    async def close_for_account(
        self,
        account_id: str,
        *,
        token_id: str | None = None,
        reason: str = "account token revoked",
        ip_filter: Callable[[str], bool] | None = None,
    ) -> int:
        """Signal every stream opened by ``account_id`` (or only through its token
        ``token_id``) to close, across all sessions. With ``ip_filter``, only
        streams whose client address it accepts are closed."""
        async with self._lock:
            hits = [
                s
                for bucket in self._subs.values()
                for s in bucket
                if s.account_id == account_id
                and (token_id is None or s.account_token_id == token_id)
                and (ip_filter is None or ip_filter(s.source_ip))
            ]
        for sub in hits:
            sub.close_reason = reason
            sub.closed.set()
        return len(hits)

    async def publish(self, session_id: str, event: dict[str, Any]) -> None:
        """Best-effort fan-out. Subscribers whose queue is full get the
        oldest entry evicted, then this event pushed, then a synthetic
        ``firehose.lagged`` marker is ensured at the tail so the client
        can resync.  The lag marker is sticky: it stays at the tail
        (potentially displacing older events) for as long as overflow
        conditions persist.
        """
        bucket = list(self._subs.get(session_id, ()))
        for sub in bucket:
            try:
                sub.queue.put_nowait(event)
            except asyncio.QueueFull:
                # Evict oldest, then insert the new event.
                try:
                    sub.queue.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    sub.queue.put_nowait(event)
                except asyncio.QueueFull:
                    pass
                # Ensure a ``firehose.lagged`` marker is at the tail.
                # If the queue is full again we evict one more item to
                # make room for it, keeping the lag signal visible.
                sub._lagged = True
                if sub.queue.full():
                    try:
                        sub.queue.get_nowait()
                    except asyncio.QueueEmpty:
                        pass
                try:
                    sub.queue.put_nowait({"name": _LAG_EVENT_NAME, "payload": {}})
                except asyncio.QueueFull:
                    pass
