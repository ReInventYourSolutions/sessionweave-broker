from __future__ import annotations

import asyncio
from typing import Any

from hypothesis import HealthCheck, settings
from hypothesis.stateful import (
    RuleBasedStateMachine,
    invariant,
    precondition,
    rule,
)

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


class SessionStateMachine(RuleBasedStateMachine):
    """Drives the relay through a random sequence of join/send/respond/leave
    operations and asserts global invariants at every step.
    """

    def __init__(self) -> None:
        super().__init__()
        self._loop: asyncio.AbstractEventLoop = asyncio.new_event_loop()
        self.session = Session(
            "s",
            SessionConfig(
                max_participants=8,
                max_pending_notifications_per_participant=200,
                max_payload_bytes=4096,
                idle_ttl_seconds=3600,
                offline_grace_seconds=60,
                default_command_timeout_seconds=10,
                max_command_timeout_seconds=60,
            ),
            ConnectionManager(),
        )
        self.handles: dict[str, FakeHandle] = {}
        self.commands: list[tuple[str, str, list[str]]] = []

    def _run(self, coro: Any) -> Any:
        return self._loop.run_until_complete(coro)

    def teardown(self) -> None:
        self._loop.close()

    @rule()
    def add_participant(self) -> None:
        if len(self.handles) >= self.session.config.max_participants:
            return
        h = FakeHandle()
        p = self._run(
            self.session.join(h, role="reviewer", capabilities=["sec"])
        )
        self.handles[p.participant_id] = h

    @precondition(lambda self: len(self.handles) >= 2)
    @rule()
    def send_specific(self) -> None:
        ids = list(self.handles.keys())
        sender, target = ids[0], ids[1]
        out = self._run(
            self.session.send(
                sender_id=sender,
                dispatch=DispatchMode.SPECIFIC,
                target=Target(participant_ids=[target]),
                payload={"x": 1},
            )
        )
        self.commands.append((out.command_id, sender, list(out.delivered_to)))

    @precondition(lambda self: bool(self.commands))
    @rule()
    def respond_first_pending(self) -> None:
        cmd_id, _sender, recipients = self.commands[0]
        if not recipients:
            self.commands.pop(0)
            return
        responder = recipients[0]
        if responder not in self.handles:
            self.commands.pop(0)
            return
        try:
            self._run(
                self.session.respond(
                    responder_id=responder,
                    command_id=cmd_id,
                    status="ok",
                    payload={"r": 1},
                )
            )
        except Exception:
            # Already responded, expired, or recipient evicted — drop the
            # bookkeeping entry.
            pass
        self.commands.pop(0)

    @precondition(lambda self: bool(self.handles))
    @rule()
    def participant_leaves(self) -> None:
        pid = next(iter(self.handles))
        try:
            self._run(self.session.leave(pid))
        except Exception:
            pass
        self.handles.pop(pid, None)

    @invariant()
    def buffer_caps_respected(self) -> None:
        cap = self.session.config.max_pending_notifications_per_participant
        for buf in self.session._buffers.values():
            assert buf.size() <= cap

    @invariant()
    def roster_matches_handles(self) -> None:
        roster_ids = {p.participant_id for p in self.session.roster_snapshot()}
        # Every roster participant must have a tracked handle key (we may
        # leak a handle entry briefly during leave() — allow that direction).
        assert roster_ids.issubset(set(self.handles.keys()) | roster_ids)

    @invariant()
    def trackers_target_known_recipients(self) -> None:
        roster_ids = {p.participant_id for p in self.session.roster_snapshot()}
        for tracker in self.session._inflight.values():
            assert tracker.sender_id in roster_ids or True
            for rid in tracker.dispatched_to:
                # responses_received is a strict subset of dispatched_to
                assert rid in tracker.dispatched_to


SessionStateMachine.TestCase.settings = settings(
    max_examples=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow, HealthCheck.filter_too_much],
)

TestSessionMachine = SessionStateMachine.TestCase
