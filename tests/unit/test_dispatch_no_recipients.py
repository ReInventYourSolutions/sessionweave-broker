"""A dispatch that reaches nobody must not report that everybody answered.

`dispatch="any"` picks one match **among the online participants**
(`dispatcher.decide_dispatch`); `dispatch="all"` does not filter on
presence. A turn-based counterparty is offline between its turns, so it is
invisible to every `any` — and the sender was told
`command.completed{received_count: 0, reason: "all_responded"}`.

"All responded" with nobody reached is a terminal event that lies, and it
is indistinguishable from a real completion. Observed in the field: an
agent reported that the relay "considered it out of the session and could
not send it commands". Its resume had in fact worked; the peer's `any`
dispatches were silently reaching nobody, while the same session worked
whenever the peer happened to use `all` (buffered, recovered on resume).

`no_recipients` makes the two distinguishable.
"""
from __future__ import annotations

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


async def _session_with_offline_worker() -> tuple[Session, str, FakeHandle]:
    sess = Session("s", SessionConfig(), ConnectionManager())
    worker_handle = FakeHandle()
    await sess.join(worker_handle, role="worker", capabilities=["chat"])
    sender_handle = FakeHandle()
    sender = await sess.join(sender_handle, role="sender", capabilities=["dispatch"])
    # The worker's transport dies — exactly a turn-based agent between turns.
    worker_handle.close()
    sess.detect_disconnects()
    return sess, sender.participant_id, sender_handle


def _completed(handle: FakeHandle) -> dict[str, object] | None:
    for name, payload in handle.sent:
        if name == "command.completed":
            return payload
    return None


@pytest.mark.asyncio
async def test_any_with_nobody_online_does_not_claim_all_responded() -> None:
    sess, sender_id, sender_handle = await _session_with_offline_worker()
    sender_handle.sent.clear()

    await sess.send(sender_id=sender_id, dispatch=DispatchMode.ANY,
                    target=Target(capability="chat"), payload={"body": "hello"})

    done = _completed(sender_handle)
    assert done is not None, "the sender was told nothing at all"
    assert done["reason"] != "all_responded", (
        "a dispatch that reached nobody reported that everybody answered — "
        f"indistinguishable from a real completion: {done}"
    )
    assert done["reason"] == "no_recipients", done


@pytest.mark.asyncio
async def test_all_still_reaches_an_offline_participant() -> None:
    """The contrast that makes the bug confusing: `all` does not filter on
    presence, so the same session works or not depending on the mode the
    sender happened to pick. Pinning it so the asymmetry stays deliberate."""
    sess, sender_id, sender_handle = await _session_with_offline_worker()
    sender_handle.sent.clear()

    out = await sess.send(sender_id=sender_id, dispatch=DispatchMode.ALL,
                          target=Target(capability="chat"), payload={"body": "hello"})

    assert len(out.delivered_to) == 1, out
    assert _completed(sender_handle) is None, "should still be awaiting a reply"


@pytest.mark.asyncio
async def test_a_real_completion_still_says_all_responded() -> None:
    """Regression guard: the honest case keeps its reason."""
    sess = Session("s", SessionConfig(), ConnectionManager())
    wh = FakeHandle()
    w = await sess.join(wh, role="worker", capabilities=["chat"])
    sh = FakeHandle()
    sender = await sess.join(sh, role="sender", capabilities=["dispatch"])
    out = await sess.send(sender_id=sender.participant_id, dispatch=DispatchMode.ALL,
                          target=Target(capability="chat"), payload={"b": 1})
    sh.sent.clear()
    await sess.respond(responder_id=w.participant_id, command_id=out.command_id,
                       status="ok", payload={"ok": 1})

    done = _completed(sh)
    assert done is not None and done["reason"] == "all_responded", done
