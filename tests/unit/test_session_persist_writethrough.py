from unittest.mock import AsyncMock, MagicMock

import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.models import Participant
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session, _utcnow


def _make_session(*, store=None, persistence_mode=None) -> Session:
    return Session(
        session_id="sid",
        config=SessionConfig(),
        connections=ConnectionManager(),
        store=store,
        persistence_mode=persistence_mode,
    )


def _join_inline(sess: Session, pid: str, *, delivery_mode: str = "pull",
                  capabilities: list[str] | None = None) -> None:
    sess._participants[pid] = Participant(
        participant_id=pid, role=None, objective=None,
        capabilities=capabilities or [],
        metadata={},
        delivery_mode=delivery_mode, online=True,
        last_online_at=_utcnow(), joined_at=_utcnow(),
        next_seq=0,
    )


def _make_handle() -> MagicMock:
    """Create a minimal ConnectionHandle mock."""
    h = MagicMock()
    h.send = MagicMock()
    h.is_alive = MagicMock(return_value=True)
    return h


@pytest.mark.asyncio
async def test_send_persists_event_when_store_set() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock()
    sess = _make_session(store=store, persistence_mode="ttl")
    _join_inline(sess, "p_sender", delivery_mode="pull")
    _join_inline(sess, "p_resp", delivery_mode="pull", capabilities=["math"])

    out = await sess.send(
        sender_id="p_sender",
        dispatch=DispatchMode.ALL,
        target=Target(capability="math"),
        payload={"text": "go"},
    )
    assert store.persist_event_atomic.await_count == 1
    kwargs = store.persist_event_atomic.await_args.kwargs
    assert kwargs["sid"] == "sid"
    assert kwargs["kind"] == "command.received"
    assert kwargs["payload"]["command_id"] == out.command_id
    assert kwargs["inflight_upsert"][0] == out.command_id
    # pull-mode responder is buffered
    assert "p_resp" in kwargs["buffered_pids"]


@pytest.mark.asyncio
async def test_send_persist_failure_aborts_no_inflight() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock(side_effect=RuntimeError("disk full"))
    sess = _make_session(store=store, persistence_mode="ttl")
    _join_inline(sess, "p_sender", delivery_mode="pull")
    _join_inline(sess, "p_resp", delivery_mode="pull", capabilities=["math"])

    with pytest.raises(RuntimeError, match="disk full"):
        await sess.send(
            sender_id="p_sender",
            dispatch=DispatchMode.ALL,
            target=Target(capability="math"),
            payload={"text": "x"},
        )
    # No inflight entry survives the failure
    assert len(sess._inflight) == 0


@pytest.mark.asyncio
async def test_send_no_store_no_persist_call() -> None:
    sess = _make_session(store=None, persistence_mode=None)
    _join_inline(sess, "p_sender", delivery_mode="pull")
    _join_inline(sess, "p_resp", delivery_mode="pull", capabilities=["x"])
    out = await sess.send(
        sender_id="p_sender",
        dispatch=DispatchMode.ALL,
        target=Target(capability="x"),
        payload={},
    )
    assert out.command_id  # success, ephemeral path unchanged


@pytest.mark.asyncio
async def test_respond_persists_result_received() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock()
    sess = _make_session(store=store, persistence_mode="ttl")
    _join_inline(sess, "p_sender", delivery_mode="pull")
    _join_inline(sess, "p_resp", delivery_mode="pull", capabilities=["x"])
    out = await sess.send(
        sender_id="p_sender", dispatch=DispatchMode.ALL,
        target=Target(capability="x"), payload={},
    )
    store.persist_event_atomic.reset_mock()
    await sess.respond(
        responder_id="p_resp", command_id=out.command_id,
        status="ok", payload={"answer": 42},
    )
    # at least one result.received persisted
    kinds = [c.kwargs["kind"] for c in store.persist_event_atomic.await_args_list]
    assert "result.received" in kinds


@pytest.mark.asyncio
async def test_join_upserts_participant_when_store_set() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock()
    store.upsert_participant = AsyncMock()
    sess = _make_session(store=store, persistence_mode="ttl")
    handle = _make_handle()
    p = await sess.join(
        handle,
        role="agent", objective=None,
        capabilities=[], metadata={}, delivery_mode="pull",
    )
    store.upsert_participant.assert_awaited()
    args = store.upsert_participant.await_args
    assert args.args[0] == "sid"
    assert args.args[1] == p.participant_id


@pytest.mark.asyncio
async def test_leave_soft_deletes_participant_when_store_set() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock()
    store.upsert_participant = AsyncMock()
    store.soft_delete_participant = AsyncMock()
    sess = _make_session(store=store, persistence_mode="ttl")
    handle = _make_handle()
    p = await sess.join(
        handle,
        role="agent", objective=None,
        capabilities=[], metadata={}, delivery_mode="pull",
    )
    await sess.leave(participant_id=p.participant_id)
    store.soft_delete_participant.assert_awaited()
    assert store.soft_delete_participant.await_args.args[0] == "sid"
    assert store.soft_delete_participant.await_args.args[1] == p.participant_id


@pytest.mark.asyncio
async def test_update_purpose_persists_when_store_set() -> None:
    store = MagicMock()
    store.persist_event_atomic = AsyncMock()
    store.upsert_participant = AsyncMock()
    store.update_session_purpose = AsyncMock()
    sess = _make_session(store=store, persistence_mode="ttl")
    handle = _make_handle()
    p = await sess.join(
        handle,
        role="agent", objective=None,
        capabilities=[], metadata={}, delivery_mode="pull",
    )
    await sess.update_purpose(requester_id=p.participant_id, new_purpose={"text": "new"})
    store.update_session_purpose.assert_awaited()
    assert store.update_session_purpose.await_args.args[0] == "sid"
