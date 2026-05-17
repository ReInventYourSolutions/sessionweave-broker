import pytest

from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.schemas import DispatchMode, Target
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


@pytest.mark.asyncio
async def test_send_specific_delivers_command_received_to_target() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    p_a = await s.join(h_a, role="sender", capabilities=[])
    p_b = await s.join(h_b, role="reviewer", capabilities=["x"])
    res = await s.send(
        sender_id=p_a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[p_b.participant_id]),
        payload={"hello": 1},
    )
    assert res.command_id
    assert res.delivered_to == [p_b.participant_id]
    names = [n for n, _ in h_b.sent]
    assert "command.received" in names


@pytest.mark.asyncio
async def test_send_payload_too_large() -> None:
    s = Session("s", SessionConfig(max_payload_bytes=10), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    with pytest.raises(ToolError) as exc:
        await s.send(
            sender_id=a.participant_id,
            dispatch=DispatchMode.SPECIFIC,
            target=Target(participant_ids=[b.participant_id]),
            payload={"x": "this string is well over 10 bytes"},
        )
    assert exc.value.code is ErrorCode.PAYLOAD_TOO_LARGE


@pytest.mark.asyncio
async def test_respond_routes_result_to_sender() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    res = await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={"q": 1},
    )
    await s.respond(
        responder_id=b.participant_id,
        command_id=res.command_id,
        status="ok",
        payload={"r": 2},
    )
    a_events = [n for n, _ in h_a.sent]
    assert "result.received" in a_events
    assert "command.completed" in a_events  # all_responded since dispatched_to=[b]


@pytest.mark.asyncio
async def test_respond_unknown_command_raises() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a = FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    with pytest.raises(ToolError) as exc:
        await s.respond(
            responder_id=a.participant_id, command_id="nope", status="ok", payload={}
        )
    assert exc.value.code is ErrorCode.COMMAND_NOT_FOUND


@pytest.mark.asyncio
async def test_respond_not_a_recipient_raises() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b, h_c = FakeHandle(), FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    c = await s.join(h_c, role="r", capabilities=[])
    res = await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    with pytest.raises(ToolError) as exc:
        await s.respond(
            responder_id=c.participant_id, command_id=res.command_id, status="ok", payload={}
        )
    assert exc.value.code is ErrorCode.NOT_A_RECIPIENT


@pytest.mark.asyncio
async def test_respond_rejects_payload_too_large() -> None:
    s = Session("s", SessionConfig(max_payload_bytes=256), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    out = await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    cmd_id = out.command_id
    # Build a payload that JSON-serialises larger than 256 bytes.
    big = {"data": "x" * 1024}
    with pytest.raises(ToolError) as exc:
        await s.respond(
            responder_id=b.participant_id,
            command_id=cmd_id,
            status="ok",
            payload=big,
        )
    assert exc.value.code is ErrorCode.PAYLOAD_TOO_LARGE


@pytest.mark.asyncio
async def test_double_respond_raises_already_responded() -> None:
    s = Session("s", SessionConfig(), ConnectionManager())
    h_a, h_b = FakeHandle(), FakeHandle()
    a = await s.join(h_a, role=None, capabilities=[])
    b = await s.join(h_b, role="r", capabilities=[])
    res = await s.send(
        sender_id=a.participant_id,
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=[b.participant_id]),
        payload={},
    )
    await s.respond(
        responder_id=b.participant_id, command_id=res.command_id, status="ok", payload={}
    )
    with pytest.raises(ToolError) as exc:
        await s.respond(
            responder_id=b.participant_id, command_id=res.command_id, status="ok", payload={}
        )
    assert exc.value.code is ErrorCode.ALREADY_RESPONDED
