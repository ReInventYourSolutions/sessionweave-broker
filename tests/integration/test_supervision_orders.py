"""Supervision orders: halt pauses, continue / redirect resume, note leaves state.

Orders concern only the addressed participant(s) — there is no cascade. A
paused participant is excluded from role / capability matching, and a command
that can reach only paused participants is refused with ``participant_paused``.
"""
from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

import httpx
import pytest
import pytest_asyncio

from sessionweave.access import generate_token, hash_token
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.firehose import FirehoseRegistry
from sessionweave.models import Participant
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import DispatchMode, PersistenceConfig, Target
from sessionweave.session import Session
from sessionweave.supervisor import NoteRecord, SupervisorNote
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess
from tests.unit.fakes import FakeHandle

# ---------- in-process fixtures ----------


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "orders.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _registry(
    store: PersistenceStore, firehose: FirehoseRegistry | None = None
) -> SessionRegistry:
    return SessionRegistry(
        defaults=load_deployment_defaults({}),
        connections=ConnectionManager(),
        metrics=None, firehose=firehose, store=store,
    )


async def _session(reg: SessionRegistry) -> Session:
    sid, _, _, _, _ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_token(generate_token()),
    )
    return await reg.get_or_raise(sid)


def _order(kind: str, target: str, message: str | None = None) -> SupervisorNote:
    return SupervisorNote(
        note_id=f"sn-{kind}-{target[-6:]}", kind=kind, target=target,
        message=message, ts_ms=1_700_000_000_000,
    )


def _names(handle: FakeHandle) -> list[str]:
    return [name for name, _ in handle.sent]


def _states(sess: Session) -> dict[str, str]:
    return {p.participant_id: p.supervision_state for p in sess.roster_snapshot()}


async def _two(sess: Session, role: str = "worker") -> tuple[
    Participant, FakeHandle, Participant, FakeHandle
]:
    ha, hb = FakeHandle(), FakeHandle()
    a = await sess.join(ha, role=role, capabilities=[])
    b = await sess.join(hb, role=role, capabilities=[])
    ha.sent.clear()
    hb.sent.clear()
    return a, ha, b, hb


# ---------- orders ----------


@pytest.mark.asyncio
async def test_halt_pauses_only_the_target_and_only_it_receives_the_note(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    a, ha, b, hb = await _two(sess)

    out = await sess.post_order(_order("halt", a.participant_id))

    assert out["affected"] == [a.participant_id]
    assert out["kind"] == "halt" and out["target"] == a.participant_id
    assert _states(sess) == {a.participant_id: "paused", b.participant_id: "active"}
    assert "supervisor.note" in _names(ha)
    assert "supervisor.note" not in _names(hb)
    # The other participant learns of the state change through the roster event.
    updated = [p for n, p in hb.sent if n == "participant.updated"]
    assert updated and updated[-1]["participant"]["supervision_state"] == "paused"


@pytest.mark.asyncio
async def test_continue_resumes_a_paused_target(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    a, _, _, _ = await _two(sess)
    await sess.post_order(_order("halt", a.participant_id))

    out = await sess.post_order(_order("continue", a.participant_id))

    assert out["affected"] == [a.participant_id]
    assert _states(sess)[a.participant_id] == "active"


@pytest.mark.asyncio
async def test_redirect_to_a_paused_target_resumes_it(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    a, ha, _, _ = await _two(sess)
    await sess.post_order(_order("halt", a.participant_id))
    ha.sent.clear()

    out = await sess.post_order(_order("redirect", a.participant_id, "do Y instead"))

    assert out["affected"] == [a.participant_id]
    assert _states(sess)[a.participant_id] == "active"
    (note,) = [p for n, p in ha.sent if n == "supervisor.note"]
    assert note["message"] == "do Y instead"


@pytest.mark.asyncio
async def test_note_leaves_state_unchanged(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    a, _, b, hb = await _two(sess)
    await sess.post_order(_order("halt", a.participant_id))
    hb.sent.clear()

    out = await sess.post_order(_order("note", "all", "remember X"))

    assert out["affected"] == []
    assert _states(sess) == {a.participant_id: "paused", b.participant_id: "active"}
    assert "participant.updated" not in _names(hb)


@pytest.mark.asyncio
async def test_halt_all_pauses_everyone_and_everyone_receives_it(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    a, ha, b, hb = await _two(sess)

    out = await sess.post_order(_order("halt", "all"))

    assert sorted(out["affected"]) == sorted([a.participant_id, b.participant_id])
    assert set(_states(sess).values()) == {"paused"}
    assert _names(ha).count("supervisor.note") == 1
    assert _names(hb).count("supervisor.note") == 1


@pytest.mark.asyncio
async def test_unknown_target_is_participant_not_found(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    await _two(sess)
    with pytest.raises(ToolError) as exc:
        await sess.post_order(_order("halt", "p-nope"))
    assert exc.value.code is ErrorCode.PARTICIPANT_NOT_FOUND
    assert sess._notes == {}


@pytest.mark.asyncio
async def test_order_is_recorded_persisted_and_survives_hydrate(
    store: PersistenceStore,
) -> None:
    reg = _registry(store)
    sess = await _session(reg)
    a, _, b, _ = await _two(sess)
    note = _order("halt", a.participant_id, "stop")

    await sess.post_order(note)

    record = NoteRecord(note.note_id, "halt", a.participant_id, note.ts_ms)
    assert sess._notes == {note.note_id: record}
    hydrated = await _registry(store).get_or_raise(sess.session_id)
    assert hydrated._notes == {note.note_id: record}
    assert _states(hydrated) == {a.participant_id: "paused", b.participant_id: "active"}


@pytest.mark.asyncio
async def test_each_order_reaches_the_firehose_exactly_once(
    store: PersistenceStore,
) -> None:
    firehose = FirehoseRegistry()
    sess = await _session(_registry(store, firehose))
    a, _, _, _ = await _two(sess)
    sub = await firehose.subscribe(sess.session_id)

    await sess.post_order(_order("halt", a.participant_id))
    await asyncio.gather(*sess._firehose_tasks)

    names = []
    while not sub.queue.empty():
        names.append(sub.queue.get_nowait()["name"])
    assert names.count("supervisor.note") == 1
    assert names.count("participant.updated") == 1


@pytest.mark.asyncio
async def test_failed_order_write_leaves_ram_and_disk_unchanged(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The paused rows and the note commit together or not at all.

    The failure is injected on the transaction's last statement, after the
    note event and the participant rows were written, so the rollback has
    to undo both.
    """
    sess = await _session(_registry(store))
    a, ha, b, hb = await _two(sess)
    conn = store._write_conn
    assert conn is not None
    real_execute = conn.execute
    armed = {"on": True}

    def failing_execute(sql: str, *args: Any, **kwargs: Any) -> Any:
        if armed["on"] and sql.startswith("UPDATE sessions SET last_activity_at"):
            raise RuntimeError("disk full")
        return real_execute(sql, *args, **kwargs)

    monkeypatch.setattr(conn, "execute", failing_execute)
    with pytest.raises(RuntimeError, match="disk full"):
        await sess.post_order(_order("halt", "all"))
    armed["on"] = False

    assert set(_states(sess).values()) == {"active"}
    assert sess._notes == {}
    assert "supervisor.note" not in _names(ha) + _names(hb)
    hydrated = await _registry(store).get_or_raise(sess.session_id)
    assert _states(hydrated) == {a.participant_id: "active", b.participant_id: "active"}
    assert hydrated._notes == {}
    page = await store.fetch_events(sid=sess.session_id, kinds=["supervisor.note"])
    assert page.events == []


@pytest.mark.asyncio
async def test_failed_roster_notice_does_not_fail_an_applied_order(
    store: PersistenceStore, monkeypatch: pytest.MonkeyPatch,
) -> None:
    sess = await _session(_registry(store))
    a, ha, _, _ = await _two(sess)
    real_dispatch = Session._dispatch_event

    async def flaky_dispatch(self: Session, **kwargs: Any) -> None:
        if kwargs["kind"] == "participant.updated":
            raise RuntimeError("disk full")
        await real_dispatch(self, **kwargs)

    monkeypatch.setattr(Session, "_dispatch_event", flaky_dispatch)
    out = await sess.post_order(_order("halt", a.participant_id))

    assert out["affected"] == [a.participant_id]
    assert _states(sess)[a.participant_id] == "paused"
    assert "supervisor.note" in _names(ha)


# ---------- paused-aware dispatch ----------


async def _sender(sess: Session) -> Participant:
    return await sess.join(FakeHandle(), role="lead", capabilities=[])


@pytest.mark.asyncio
async def test_role_dispatch_skips_the_paused_participant(store: PersistenceStore) -> None:
    sess = await _session(_registry(store))
    a, _, b, _ = await _two(sess)
    lead = await _sender(sess)
    await sess.post_order(_order("halt", a.participant_id))

    for mode in (DispatchMode.ALL, DispatchMode.ANY):
        out = await sess.send(
            sender_id=lead.participant_id, dispatch=mode,
            target=Target(role="worker"), payload={"do": "it"},
        )
        assert out.delivered_to == [b.participant_id], mode


@pytest.mark.asyncio
async def test_role_dispatch_to_only_paused_participants_is_refused(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    await _two(sess)
    lead = await _sender(sess)
    await sess.post_order(_order("halt", "all"))
    await sess.post_order(_order("continue", lead.participant_id))

    for mode in (DispatchMode.ALL, DispatchMode.ANY):
        with pytest.raises(ToolError) as exc:
            await sess.send(
                sender_id=lead.participant_id, dispatch=mode,
                target=Target(role="worker"), payload={"do": "it"},
            )
        assert exc.value.code is ErrorCode.PARTICIPANT_PAUSED, mode
        assert exc.value.message == "participant is paused by the supervisor"


@pytest.mark.asyncio
async def test_any_with_only_paused_online_and_an_active_offline_match_has_no_recipients(
    store: PersistenceStore,
) -> None:
    """Paused is reported only when every match is paused; presence does not count."""
    sess = await _session(_registry(store))
    a, _, b, _ = await _two(sess)
    lead = await _sender(sess)
    await sess.post_order(_order("halt", a.participant_id))
    sess._participants[b.participant_id].online = False

    out = await sess.send(
        sender_id=lead.participant_id, dispatch=DispatchMode.ANY,
        target=Target(role="worker"), payload={"do": "it"},
    )
    assert out.delivered_to == []


@pytest.mark.asyncio
async def test_specific_dispatch_to_a_paused_participant_is_refused(
    store: PersistenceStore,
) -> None:
    sess = await _session(_registry(store))
    a, _, b, _ = await _two(sess)
    lead = await _sender(sess)
    await sess.post_order(_order("halt", a.participant_id))

    with pytest.raises(ToolError) as exc:
        await sess.send(
            sender_id=lead.participant_id, dispatch=DispatchMode.SPECIFIC,
            target=Target(participant_ids=[b.participant_id, a.participant_id]),
            payload={"do": "it"},
        )
    assert exc.value.code is ErrorCode.PARTICIPANT_PAUSED
    assert sess._inflight == {}


# ---------- over the wire ----------


async def _join(c: httpx.AsyncClient, base: str, sid: str, jt: str, role: str) -> dict[str, Any]:
    r = await c.post(
        f"{base}/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": role}
    )
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_orders_pause_refuse_commands_and_land_in_history(tmp_path: Path) -> None:
    db = str(tmp_path / "relay.db")
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            made = r.json()
            sid, sup, jt = made["session_id"], made["supervisor_token"], made["join_token"]
            lead = await _join(c, base, sid, jt, "lead")
            worker = await _join(c, base, sid, jt, "worker")

            r = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes", headers=bearer(sup),
                json={"kind": "halt", "target": worker["participant_id"]},
            )
            assert r.status_code == 200, r.text
            assert r.json()["affected"] == [worker["participant_id"]]

            for body in (
                {"dispatch": "specific",
                 "target": {"participant_ids": [worker["participant_id"]]}},
                {"dispatch": "all", "target": {"role": "worker"}},
            ):
                rs = await c.post(
                    f"{base}/v1/sessions/{sid}/commands",
                    headers=bearer(lead["participant_token"]),
                    json={**body, "payload": {"n": 1}, "timeout_seconds": 30},
                )
                assert rs.status_code == 409, rs.text
                assert rs.json()["error"]["code"] == "participant_paused"

            roster = await c.get(
                f"{base}/v1/sessions/{sid}/participants", headers=bearer(jt)
            )
            assert roster.status_code == 200, roster.text
            states = {
                p["participant_id"]: p["supervision_state"]
                for p in roster.json()["roster"]
            }
            assert states[worker["participant_id"]] == "paused"

            # An order to one participant is private: supervision holders
            # and its target see it in history, a join token does not.
            for reader in (sup, worker["participant_token"]):
                ev = await c.get(
                    f"{base}/v1/sessions/{sid}/events", headers=bearer(reader),
                    params={"kind": "supervisor.note"},
                )
                assert ev.status_code == 200, ev.text
                (note,) = ev.json()["events"]
                assert note["payload"]["kind"] == "halt"
                assert note["payload"]["target"] == worker["participant_id"]
            hidden = await c.get(
                f"{base}/v1/sessions/{sid}/events", headers=bearer(jt),
                params={"kind": "supervisor.note"},
            )
            assert hidden.status_code == 200, hidden.text
            assert hidden.json()["events"] == []


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_post_order_needs_a_supervision_token() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(f"{base}/v1/sessions", json={})
            assert r.status_code == 201, r.text
            made = r.json()
            sid = made["session_id"]
            worker = await _join(c, base, sid, made["join_token"], "worker")

        async with sw_client(base) as mcp:
            out = await mcp.call_tool("session_post_order", {
                "session_id": sid, "token": made["supervisor_token"],
                "kind": "halt", "target": worker["participant_id"],
            })
            assert out["affected"] == [worker["participant_id"]]
            assert out["kind"] == "halt"

            with pytest.raises(RuntimeError, match="permission_denied"):
                await mcp.call_tool("session_post_order", {
                    "session_id": sid, "token": worker["participant_token"],
                    "kind": "note", "target": "all", "message": "hi",
                })
