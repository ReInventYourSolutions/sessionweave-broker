"""Note threads: participants reply under a supervisor note; the thread is flat.

A note to ``all`` is answered in the open (every participant but the replier,
plus supervision holders on the firehose); a note to one participant is
answered privately (firehose only, and only by that participant).
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
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from sessionweave.session import Session
from sessionweave.supervisor import SupervisorNote
from tests.integration.client import bearer, sw_client
from tests.integration.harness import relay_subprocess
from tests.unit.fakes import FakeHandle


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "threads.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


async def _session(store: PersistenceStore, firehose: FirehoseRegistry) -> Session:
    reg = SessionRegistry(
        defaults=load_deployment_defaults({}), connections=ConnectionManager(),
        metrics=None, firehose=firehose, store=store,
    )
    sid, _, _, _, _ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_token(generate_token()),
    )
    return await reg.get_or_raise(sid)


def _note(target: str) -> SupervisorNote:
    return SupervisorNote(
        note_id=f"sn-{target[-6:]}", kind="note", target=target,
        message="how is it going?", ts_ms=1_700_000_000_000,
    )


def _replies(handle: FakeHandle) -> list[dict[str, Any]]:
    return [p for n, p in handle.sent if n == "supervisor.note_reply"]


async def _setup(store: PersistenceStore):
    firehose = FirehoseRegistry()
    sess = await _session(store, firehose)
    ha, hb = FakeHandle(), FakeHandle()
    a = await sess.join(ha, role="worker", capabilities=[])
    b = await sess.join(hb, role="worker", capabilities=[])
    sub = await firehose.subscribe(sess.session_id)
    return sess, firehose, sub, a, ha, b, hb


def _drain(sub: Any) -> list[str]:
    names = []
    while not sub.queue.empty():
        names.append(sub.queue.get_nowait()["name"])
    return names


@pytest.mark.asyncio
async def test_broadcast_reply_reaches_the_others_and_the_firehose(
    store: PersistenceStore,
) -> None:
    sess, _, sub, a, ha, _b, hb = await _setup(store)
    await sess.post_order(_note("all"))
    ha.sent.clear()
    hb.sent.clear()
    await asyncio.gather(*sess._firehose_tasks)
    _drain(sub)

    out = await sess.reply_to_note(a.participant_id, "sn-all", "on track")

    assert out["note_id"] == "sn-all" and out["participant_id"] == a.participant_id
    assert out["message"] == "on track"
    assert out["reply_id"].startswith("nr-") and len(out["reply_id"]) == 15
    assert _replies(hb) == [out]
    assert _replies(ha) == []
    await asyncio.gather(*sess._firehose_tasks)
    assert _drain(sub) == ["supervisor.note_reply"]


@pytest.mark.asyncio
async def test_private_reply_goes_to_the_firehose_only(store: PersistenceStore) -> None:
    sess, _, sub, a, ha, _b, hb = await _setup(store)
    await sess.post_order(_note(a.participant_id))
    ha.sent.clear()
    hb.sent.clear()
    await asyncio.gather(*sess._firehose_tasks)
    _drain(sub)

    await sess.reply_to_note(a.participant_id, "sn-" + a.participant_id[-6:], "blocked")

    assert _replies(ha) == [] and _replies(hb) == []
    await asyncio.gather(*sess._firehose_tasks)
    assert _drain(sub) == ["supervisor.note_reply"]


@pytest.mark.asyncio
async def test_only_the_addressed_participant_replies_privately(
    store: PersistenceStore,
) -> None:
    sess, _, _, a, _, b, _ = await _setup(store)
    await sess.post_order(_note(a.participant_id))

    with pytest.raises(ToolError) as exc:
        await sess.reply_to_note(b.participant_id, "sn-" + a.participant_id[-6:], "hi")
    assert exc.value.code is ErrorCode.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_unknown_note_and_bad_messages_are_refused(store: PersistenceStore) -> None:
    sess, _, _, a, _, _, _ = await _setup(store)
    await sess.post_order(_note("all"))

    with pytest.raises(ToolError) as exc:
        await sess.reply_to_note(a.participant_id, "sn-nope", "hi")
    assert exc.value.code is ErrorCode.NOTE_NOT_FOUND
    for bad in ("", "x" * 2049):
        with pytest.raises(ToolError) as exc:
            await sess.reply_to_note(a.participant_id, "sn-all", bad)
        assert exc.value.code is ErrorCode.INVALID_REQUEST


@pytest.mark.asyncio
async def test_a_broadcast_thread_outlives_whoever_left(store: PersistenceStore) -> None:
    sess, _, _, a, _, b, hb = await _setup(store)
    await sess.post_order(_note("all"))
    await sess.leave(a.participant_id)
    hb.sent.clear()

    out = await sess.reply_to_note(b.participant_id, "sn-all", "still here")

    assert out["participant_id"] == b.participant_id


@pytest.mark.asyncio
async def test_private_threads_stay_private_in_cold_history(
    store: PersistenceStore,
) -> None:
    """History of a session not in RAM filters private threads too."""
    from sessionweave import tools

    firehose = FirehoseRegistry()
    reg = SessionRegistry(
        defaults=load_deployment_defaults({}), connections=ConnectionManager(),
        metrics=None, firehose=firehose, store=store,
    )
    creator = generate_token()
    sid, _, _, _, _ = await reg.create(
        overrides={}, purpose=None,
        persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600),
        creator_token_hash=hash_token(creator),
    )
    sess = await reg.get_or_raise(sid)
    a = await sess.join(FakeHandle(), role="worker", capabilities=[])
    b = await sess.join(FakeHandle(), role="worker", capabilities=[])
    await sess.post_order(_note("all"))
    await sess.post_order(_note(a.participant_id))
    await sess.reply_to_note(a.participant_id, "sn-all", "open answer")
    await sess.reply_to_note(a.participant_id, "sn-" + a.participant_id[-6:], "secret")
    await asyncio.gather(*sess._firehose_tasks)
    reg._sessions.pop(sid)  # cold: answered from disk only

    async def messages(token: str | None) -> list[str]:
        out = await tools.session_get_events(
            tools.ToolDeps(reg),
            {"session_id": sid, "access_token": token,
             "kind": ["supervisor.note", "supervisor.note_reply"]},
        )
        assert out["session_state"] == "cold"
        return [e["payload"]["message"] for e in out["events"]]

    everything = ["how is it going?", "how is it going?", "open answer", "secret"]
    assert await messages(creator) == everything
    assert await messages(a.issued_token) == everything
    assert await messages(b.issued_token) == ["how is it going?", "open answer"]


# ---------- over the wire ----------


async def _join(c: httpx.AsyncClient, base: str, sid: str, jt: str) -> dict[str, Any]:
    r = await c.post(
        f"{base}/v1/sessions/{sid}/participants", headers=bearer(jt), json={"role": "worker"}
    )
    assert r.status_code == 201, r.text
    out: dict[str, Any] = r.json()
    return out


@pytest.mark.integration
@pytest.mark.asyncio
async def test_http_replies_and_history(tmp_path: Path) -> None:
    db = str(tmp_path / "relay.db")
    async with relay_subprocess(persistence_db_path=db) as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            made = r.json()
            sid, sup, jt = made["session_id"], made["supervisor_token"], made["join_token"]
            a = await _join(c, base, sid, jt)
            b = await _join(c, base, sid, jt)
            ta, tb = a["participant_token"], b["participant_token"]

            posted = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes", headers=bearer(sup),
                json={"kind": "note", "target": a["participant_id"], "message": "status?"},
            )
            assert posted.status_code == 200, posted.text
            nid = posted.json()["note_id"]
            url = f"{base}/v1/sessions/{sid}/supervisor/notes/{nid}/replies"

            ok = await c.post(url, headers=bearer(ta), json={"message": "fine"})
            assert ok.status_code == 201, ok.text
            assert ok.json()["note_id"] == nid

            denied = await c.post(url, headers=bearer(tb), json={"message": "me too"})
            assert denied.status_code == 403, denied.text
            missing = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes/sn-nope/replies",
                headers=bearer(ta), json={"message": "hi"},
            )
            assert missing.status_code == 404, missing.text
            assert missing.json()["error"]["code"] == "note_not_found"
            empty = await c.post(url, headers=bearer(ta), json={"message": ""})
            assert empty.status_code == 400, empty.text
            unauth = await c.post(url, json={"message": "x"})
            assert unauth.status_code == 401

            # A private thread is for supervision holders and the addressee:
            # the join token and the other participant see neither the note
            # nor the reply in history.
            kinds = {"kind": "supervisor.note,supervisor.note_reply"}
            for outsider in (jt, tb):
                ev = await c.get(
                    f"{base}/v1/sessions/{sid}/events", headers=bearer(outsider),
                    params=kinds,
                )
                assert ev.status_code == 200, ev.text
                assert ev.json()["events"] == []
            for insider in (sup, ta):
                ev = await c.get(
                    f"{base}/v1/sessions/{sid}/events", headers=bearer(insider),
                    params=kinds,
                )
                assert ev.status_code == 200, ev.text
                got = [(e["kind"], e["payload"]["message"]) for e in ev.json()["events"]]
                assert got == [
                    ("supervisor.note", "status?"), ("supervisor.note_reply", "fine"),
                ]


@pytest.mark.integration
@pytest.mark.asyncio
async def test_mcp_reply_with_explicit_identity() -> None:
    async with relay_subprocess() as base:
        async with httpx.AsyncClient(timeout=15.0) as c:
            made = (await c.post(f"{base}/v1/sessions", json={})).json()
            sid = made["session_id"]
            a = await _join(c, base, sid, made["join_token"])
            posted = await c.post(
                f"{base}/v1/sessions/{sid}/supervisor/notes",
                headers=bearer(made["supervisor_token"]),
                json={"kind": "note", "target": "all", "message": "hello"},
            )
            nid = posted.json()["note_id"]

        async with sw_client(base) as mcp:
            out = await mcp.call_tool("session_reply_to_note", {
                "note_id": nid, "message": "hi back",
                "session_id": sid, "participant_token": a["participant_token"],
            })
            assert out["note_id"] == nid and out["participant_id"] == a["participant_id"]
            with pytest.raises(RuntimeError, match="note_not_found"):
                await mcp.call_tool("session_reply_to_note", {
                    "note_id": "sn-nope", "message": "x",
                    "session_id": sid, "participant_token": a["participant_token"],
                })
