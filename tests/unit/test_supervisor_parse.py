"""Unit tests for the supervision order model (kinds, message rules, states)."""
from __future__ import annotations

import pytest

from sessionweave.supervisor import (
    ORDER_KINDS,
    NoteRecord,
    SupervisorRequestError,
    next_state,
    parse_supervisor_request,
)


def test_order_kinds():
    assert ORDER_KINDS == frozenset({"note", "halt", "continue", "redirect", "undo"})


def test_note_without_message_rejected():
    with pytest.raises(SupervisorRequestError):
        parse_supervisor_request({"kind": "note", "target": "all"})


def test_redirect_with_empty_message_rejected():
    with pytest.raises(SupervisorRequestError):
        parse_supervisor_request({"kind": "redirect", "target": "all", "message": ""})


@pytest.mark.parametrize("kind", ["note", "redirect"])
@pytest.mark.parametrize("blank", [" ", "  \n\t "])
def test_whitespace_only_message_rejected_like_empty(kind, blank):
    with pytest.raises(SupervisorRequestError, match="message is required"):
        parse_supervisor_request({"kind": kind, "target": "all", "message": blank})


@pytest.mark.parametrize("kind", ["halt", "continue"])
def test_halt_continue_message_optional(kind):
    order = parse_supervisor_request({"kind": kind, "target": "all"})
    assert order.kind == kind
    assert order.message is None


@pytest.mark.parametrize("message", [None, "", "   "])
def test_undo_requires_a_message(message):
    body = {"kind": "undo", "target": "all"}
    if message is not None:
        body["message"] = message
    with pytest.raises(SupervisorRequestError, match="message is required"):
        parse_supervisor_request(body)


def test_undo_with_message_parses():
    order = parse_supervisor_request(
        {"kind": "undo", "target": "p1", "message": "revert the migration"}
    )
    assert (order.kind, order.message) == ("undo", "revert the migration")


def test_note_record_fields():
    rec = NoteRecord(note_id="sn-1", kind="note", target="all", ts_ms=5)
    assert (rec.note_id, rec.kind, rec.target, rec.ts_ms) == ("sn-1", "note", "all", 5)


@pytest.mark.parametrize(
    ("kind", "current", "expected"),
    [
        ("halt", "active", "paused"),
        ("undo", "active", "paused"),
        ("undo", "paused", "paused"),
        ("halt", "paused", "paused"),
        ("continue", "paused", "active"),
        ("continue", "active", "active"),
        ("redirect", "paused", "active"),
        ("redirect", "active", "active"),
        ("note", "paused", "paused"),
        ("note", "active", "active"),
    ],
)
def test_next_state(kind, current, expected):
    assert next_state(kind, current) == expected
