import pytest

from sessionweave.notifications import (
    BufferOverflow,
    NotificationBuffer,
)


def test_enqueue_dequeue_fifo() -> None:
    buf = NotificationBuffer(cap=10)
    buf.enqueue({"a": 1})
    buf.enqueue({"a": 2})
    out = list(buf.drain())
    assert out == [{"a": 1}, {"a": 2}]


def test_drain_empties_buffer() -> None:
    buf = NotificationBuffer(cap=10)
    buf.enqueue({"x": 1})
    list(buf.drain())
    assert list(buf.drain()) == []


def test_overflow_raises() -> None:
    buf = NotificationBuffer(cap=2)
    buf.enqueue({"x": 1})
    buf.enqueue({"x": 2})
    with pytest.raises(BufferOverflow):
        buf.enqueue({"x": 3})


def test_zero_cap_means_no_buffering() -> None:
    buf = NotificationBuffer(cap=0)
    with pytest.raises(BufferOverflow):
        buf.enqueue({"x": 1})


def test_drop_predicate_removes_matching_items() -> None:
    buf = NotificationBuffer(cap=10)
    buf.enqueue({"event": "command.received", "command_id": "c1"})
    buf.enqueue({"event": "command.received", "command_id": "c2"})
    buf.enqueue({"event": "participant.joined"})
    buf.drop_where(
        lambda n: n.get("event") == "command.received" and n.get("command_id") == "c1"
    )
    assert [n.get("command_id") for n in buf.drain() if "command_id" in n] == ["c2"]


def test_size_reports_pending() -> None:
    buf = NotificationBuffer(cap=10)
    assert buf.size() == 0
    buf.enqueue({"x": 1})
    assert buf.size() == 1
