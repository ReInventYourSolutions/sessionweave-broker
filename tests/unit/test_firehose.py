"""Unit tests for the firehose subscriber registry."""
from __future__ import annotations

import asyncio

import pytest

from sessionweave.firehose import FirehoseRegistry, FirehoseSubscriber


@pytest.mark.asyncio
async def test_subscribe_and_receive():
    reg = FirehoseRegistry(per_session_cap=4, queue_capacity=8)
    sub = await reg.subscribe("sid-1")
    assert isinstance(sub, FirehoseSubscriber)
    await reg.publish("sid-1", {"name": "participant.joined", "payload": {"x": 1}})
    evt = await asyncio.wait_for(sub.queue.get(), timeout=0.5)
    assert evt == {"name": "participant.joined", "payload": {"x": 1}}


@pytest.mark.asyncio
async def test_publish_to_other_session_does_not_leak():
    reg = FirehoseRegistry(per_session_cap=4, queue_capacity=8)
    sub = await reg.subscribe("sid-1")
    await reg.publish("sid-2", {"name": "x", "payload": {}})
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(sub.queue.get(), timeout=0.05)


@pytest.mark.asyncio
async def test_per_session_subscriber_cap():
    reg = FirehoseRegistry(per_session_cap=2, queue_capacity=4)
    a = await reg.subscribe("sid-1")
    _ = await reg.subscribe("sid-1")
    with pytest.raises(RuntimeError, match="firehose_cap"):
        await reg.subscribe("sid-1")
    await reg.unsubscribe(a)
    c = await reg.subscribe("sid-1")
    assert c is not None


@pytest.mark.asyncio
async def test_drop_oldest_on_overflow_emits_lag_marker():
    reg = FirehoseRegistry(per_session_cap=4, queue_capacity=2)
    sub = await reg.subscribe("sid-1")
    for i in range(5):
        await reg.publish("sid-1", {"name": "evt", "payload": {"i": i}})
    received = []
    while not sub.queue.empty():
        received.append(sub.queue.get_nowait())
    names = [e["name"] for e in received]
    assert "firehose.lagged" in names
