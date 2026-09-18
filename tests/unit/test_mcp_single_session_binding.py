"""One MCP connection may participate in at most one session at a time.

This is a documented invariant — `docs/superpowers/specs/2026-05-06-mcp-core-
session-relay-design.md` lists `already_in_session`, and
`skills/sessionweave-participant/SKILL.md` states "One MCP connection = one
session" — but it had no test. It is load-bearing: every other tool
(`session_send`, `session_check_inbox`, `session_respond`, ...) takes no
`session_id` and resolves the session from the connection's binding via
`_session_for_handle`. If a connection could hold two bindings, those tools
would have no way to say which session they meant.

Note the constraint is "at a time", not "ever": `session_leave` unbinds, so
the same connection can then join somewhere else. The last test pins that,
so a future tightening of the guard can't silently break reuse.

The HTTP API deliberately has no such limit — it binds a fresh ephemeral
handle per call (`http_api.py:_bind_participant`), so one HTTP client can
act in many sessions concurrently. That asymmetry is by design.
"""
from __future__ import annotations

from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.config import DeploymentDefaults, load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.persistence_token import DeleteRateLimiter
from sessionweave.registry import SessionRegistry
from sessionweave.tools import (
    ToolDeps,
    session_create,
    session_join,
    session_leave,
    session_resume,
)
from tests.unit.fakes import FakeHandle


def _defaults() -> DeploymentDefaults:
    return load_deployment_defaults({})


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "test.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest_asyncio.fixture
async def deps(store):
    defaults = _defaults()
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )
    return ToolDeps(
        registry=registry, bounds=defaults.bounds, metrics=None,
        delete_rate_limiter=DeleteRateLimiter(max_failures=5, window_seconds=60.0),
    )


async def _new_session(deps: ToolDeps) -> str:
    created = await session_create(deps, {})
    return str(created["session_id"])


@pytest.mark.asyncio
async def test_second_join_on_the_same_connection_is_rejected(deps) -> None:
    sid = await _new_session(deps)
    handle = FakeHandle()
    await session_join(deps, handle, {"session_id": sid, "role": "a"})

    with pytest.raises(ToolError) as exc:
        await session_join(deps, handle, {"session_id": sid, "role": "b"})
    assert exc.value.code is ErrorCode.ALREADY_IN_SESSION


@pytest.mark.asyncio
async def test_join_to_a_different_session_on_the_same_connection_is_rejected(
    deps,
) -> None:
    """The case that matters: the connection is the session context, so a
    second binding would make every session_id-less tool ambiguous."""
    sid_a = await _new_session(deps)
    sid_b = await _new_session(deps)
    handle = FakeHandle()
    await session_join(deps, handle, {"session_id": sid_a, "role": "a"})

    with pytest.raises(ToolError) as exc:
        await session_join(deps, handle, {"session_id": sid_b, "role": "a"})
    assert exc.value.code is ErrorCode.ALREADY_IN_SESSION


@pytest.mark.asyncio
async def test_resume_on_a_connection_already_in_a_session_is_rejected(
    deps,
) -> None:
    sid = await _new_session(deps)
    handle = FakeHandle()
    joined = await session_join(deps, handle, {"session_id": sid, "role": "a"})

    with pytest.raises(ToolError) as exc:
        await session_resume(deps, handle, {
            "session_id": sid,
            "participant_id": joined["participant_id"],
        })
    assert exc.value.code is ErrorCode.ALREADY_IN_SESSION


@pytest.mark.asyncio
async def test_a_second_connection_may_join_the_same_session(deps) -> None:
    """The limit is per connection, not per session — the whole point of
    the relay is several agents in one session."""
    sid = await _new_session(deps)
    a = await session_join(deps, FakeHandle(), {"session_id": sid, "role": "a"})
    b = await session_join(deps, FakeHandle(), {"session_id": sid, "role": "b"})
    assert a["participant_id"] != b["participant_id"]


@pytest.mark.asyncio
async def test_after_leave_the_same_connection_can_join_another_session(
    deps,
) -> None:
    """"One at a time", not "one ever" — leave unbinds the handle."""
    sid_a = await _new_session(deps)
    sid_b = await _new_session(deps)
    handle = FakeHandle()
    await session_join(deps, handle, {"session_id": sid_a, "role": "a"})
    await session_leave(deps, handle)

    rejoined = await session_join(deps, handle, {"session_id": sid_b, "role": "a"})
    assert rejoined["participant_id"]
