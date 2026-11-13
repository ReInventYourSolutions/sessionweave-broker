"""A participant identity that does not depend on the connection.

The relay normally treats the MCP connection *as* the identity: a
participant is bound to it by `session_join`, and every later tool call
on that connection finds it. Some runtimes cannot work that way — they
open a fresh connection per tool call, so the binding never survives the
call that created it. Measured: `claude -p` does exactly this (a new TCP
connection per call, no `Mcp-Session-Id`), and so does anything on the
2026-07-28 single-exchange protocol era, which is the SDK's own
`LATEST_PROTOCOL_VERSION`.

For those clients, `session_resume` is not a workaround: it succeeds, and
the very next call — on a new connection — is out of session again.

So the identity can be presented explicitly instead, exactly as a REST
client presents its Bearer token. The pair is validated against the live
session and bound to an ephemeral handle for the duration of one call,
reusing `http_api._bind_participant`. Nothing is remembered, so it works
regardless of how the transport behaves.

These tests pin the tool surface. The end-to-end behaviour over three
separate MCP connections is covered in the integration suite.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest

from sessionweave.mcp_adapter import HandleRegistry
from sessionweave.server import _build_fastmcp

# Every tool that resolves its session from the connection binding.
CONNECTION_BOUND = [
    "session_check_inbox",
    "session_leave",
    "session_list_participants",
    "session_respond",
    "session_send",
    "session_update_self",
    "session_wait_for_event",
]


@pytest.fixture(scope="module")
def schemas() -> dict[str, Any]:
    mcp = _build_fastmcp(MagicMock(), HandleRegistry())
    return {t.name: t.input_schema for t in asyncio.run(mcp.list_tools())}


@pytest.mark.parametrize("name", CONNECTION_BOUND)
def test_connection_bound_tool_accepts_an_explicit_participant(
    name: str, schemas: dict[str, Any]
) -> None:
    """Without this a one-call-per-connection client cannot participate
    at all — not merely inconveniently: no configuration fixes it,
    because the binding cannot outlive the call that made it."""
    props = schemas[name].get("properties", {})
    assert "participant_token" in props, (
        f"{name} has no way to be told who is calling when the connection "
        f"carries no binding: {sorted(props)}"
    )


@pytest.mark.parametrize("name", CONNECTION_BOUND)
def test_explicit_participant_needs_a_session_to_be_checked_against(
    name: str, schemas: dict[str, Any]
) -> None:
    """The token is validated against a named session's live roster —
    the relay keeps no participant_id -> session index — so the pair must
    travel together."""
    props = schemas[name].get("properties", {})
    assert "session_id" in props, (
        f"{name} takes participant_token but no session_id to validate it "
        f"against: {sorted(props)}"
    )


@pytest.mark.parametrize("name", CONNECTION_BOUND)
def test_the_explicit_pair_stays_optional(
    name: str, schemas: dict[str, Any]
) -> None:
    """A client that holds its connection open must keep working
    unchanged — the connection binding still wins when there is one."""
    required = schemas[name].get("required", [])
    assert "participant_token" not in required, f"{name}: made mandatory"
    if name == "session_wait_for_event":
        # Pre-existing: this one has always required session_id, unlike its
        # siblings. Left alone — relaxing it would break existing callers,
        # and a client using the explicit form passes it anyway.
        return
    assert "session_id" not in required, f"{name}: session_id made mandatory"


def test_the_ephemeral_bind_is_released_after_each_call() -> None:
    """The handle exists for one call only. Leaking it would leave a
    stale binding that a later caller could inherit."""
    src = inspect.getsource(_build_fastmcp)
    assert "_unbind_participant" in src, "the ephemeral handle is never released"
    assert "finally:" in src, "release is not on a finally path"
