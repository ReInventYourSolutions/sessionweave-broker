"""Every MCP tool must advertise a description.

An agent that discovers this relay through MCP sees only what
``tools/list`` returns. Twelve of the fourteen tools advertised an empty
description, so that agent got a list of names and nothing else — no way
to tell `session_send` from `session_respond`, or to learn that most
tools need a `session_join` first. This was reported from the field by an
agent that tried to use the relay and could not work out the tools.

The schema alone cannot carry it:

* the tools that operate on the caller's current session take **no**
  ``session_id`` — the connection is the session context — so nothing in
  their schema says a join must come first, or what happens without one;
* ``dispatch``, ``delivery_mode``, ``status`` and ``order`` are plain
  ``string`` in the schema, so their accepted values appear nowhere.

These tests pin the floor, not the prose: a description exists, it is
long enough to say something, connection-bound tools mention the join,
and the string-typed enums list their values.
"""
from __future__ import annotations

import asyncio
import inspect
from typing import Any
from unittest.mock import MagicMock

import pytest
from mcp.types import Tool

from sessionweave.mcp_adapter import HandleRegistry
from sessionweave.server import _build_fastmcp

# Tools that resolve their session from the connection's binding via
# ``tools._session_for_handle`` — i.e. they fail with ``not_in_session``
# unless ``session_join`` (or ``session_resume``) ran on this connection
# first. Kept explicit so adding a tool to that set forces a decision here.
CONNECTION_BOUND = {
    "session_check_inbox",
    "session_leave",
    "session_list_participants",
    "session_reply_to_note",
    "session_respond",
    "session_send",
    "session_update_purpose",
    "session_update_self",
    "session_wait_for_event",
}

# Parameters typed as a bare ``string`` in the JSON schema whose accepted
# values therefore have to live in the description.
ENUM_VALUES = {
    "session_send": ["specific", "all", "any"],
    "session_join": ["push", "pull"],
    "session_respond": ["ok", "error"],
    "session_get_events": ["asc", "desc"],
    "session_post_order": ["note", "halt", "continue", "redirect"],
}

MIN_DESCRIPTION_CHARS = 40


@pytest.fixture(scope="module")
def tools() -> dict[str, Tool]:
    mcp = _build_fastmcp(MagicMock(), HandleRegistry())
    return {t.name: t for t in asyncio.run(mcp.list_tools())}


def test_the_server_advertises_every_tool(tools: dict[str, Tool]) -> None:
    """Guards the fixture itself — an empty list would make the rest vacuous."""
    assert len(tools) == 21, sorted(tools)


def test_every_tool_has_a_description(tools: dict[str, Tool]) -> None:
    missing = sorted(n for n, t in tools.items() if not (t.description or "").strip())
    assert missing == [], f"tools advertised with no description: {missing}"


def test_every_description_says_something(tools: dict[str, Tool]) -> None:
    """A description exists but is three words long is barely better than none."""
    thin = sorted(
        f"{n} ({len((t.description or '').strip())} chars)"
        for n, t in tools.items()
        if len((t.description or "").strip()) < MIN_DESCRIPTION_CHARS
    )
    assert thin == [], f"descriptions too thin to be useful: {thin}"


@pytest.mark.parametrize("name", sorted(CONNECTION_BOUND))
def test_connection_bound_tools_mention_the_join(
    name: str, tools: dict[str, Tool]
) -> None:
    """These take no session_id, so the description is the only place an
    agent can learn that a join has to come first."""
    description = (tools[name].description or "").lower()
    assert "join" in description, (
        f"{name} resolves its session from the connection binding, but its "
        "description never mentions session_join — an agent has no way to "
        "learn the call order"
    )


@pytest.mark.parametrize("name,values", sorted(ENUM_VALUES.items()))
def test_string_typed_enums_list_their_values(
    name: str, values: list[str], tools: dict[str, Tool]
) -> None:
    description = tools[name].description or ""
    missing = [v for v in values if v not in description]
    assert missing == [], (
        f"{name}: accepted values {missing} appear neither in the JSON schema "
        "(the parameter is a bare string) nor in the description"
    )


def test_descriptions_reach_the_client_dedented(tools: dict[str, Tool]) -> None:
    """FastMCP takes ``fn.__doc__`` verbatim — ``description or fn.__doc__
    or ""`` — so a docstring's source indentation is baked into what the
    client renders.

    ``cleandoc`` is idempotent, so a description that already equals its
    own ``cleandoc`` is one that was cleaned. Deliberate inner alignment
    (the dispatch-mode block in session_send) survives that check; a
    whole docstring carrying its source indent does not.
    """
    dirty = sorted(
        name
        for name, t in tools.items()
        if (t.description or "") != inspect.cleandoc(t.description or "")
    )
    assert dirty == [], (
        f"descriptions carry source indentation to the client: {dirty}"
    )


def test_the_server_advertises_instructions() -> None:
    """The rules that span every tool — one connection is one session, join
    before the session_id-less tools — belong once at the server level
    rather than repeated in fourteen descriptions."""
    mcp = _build_fastmcp(MagicMock(), HandleRegistry())
    instructions: Any = mcp.instructions
    assert instructions and instructions.strip(), (
        "the server advertises no instructions"
    )
    assert "session_join" in instructions
