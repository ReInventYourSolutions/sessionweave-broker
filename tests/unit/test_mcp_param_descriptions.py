"""Every MCP tool parameter must say what it is.

Tool-level descriptions were added earlier; parameter-level ones were
not, and all 53 parameters reached clients with nothing but a type. An
agent saw `target: object`, `payload: object`, `dispatch: string` and had
to guess — the tool description covered some of it in prose, but a
client that renders per-argument help, or a model filling arguments one
at a time, gets nothing from prose three paragraphs away.

The string-valued enums also become real JSON-schema `enum`s: a model
that sees the allowed values in the schema does not have to find them in
a description, and a wrong value is refused before it reaches the relay.

The required set is pinned so this change cannot quietly make an
optional argument mandatory, or the reverse.
"""
from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import MagicMock

import pytest

from sessionweave.mcp_adapter import HandleRegistry
from sessionweave.server import _build_fastmcp

# Snapshot taken before the change. Descriptions must not move these.
REQUIRED = {
    "session_create": [],
    "session_describe": ["session_id"],
    "session_update_purpose": ["session_id"],
    "session_join": ["session_id"],
    "session_resume": ["participant_token", "session_id"],
    "session_leave": [],
    "session_update_self": [],
    "session_list_participants": [],
    "session_check_inbox": [],
    "session_wait_for_event": ["session_id"],
    "session_send": ["dispatch", "payload", "target"],
    "session_respond": ["command_id", "payload"],
    "session_delete": ["session_id"],
    "session_get_events": ["session_id"],
    "session_mint_join_token": ["session_id"],
    "session_mint_supervisor_token": ["session_id"],
    "session_list_supervisor_tokens": ["session_id"],
    "session_revoke_supervisor_token": ["session_id", "token_id"],
    "session_post_order": ["kind", "session_id", "target"],
    "session_remove_participant": ["participant_id", "session_id"],
    "session_reply_to_note": ["message", "note_id"],
    "session_set_coordinator": ["participant_id", "session_id"],
}

# String parameters with a closed set of values.
ENUMS = {
    ("session_send", "dispatch"): {"specific", "all", "any"},
    ("session_join", "delivery_mode"): {"push", "pull"},
    ("session_respond", "status"): {"ok", "error"},
    ("session_get_events", "order"): {"asc", "desc"},
    ("session_post_order", "kind"): {"note", "halt", "continue", "redirect", "undo"},
}

MIN_CHARS = 15


@pytest.fixture(scope="module")
def schemas() -> dict[str, dict[str, Any]]:
    mcp = _build_fastmcp(MagicMock(), HandleRegistry())
    return {t.name: t.input_schema for t in asyncio.run(mcp.list_tools())}


def _props(schema: dict[str, Any]) -> dict[str, Any]:
    return schema.get("properties", {})  # type: ignore[no-any-return]


def test_every_parameter_is_described(schemas: dict[str, dict[str, Any]]) -> None:
    missing = [
        f"{tool}.{name}"
        for tool, schema in sorted(schemas.items())
        for name, prop in _props(schema).items()
        if not str(prop.get("description", "")).strip()
    ]
    assert missing == [], f"{len(missing)} parameters with no description: {missing}"


def test_every_description_says_something(schemas: dict[str, dict[str, Any]]) -> None:
    thin = [
        f"{tool}.{name}"
        for tool, schema in sorted(schemas.items())
        for name, prop in _props(schema).items()
        if 0 < len(str(prop.get("description", "")).strip()) < MIN_CHARS
    ]
    assert thin == [], f"descriptions too thin to help: {thin}"


@pytest.mark.parametrize(("key", "values"), sorted(ENUMS.items()))
def test_closed_string_parameters_are_real_enums(
    key: tuple[str, str], values: set[str], schemas: dict[str, dict[str, Any]]
) -> None:
    tool, name = key
    prop = _props(schemas[tool])[name]
    found: set[str] = set(prop.get("enum", []))
    for branch in prop.get("anyOf", []):
        found |= set(branch.get("enum", []))
    assert found == values, f"{tool}.{name} enum is {sorted(found)}, want {sorted(values)}"


@pytest.mark.parametrize("tool", sorted(REQUIRED))
def test_required_parameters_did_not_change(
    tool: str, schemas: dict[str, dict[str, Any]]
) -> None:
    assert sorted(schemas[tool].get("required", [])) == REQUIRED[tool]


def test_no_tool_was_lost(schemas: dict[str, dict[str, Any]]) -> None:
    assert sorted(schemas) == sorted(REQUIRED)
