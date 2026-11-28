"""Guards that the skill bundled into the broker package does not drift from
the canonical skill in this repo (``skills/sessionweave-participant/``).

The broker ships its own copy under ``src/sessionweave/_data/skill.md`` so a
``pip install sessionweave-broker`` is self-contained and offline (mirrors how
``openapi.yaml`` is shipped). This test keeps the two byte-identical. It skips
when the canonical source is absent (e.g. an sdist built without ``skills/``).
"""
from __future__ import annotations

from pathlib import Path

import pytest


def _broker_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def test_bundled_skill_matches_canonical_source() -> None:
    bundled = _broker_root() / "src" / "sessionweave" / "_data" / "skill.md"
    canonical = _broker_root() / "skills" / "sessionweave-participant" / "SKILL.md"
    if not canonical.exists():
        pytest.skip("canonical skill source not present (isolated build)")
    assert bundled.read_text(encoding="utf-8") == canonical.read_text(
        encoding="utf-8"
    ), (
        "broker bundled skill drifted from skills/sessionweave-participant/"
        "SKILL.md. Re-copy the canonical skill into src/sessionweave/_data/."
    )


def test_skill_states_the_note_reply_and_leave_rules() -> None:
    """Live-test findings: reply only when needed, never leave once removed."""
    text = (
        _broker_root() / "src" / "sessionweave" / "_data" / "skill.md"
    ).read_text(encoding="utf-8")
    assert "Do not reply to `halt`, `continue` or `redirect` just to acknowledge" in text
    assert "Always reply to a private note" in text
    assert "reaches every participant" in text
    assert "except when you were removed" in text.lower()
    assert "a wait in progress returns `session.ended`" in text


def test_skill_names_every_registered_mcp_tool() -> None:
    """Every tool the broker registers is mentioned in the bundled skill."""
    import asyncio
    from unittest.mock import MagicMock

    from sessionweave.mcp_adapter import HandleRegistry
    from sessionweave.server import _build_fastmcp

    text = (
        _broker_root() / "src" / "sessionweave" / "_data" / "skill.md"
    ).read_text(encoding="utf-8")
    tools = asyncio.run(_build_fastmcp(MagicMock(), HandleRegistry()).list_tools())
    missing = sorted(t.name for t in tools if f"`{t.name}`" not in text)
    assert missing == [], f"tools the skill never names: {missing}"
