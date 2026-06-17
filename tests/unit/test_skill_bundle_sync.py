"""Guards that the skill bundled into the broker package does not drift from
the canonical skill at the repo root (``skills/sessionweave-participant/``).

The broker ships its own copy under ``src/sessionweave/_data/skill.md`` so a
``pip install sessionweave`` is self-contained and offline (mirrors how
``openapi.yaml`` is shipped). This test keeps the two byte-identical. It skips
when the canonical source is absent (e.g. broker built/tested in isolation
without the monorepo checkout).
"""
from __future__ import annotations

from pathlib import Path

import pytest


def _broker_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def test_bundled_skill_matches_canonical_source() -> None:
    bundled = _broker_root() / "src" / "sessionweave" / "_data" / "skill.md"
    canonical = (
        _broker_root().parent / "skills" / "sessionweave-participant" / "SKILL.md"
    )
    if not canonical.exists():
        pytest.skip("canonical skill source not present (isolated build)")
    assert bundled.read_text(encoding="utf-8") == canonical.read_text(
        encoding="utf-8"
    ), (
        "broker bundled skill drifted from skills/sessionweave-participant/"
        "SKILL.md. Re-copy the canonical skill into src/sessionweave/_data/."
    )
