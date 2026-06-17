"""Guards that the SDK source vendored into the broker package does not drift
from the canonical SDK at the repo root (``SDK/python/``).

The broker ships a copy under ``src/sessionweave/_data/sdk/`` so it can serve a
pip-installable tarball offline. This test keeps every vendored file
byte-identical to its canonical counterpart. It skips when the canonical source
is absent (e.g. broker built/tested in isolation).
"""
from __future__ import annotations

from pathlib import Path

import pytest


def _broker_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def test_vendored_sdk_matches_canonical_source() -> None:
    vendored_root = _broker_root() / "src" / "sessionweave" / "_data" / "sdk"
    canonical_root = _broker_root().parent / "SDK" / "python"
    if not canonical_root.exists():
        pytest.skip("canonical SDK source not present (isolated build)")

    def _is_source(p: Path) -> bool:
        rel_parts = p.relative_to(vendored_root).parts
        # Ignore caches / hidden tooling dirs (e.g. .ruff_cache) and bytecode.
        return (
            p.is_file()
            and p.suffix != ".pyc"
            and not any(part.startswith(".") or part == "__pycache__" for part in rel_parts)
        )

    vendored_files = [p for p in vendored_root.rglob("*") if _is_source(p)]
    assert vendored_files, "no vendored SDK files found"

    mismatches = []
    for p in vendored_files:
        rel = p.relative_to(vendored_root)
        canonical = canonical_root / rel
        if (
            not canonical.exists()
            or canonical.read_bytes() != p.read_bytes()
        ):
            mismatches.append(str(rel))

    assert not mismatches, (
        f"vendored SDK drifted from SDK/python for: {mismatches}. "
        f"Re-copy the canonical SDK source into src/sessionweave/_data/sdk/."
    )
