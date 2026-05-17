"""Guards that the OpenAPI spec stays in sync with the package version.

Uses only stdlib (tomllib + regex) so the test runs without any optional
dev dep — the openapi.yaml is hand-parsed for the single `version:` line
under `info:` rather than fully parsed.
"""
from __future__ import annotations

import re
import tomllib
from pathlib import Path


def _project_root() -> Path:
    return Path(__file__).resolve().parent.parent.parent


def _openapi_info_version(text: str) -> str:
    """Extract `info.version` from a YAML document via regex.

    Looks for the first `version: "X.Y.Z"` line that appears anywhere
    after an `info:` line and before the next top-level key.
    """
    m = re.search(
        r"(?ms)^info:\s*$.*?^\s+version:\s*\"([^\"]+)\"",
        text,
    )
    assert m is not None, "openapi.yaml has no info.version line"
    return m.group(1)


def test_openapi_yaml_info_version_matches_pyproject() -> None:
    root = _project_root()
    pyproject = tomllib.loads((root / "pyproject.toml").read_text())
    pyproject_version = pyproject["project"]["version"]

    openapi_text = (root / "src" / "sessionweave" / "openapi.yaml").read_text()
    openapi_version = _openapi_info_version(openapi_text)

    assert openapi_version == pyproject_version, (
        f"openapi.yaml info.version ({openapi_version!r}) drifted from "
        f"pyproject.toml version ({pyproject_version!r}). Bump both together."
    )


def test_openapi_session_config_schema_lists_every_config_field() -> None:
    """Every field on SessionConfig (config.py) must appear in the
    SessionConfig component in openapi.yaml. Prevents the failure mode
    we hit in v0.13.5 where new fields were added to config without
    being surfaced to API consumers via the public spec.
    """
    from dataclasses import fields

    from sessionweave.config import SessionConfig

    root = _project_root()
    openapi_text = (root / "src" / "sessionweave" / "openapi.yaml").read_text()

    # Grab the SessionConfig schema block (up to the next sibling schema).
    block = re.search(
        r"(?ms)^    SessionConfig:\s*$.*?(?=^    [A-Z][A-Za-z0-9]+:\s*$)",
        openapi_text,
    )
    assert block is not None, "openapi.yaml has no SessionConfig schema component"

    schema_text = block.group(0)
    missing = [
        f.name
        for f in fields(SessionConfig)
        if not re.search(rf"^\s+{re.escape(f.name)}\s*:", schema_text, re.MULTILINE)
    ]
    assert not missing, (
        f"openapi.yaml SessionConfig schema is missing fields: {missing}. "
        f"Every dataclass field on config.SessionConfig must be documented."
    )
