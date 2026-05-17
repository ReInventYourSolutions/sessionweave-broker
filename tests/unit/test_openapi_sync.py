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
