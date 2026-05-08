from __future__ import annotations

import json
from pathlib import Path

import pytest

from sessionweave.host_cli import _load_purpose, _parse_args


def test_purpose_inline_json() -> None:
    args = _parse_args(
        [
            "--my-role",
            "player1",
            "--purpose-json",
            '{"kind":"k","your_role":"O"}',
        ]
    )
    assert _load_purpose(args) == {"kind": "k", "your_role": "O"}


def test_purpose_from_file(tmp_path: Path) -> None:
    f = tmp_path / "p.json"
    f.write_text(json.dumps({"kind": "k", "instructions": "go"}))
    args = _parse_args(
        ["--my-role", "host", "--purpose-file", str(f)]
    )
    assert _load_purpose(args) == {"kind": "k", "instructions": "go"}


def test_purpose_can_be_omitted() -> None:
    args = _parse_args(["--my-role", "host"])
    assert _load_purpose(args) is None


def test_inline_and_file_are_mutually_exclusive() -> None:
    with pytest.raises(SystemExit):
        _parse_args(
            [
                "--my-role",
                "host",
                "--purpose-json",
                "{}",
                "--purpose-file",
                "x.json",
            ]
        )


def test_default_delivery_mode_is_pull() -> None:
    args = _parse_args(["--my-role", "host"])
    assert args.delivery_mode == "pull"


def test_no_poller_flag() -> None:
    args = _parse_args(["--my-role", "host", "--no-poller"])
    assert args.no_poller is True
