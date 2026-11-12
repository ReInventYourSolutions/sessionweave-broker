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


def test_spawn_poller_passes_token_via_env_not_participant_id_credential(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from sessionweave import host_cli

    captured: dict = {}

    class P:
        pid = 42

    def fake_popen(cmd, **kw):
        captured["cmd"] = cmd
        captured["env"] = kw.get("env")
        return P()

    monkeypatch.setattr(host_cli.subprocess, "Popen", fake_popen)
    pid = host_cli._spawn_poller("http://x/mcp", "sid", "TOK", "PID", tmp_path / "l.log", 1.0)
    assert pid == 42
    assert "TOK" not in captured["cmd"]
    assert captured["env"]["SW_PARTICIPANT_TOKEN"] == "TOK"
    assert captured["cmd"][captured["cmd"].index("--participant-id") + 1] == "PID"


def test_hand_back_labels_every_credential() -> None:
    from sessionweave.host_cli import _hand_back

    out = _hand_back(
        {
            "session_id": "s", "invite_link": "swvs://h/s?jt=j", "invite_url": "https://h/i/s?jt=j",
            "supervisor_token": "sup", "creator_token": "cr",
        },
        "pid", "ptok", 7, "/tmp/l",
    )
    assert out["invite_link"] == "swvs://h/s?jt=j"
    assert out["invite_url"] == "https://h/i/s?jt=j"
    assert out["supervisor_token"] == "sup"
    assert out["creator_token"] == "cr"
    assert out["my_participant_id"] == "pid"
    assert out["my_participant_token"] == "ptok"
    assert out["poller_pid"] == 7
    assert "who_gets_what" in out
