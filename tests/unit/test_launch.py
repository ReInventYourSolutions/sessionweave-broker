from __future__ import annotations

from sessionweave.invite import InviteLink
from sessionweave.launch import _human_summary


def test_human_summary_with_prescribed_role() -> None:
    inv = InviteLink(
        relay_url="http://localhost:8765/mcp",
        session_id="abc",
        purpose=None,
    )
    desc = {
        "session_id": "abc",
        "expires_at": "2026-05-07T12:00:00Z",
        "config": {"max_participants": 4},
        "roster": [{"online": True}, {"online": False}],
        "purpose": {
            "kind": "tic-tac-toe-v1",
            "your_role": "O",
            "your_capabilities": ["tictactoe-o"],
            "instructions": "X moves first.",
        },
    }
    out = _human_summary(inv, desc)
    assert "relay      : http://localhost:8765/mcp" in out
    assert "kind       : tic-tac-toe-v1" in out
    assert "your_role  : O  caps=['tictactoe-o']" in out
    assert "1 online / 2 total" in out
    assert "X moves first." in out


def test_human_summary_with_available_roles() -> None:
    inv = InviteLink("http://h:1/mcp", "sid-1", None)
    desc = {
        "session_id": "sid-1",
        "expires_at": "2026-05-07T12:00:00Z",
        "roster": [],
        "purpose": {
            "kind": "brainstorm-v1",
            "available_roles": [
                {"role": "moderator", "capabilities": ["mod"], "description": "Drives the agenda"},
                {"role": "contributor", "capabilities": ["c"], "description": "Brings ideas"},
            ],
        },
    }
    out = _human_summary(inv, desc)
    assert "available_roles:" in out
    assert "moderator" in out
    assert "Drives the agenda" in out


def test_human_summary_without_purpose() -> None:
    inv = InviteLink("http://h:1/mcp", "sid-2", None)
    desc: dict[str, object] = {
        "session_id": "sid-2",
        "expires_at": "2026-05-07T12:00:00Z",
        "roster": [],
        "purpose": None,
    }
    out = _human_summary(inv, desc)
    assert "kind       : (none)" in out


def test_missing_join_token_exits_3(capsys) -> None:
    from sessionweave.launch import main

    rc = main(["swv://localhost:8765/abc"])
    assert rc == 3
    assert (
        "invite link is missing its join token — ask the session creator for a new invite link"
        in capsys.readouterr().err
    )


def test_describe_uses_access_token(monkeypatch, capsys) -> None:
    from sessionweave import launch

    seen: dict = {}

    async def fake_describe(relay_url: str, session_id: str, join_token: str):
        seen["args"] = (relay_url, session_id, join_token)
        return {"expires_at": "x", "roster": [], "purpose": None}

    monkeypatch.setattr(launch, "_describe", fake_describe)
    rc = launch.main(["swv://localhost:8765/abc?jt=JT"])
    assert rc == 0
    assert seen["args"] == ("http://localhost:8765/mcp", "abc", "JT")


def test_invalid_token_is_nonzero_with_relay_message(monkeypatch, capsys) -> None:
    from sessionweave import launch

    async def fake_describe(relay_url, session_id, join_token):
        raise RuntimeError("session_describe error: join token expired")

    monkeypatch.setattr(launch, "_describe", fake_describe)
    rc = launch.main(["swv://localhost:8765/abc?jt=JT"])
    assert rc == 4
    assert "join token expired" in capsys.readouterr().err
