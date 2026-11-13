from __future__ import annotations

from pathlib import Path

import pytest

from sessionweave import poller
from sessionweave.poller import _is_terminal


def test_terminal_on_session_ended() -> None:
    assert _is_terminal(
        {"name": "session.ended", "payload": {"reason": "idle_ttl"}}, "any-pid"
    )


def test_terminal_on_own_left() -> None:
    own = "abc-123"
    assert _is_terminal(
        {"name": "participant.left", "payload": {"participant_id": own}}, own
    )


def test_not_terminal_on_other_left() -> None:
    own = "abc-123"
    assert not _is_terminal(
        {"name": "participant.left", "payload": {"participant_id": "other"}},
        own,
    )


def test_not_terminal_on_command_received() -> None:
    assert not _is_terminal(
        {"name": "command.received", "payload": {"command_id": "x"}}, "pid"
    )



def test_args_take_participant_token_and_id() -> None:
    args = poller._parse_args(
        ["--session-id", "s", "--participant-token", "tok", "--participant-id", "pid"]
    )
    assert args.participant_token == "tok"
    assert args.participant_id == "pid"


def test_args_require_participant_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("SW_PARTICIPANT_TOKEN", raising=False)
    with pytest.raises(SystemExit):
        poller._parse_args(["--session-id", "s", "--participant-id", "pid"])


def test_args_token_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("SW_PARTICIPANT_TOKEN", "envtok")
    args = poller._parse_args(["--session-id", "s", "--participant-id", "pid"])
    assert args.participant_token == "envtok"


@pytest.mark.asyncio
async def test_run_resumes_with_token_only_and_never_logs_it(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    seen: list[tuple[str, str]] = []

    async def fake_drain(relay_url: str, session_id: str, participant_token: str):
        seen.append((session_id, participant_token))
        return [{"name": "participant.left", "payload": {"participant_id": "pid"}}]

    monkeypatch.setattr(poller, "_drain_once", fake_drain)
    log = tmp_path / "ev.log"
    rc = await poller.run("http://x/mcp", "sid", "SECRET-TOKEN", "pid", log, 0.01)
    assert rc == 0
    assert seen == [("sid", "SECRET-TOKEN")]
    assert "SECRET-TOKEN" not in log.read_text()
    assert "poller-stop" in log.read_text()
