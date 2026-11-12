"""host_cli -> sessionweave-join -> poller against a real relay subprocess."""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from tests.integration.client import sw_client
from tests.integration.harness import relay_subprocess


def _run(*args: str, timeout: float = 60) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, "-m", *args],
        capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL,
    )


def _wait_for(path: Path, needle: str, secs: float = 20) -> str:
    deadline = time.time() + secs
    while time.time() < deadline:
        if path.exists() and needle in path.read_text():
            return path.read_text()
        time.sleep(0.3)
    return path.read_text() if path.exists() else ""


@pytest.mark.integration
@pytest.mark.asyncio
async def test_host_then_join_describe_then_poll(tmp_path: Path) -> None:
    async with relay_subprocess() as base:
        log = tmp_path / "ev.log"
        host = _run(
            "sessionweave.host_cli", "--relay-url", f"{base}/mcp", "--my-role", "host",
            "--poller-log", str(log), "--poller-interval", "0.3",
        )
        assert host.returncode == 0, host.stderr
        out = json.loads(host.stdout)
        try:
            for k in ("invite_link", "invite_url", "supervisor_token", "creator_token",
                      "my_participant_id", "my_participant_token", "who_gets_what"):
                assert out[k], k
            assert out["my_participant_token"] != out["my_participant_id"]

            # sessionweave-join: with the link's jt works; without it exits 3.
            ok = _run("sessionweave.launch", out["invite_link"], "--json")
            assert ok.returncode == 0, ok.stderr
            assert json.loads(ok.stdout)["session_id"] == out["session_id"]
            bare = out["invite_link"].split("?")[0]
            no_jt = _run("sessionweave.launch", bare)
            assert no_jt.returncode == 3
            assert "missing its join token" in no_jt.stderr
            bad = _run("sessionweave.launch", bare + "?jt=not-a-real-token")
            assert bad.returncode == 4

            # Poller (spawned with the secret token) sees a peer's join.
            assert "poller-start" in _wait_for(log, "poller-start")
            link_jt = out["invite_link"].split("jt=")[1].split("&")[0]
            async with sw_client(base) as peer:
                await peer.call_tool(
                    "session_join",
                    {"session_id": out["session_id"], "join_token": link_jt, "role": "guest"},
                )
                text = _wait_for(log, "participant.joined")
            assert "participant.joined" in text
            assert out["my_participant_token"] not in text
            assert '"error"' not in text
        finally:
            # Stop only the poller this test spawned, never someone else's.
            pid = out.get("poller_pid")
            if pid:
                try:
                    os.kill(int(pid), signal.SIGTERM)
                except ProcessLookupError:
                    pass
