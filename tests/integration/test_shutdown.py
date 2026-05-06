from __future__ import annotations

import asyncio
import os
import signal
import subprocess
import sys

import httpx
import pytest

from tests.integration.harness import _free_port


@pytest.mark.integration
@pytest.mark.asyncio
async def test_sigterm_exits_cleanly() -> None:
    port = _free_port()
    env = {**os.environ, "SW_HOST": "127.0.0.1", "SW_PORT": str(port)}
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "sessionweave.main",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        for _ in range(50):
            try:
                async with httpx.AsyncClient(timeout=1) as c:
                    r = await c.get(f"http://127.0.0.1:{port}/healthz")
                    if r.status_code == 200:
                        break
            except Exception:
                pass
            await asyncio.sleep(0.1)
        else:
            stdout, stderr = proc.communicate(timeout=2)
            raise RuntimeError(
                f"relay did not become healthy. stdout={stdout!r} stderr={stderr!r}"
            )

        proc.send_signal(signal.SIGTERM)
        rc = proc.wait(timeout=10)
        # uvicorn runs lifespan shutdown on SIGTERM, then re-raises the
        # signal so the OS-level exit code is -SIGTERM. Treat both that
        # and a clean ``sys.exit(0)`` as graceful termination.
        assert rc in (0, -signal.SIGTERM), (
            f"unexpected exit code {rc}. stderr={proc.stderr.read()!r}"
        )
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=5)
