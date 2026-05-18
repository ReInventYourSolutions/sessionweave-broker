"""Integration test: creator_token rate limiter blocks correct token after 5 bad attempts.

The relay's DeleteRateLimiter allows max_failures=5 bad DELETE attempts per
session within a 60-second window. After 5 wrong-token 403s, any further DELETE
attempt — even with the CORRECT creator_token — is rejected with 403
(rate-limited) and does NOT delete the session.
"""
from __future__ import annotations

import httpx
import pytest

from tests.integration.harness import relay_subprocess


@pytest.mark.integration
@pytest.mark.asyncio
async def test_rate_limiter_blocks_correct_token_after_five_failures(
    tmp_path: object,
) -> None:
    db = str(tmp_path / "relay.db")  # type: ignore[operator]
    async with relay_subprocess(persistence_db_path=db) as base:
        # Use a long timeout because rate-limited responses sleep 0.5 s each
        async with httpx.AsyncClient(timeout=30.0) as c:
            # 1. Create persisted session — capture creator_token
            r = await c.post(
                f"{base}/v1/sessions",
                json={"persistence": {"mode": "ttl", "ttl_seconds": 3600}},
            )
            assert r.status_code == 201, r.text
            created = r.json()
            sid = created["session_id"]
            correct_token = created.get("creator_token")
            assert correct_token, "creator_token must be present in create response"

            # 2. Send 5 DELETE attempts with a wrong token → each must return 403
            for attempt in range(1, 6):
                r_bad = await c.delete(
                    f"{base}/v1/sessions/{sid}",
                    headers={"Authorization": "Bearer wrong-token-attempt"},
                )
                assert r_bad.status_code == 403, (
                    f"Attempt {attempt}: expected 403 for wrong token, "
                    f"got {r_bad.status_code}: {r_bad.text}"
                )

            # 3. 6th DELETE with the CORRECT creator_token → still 403 (rate-limited)
            r_rl = await c.delete(
                f"{base}/v1/sessions/{sid}",
                headers={"Authorization": f"Bearer {correct_token}"},
            )
            assert r_rl.status_code == 403, (
                f"6th attempt with correct token should be rate-limited (403), "
                f"got {r_rl.status_code}: {r_rl.text}"
            )

            # 4. Confirm session still exists (was NOT deleted)
            r_get = await c.get(f"{base}/v1/sessions/{sid}")
            assert r_get.status_code == 200, (
                f"Session should still exist after rate-limited delete, "
                f"got {r_get.status_code}: {r_get.text}"
            )
