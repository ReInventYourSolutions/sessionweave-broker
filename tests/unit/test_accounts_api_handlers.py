"""PATCH batch-failure handling, exercised in-process."""
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.applications import Starlette
from starlette.testclient import TestClient

from sessionweave import accounts_api
from sessionweave.accounts import AccountStore
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore


class _Registry:
    def __init__(self) -> None:
        self.fail = True
        self.calls = 0

    async def suspend_owned(self, account_id: str) -> int:
        self.calls += 1
        if self.fail:
            raise ToolError(ErrorCode.PERSISTENCE_FAILED, "suspend failed for 1 of 2 sessions")
        return 2


@pytest.mark.asyncio
async def test_patch_disabled_batch_failure_keeps_status_and_retry_succeeds(
    tmp_path: Path,
) -> None:
    store = PersistenceStore(str(tmp_path / "t.db"))
    await store.init(pool_size=2)
    accounts = AccountStore(store)
    acc = await accounts.create("alice")
    reg = _Registry()
    deps: Any = SimpleNamespace(
        accounts=accounts, registry=reg, admin_token="adm",
        auth_limiter=SimpleNamespace(
            is_throttled=lambda *a: False, record_failure=lambda *a: None
        ),
    )
    app = Starlette(routes=accounts_api.routes())
    app.state.tool_deps = deps
    h = {"Authorization": "Bearer adm"}
    with TestClient(app) as c:
        r = c.patch(f"/v1/accounts/{acc.account_id}", json={"status": "disabled"}, headers=h)
        assert r.status_code == 503
        assert r.json()["error"]["code"] == "persistence_failed"
        assert (await accounts.get(acc.account_id)).status == "disabled"
        reg.fail = False
        r = c.patch(f"/v1/accounts/{acc.account_id}", json={"status": "disabled"}, headers=h)
        assert r.status_code == 200 and r.json()["sessions_affected"] == 2
        assert reg.calls == 2
    await store.close()
