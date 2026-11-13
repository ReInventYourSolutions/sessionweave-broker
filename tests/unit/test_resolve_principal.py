from __future__ import annotations

import time
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.access import (
    AuthFailureLimiter,
    Principal,
    generate_token,
    hash_token,
    require,
    resolve_principal,
)
from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore
from sessionweave.registry import SessionRegistry
from sessionweave.schemas import PersistenceConfig
from sessionweave.session import Session
from sessionweave.tools import ToolDeps

ADMIN = "relay-admin-secret"
EXPIRED_MSG = "join link expired — ask the session creator for a new invite link"


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


def _hot(deps: ToolDeps, sid: str) -> Session:
    sess = deps.registry.get(sid)
    assert sess is not None
    return sess


def _deps(store, *, admin_token=ADMIN, limiter=None) -> ToolDeps:
    defaults = load_deployment_defaults({})
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(),
        metrics=None, firehose=None, store=store,
    )
    return ToolDeps(
        registry, bounds=defaults.bounds, auth_limiter=limiter, admin_token=admin_token
    )


async def _session(deps: ToolDeps, **kw) -> tuple[str, str]:
    creator = generate_token()
    sid, *_ = await deps.registry.create(
        overrides={}, creator_token_hash=hash_token(creator), **kw
    )
    return sid, creator


@pytest.mark.asyncio
async def test_resolves_each_role_on_hot_session(store) -> None:
    deps = _deps(store)
    sid, creator = await _session(deps)
    auth = _hot(deps, sid).auth
    now = int(time.time() * 1000)
    join, _ = auth.mint_join(now)
    sup, _ = auth.mint_supervisor(now)
    ptok = generate_token()
    auth.register_participant("p1", hash_token(ptok))

    assert await resolve_principal(deps, sid, ADMIN) == Principal("admin")
    assert await resolve_principal(deps, sid, creator) == Principal("creator")
    assert await resolve_principal(deps, sid, sup) == Principal("supervisor")
    assert await resolve_principal(deps, sid, ptok) == Principal("participant", "p1")
    assert await resolve_principal(deps, sid, join) == Principal("joiner")


@pytest.mark.asyncio
async def test_cold_session_resolves_without_hydrating(store) -> None:
    deps = _deps(store)
    sid, creator = await _session(
        deps, persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600)
    )
    deps.registry._sessions.pop(sid)
    p = await resolve_principal(deps, sid, creator, hydrate=False)
    assert p.role == "creator"
    assert (await resolve_principal(deps, sid, ADMIN, hydrate=False)).role == "admin"
    assert sid not in deps.registry._sessions


@pytest.mark.asyncio
async def test_unknown_session_not_found(store) -> None:
    deps = _deps(store)
    for hydrate in (True, False):
        with pytest.raises(ToolError) as exc:
            await resolve_principal(deps, "nope", ADMIN, hydrate=hydrate)
        assert exc.value.code is ErrorCode.SESSION_NOT_FOUND


@pytest.mark.asyncio
async def test_session_id_is_never_a_token(store) -> None:
    deps = _deps(store, admin_token=None)
    sid, _ = await _session(deps)
    with pytest.raises(ToolError) as exc:
        await resolve_principal(deps, sid, sid)
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    assert exc.value.message == "missing or invalid token"


@pytest.mark.asyncio
async def test_missing_token_denied(store) -> None:
    deps = _deps(store)
    sid, _ = await _session(deps)
    for tok in (None, ""):
        with pytest.raises(ToolError) as exc:
            await resolve_principal(deps, sid, tok)
        assert exc.value.message == "missing or invalid token"


@pytest.mark.asyncio
async def test_expired_join_token_message(store) -> None:
    deps = _deps(store)
    sid, _ = await _session(deps)
    tok, _ = _hot(deps, sid).auth.mint_join(0)  # expired long ago
    with pytest.raises(ToolError) as exc:
        await resolve_principal(deps, sid, tok)
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    assert exc.value.message == EXPIRED_MSG


@pytest.mark.asyncio
async def test_throttle_per_sid_and_ip_rejects_invalid_only(store) -> None:
    deps = _deps(store, limiter=AuthFailureLimiter(max_failures=2))
    sid, _ = await _session(deps)
    for _ in range(2):
        with pytest.raises(ToolError) as exc:
            await resolve_principal(deps, sid, "bad", source_ip="1.1.1.1")
        assert exc.value.code is ErrorCode.PERMISSION_DENIED
    with pytest.raises(ToolError) as exc:
        await resolve_principal(deps, sid, "bad", source_ip="1.1.1.1")
    assert exc.value.code is ErrorCode.RATE_LIMITED
    # a different source IP is unaffected
    with pytest.raises(ToolError) as exc:
        await resolve_principal(deps, sid, "bad", source_ip="2.2.2.2")
    assert exc.value.code is ErrorCode.PERMISSION_DENIED


@pytest.mark.asyncio
async def test_valid_token_passes_while_throttled(store) -> None:
    deps = _deps(store, limiter=AuthFailureLimiter(max_failures=2))
    sid, creator = await _session(deps)
    for _ in range(2):
        with pytest.raises(ToolError):
            await resolve_principal(deps, sid, "bad", source_ip="9.9.9.9")
    assert deps.auth_limiter.is_throttled(sid, "9.9.9.9")
    assert (await resolve_principal(deps, sid, creator, source_ip="9.9.9.9")).role == "creator"
    assert (await resolve_principal(deps, sid, ADMIN, source_ip="9.9.9.9")).role == "admin"


def test_require_rejects_other_roles() -> None:
    require(Principal("creator"), "creator", "admin")
    with pytest.raises(ToolError) as exc:
        require(Principal("participant", "p"), "creator", "admin")
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    assert exc.value.message == "requires one of: creator, admin"


@pytest.mark.asyncio
async def test_cold_resolve_on_ended_session(store) -> None:
    deps = _deps(store)
    sid, creator = await _session(
        deps, persistence=PersistenceConfig(mode="ttl", ttl_seconds=3600)
    )
    deps.registry._sessions.pop(sid)
    await store.mark_session_ended(sid, "host_deleted", int(time.time() * 1000))
    assert (await resolve_principal(deps, sid, creator, hydrate=False)).role == "creator"
    assert (await resolve_principal(deps, sid, ADMIN, hydrate=False)).role == "admin"
    with pytest.raises(ToolError) as exc:
        await resolve_principal(deps, sid, "bad", hydrate=False)
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    assert sid not in deps.registry._sessions


@pytest.mark.asyncio
async def test_spoofed_x_forwarded_for_shares_one_throttle_bucket(store) -> None:
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from sessionweave.http_api import _delete_session_handler

    deps = _deps(store, limiter=AuthFailureLimiter(max_failures=2))
    sid, _ = await _session(deps)
    route = Route("/v1/sessions/{sid}", _delete_session_handler, methods=["DELETE"])
    app = Starlette(routes=[route])
    app.state.tool_deps = deps
    with TestClient(app) as c:
        codes = [
            c.delete(
                f"/v1/sessions/{sid}",
                headers={"authorization": "Bearer bad", "x-forwarded-for": f"10.0.0.{i}"},
            ).status_code
            for i in range(3)
        ]
    assert codes == [403, 403, 429]


@pytest.mark.asyncio
async def test_invalid_participant_tokens_are_throttled_valid_one_never(store) -> None:
    from starlette.applications import Starlette
    from starlette.routing import Route
    from starlette.testclient import TestClient

    from sessionweave.http_api import patch_self
    from tests.unit.fakes import FakeHandle

    deps = _deps(store, limiter=AuthFailureLimiter(max_failures=2))
    sid, _ = await _session(deps)
    p = await _hot(deps, sid).join(FakeHandle(), role="worker")
    ptoken = p.issued_token
    assert ptoken is not None
    route = Route("/v1/sessions/{sid}/participants/me", patch_self, methods=["PATCH"])
    app = Starlette(routes=[route])
    app.state.tool_deps = deps
    with TestClient(app) as c:
        def patch(token: str) -> int:
            return c.patch(
                f"/v1/sessions/{sid}/participants/me",
                headers={"authorization": f"Bearer {token}"},
                json={"objective": "busy"},
            ).status_code

        codes = [patch("bad") for _ in range(3)]
        valid = patch(ptoken)
    assert codes[2] == 429 and codes[0] == codes[1] != 429
    assert valid == 200
    assert deps.auth_limiter.is_throttled(sid, "testclient")
