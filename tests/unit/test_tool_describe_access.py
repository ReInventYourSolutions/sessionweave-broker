"""session_describe: a token, or a connection already bound as a participant."""
import pytest

from sessionweave.config import load_deployment_defaults
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.http_api import HTTPParticipantHandle
from sessionweave.registry import SessionRegistry
from sessionweave.tools import ToolDeps, session_create, session_describe


@pytest.fixture
def deps() -> ToolDeps:
    defaults = load_deployment_defaults({})
    registry = SessionRegistry(
        defaults=defaults, connections=ConnectionManager(), metrics=None, firehose=None
    )
    return ToolDeps(registry=registry, bounds=defaults.bounds, admin_token="adm")


@pytest.mark.asyncio
async def test_describe_needs_a_token(deps: ToolDeps) -> None:
    sid = (await session_create(deps, {}))["session_id"]
    with pytest.raises(ToolError) as exc:
        await session_describe(deps, {"session_id": sid})
    assert exc.value.code is ErrorCode.PERMISSION_DENIED
    with pytest.raises(ToolError):
        await session_describe(deps, {"session_id": sid, "access_token": sid})


@pytest.mark.asyncio
async def test_describe_accepts_any_reader_token(deps: ToolDeps) -> None:
    made = await session_create(deps, {})
    for tok in (made["join_token"], made["supervisor_token"], made["creator_token"], "adm"):
        out = await session_describe(
            deps, {"session_id": made["session_id"], "access_token": tok}
        )
        assert out["session_id"] == made["session_id"]


@pytest.mark.asyncio
async def test_bound_participant_connection_needs_no_token(deps: ToolDeps) -> None:
    made = await session_create(deps, {})
    other = (await session_create(deps, {}))["session_id"]
    handle = HTTPParticipantHandle()
    deps.registry.connections.bind(handle, made["session_id"], "p1")
    out = await session_describe(deps, {"session_id": made["session_id"]}, handle=handle)
    assert out["session_id"] == made["session_id"]
    # the binding is only good for its own session
    with pytest.raises(ToolError):
        await session_describe(deps, {"session_id": other}, handle=handle)
