import time

import pytest

from sessionweave.access import Principal
from sessionweave.config import SessionConfig
from sessionweave.connections import ConnectionManager
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.session import Session
from tests.unit.fakes import FakeHandle


def _now() -> int:
    return int(time.time() * 1000)


def _session() -> Session:
    return Session("sid", SessionConfig(), ConnectionManager())


@pytest.mark.asyncio
async def test_join_issues_secret_token() -> None:
    sess = _session()
    p = await sess.join(FakeHandle(), role="a")
    assert p.issued_token
    assert p.issued_token != p.participant_id
    assert sess.auth.match(p.issued_token, _now()) == Principal(
        "participant", p.participant_id
    )
    assert sess.auth.match(p.participant_id, _now()) is None


@pytest.mark.asyncio
async def test_leave_forgets_participant_token() -> None:
    sess = _session()
    p = await sess.join(FakeHandle(), role="a")
    assert p.issued_token is not None
    await sess.leave(p.participant_id)
    assert sess.auth.match(p.issued_token, _now()) is None


@pytest.mark.asyncio
async def test_evict_forgets_participant_token() -> None:
    sess = _session()
    p = await sess.join(FakeHandle(), role="a")
    assert p.issued_token is not None
    sess._evict(p.participant_id, reason="offline_timeout")
    assert sess.auth.match(p.issued_token, _now()) is None


@pytest.mark.asyncio
async def test_issued_token_never_serialised() -> None:
    sess = _session()
    p = await sess.join(FakeHandle(), role="a")
    dumped = p.to_public().model_dump()
    assert not any("token" in k for k in dumped)
    assert p.issued_token not in {str(v) for v in dumped.values()}


@pytest.mark.asyncio
async def test_mint_and_revoke_tokens_in_ram() -> None:
    sess = _session()
    jt, _ = await sess.mint_join_token()
    st, issued = await sess.mint_supervisor_token()
    assert sess.auth.match(jt, _now()) == Principal("joiner")
    assert sess.auth.match(st, _now()) == Principal("supervisor")
    await sess.revoke_supervisor_token(issued.token_id)
    assert sess.auth.match(st, _now()) is None


@pytest.mark.asyncio
async def test_revoke_unknown_token_raises_token_not_found() -> None:
    sess = _session()
    with pytest.raises(ToolError) as ei:
        await sess.revoke_supervisor_token("nope")
    assert ei.value.code == ErrorCode.TOKEN_NOT_FOUND
