import time
from pathlib import Path

import pytest
import pytest_asyncio

from sessionweave.account_events import EventLog, Origin
from sessionweave.accounts import AccountStore, AuthResult
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.persistence import PersistenceStore


@pytest_asyncio.fixture
async def store(tmp_path: Path):
    s = PersistenceStore(str(tmp_path / "t.db"))
    await s.init(pool_size=2)
    yield s
    await s.close()


@pytest.fixture
def log(store: PersistenceStore) -> EventLog:
    return EventLog(store)


@pytest.fixture
def accounts(store: PersistenceStore, log: EventLog) -> AccountStore:
    return AccountStore(store, events=log)


def _now() -> int:
    return int(time.time() * 1000)


def _o(ip: str) -> Origin:
    return Origin(ip=ip, client="ua/1")


def _aid(r: AuthResult) -> str:
    assert r.approval_id is not None
    return r.approval_id


async def _kinds(log: EventLog) -> list[str]:
    events, _, _ = await log.list_events(0, account_id=None, limit=1000)
    return [e["kind"] for e in events]


async def _setup(accounts: AccountStore, **kw):
    acc = await accounts.create("Alice")
    tok, meta = await accounts.create_token(acc.account_id, "cli", **kw)
    return acc, tok, meta


async def test_any_from_anywhere(accounts: AccountStore) -> None:
    acc, tok, meta = await _setup(accounts)
    for ip in ("1.2.3.4", "2001:db8::1"):
        r = await accounts.authenticate(tok, _now(), _o(ip))
        assert r.account == acc and r.token_id == meta.token_id and r.refusal is None
    assert (await accounts.authenticate("swa_nope", _now(), _o("1.2.3.4"))).account is None


async def test_none_origin_skips_check(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(accounts, network_policy="approved_only")
    assert (await accounts.authenticate(tok, _now(), None)).account == acc
    assert await accounts.match_token(tok, _now()) == acc


async def test_first_use_binds_then_refuses(accounts: AccountStore, log: EventLog) -> None:
    acc, tok, meta = await _setup(accounts, network_policy="first_use")
    assert (await accounts.authenticate(tok, _now(), _o("203.0.113.7"))).account == acc
    (t,) = await accounts.list_tokens(acc.account_id)
    assert t.approved_networks == ["203.0.113.7/32"]
    assert (await accounts.authenticate(tok, _now(), _o("203.0.113.7"))).account == acc
    r1 = await accounts.authenticate(tok, _now(), _o("198.51.100.1"))
    r2 = await accounts.authenticate(tok, _now(), _o("198.51.100.1"))
    assert r1.account is None and r1.refusal == "network"
    assert _aid(r1) == _aid(r2)
    assert _aid(r1).startswith("apr_") and len(_aid(r1)) == 20
    pend = await accounts.list_approvals(acc.account_id)
    assert len(pend) == 1 and pend[0]["ip"] == "198.51.100.1"
    assert pend[0]["token_id"] == meta.token_id and pend[0]["client"] == "ua/1"
    assert await _kinds(log) == ["token.network_pending"]
    events, _, _ = await log.list_events(0, account_id=None, limit=10)
    assert tok not in str(events)


async def test_approved_only_cidr(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(
        accounts, network_policy="approved_only", approved_networks=["10.0.5.99/24"]
    )
    (t,) = await accounts.list_tokens(acc.account_id)
    assert t.approved_networks == ["10.0.5.0/24"]
    assert (await accounts.authenticate(tok, _now(), _o("10.0.5.200"))).account == acc
    assert (await accounts.authenticate(tok, _now(), _o("10.0.6.1"))).refusal == "network"


async def test_ipv6_and_mapped(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(
        accounts,
        network_policy="approved_only",
        approved_networks=["2001:db8:1:2::/64", "203.0.113.7"],
    )
    assert (await accounts.authenticate(tok, _now(), _o("2001:db8:1:2::abcd"))).account == acc
    assert (await accounts.authenticate(tok, _now(), _o("2001:db8:1:3::1"))).refusal == "network"
    assert (await accounts.authenticate(tok, _now(), _o("::ffff:203.0.113.7"))).account == acc


async def test_pending_cap_10(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(accounts, network_policy="approved_only")
    for i in range(11):
        await accounts.authenticate(tok, _now(), _o(f"198.51.100.{i + 1}"))
    pend = await accounts.list_approvals(acc.account_id)
    assert len(pend) == 10
    assert "198.51.100.1" not in {p["ip"] for p in pend}


async def test_approve_ip_and_network(accounts: AccountStore, log: EventLog) -> None:
    acc, tok, meta = await _setup(accounts, network_policy="approved_only")
    r = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    t = await accounts.approve(acc.account_id, _aid(r), "ip")
    assert t.approved_networks == ["198.51.100.9/32"]
    assert await accounts.list_approvals(acc.account_id) == []
    assert (await accounts.authenticate(tok, _now(), _o("198.51.100.9"))).account == acc
    r = await accounts.authenticate(tok, _now(), _o("2001:db8:1:2::5"))
    t = await accounts.approve(acc.account_id, _aid(r), "network")
    assert t.approved_networks == ["198.51.100.9/32", "2001:db8:1:2::/64"]
    r = await accounts.authenticate(tok, _now(), _o("192.0.2.77"))
    t = await accounts.approve(acc.account_id, _aid(r), "network")
    assert "192.0.2.0/24" in t.approved_networks
    assert (await _kinds(log)).count("token.network_approved") == 3
    events, _, _ = await log.list_events(0, account_id=None, limit=100)
    ap = next(e for e in events if e["kind"] == "token.network_approved")["data"]
    assert ap["cidr"] == "198.51.100.9/32" and ap["ip"] == "198.51.100.9"
    assert ap["token_id"] == meta.token_id and ap["label"] == "cli"


async def test_deny(accounts: AccountStore, log: EventLog) -> None:
    acc, tok, _ = await _setup(accounts, network_policy="approved_only")
    r = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    await accounts.deny(acc.account_id, _aid(r))
    assert await accounts.list_approvals(acc.account_id) == []
    assert "token.network_denied" in await _kinds(log)
    r2 = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    assert r2.refusal == "network" and _aid(r2) != _aid(r)


async def test_approval_errors(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(accounts, network_policy="approved_only")
    r = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    for fn in (
        lambda: accounts.approve(acc.account_id, "apr_missing", "ip"),
        lambda: accounts.deny(acc.account_id, "apr_missing"),
    ):
        with pytest.raises(ToolError) as ei:
            await fn()
        assert ei.value.code == ErrorCode.APPROVAL_NOT_FOUND
    other = await accounts.create("Bob")
    with pytest.raises(ToolError) as ei:
        await accounts.deny(other.account_id, _aid(r))
    assert ei.value.code == ErrorCode.APPROVAL_NOT_FOUND
    with pytest.raises(ToolError) as ei:
        await accounts.approve(acc.account_id, _aid(r), "bogus")  # type: ignore[arg-type]
    assert ei.value.code == ErrorCode.INVALID_REQUEST


async def test_expired_not_listed(accounts: AccountStore, store: PersistenceStore) -> None:
    acc, tok, _ = await _setup(accounts, network_policy="approved_only")
    r = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    assert store._write_conn is not None
    await store._write_conn.execute(
        "UPDATE network_approvals SET expires_at = ?", (_now() - 1,)
    )
    assert await accounts.list_approvals(acc.account_id) == []
    with pytest.raises(ToolError):
        await accounts.approve(acc.account_id, _aid(r), "ip")
    r2 = await accounts.authenticate(tok, _now(), _o("198.51.100.9"))
    assert _aid(r2) != _aid(r)


async def test_set_token_networks_and_validation(accounts: AccountStore) -> None:
    acc, _tok, meta = await _setup(accounts)
    t = await accounts.set_token_networks(
        acc.account_id, meta.token_id, network_policy="approved_only",
        approved_networks=["192.0.2.5/24"],
    )
    assert t.network_policy == "approved_only" and t.approved_networks == ["192.0.2.0/24"]
    t = await accounts.set_token_networks(
        acc.account_id, meta.token_id, network_policy=None, approved_networks=None
    )
    assert t.network_policy == "approved_only"
    for pol, nets in (("bogus", None), (None, ["not-a-cidr"])):
        with pytest.raises(ToolError) as ei:
            await accounts.set_token_networks(
                acc.account_id, meta.token_id, network_policy=pol, approved_networks=nets
            )
        assert ei.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(ToolError) as ei:
        await accounts.create_token(acc.account_id, "x", network_policy="bogus")
    assert ei.value.code == ErrorCode.INVALID_REQUEST
    with pytest.raises(ToolError) as ei:
        await accounts.set_token_networks(
            acc.account_id, "nope", network_policy="any", approved_networks=None
        )
    assert ei.value.code == ErrorCode.TOKEN_NOT_FOUND
    pub = t.to_public()
    assert {"network_policy", "approved_networks", "last_used_at", "last_used_ip"} <= set(pub)


async def test_rejected_rate_limited(accounts: AccountStore, log: EventLog) -> None:
    acc, tok, meta = await _setup(accounts)
    await accounts.revoke_token(acc.account_id, meta.token_id)
    for _ in range(3):
        assert (await accounts.authenticate(tok, _now(), _o("1.2.3.4"))).account is None
    assert await _kinds(log) == ["token.rejected"]
    events, _, _ = await log.list_events(0, account_id=None, limit=10)
    d = events[0]["data"]
    assert d["reason"] == "revoked" and d["token_id"] == meta.token_id and d["ip"] == "1.2.3.4"
    # after the window it fires again
    accounts._rejected[(meta.token_id, "revoked")] -= 601_000
    await accounts.authenticate(tok, _now(), _o("1.2.3.4"))
    assert await _kinds(log) == ["token.rejected"] * 2


async def test_rejected_account_disabled_and_expired(
    accounts: AccountStore, log: EventLog
) -> None:
    acc, tok, _meta = await _setup(accounts)
    await accounts.update(acc.account_id, status="disabled")
    assert (await accounts.authenticate(tok, _now(), None)).account is None
    acc2 = await accounts.create("Bob")
    tok2, _m2 = await accounts.create_token(acc2.account_id, "e", expires_at_ms=_now() - 5)
    assert (await accounts.authenticate(tok2, _now(), None)).account is None
    events, _, _ = await log.list_events(0, account_id=None, limit=10)
    assert [e["data"]["reason"] for e in events] == ["account_disabled", "expired"]


async def test_last_used_written_and_throttled(accounts: AccountStore) -> None:
    acc, tok, _ = await _setup(accounts)
    await accounts.authenticate(tok, _now(), _o("1.2.3.4"))
    (t,) = await accounts.list_tokens(acc.account_id)
    assert t.last_used_at is not None and t.last_used_ip == "1.2.3.4"
    first = t.last_used_at
    await accounts.authenticate(tok, _now(), _o("5.6.7.8"))
    (t,) = await accounts.list_tokens(acc.account_id)
    assert t.last_used_at == first and t.last_used_ip == "1.2.3.4"
