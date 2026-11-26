"""Launcher registry: profile validation, visibility matrix, domains, unregister."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from sessionweave.launchers import (
    KNOWN_CAPABILITIES,
    MAX_MACHINES_OPEN,
    MAX_MACHINES_PER_ACCOUNT,
    OPEN_OWNER,
    PROTOCOL_VERSION,
    HelloError,
    LauncherRegistry,
    owner_key,
    parse_hello_v2,
    validate_profile,
)

KEY = "lk_" + "ab" * 16
V2: dict[str, Any] = {
    "protocol": 2, "launcher_key": KEY, "mode": "local", "ceiling": "/srv/app",
    "capabilities": ["definitions", "browse", "processes", "teleport"],
    "sandbox": "bwrap", "run_as": False, "platform": "linux",
}


class _Conn:
    async def send_json(self, data: Any) -> None:
        return None


def _sess(owner: str | None) -> Any:
    return SimpleNamespace(auth=SimpleNamespace(owner_account_id=owner))


def _prof(**kw: Any) -> dict[str, Any]:
    base: dict[str, Any] = {
        "id": "p1", "domain": "backend", "role": "analyst", "capabilities": ["sql"],
        "description": "d", "runtime": "claude", "model": "m",
        "access": {"preset": "reader"}, "limits": {}, "max_instances": 2,
    }
    base.update(kw)
    return base


def _hello(profiles: list[Any], **kw: Any) -> dict[str, Any]:
    return {"machine": "m1", "visibility": "personal", "allowed_accounts": [],
            "profiles": profiles, **kw}


def test_validate_ok() -> None:
    p = validate_profile(_prof())
    assert p["access_preset"] == "reader" and p["max_instances"] == 2


@pytest.mark.parametrize("bad", [
    {"id": ""}, {"id": "a b"}, {"id": "x" * 65}, {"domain": 3}, {"role": ""},
    {"capabilities": ["x"] * 33}, {"capabilities": ["x" * 65]}, {"description": "x" * 1001},
    {"runtime": "vim"}, {"access": {"preset": "root"}}, {"access": {}},
    {"max_instances": 0}, {"max_instances": 33}, {"max_instances": True},
    {"max_instances": "2"},
])
def test_validate_rejects(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        validate_profile(_prof(**bad))


def test_invalid_and_duplicate_profiles_reported() -> None:
    reg = LauncherRegistry()
    lid, rejected = reg.register(
        _Conn(), None, _hello([_prof(), _prof(), _prof(id="p2", runtime="vim"), "junk"])
    )
    assert lid.startswith("lnc_") and len(lid) == 20
    assert [r["id"] for r in rejected] == ["p1", "p2", ""]
    assert rejected[0]["reason"] == "duplicate profile id"
    items = reg.profiles_for(_sess(None), "backend")
    assert [i["profile_id"] for i in items] == ["p1"]
    assert items[0]["free_slots"] == 2 and items[0]["online"] is True
    assert items[0]["access_preset"] == "reader"


@pytest.mark.parametrize("hello", [
    {"machine": "", "profiles": []}, {"machine": "m", "visibility": "x", "profiles": []},
    {"machine": "m", "allowed_accounts": "bob", "profiles": []},
    {"machine": "m", "profiles": "no"},
    {"machine": "m\nfake", "profiles": []},
])
def test_bad_hello(hello: dict[str, Any]) -> None:
    with pytest.raises(HelloError):
        LauncherRegistry().register(_Conn(), None, hello)


def test_open_mode_no_visibility_filter_and_filters() -> None:
    reg = LauncherRegistry()
    p2 = _prof(id="p2", role="tester", capabilities=["ui"])
    reg.register(_Conn(), None, _hello([_prof(), p2]))
    s = _sess(None)
    assert len(reg.profiles_for(s, "BACKEND")) == 2
    assert [i["profile_id"] for i in reg.profiles_for(s, "backend", role="tester")] == ["p2"]
    assert [i["profile_id"] for i in reg.profiles_for(s, "backend", capability="UI")] == ["p2"]
    assert reg.profiles_for(s, "frontend") == []
    assert reg.domains_for(s) == ["backend"]


def test_visibility_matrix() -> None:
    reg = LauncherRegistry()
    reg.register(_Conn(), "a1", _hello([_prof(id="pers")], machine="personal"))
    reg.register(_Conn(), "a1", _hello([_prof(id="gen")], visibility="general", machine="gen"))
    reg.register(
        _Conn(), "a1",
        _hello([_prof(id="lim", domain="other")], visibility="general",
               allowed_accounts=["BOB"], machine="lim"),
    )

    def ids(owner: str | None, name: str | None, domain: str) -> set[str]:
        return {i["profile_id"] for i in reg.profiles_for(
            _sess(owner), domain, owner_name=name)}

    assert ids("a1", "Alice", "backend") == {"pers", "gen"}  # same owner sees all
    assert ids("a2", "Carol", "backend") == {"gen"}  # general, no list
    assert ids("a2", "Carol", "other") == set()  # not on allowed list
    assert ids("a3", "bob", "other") == {"lim"}  # case-insensitive name match
    assert ids("a1", "Alice", "other") == {"lim"}
    # ownerless (admin-created) session: general profiles open to every account only
    assert ids(None, None, "backend") == {"gen"}
    assert ids(None, None, "other") == set()
    assert reg.domains_for(_sess("a2"), owner_name="Carol") == ["backend"]
    assert reg.domains_for(_sess("a3"), owner_name="bob") == ["backend", "other"]


def test_unregister_removes_profiles() -> None:
    reg = LauncherRegistry()
    lid, _ = reg.register(_Conn(), None, _hello([_prof()]))
    assert reg.get_conn(lid) is not None
    reg.unregister(lid)
    reg.unregister(lid)
    assert reg.profiles_for(_sess(None), "backend") == []
    assert reg.domains_for(_sess(None)) == []
    assert reg.get_conn(lid) is None


def test_profile_visibility_overrides_launcher() -> None:
    reg = LauncherRegistry()
    reg.register(_Conn(), "a1", _hello(
        [_prof(id="open"), _prof(id="mine", visibility="personal"),
         _prof(id="bob", allowed_accounts=["Bob"])],
        visibility="general", machine="gen",
    ), owner_name="alice")
    reg.register(_Conn(), "a1", _hello(
        [_prof(id="pub", visibility="general")], machine="pers"), owner_name="alice")

    def ids(owner: str | None, name: str | None) -> set[str]:
        return {i["profile_id"] for i in reg.profiles_for(
            _sess(owner), "backend", owner_name=name)}

    assert ids("a1", "alice") == {"open", "mine", "bob", "pub"}
    assert ids("a2", "carol") == {"open", "pub"}
    assert ids("a3", "bob") == {"open", "bob", "pub"}
    assert reg.domains_for(_sess("a2"), owner_name="carol") == ["backend"]
    # select applies the profile's visibility too
    with pytest.raises(Exception, match="no online launcher"):
        reg.select(_sess("a2"), "carol", launcher_id=next(
            i["launcher_id"] for i in reg.profiles_for(_sess("a1"), "backend", owner_name="a")
            if i["profile_id"] == "mine"), profile_id="mine")


@pytest.mark.parametrize("bad", [
    {"visibility": "secret"}, {"allowed_accounts": "bob"}, {"allowed_accounts": [""]},
])
def test_validate_rejects_bad_profile_visibility(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        validate_profile(_prof(**bad))


def test_discovery_says_owner_and_own_first() -> None:
    reg = LauncherRegistry()
    reg.register(_Conn(), "a2", _hello([_prof(id="foreign")], visibility="general",
                                       machine="alice-box"), owner_name="mallory")
    reg.register(_Conn(), "a1", _hello([_prof(id="mine")], machine="m"), owner_name="alice")
    items = reg.profiles_for(_sess("a1"), "backend", owner_name="alice")
    assert [(i["profile_id"], i["owner"], i["own"]) for i in items] == [
        ("mine", "alice", True), ("foreign", "mallory", False)]
    # open mode: no owner, always own
    reg2 = LauncherRegistry()
    reg2.register(_Conn(), None, _hello([_prof()]))
    (item,) = reg2.profiles_for(_sess(None), "backend")
    assert item["owner"] is None and item["own"] is True


def test_owner_fields_open_mode_launcher_is_own_for_account_session() -> None:
    session: Any = SimpleNamespace(auth=SimpleNamespace(owner_account_id="acct-1"))
    assert LauncherRegistry.owner_fields(None, None, session) == {"owner": None, "own": True}
    assert LauncherRegistry.owner_fields("acct-1", "alice", session)["own"] is True
    assert LauncherRegistry.owner_fields("acct-2", "bob", session)["own"] is False


def test_constants() -> None:
    assert PROTOCOL_VERSION == 2
    assert (MAX_MACHINES_PER_ACCOUNT, MAX_MACHINES_OPEN) == (20, 100)
    assert KNOWN_CAPABILITIES == frozenset({"definitions", "browse", "processes"})
    assert OPEN_OWNER == ""


def test_hello_v2_parsed_and_indexed_by_key() -> None:
    reg = LauncherRegistry()
    lid, _ = reg.register(_Conn(), "acc_1", _hello([_prof()], **V2))
    lnc = reg.by_key("acc_1", KEY)
    assert lnc is not None and lnc.launcher_id == lid
    assert lnc.v2.capabilities == frozenset({"definitions", "browse", "processes"})
    assert (lnc.v2.mode, lnc.v2.ceiling, lnc.v2.sandbox) == ("local", "/srv/app", "bwrap")
    assert reg.by_key("acc_2", KEY) is None and reg.by_key(OPEN_OWNER, KEY) is None
    assert owner_key(lnc) == "acc_1"
    assert [x.launcher_id for x in reg.for_account("acc_1")] == [lid]
    assert reg.for_account(OPEN_OWNER) == []


def test_open_owner_lists_every_ownerless_launcher() -> None:
    reg = LauncherRegistry()
    a, _ = reg.register(_Conn(), None, _hello([_prof()], **V2))
    b, _ = reg.register(_Conn(), None, _hello([_prof()]))
    reg.register(_Conn(), "acc_1", _hello([_prof()]))
    assert {x.launcher_id for x in reg.for_account(OPEN_OWNER)} == {a, b}
    lnc = reg.by_key(OPEN_OWNER, KEY)
    assert lnc is not None and owner_key(lnc) == OPEN_OWNER
    # open mode without an explicit v2: register strips capabilities (no DB known)
    assert lnc.v2.capabilities == frozenset()


def test_register_takes_parsed_v2() -> None:
    reg = LauncherRegistry()
    v2 = parse_hello_v2(V2, deploy_allowed=True)
    reg.register(_Conn(), None, _hello([_prof()], **V2), v2=v2)
    lnc = reg.by_key(OPEN_OWNER, KEY)
    assert lnc is not None and lnc.v2 is v2


def test_same_key_does_not_count_against_launcher_limit() -> None:
    reg = LauncherRegistry()
    for i in range(5):
        reg.register(_Conn(), "acc_1", _hello([_prof()], **{**V2, "launcher_key": f"lk_{i:032x}"}))
    # a reconnect with an existing key is let in (the old one is replaced by the caller)
    reg.register(_Conn(), "acc_1", _hello([_prof()], **{**V2, "launcher_key": f"lk_{0:032x}"}))
    with pytest.raises(Exception, match="5 launchers"):
        reg.register(_Conn(), "acc_1", _hello([_prof()], **V2))


def test_legacy_hello_is_config_mode_without_key() -> None:
    v2 = parse_hello_v2({"type": "hello", "machine": "m"}, deploy_allowed=True)
    assert (v2.protocol, v2.launcher_key, v2.mode, v2.capabilities) == (
        1, None, "config", frozenset())
    assert (v2.ceiling, v2.sandbox, v2.run_as, v2.prereqs) == (None, None, False, ())


def test_config_mode_advertises_no_capability() -> None:
    v2 = parse_hello_v2({**V2, "mode": "config", "ceiling": None}, deploy_allowed=True)
    assert v2.capabilities == frozenset() and v2.launcher_key == KEY


def test_root_mode_ceiling_is_slash() -> None:
    v2 = parse_hello_v2({**V2, "mode": "root", "ceiling": "/", "run_as": True},
                        deploy_allowed=True)
    assert (v2.mode, v2.ceiling, v2.run_as) == ("root", "/", True)
    v2 = parse_hello_v2({**V2, "mode": "root", "ceiling": None}, deploy_allowed=True)
    assert v2.ceiling == "/"


@pytest.mark.parametrize("bad", [
    {"launcher_key": "lk_short"}, {"mode": "god"}, {"mode": "local", "ceiling": "rel"},
    {"mode": "local", "ceiling": "/srv/../etc"}, {"mode": "root", "ceiling": "/srv"},
    {"run_as": "yes"}, {"protocol": 2, "launcher_key": None},
    {"prereqs": [{"name": "bwrap", "state": "fine"}]},
    {"prereqs": [{"name": "x", "state": "ok"}] * 33},
    {"launcher_key": "LK_" + "ab" * 16}, {"launcher_key": "lk_" + "AB" * 16},
    {"protocol": "2"}, {"protocol": 0}, {"mode": "local", "ceiling": None},
    {"mode": "local", "ceiling": "/srv/app/"}, {"mode": "local", "ceiling": "//srv"},
    {"mode": "local", "ceiling": "/srv\x00x"}, {"mode": "local", "ceiling": "/" + "a" * 4096},
    {"mode": "config", "ceiling": "/srv"}, {"run_as": True},
    {"capabilities": "browse"}, {"capabilities": [1]}, {"sandbox": 3},
    {"platform": "x" * 33}, {"platform": 3}, {"prereqs": "none"},
    {"prereqs": ["x"]}, {"prereqs": [{"name": "x" * 33, "state": "ok"}]},
    {"prereqs": [{"name": "x", "state": "ok", "version": "\n"}]},
    {"prereqs": [{"name": "x", "state": "ok", "command": "y" * 301}]},
    {"prereqs": [{"name": "x", "state": "ok", "required": "yes"}]},
    {"prereqs": [{"name": "x", "state": "ok", "needed_by": "cline"}]},
    {"sandbox": "firejail"}, {"sandbox": "BWRAP"},
    {"mode": "local", "ceiling": "/srv/a\nb"}, {"mode": "local", "ceiling": "/srv/\x7f"},
])
def test_hello_v2_rejects(bad: dict[str, Any]) -> None:
    with pytest.raises(HelloError):
        parse_hello_v2({**V2, **bad}, deploy_allowed=True)


def test_prereqs_parsed() -> None:
    p = {"name": "bwrap", "state": "missing", "version": None, "required": True,
         "needed_by": ["--local"], "command": "sudo apt-get install bubblewrap"}
    assert parse_hello_v2({**V2, "prereqs": [p]}, deploy_allowed=True).prereqs == (p,)


def test_prereqs_defaults_and_unknown_keys_dropped() -> None:
    v2 = parse_hello_v2({**V2, "prereqs": [{"name": "git", "state": "ok", "extra": 1}]},
                        deploy_allowed=True)
    assert v2.prereqs == ({"name": "git", "state": "ok", "version": None, "required": False,
                           "needed_by": [], "command": None},)


def test_capabilities_stripped_when_deploy_not_allowed() -> None:
    assert parse_hello_v2(V2, deploy_allowed=False).capabilities == frozenset()


def test_profiles_frame_replaces_published_profiles() -> None:
    reg = LauncherRegistry()
    lid, _ = reg.register(_Conn(), None, _hello([_prof(id="a")]))
    rejected = reg.replace_profiles(
        lid, [_prof(id="b", source="remote", kind="observer"), {"id": "bad"}])
    assert [p["profile_id"] for p in reg.profiles_for(_sess(None), "backend")] == ["b"]
    assert reg.profiles_for(_sess(None), "backend")[0]["kind"] == "observer"
    assert reg.profiles_for(_sess(None), "backend")[0]["source"] == "remote"
    assert rejected and rejected[0]["id"] == "bad"
    # not a list: nothing replaced, one rejection
    assert reg.replace_profiles(lid, "junk")[0]["id"] == ""
    assert [p["profile_id"] for p in reg.profiles_for(_sess(None), "backend")] == ["b"]
    assert reg.replace_profiles("lnc_gone", []) == []


def test_profile_source_kind_coordinator_defaults() -> None:
    p = validate_profile(_prof())
    assert (p["source"], p["kind"], p["coordinator_capable"]) == ("local", None, False)
    p = validate_profile(_prof(source="remote", kind="operator", coordinator_capable=True))
    assert (p["source"], p["kind"], p["coordinator_capable"]) == ("remote", "operator", True)
    reg = LauncherRegistry()
    reg.register(_Conn(), None, _hello([_prof()]))
    item = reg.profiles_for(_sess(None), "backend")[0]
    assert (item["source"], item["kind"], item["coordinator_capable"]) == ("local", None, False)


@pytest.mark.parametrize("bad", [
    {"source": "cloud"}, {"kind": "god"}, {"coordinator_capable": "yes"},
])
def test_validate_rejects_bad_source_kind(bad: dict[str, Any]) -> None:
    with pytest.raises(ValueError):
        validate_profile(_prof(**bad))


def test_prereqs_accept_not_logged_in_and_optional_runtimes() -> None:
    claude = {"name": "claude", "state": "not_logged_in", "version": "2.1.0",
              "required": True, "needed_by": ["claude"], "command": "claude /login"}
    opencode = {"name": "opencode", "state": "ok", "version": "2.0.1",
                "required": False, "needed_by": [], "command": None}
    v2 = parse_hello_v2({**V2, "prereqs": [claude, opencode]}, deploy_allowed=True)
    assert v2.prereqs == (claude, opencode)


def test_claim_key_without_db() -> None:
    from sessionweave.launchers import LauncherKeyInUseError

    reg = LauncherRegistry()
    a, _ = reg.prepare(_Conn(), None, _hello([_prof()], **V2))
    secret = reg.claim_key(a, None)
    assert secret is not None
    reg.add(a)
    b, _ = reg.prepare(_Conn(), None, _hello([_prof()], **V2))
    for bad in (None, "lks_wrong"):
        with pytest.raises(LauncherKeyInUseError):
            reg.claim_key(b, bad)
    assert reg.claim_key(b, secret) is None  # holder's secret: claim ok, no new secret
    assert reg.by_key(OPEN_OWNER, KEY) is a  # b is not registered by prepare/claim
    # nobody holds the key any more: a new secret
    reg.unregister(a.launcher_id)
    c, _ = reg.prepare(_Conn(), None, _hello([_prof()], **V2))
    assert reg.claim_key(c, None) not in (None, secret)


def test_add_replaces_same_key_and_rechecks_limit() -> None:
    reg = LauncherRegistry()
    a, _ = reg.prepare(_Conn(), "acc_1", _hello([_prof()], **V2))
    assert reg.add(a) == []
    b, _ = reg.prepare(_Conn(), "acc_1", _hello([_prof()], **V2))
    assert [x.launcher_id for x in reg.add(b)] == [a.launcher_id]
    assert reg.by_key("acc_1", KEY) is b and reg.get(a.launcher_id) is None
    # the limit is checked again when adding
    pending = [reg.prepare(_Conn(), "acc_1", _hello([_prof()], **{
        **V2, "launcher_key": f"lk_{i:032x}"}))[0] for i in range(5)]
    for p in pending[:4]:
        reg.add(p)
    with pytest.raises(Exception, match="5 launchers"):
        reg.add(pending[4])


@pytest.mark.parametrize("bad", ["", 3, "x" * 201])
def test_hello_key_secret_validated(bad: Any) -> None:
    from sessionweave.launchers import hello_key_secret

    with pytest.raises(HelloError):
        hello_key_secret({"key_secret": bad})
    assert hello_key_secret({}) is None


def test_sandbox_none_means_no_sandbox() -> None:
    assert parse_hello_v2({**V2, "sandbox": "none"}, deploy_allowed=True).sandbox is None
    assert parse_hello_v2({**V2, "sandbox": None}, deploy_allowed=True).sandbox is None


# --- relayed requests (browse / processes) -----------------------------------


class _SilentConn:
    """A launcher connection that records frames and never answers."""

    def __init__(self) -> None:
        self.sent: list[dict[str, Any]] = []

    async def send_json(self, data: Any) -> None:
        self.sent.append(data)


class _BrokenConn:
    async def send_json(self, data: Any) -> None:
        raise RuntimeError("socket closed")


async def test_request_adds_request_id_and_resolves_on_reply() -> None:
    import asyncio

    reg, conn = LauncherRegistry(), _SilentConn()
    lid, _ = reg.register(conn, "acc_1", _hello([], **V2))
    task = asyncio.create_task(reg.request(lid, "browse", {"path": "/srv/app"}))
    await asyncio.sleep(0)
    (frame,) = conn.sent
    assert frame["type"] == "browse" and frame["path"] == "/srv/app"
    rid = frame["request_id"]
    assert rid.startswith("req_") and len(rid) == 20
    int(rid[4:], 16)
    # a reply of the wrong kind, an unknown id and another launcher's id are ignored
    other, _ = reg.register(_SilentConn(), "acc_1", _hello([], **{
        **V2, "launcher_key": "lk_" + "ef" * 16}))
    reg.on_reply(lid, {"type": "processes_result", "request_id": rid, "ok": True})
    reg.on_reply(lid, {"type": "browse_result", "request_id": "req_" + "0" * 16})
    reg.on_reply(other, {"type": "browse_result", "request_id": rid, "ok": True})
    reg.on_reply(lid, {"type": "browse_result", "request_id": 3})
    await asyncio.sleep(0)
    assert not task.done()
    reply = {"type": "browse_result", "request_id": rid, "ok": True, "entries": []}
    reg.on_reply(lid, reply)
    assert await task == reply
    # the pending entry is gone: a late duplicate is ignored
    reg.on_reply(lid, reply)
    assert reg.pending_count(lid) == 0


async def test_unregister_fails_pending_requests() -> None:
    import asyncio

    from sessionweave.errors import ErrorCode, ToolError

    reg = LauncherRegistry()
    lid, _ = reg.register(_SilentConn(), "acc_1", _hello([], **V2))
    task = asyncio.create_task(reg.request(lid, "browse", {"path": "/srv/app"}))
    await asyncio.sleep(0)
    reg.unregister(lid)
    with pytest.raises(ToolError) as e:
        await task
    assert e.value.code is ErrorCode.LAUNCHER_OFFLINE
    assert reg.pending_count(lid) == 0


async def test_request_offline_timeout_and_inflight_limit() -> None:
    import asyncio

    from sessionweave.errors import ErrorCode, ToolError
    from sessionweave.launchers import MAX_INFLIGHT

    reg = LauncherRegistry()
    with pytest.raises(ToolError) as e:
        await reg.request("lnc_missing", "browse", {})
    assert e.value.code is ErrorCode.LAUNCHER_OFFLINE
    broken, _ = reg.register(_BrokenConn(), "acc_2", _hello([], **V2))
    with pytest.raises(ToolError) as e:
        await reg.request(broken, "browse", {})
    assert e.value.code is ErrorCode.LAUNCHER_OFFLINE
    assert reg.pending_count(broken) == 0

    lid, _ = reg.register(_SilentConn(), "acc_1", _hello([], **V2))
    with pytest.raises(ToolError) as e:
        await reg.request(lid, "processes", {}, timeout=0.05)
    assert e.value.code is ErrorCode.LAUNCHER_TIMEOUT
    assert reg.pending_count(lid) == 0

    tasks = [asyncio.create_task(reg.request(lid, "browse", {}, timeout=5))
             for _ in range(MAX_INFLIGHT)]
    await asyncio.sleep(0)
    with pytest.raises(ToolError) as e:
        await reg.request(lid, "browse", {})
    assert e.value.code is ErrorCode.RATE_LIMITED
    reg.unregister(lid)
    for t in tasks:
        with pytest.raises(ToolError):
            await t


def test_relay_timeout_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    from sessionweave.launchers import RELAY_TIMEOUT_S, relay_timeout_from_env

    monkeypatch.delenv("SW_LAUNCHER_RELAY_TIMEOUT_S", raising=False)
    assert relay_timeout_from_env() == RELAY_TIMEOUT_S == 10.0
    for raw, want in (("0.2", 0.2), ("0", 0.1), ("999", 60.0), ("junk", 10.0)):
        monkeypatch.setenv("SW_LAUNCHER_RELAY_TIMEOUT_S", raw)
        assert relay_timeout_from_env() == want
