"""Launcher registry: profile validation, visibility matrix, domains, unregister."""
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from sessionweave.launchers import HelloError, LauncherRegistry, validate_profile


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
    assert ids(None, None, "backend") == set()  # account launcher, ownerless session
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
