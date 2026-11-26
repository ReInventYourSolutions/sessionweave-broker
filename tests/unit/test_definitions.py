"""Agent definitions: the schema the broker validates before a deploy."""
from __future__ import annotations

import copy
import dataclasses
import json
from typing import Any

import pytest

from sessionweave.definitions import (
    APPROVAL_POLICIES,
    COMMAND_MODES,
    DEFINITION_MAX_BYTES,
    INSTRUCTIONS_MAX_BYTES,
    KINDS,
    MAX_DEFINITIONS_PER_LAUNCHER,
    METRIC_KINDS,
    DefinitionError,
    inside,
    validate_definition,
)
from sessionweave.errors import ErrorCode
from sessionweave.machines import Machine

KEY = "lk_" + "ab" * 16
LOCAL = Machine("acc_1", KEY, "box", "local", "/srv/app", ["definitions"], "bwrap", False,
                "linux", [], 0, 0)
ROOT = dataclasses.replace(LOCAL, mode="root", ceiling="/", run_as=True)
CONFIG = dataclasses.replace(LOCAL, mode="config", ceiling=None, capabilities=[])


def _def(**kw: Any) -> dict[str, Any]:
    return {"id": "obs", "domain": "shop", "role": "logs", "runtime": "claude", "kind": "observer",
            "workdir": "/srv/app", "max_instances": 1, **kw}


def test_constants() -> None:
    assert DEFINITION_MAX_BYTES == 64 * 1024
    assert INSTRUCTIONS_MAX_BYTES == 16 * 1024
    assert MAX_DEFINITIONS_PER_LAUNCHER == 50
    assert KINDS == ("observer", "operator")
    assert APPROVAL_POLICIES == ("auto", "listed", "always")
    assert COMMAND_MODES == ("none", "list", "shell")
    assert METRIC_KINDS == ("cpu", "memory", "disk", "load")
    assert ErrorCode.DEFINITION_NOT_FOUND.value == "definition_not_found"
    assert ErrorCode.OUTSIDE_CEILING.value == "outside_ceiling"


def test_observer_defaults() -> None:
    d = validate_definition(_def(), LOCAL)
    assert d["access"]["commands"] == {"mode": "list", "allow": [], "privileged": []}
    assert d["approval"] == {"policy": "auto", "patterns": []}
    assert d["coordinator_capable"] is False and d["run_as"] is None
    assert d["observe"]["sources"] == [] and d["observe"]["logs"] == []
    assert d["observe"]["processes"] == [] and d["observe"]["metrics"]["enabled"] is False
    assert d["access"]["files"] == {"read": [], "write": [], "deny": []}
    assert (d["description"], d["model"], d["instructions"], d["capabilities"]) == ("", "", "", [])
    assert d["limits"] == {} and d["access"]["network"] is None
    assert d["visibility"] is None and d["allowed_accounts"] is None
    json.dumps(d)  # stored as JSON


def test_operator_defaults_to_shell() -> None:
    d = validate_definition(_def(kind="operator"), LOCAL)
    assert d["access"]["commands"] == {"mode": "shell", "allow": [], "privileged": []}


def test_full_definition_round_trips_and_input_untouched() -> None:
    raw = _def(
        kind="operator", capabilities=["logs"], description="d", model="m",
        coordinator_capable=True,
        observe={"sources": ["/srv/app/src"], "logs": ["/srv/app/log/x.log"],
                 "processes": [{"name": "nginx", "pid": 12}, {"name": "app"}],
                 "metrics": {"enabled": True, "interval_s": 30, "kinds": ["cpu", "load"]}},
        access={"files": {"read": ["**/*.py"], "write": ["out/"], "deny": [".env"]},
                "network": ["example.com"],
                "commands": {"mode": "list", "allow": ["git status", "ls"]}},
        limits={"max_duration_s": 600, "max_idle_s": 60, "max_cost_usd": 1.5},
        approval={"policy": "listed", "patterns": ["git push"]},
        instructions="Watch.", visibility="personal", allowed_accounts=["bob"],
    )
    before = copy.deepcopy(raw)
    d = validate_definition(raw, LOCAL)
    assert raw == before
    assert d["observe"]["processes"] == [{"name": "nginx", "pid": 12}, {"name": "app", "pid": None}]
    assert d["observe"]["metrics"] == {"enabled": True, "interval_s": 30, "kinds": ["cpu", "load"]}
    assert d["access"]["commands"] == {"mode": "list", "allow": ["git status", "ls"],
                                       "privileged": []}
    assert d["access"]["network"] == ["example.com"]
    assert d["limits"] == {"max_duration_s": 600, "max_idle_s": 60, "max_cost_usd": 1.5}
    assert d["approval"] == {"policy": "listed", "patterns": ["git push"]}
    assert d["allowed_accounts"] == ["bob"] and d["coordinator_capable"] is True
    assert validate_definition(d, LOCAL) == d  # normalised output is valid input


def test_paths_are_normalised_when_checked() -> None:
    # Not normalised -> refused (the launcher compares literally after realpath).
    with pytest.raises(DefinitionError, match="normalised"):
        validate_definition(_def(workdir="/srv/app/../app"), LOCAL)
    with pytest.raises(DefinitionError, match="absolute"):
        validate_definition(_def(workdir="srv/app"), LOCAL)


@pytest.mark.parametrize("bad,why", [
    (_def(workdir="/etc"), "outside"),                                   # beyond --local ceiling
    (_def(observe={"logs": ["/var/log/app"]}), "outside"),
    (_def(run_as="alice"), "run_as"),                                    # local launcher cannot
    (_def(access={"files": {"write": ["."]}}), "observer"),              # observers never write
    (_def(access={"commands": {"mode": "shell"}}), "observer"),
    (_def(access={"commands": {"mode": "list", "allow": ["ls; rm"]}}), "command"),
    (_def(instructions="x" * (16 * 1024 + 1)), "instructions"),
    (_def(secret="x"), "unknown"),
    (_def(approval={"policy": "sometimes"}), "approval"),
    (_def(observe={"metrics": {"enabled": True, "interval_s": 1}}), "interval_s"),
])
def test_rejects(bad: dict[str, Any], why: str) -> None:
    with pytest.raises(DefinitionError, match=why):
        validate_definition(bad, LOCAL)


@pytest.mark.parametrize("bad,why", [
    ("not a dict", "object"),
    ({**_def(), "id": "a b"}, "id"),
    ({k: v for k, v in _def().items() if k != "kind"}, "kind"),
    (_def(kind="admin"), "kind"),
    (_def(runtime="gpt"), "runtime"),
    (_def(max_instances=0), "max_instances"),
    (_def(max_instances=True), "max_instances"),
    (_def(coordinator_capable="yes"), "coordinator_capable"),
    (_def(description="d" * 1001), "description"),
    (_def(model="m" * 201), "model"),
    (_def(capabilities=["c"] * 33), "capabilities"),
    (_def(instructions="é" * (8 * 1024 + 1)), "instructions"),           # bytes, not chars
    (_def(workdir="/srv/application"), "outside"),                      # prefix is not inside
    (_def(workdir="//srv/app"), "workdir"),
    (_def(workdir="/srv/app/\x00"), "workdir"),
    (_def(observe={"sources": ["rel"]}), "absolute"),
    (_def(observe={"sources": ["/srv/app"] * 51}), "observe.sources"),
    (_def(observe={"extra": []}), "unknown"),
    (_def(observe={"processes": [{"name": "x", "pid": 0}]}), "pid"),
    (_def(observe={"processes": [{"name": "x", "pid": True}]}), "pid"),
    (_def(observe={"processes": [{"pid": 3}]}), "name"),
    (_def(observe={"processes": [{"name": "x", "user": "root"}]}), "unknown"),
    (_def(observe={"metrics": {"enabled": True, "interval_s": 3601}}), "interval_s"),
    (_def(observe={"metrics": {"enabled": True, "kinds": ["gpu"]}}), "kinds"),
    (_def(observe={"metrics": {"enabled": "yes"}}), "enabled"),
    (_def(access={"preset": "reader"}), "unknown"),
    (_def(access={"files": {"read": ["../x"]}}), "glob"),
    (_def(access={"files": {"read": ["/etc/*"]}}), "glob"),
    (_def(access={"files": {"read": ["~/x"]}}), "glob"),
    (_def(access={"files": {"exec": ["x"]}}), "unknown"),
    (_def(access={"network": "some"}), "network"),
    (_def(access={"commands": {"mode": "always"}}), "mode"),
    (_def(access={"commands": {"mode": "list", "allow": ["$(rm)"]}}), "command"),
    (_def(access={"commands": {"mode": "list", "allow": ["tail f | tee g"]}}), "command"),
    (_def(access={"commands": {"mode": "list", "allow": ["a b c d e f g h i"]}}), "command"),
    (_def(access={"commands": {"mode": "list", "allow": ["ls"] * 101}}), "allow"),
    (_def(kind="operator", access={"commands": {"mode": "shell", "allow": ["ls"]}}), "allow"),
    (_def(access={"commands": {"mode": "list", "privileged": ["journalctl"]}}), "privileged"),
    (_def(access={"commands": {"mode": "list", "sudo": []}}), "unknown"),
    (_def(limits={"max_turns": 3}), "unknown"),
    (_def(limits={"max_duration_s": -1}), "limits"),
    (_def(limits={"max_duration_s": "1"}), "limits"),
    (_def(approval={"policy": "listed", "patterns": ["rm -rf /; x"]}), "approval"),
    (_def(approval={"policy": "auto", "why": "x"}), "unknown"),
    (_def(visibility="public"), "visibility"),
    (_def(allowed_accounts="bob"), "allowed_accounts"),
    (_def(run_as="Root!"), "run_as"),
])
def test_rejects_more(bad: Any, why: str) -> None:
    with pytest.raises(DefinitionError, match=why):
        validate_definition(bad, LOCAL)


def test_outside_ceiling_errors_carry_their_code() -> None:
    with pytest.raises(DefinitionError) as e:
        validate_definition(_def(workdir="/etc"), LOCAL)
    assert e.value.code is ErrorCode.OUTSIDE_CEILING
    with pytest.raises(DefinitionError) as e:
        validate_definition(_def(secret="x"), LOCAL)
    assert e.value.code is ErrorCode.INVALID_REQUEST


def test_config_mode_machine_accepts_no_definition() -> None:
    with pytest.raises(DefinitionError, match="--local or --root"):
        validate_definition(_def(), CONFIG)


def test_run_as_needs_root_machine_with_run_as() -> None:
    no_run_as = dataclasses.replace(ROOT, run_as=False)
    with pytest.raises(DefinitionError, match="run_as"):
        validate_definition(_def(run_as="deploy"), no_run_as)
    assert validate_definition(_def(run_as=None), LOCAL)["run_as"] is None


def test_root_machine_accepts_any_absolute_path_and_run_as() -> None:
    d = validate_definition(_def(kind="operator", workdir="/opt/shop", run_as="deploy",
                                 access={"commands": {"mode": "list", "allow": ["systemctl status"],
                                                      "privileged": ["journalctl"]}}), ROOT)
    assert d["run_as"] == "deploy" and d["access"]["commands"]["privileged"] == ["journalctl"]


def test_total_size_limit() -> None:
    with pytest.raises(DefinitionError, match="64 KiB"):
        validate_definition(_def(description="d", capabilities=["c" * 64] * 32,
                                 observe={"sources": ["/srv/app/" + "x" * 1300] * 50}), LOCAL)


@pytest.mark.parametrize("path,root,expected", [
    ("/srv/app", "/srv/app", True),
    ("/srv/app/x/y", "/srv/app", True),
    ("/srv/application", "/srv/app", False),
    ("/srv", "/srv/app", False),
    ("/srv/app/../etc", "/srv/app", False),
    ("/srv/app/./x", "/srv/app/", True),
    ("/anything", "/", True),
    ("/", "/", True),
    ("relative", "/", False),
    ("/srv/app", "relative", False),
])
def test_inside(path: str, root: str, expected: bool) -> None:
    assert inside(path, root) is expected
