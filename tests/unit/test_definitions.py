"""Agent definitions: the schema the broker validates before a deploy."""
from __future__ import annotations

import copy
import dataclasses
import json
from pathlib import Path
from typing import Any

import pytest

from sessionweave.definitions import (
    APPROVAL_POLICIES,
    COMMAND_MODES,
    COMMAND_WRAPPERS,
    DEFINITION_MAX_BYTES,
    INSTRUCTIONS_MAX_BYTES,
    KINDS,
    LIMIT_MAX,
    LIST_IS_GUIDANCE,
    LISTED_WITH_WRITES,
    MAX_DEFINITIONS_PER_LAUNCHER,
    METRIC_KINDS,
    OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS,
    PRIVILEGED_RUNNER_PROBLEM,
    PROTECTED_WRITE_NAMES,
    PROTECTED_WRITE_PROBLEM,
    PUBLISHED_PROFILES_BUDGET,
    READ_COMMANDS,
    READ_ONLY_NEEDS_SANDBOX,
    READ_ONLY_NETWORK_WARNING,
    UNSANDBOXED_WRITER_WARNING,
    DefinitionError,
    _listed_overlap,
    access_warnings,
    budget_problem,
    definition_warnings,
    inside,
    is_command_wrapper,
    json_size,
    profiles_frame_size,
    protected_write_problem,
    published_profile,
    read_only_sandbox_problem,
    runs_sandboxed,
    validate_definition,
    wrapper_name,
)
from sessionweave.errors import ErrorCode
from sessionweave.machines import Machine

KEY = "lk_" + "ab" * 16
LOCAL = Machine("acc_1", KEY, "box", "local", "/srv/app", ["definitions"], "bwrap", False,
                "linux", [], 0, 0)
ROOT = dataclasses.replace(LOCAL, mode="root", ceiling="/", run_as=True)
CONFIG = dataclasses.replace(LOCAL, mode="config", ceiling=None, capabilities=[])


def _def(**kw: Any) -> dict[str, Any]:
    return {"id": "obs", "domain": "shop", "role": "logs", "runtime": "opencode",
            "kind": "observer", "workdir": "/srv/app", "max_instances": 1, **kw}


# observer_as_root needs commands "none" (the Runner's interim rule).
_NO_CMDS: dict[str, Any] = {"commands": {"mode": "none"}}


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
    # access model v2: an observer may run any command (its sandbox keeps it read-only)
    assert d["access"]["commands"] == {"mode": "shell", "allow": [], "privileged": []}
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
        access={"files": {"read": ["src/**"], "write": ["out/"], "deny": [".env"]},
                "network": "any",
                "commands": {"mode": "list", "allow": ["systemctl status", "ls"]}},
        limits={"max_duration_s": 600, "max_idle_s": 60, "max_cost_usd": 1.5},
        approval={"policy": "auto", "patterns": ["systemctl restart"]},
        instructions="Watch.", visibility="personal", allowed_accounts=["bob"],
    )
    before = copy.deepcopy(raw)
    d = validate_definition(raw, LOCAL)
    assert raw == before
    assert d["observe"]["processes"] == [{"name": "nginx", "pid": 12}, {"name": "app", "pid": None}]
    assert d["observe"]["metrics"] == {"enabled": True, "interval_s": 30, "kinds": ["cpu", "load"]}
    assert d["access"]["commands"] == {"mode": "list", "allow": ["systemctl status", "ls"],
                                       "privileged": []}
    assert d["access"]["network"] == "any"
    assert d["limits"] == {"max_duration_s": 600, "max_idle_s": 60, "max_cost_usd": 1.5}
    assert d["approval"] == {"policy": "auto", "patterns": ["systemctl restart"]}
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
    # Fix round 1
    (_def(limits={"max_cost_usd": 10**400}), "limits"),                 # OverflowError
    (_def(limits={"max_duration_s": float("inf")}), "limits"),
    (_def(access={"files": {"read": ["{..,x}/etc/*"]}}), "glob"),       # brace expansion
    (_def(access={"files": {"deny": ["a{b,c}"]}}), "glob"),
    (_def(access={"files": {"read": ["\\.\\./etc"]}}), "glob"),       # escaped dots
    (_def(capabilities=["a\x00b"]), "capabilities"),
    (_def(capabilities=["a\nb"]), "capabilities"),
    (_def(access={"network": ["ok.com", "bad\x00host"]}), "network"),
    (_def(allowed_accounts=["bob\n"]), "allowed_accounts"),
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


def test_observer_never_gets_privileged_commands() -> None:
    with pytest.raises(DefinitionError, match="observer"):
        validate_definition(
            _def(run_as="deploy", access={"commands": {"mode": "list", "privileged": ["ls"]}}),
            ROOT,
        )


def test_normalised_output_size_limit() -> None:
    # A raw definition just under the limit whose defaults push it over.
    raw = _def(observe={"sources": []})
    base = len(json.dumps(raw).encode())
    room = DEFINITION_MAX_BYTES - base - 50
    n = 40
    path_len = (room - 4 * n) // n  # each entry: quotes, comma, space
    raw["observe"]["sources"] = ["/srv/app/" + "x" * (path_len - 9)] * n
    assert len(json.dumps(raw).encode()) <= DEFINITION_MAX_BYTES
    with pytest.raises(DefinitionError, match="64 KiB"):
        validate_definition(raw, LOCAL)


def test_observer_as_root_defaults_false() -> None:
    assert validate_definition(_def(), LOCAL)["observer_as_root"] is False
    assert validate_definition(_def(run_as="deploy"), ROOT)["observer_as_root"] is False


def test_observer_as_root_only_for_observers_on_root_machines() -> None:
    d = validate_definition(_def(observer_as_root=True, access=_NO_CMDS), ROOT)
    assert d["observer_as_root"] is True
    assert validate_definition(d, ROOT) == d  # round trip
    assert validate_definition(_def(observer_as_root=False), LOCAL)["observer_as_root"] is False
    with pytest.raises(DefinitionError, match="--root") as exc:
        validate_definition(_def(observer_as_root=True), LOCAL)
    assert exc.value.code is ErrorCode.INVALID_REQUEST
    with pytest.raises(DefinitionError, match="observer") as exc:
        validate_definition(_def(kind="operator", observer_as_root=True), ROOT)
    assert exc.value.code is ErrorCode.INVALID_REQUEST
    for bad in ("yes", 1, None):
        with pytest.raises(DefinitionError, match="boolean"):
            validate_definition(_def(observer_as_root=bad), ROOT)


# --- final wave: parity with the launcher (Runner) --------------------------------------

@pytest.mark.parametrize("field,raw", [
    ("allow", _def(kind="operator", access={"commands": {"mode": "list", "allow": ["-rf"]}})),
    ("allow", _def(kind="operator", access={"commands": {"mode": "list",
                                                         "allow": ["--help x"]}})),
    ("privileged", _def(kind="operator", workdir="/opt",
                        access={"commands": {"mode": "list", "privileged": ["-x"]}})),
    ("patterns", _def(kind="operator", access={"commands": {"mode": "list", "allow": ["ls"]}},
                      approval={"policy": "listed", "patterns": ["-f"]})),
])
def test_command_entry_starting_with_an_option_refused(field: str, raw: dict[str, Any]) -> None:
    with pytest.raises(DefinitionError, match="start with a command") as exc:
        validate_definition(raw, ROOT)
    assert field in str(exc.value)
    assert exc.value.code is ErrorCode.INVALID_REQUEST


def test_read_commands_match_the_launcher() -> None:
    # A copy of the Runner's permissions.READ_COMMANDS (parity: keep both in step).
    assert READ_COMMANDS == (
        "ls", "cat", "head", "tail", "grep", "zcat", "zgrep", "wc", "stat", "file", "du",
        "df", "free", "uptime", "uname", "hostname", "id", "whoami", "date", "ps", "pgrep",
        "top -b", "vmstat", "iostat", "journalctl", "dmesg", "ss", "lsof",
        "systemctl status",
    )


@pytest.mark.parametrize("entry", ["rm", "systemctl restart", "top", "git status", "lsblk",
                                   "/bin/ls", "sh", "python3", "env git", "eval"])
def test_any_list_entry_accepted_for_both_kinds(entry: str) -> None:
    """Access model v2: a command list is guidance; an observer may list any command
    (its sandbox keeps it read-only) and so may an operator (runners and git
    included)."""
    for kind in ("observer", "operator"):
        d = validate_definition(
            _def(kind=kind, access={"commands": {"mode": "list", "allow": ["ls", entry]}}),
            LOCAL)
        assert d["access"]["commands"]["allow"] == ["ls", entry]


def test_observer_read_command_entries_with_arguments_accepted() -> None:
    allow = ["journalctl -u", "top -b -n1", "systemctl status nginx", "tail -n", "ps aux"]
    d = validate_definition(_def(access={"commands": {"mode": "list", "allow": allow}}), LOCAL)
    assert d["access"]["commands"]["allow"] == allow
    # mode none, shell and an empty list stay valid
    assert validate_definition(
        _def(access={"commands": {"mode": "none"}}), LOCAL)["access"]["commands"]["mode"] == "none"
    assert validate_definition(_def(access={"commands": {"mode": "shell"}}), LOCAL)
    assert validate_definition(_def(access={"commands": {"mode": "list"}}), LOCAL)


def test_privileged_entries_keep_their_options_exactly() -> None:
    # The launcher's root helper runs only the options a privileged entry spells:
    # the broker stores them as written (no reordering, no merging).
    privileged = ["journalctl -u", "journalctl -n", "systemctl restart nginx",
                  "journalctl --vacuum-size"]
    d = validate_definition(
        _def(kind="operator", workdir="/opt",
             access={"commands": {"mode": "list", "privileged": privileged}}), ROOT)
    assert d["access"]["commands"]["privileged"] == privileged


def test_observer_as_root_excludes_run_as() -> None:
    with pytest.raises(DefinitionError, match="exclude each other") as exc:
        validate_definition(_def(observer_as_root=True, run_as="deploy"), ROOT)
    assert exc.value.code is ErrorCode.INVALID_REQUEST
    assert validate_definition(_def(run_as="deploy"), ROOT)["run_as"] == "deploy"


def test_root_execution_needs_a_run_as_capable_root_launcher() -> None:
    no_run_as = dataclasses.replace(ROOT, run_as=False)
    with pytest.raises(DefinitionError, match="--root") as exc:
        validate_definition(
            _def(kind="operator", workdir="/opt",
                 access={"commands": {"mode": "list", "privileged": ["journalctl -u"]}}),
            no_run_as)
    assert exc.value.code is ErrorCode.OUTSIDE_CEILING
    with pytest.raises(DefinitionError, match="--root") as exc:
        validate_definition(_def(observer_as_root=True), no_run_as)
    assert exc.value.code is ErrorCode.OUTSIDE_CEILING
    with pytest.raises(DefinitionError, match="run_as") as exc:
        validate_definition(_def(run_as="deploy"), no_run_as)
    assert exc.value.code is ErrorCode.OUTSIDE_CEILING
    # the same definitions are accepted by a root launcher that runs as root
    validate_definition(_def(observer_as_root=True, access=_NO_CMDS), ROOT)
    validate_definition(
        _def(kind="operator", workdir="/opt",
             access={"commands": {"mode": "list", "privileged": ["journalctl -u"]}}), ROOT)


# --- final wave: upper bounds of the limits ---------------------------------------------

def test_limit_caps() -> None:
    assert LIMIT_MAX == {"max_duration_s": 30 * 86400, "max_idle_s": 30 * 86400,
                         "max_cost_usd": 10000}


@pytest.mark.parametrize("key", ["max_duration_s", "max_idle_s", "max_cost_usd"])
def test_limits_capped(key: str) -> None:
    cap = LIMIT_MAX[key]
    assert validate_definition(_def(limits={key: cap}), LOCAL)["limits"] == {key: cap}
    assert validate_definition(_def(limits={key: None}), LOCAL)["limits"] == {key: None}
    for over in (cap + 1, float(cap) + 0.5, 10**30):
        with pytest.raises(DefinitionError, match="at most") as exc:
            validate_definition(_def(limits={key: over}), LOCAL)
        assert key in str(exc.value)


# --- final wave: listed approval vs allowed entries (the launcher's rule) ---------------

# (kind, allow, privileged, pattern, accepted)
_LISTED_TABLE: list[tuple[str, list[str], list[str], str, bool]] = [
    ("operator", ["systemctl status", "systemctl show"], [], "systemctl restart", True),
    ("operator", ["systemctl status", "systemctl show", "ls -la", "cat"], [],
     "systemctl restart", True),
    ("operator", ["systemctl status -l"], [], "systemctl restart", True),
    ("operator", ["systemctl status"], [], "systemctl restart --now", True),
    ("operator", ["/usr/bin/systemctl status"], [], "systemctl restart", True),
    ("operator", ["docker compose ps", "docker compose logs"], [], "docker compose up", True),
    ("operator", ["ls"], ["systemctl status"], "systemctl restart", True),
    ("observer", ["journalctl -u"], [], "rm", True),
    ("operator", ["systemctl"], [], "systemctl restart", False),    # entry runs the pattern
    ("operator", ["systemctl restart nginx"], [], "systemctl restart", False),  # covered
    ("operator", ["systemctl restart --dry-run"], [], "systemctl restart", False),
    ("operator", ["systemctl status"], [], "systemctl", False),
    ("operator", ["systemctl --no-pager status"], [], "systemctl --no-pager restart",
     False),                                                         # option first
    ("operator", ["systemctl -H x status"], [], "systemctl -H x restart", False),
    ("operator", ["systemctl -H"], [], "systemctl restart", False),
    ("operator", ["tail /var/log/syslog"], [], "tail -f", False),  # differ at an option
    ("operator", ["tail -n"], [], "tail -f", False),
    ("operator", ["/usr/bin/systemctl"], [], "systemctl restart", False),  # basename
    ("operator", ["ls"], ["systemctl"], "systemctl restart", False),
    ("operator", ["ls"], ["systemctl restart nginx"], "systemctl restart", False),
    ("operator", ["ls"], ["/bin/systemctl"], "systemctl restart", False),
    ("operator", ["ls"], ["systemctl --no-block"], "systemctl restart", False),
    ("observer", [], [], "journalctl --vacuum-size", True),        # empty list: no commands
    ("observer", ["systemctl status"], [], "systemctl status nginx", False),
]


def _listed_def(kind: str, allow: list[str], privileged: list[str], pattern: str,
                ) -> dict[str, Any]:
    return _def(kind=kind, access={"commands": {"mode": "list", "allow": allow,
                                                "privileged": privileged}},
                approval={"policy": "listed", "patterns": [pattern]},
                # an observer on a --root launcher runs as a non-root machine user
                run_as="deploy" if kind == "observer" else None)


@pytest.mark.parametrize("kind,allow,privileged,pattern,accepted", _LISTED_TABLE)
def test_listed_pattern_vs_allowed_entries(
    kind: str, allow: list[str], privileged: list[str], pattern: str, accepted: bool,
) -> None:
    # The rule itself (an operator can write, so through validate_definition its
    # listed patterns are refused before this rule: LISTED_WITH_WRITES).
    cmds = {"mode": "list", "allow": allow, "privileged": privileged}
    appr = {"policy": "listed", "patterns": [pattern]}
    raw = _listed_def(kind, allow, privileged, pattern)
    if accepted:
        _listed_overlap(kind, cmds, appr)
    else:
        with pytest.raises(DefinitionError, match="overlaps") as exc:
            _listed_overlap(kind, cmds, appr)
        assert exc.value.code is ErrorCode.INVALID_REQUEST
        assert repr(pattern) in str(exc.value)
    if kind == "operator":
        with pytest.raises(DefinitionError) as exc:
            validate_definition(raw, ROOT)
        assert str(exc.value) == LISTED_WITH_WRITES
    elif accepted:
        assert validate_definition(raw, ROOT)["approval"]["patterns"] == [pattern]
    else:
        with pytest.raises(DefinitionError, match="overlaps"):
            validate_definition(raw, ROOT)


def test_listed_check_only_with_listed_patterns() -> None:
    for approval in ({"policy": "auto", "patterns": ["git push"]},
                     {"policy": "always", "patterns": ["git push"]},
                     {"policy": "listed", "patterns": []}):
        validate_definition(_def(kind="operator", access={"commands": {
            "mode": "list", "allow": ["systemctl"]}}, approval=approval), LOCAL)
    # mode none: no command runs, the patterns change nothing
    validate_definition(_def(access={"commands": {"mode": "none"}},
                             approval={"policy": "listed", "patterns": ["journalctl -f"]}), LOCAL)


def test_listed_table_matches_the_launcher() -> None:
    """The same table through the Runner's own rule (not a dependency of the broker:
    runs only where the Runner is installed next to it)."""
    perms = pytest.importorskip(
        "sessionweave_runner.permissions",
        reason="sessionweave-runner is not installed in this environment (not a broker"
               " dependency); the launcher runs the same table in its own tests")
    for kind, allow, privileged, pattern, accepted in _LISTED_TABLE:
        cmds = perms._resolve_commands(
            kind, perms.Commands("list", tuple(allow), (), tuple(privileged)))
        try:
            perms._listed_denies(cmds, (pattern,))
            runner_ok = True
        except ValueError:
            runner_ok = False
        assert runner_ok is accepted, (kind, allow, privileged, pattern)


# --- final wave: listed approvals refuse shells and command wrappers --------------------

# (kind, mode, allow, privileged, pattern, accepted)
_LISTED_WRAPPER_TABLE: list[tuple[str, str, list[str], list[str], str, bool]] = [
    ("operator", "shell", [], [], "git push", False),               # a shell with patterns
    ("operator", "shell", [], ["journalctl -u"], "rm", False),
    ("operator", "list", ["sh"], [], "git push", False),
    ("operator", "list", ["bash -c"], [], "git push", False),
    ("operator", "list", ["/usr/bin/env"], [], "git push", False),  # by basename
    ("operator", "list", ["xargs"], [], "rm", False),
    ("operator", "list", ["python3.12"], [], "git push", False),    # python*
    ("operator", "list", ["python"], [], "git push", False),
    ("operator", "list", ["find /srv"], [], "rm", False),
    ("operator", "list", ["ls", "sudo"], [], "rm", False),
    ("operator", "list", ["ls"], ["systemd-run"], "rm", False),     # privileged too
    ("operator", "list", ["ls"], ["/usr/bin/nsenter"], "rm", False),
    ("operator", "list", ["git status", "pythonic"], [], "git push", False),  # prefix match
    ("operator", "list", ["git status"], [], "rm", False),         # git runs repo config
    ("operator", "list", ["ls"], ["/usr/bin/git log"], "rm", False),
    ("operator", "list", ["perl5.36"], [], "rm", False),           # version suffixes
    ("operator", "list", ["node18"], [], "rm", False),
    ("operator", "list", ["ld-linux-x86-64.so.2"], [], "rm", False),
    ("operator", "list", ["ls", "systemctl status", "shred"], [], "rm", True),  # not wrappers
    ("operator", "list", ["shx", "bashful", "envoy"], [], "rm", True),
    ("operator", "list", ["ls"], ["systemctl status"], "rm", True),
]


@pytest.mark.parametrize("kind,mode,allow,privileged,pattern,accepted", _LISTED_WRAPPER_TABLE)
def test_listed_refuses_shell_and_wrappers(
    kind: str, mode: str, allow: list[str], privileged: list[str], pattern: str,
    accepted: bool,
) -> None:
    cmds = {"mode": mode, "allow": allow, "privileged": privileged}
    appr = {"policy": "listed", "patterns": [pattern]}
    if accepted:
        _listed_overlap(kind, cmds, appr)
    else:
        with pytest.raises(DefinitionError, match=r"'list' until approvals|runs other commands"
                           ) as exc:
            _listed_overlap(kind, cmds, appr)
        assert exc.value.code is ErrorCode.INVALID_REQUEST
    # Through validate_definition an operator (it writes) gets LISTED_WITH_WRITES first.
    raw = _def(kind=kind, access={"commands": cmds}, approval=appr)
    with pytest.raises(DefinitionError) as exc:
        validate_definition(raw, ROOT)
    assert str(exc.value) == LISTED_WITH_WRITES


def test_shell_and_wrappers_fine_without_listed_patterns() -> None:
    for approval in ({"policy": "auto", "patterns": ["git push"]},
                     {"policy": "listed", "patterns": []}):
        validate_definition(_def(kind="operator", approval=approval), LOCAL)  # shell
    # A read-only agent may list them (approval "always" leaves no commands at all).
    validate_definition(_def(kind="operator", access={"commands": {
        "mode": "list", "allow": ["sh", "python3"]}}, approval={"policy": "always"}), LOCAL)


def test_command_wrappers_match_the_launcher() -> None:
    # A copy of the Runner's permissions.COMMAND_WRAPPERS (parity: keep both in step).
    assert len(COMMAND_WRAPPERS) == len(set(COMMAND_WRAPPERS)) == 114
    assert {"sh", "env", "xargs", "python*", "find", "ssh", "man", "git", "ld-linux*",
            "time", "mksh", "socat"} <= set(COMMAND_WRAPPERS)
    assert not {"docker", "kubectl", "systemctl"} & set(COMMAND_WRAPPERS)
    perms = pytest.importorskip(
        "sessionweave_runner.permissions",
        reason="sessionweave-runner is not installed in this environment (not a broker"
               " dependency)")
    assert perms.COMMAND_WRAPPERS == COMMAND_WRAPPERS


def test_listed_wrapper_table_matches_the_launcher() -> None:
    perms = pytest.importorskip(
        "sessionweave_runner.permissions",
        reason="sessionweave-runner is not installed in this environment (not a broker"
               " dependency); the launcher runs its own tests")
    for kind, mode, allow, privileged, pattern, accepted in _LISTED_WRAPPER_TABLE:
        try:
            cmds = perms._resolve_commands(
                kind, perms.Commands(mode, tuple(allow), (), tuple(privileged)))
            perms._listed_denies(cmds, (pattern,))
            runner_ok = True
        except ValueError:
            runner_ok = False
        assert runner_ok is accepted, (kind, mode, allow, privileged, pattern)


# --- final review: dot-only ids, wrapper names, command runners in writers' lists -------

@pytest.mark.parametrize("value", [".", "..", "...", "." * 64])
@pytest.mark.parametrize("key", ["id", "domain", "role"])
def test_dot_only_ids_refused(key: str, value: str) -> None:
    with pytest.raises(DefinitionError, match="not only dots"):
        validate_definition({**_def(), key: value}, LOCAL)


@pytest.mark.parametrize("value", ["a.", ".a", "a..b", "-", "_", "v1.2"])
def test_ids_with_dots_and_more_accepted(value: str) -> None:
    assert validate_definition({**_def(), "id": value}, LOCAL)["id"] == value


@pytest.mark.parametrize("word,name", [
    ("perl5.36", "perl"), ("python3.12", "python"), ("node18", "node"), ("lua-5.4", "lua"),
    ("/usr/bin/ruby3.2", "ruby"), ("ld-linux-x86-64.so.2", "ld-linux-x86-64.so."),
    ("123", "123"), ("git", "git"), ("/usr/bin/git", "git"),
])
def test_wrapper_name_strips_version_suffix(word: str, name: str) -> None:
    assert wrapper_name(word) == name


@pytest.mark.parametrize("word,expected", [
    ("perl5.36", True), ("ruby3.2", True), ("php8.1", True), ("node18", True),
    ("tclsh8.6", True), ("lua5.4", True), ("python3.12", True), ("/usr/bin/git", True),
    ("ld-linux-x86-64.so.2", True), ("ld.so", True), ("time", True), ("mksh", True),
    ("Rscript", True), ("docker", False), ("kubectl", False), ("systemctl", False),
    ("pythonic", True), ("gitk", False), ("shred", False),
])
def test_is_command_wrapper(word: str, expected: bool) -> None:
    assert is_command_wrapper(word) is expected


# (kind, write, policy, allow, privileged, accepted): list mode, no listed patterns.
# Access model v2: a writer's list is guidance (runners and git accepted, with
# LIST_IS_GUIDANCE); a privileged runner stays refused (swv-root runs it as root).
_LIST_RUNNER_TABLE: list[tuple[str, list[str], str, list[str], list[str], bool]] = [
    ("operator", [], "auto", ["git status", "git log"], [], True),
    ("operator", ["out"], "auto", ["git log", "env git"], [], True),
    ("operator", [], "auto", ["nice git", "xargs git", "bash", "python3", "eval", "make"], [],
     True),
    ("operator", [], "auto", ["ls"], ["git pull"], False),         # privileged runners
    ("operator", [], "auto", ["git status"], ["/usr/bin/git status"], False),
    ("operator", [], "auto", ["ls"], ["env ls"], False),
    ("operator", [], "auto", ["ls"], ["bash"], False),
    ("operator", [], "auto", ["ls"], ["systemctl restart"], True),
    ("operator", [], "always", ["git status", "sh"], ["bash"], True),  # always: no commands
    ("observer", [], "auto", ["ls", "python3", "git log"], [], True),
]


@pytest.mark.parametrize("kind,write,policy,allow,privileged,accepted", _LIST_RUNNER_TABLE)
def test_list_runners_guidance_privileged_runners_refused(
    kind: str, write: list[str], policy: str, allow: list[str], privileged: list[str],
    accepted: bool,
) -> None:
    raw = _def(kind=kind, access={"files": {"write": write}, "commands": {
        "mode": "list", "allow": allow, "privileged": privileged}},
        approval={"policy": policy}, run_as="deploy" if kind == "observer" else None)
    if accepted:
        validate_definition(raw, ROOT)
        return
    entry = next(e for e in privileged if is_command_wrapper(e.split()[0]))
    with pytest.raises(DefinitionError) as exc:
        validate_definition(raw, ROOT)
    assert str(exc.value) == PRIVILEGED_RUNNER_PROBLEM.format(entry=entry)


def test_list_runner_rule_matches_the_launcher() -> None:
    perms = pytest.importorskip(
        "sessionweave_runner.permissions",
        reason="sessionweave-runner is not installed in this environment (not a broker"
               " dependency)")
    from sessionweave import definitions as ours
    for name in ("LIST_IS_GUIDANCE", "UNSANDBOXED_WRITER_WARNING", "READ_ONLY_NEEDS_SANDBOX",
                 "PRIVILEGED_RUNNER_PROBLEM", "READ_ONLY_NETWORK_WARNING",
                 "OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS", "DENY_SUMMARY_SANDBOXED",
                 "DENY_SUMMARY_UNSANDBOXED", "DENY_SUMMARY_ROOT_OBSERVER",
                 "DENY_ROOT_OBSERVER_WARNING", "NETWORK_SUMMARY_NONE_WITH_COMMANDS"):
        assert getattr(perms, name) == getattr(ours, name), name
    for gone in ("LIST_RUNNER_WITH_WRITES", "GIT_LIST_WARNING", "GIT_LIST_WARNING_UNSANDBOXED",
                 "list_git_warning"):
        assert not hasattr(ours, gone), gone
    for word in ["perl5.36", "node18", "ld-linux-x86-64.so.2", "123", "/usr/bin/git",
                 "python3.12", "docker", "gitk"]:
        assert perms.wrapper_name(word) == wrapper_name(word)
        assert perms.is_command_wrapper(word) is is_command_wrapper(word)
    for kind, write, policy, allow, privileged, accepted in _LIST_RUNNER_TABLE:
        try:
            perms.resolve_definition_permissions(
                Path("/srv/app"), kind, read=None, write=write or None, deny=None,
                network=None, commands=perms.Commands("list", tuple(allow), (),
                                                      tuple(privileged)),
                extra_read=(), approval=perms.Approval(policy, ()))
            runner_ok = True
        except ValueError:
            runner_ok = False
        assert runner_ok is accepted, (kind, write, policy, allow, privileged)


# --- final review I2: the launcher's rules that depend only on the definition, its
# runtime and the machine's start mode ---------------------------------------------------

NO_BWRAP_ROOT = dataclasses.replace(ROOT, sandbox=None)
USER_ROOT = dataclasses.replace(ROOT, run_as=False)
MACHINES = {"local": LOCAL, "root": ROOT, "root-nobw": NO_BWRAP_ROOT, "root-user": USER_ROOT}
_OP = {"kind": "operator"}
_NONE = {"commands": {"mode": "none"}}

# (name, definition overrides, machines that accept it)
_LAUNCHER_RULES: list[tuple[str, dict[str, Any], set[str]]] = [
    ("host list + default shell", {**_OP, "access": {"network": ["example.com"]}}, set()),
    ("empty host list + shell", {**_OP, "access": {"network": []}}, set()),
    ("host list + list", {**_OP, "access": {"network": ["a.com"], "commands": {
        "mode": "list", "allow": ["ls"]}}}, set()),
    ("host list observer default list", {"run_as": "deploy",
                                         "access": {"network": ["a.com"]}}, set()),
    ("host list + none", {**_OP, "access": {"network": ["a.com"], **_NONE}}, set(MACHINES)),
    ("host list + always", {**_OP, "access": {"network": ["a.com"]},
                            "approval": {"policy": "always"}}, set(MACHINES)),
    ("uppercase host", {**_OP, "access": {"network": ["Example.com"], **_NONE}}, set()),
    ("ipv6 host", {**_OP, "access": {"network": ["[::1]"], **_NONE}}, set()),
    ("bare ipv6", {**_OP, "access": {"network": ["::1"], **_NONE}}, set()),
    ("wildcard tld", {**_OP, "access": {"network": ["*.com"], **_NONE}}, set()),
    ("port 0", {**_OP, "access": {"network": ["a.com:0"], **_NONE}}, set()),
    ("port", {**_OP, "access": {"network": ["a.com:8443"], **_NONE}}, set(MACHINES)),
    ("wildcard host", {**_OP, "runtime": "claude",
                       "access": {"network": ["*.example.com"], **_NONE}}, set(MACHINES)),
    ("tilde inside glob", {**_OP, "access": {"files": {"read": ["a/~b"]}}}, set()),
    ("padded glob", {**_OP, "access": {"files": {"deny": [" a"]}}}, set()),
    ("trailing space glob", {**_OP, "access": {"files": {"write": ["a "]}}}, set()),
    ("model leading dash", {**_OP, "model": "-x"}, set()),
    ("model with space", {**_OP, "model": "big pickle"}, set()),
    ("model ok", {**_OP, "model": "opencode/big-pickle"}, set(MACHINES)),
    ("observe glob *", {"run_as": "deploy", "observe": {"logs": ["/srv/app/*.log"]}}, set()),
    ("observe glob [", {"run_as": "deploy", "observe": {"sources": ["/srv/app/a[1]"]}},
     set()),
    ("run_as root", {**_OP, "run_as": "root"}, set()),
    ("run_as install user", {**_OP, "run_as": "sessionweave"}, set()),
    ("root observer without run_as", {}, {"local", "root-user"}),
    ("observer_as_root without bwrap", {"observer_as_root": True, "access": _NONE}, {"root"}),
    ("observer_as_root with commands", {"observer_as_root": True}, set()),
    ("opencode single star", {**_OP, "access": {"files": {"read": ["src/*.py"]}}},
     set()),
    ("opencode double star", {**_OP, "access": {"files": {"read": ["src/**"]}}},
     set(MACHINES)),
    ("opencode wildcard host", {**_OP, "access": {"network": ["*.a.com"], **_NONE}}, set()),
    ("cline host list", {**_OP, "runtime": "cline", "access": {"network": ["a.com"],
                                                               **_NONE}}, set()),
    ("cline bad provider", {**_OP, "runtime": "cline", "model": "Open:x"}, set()),
    ("cline provider", {**_OP, "runtime": "cline", "model": "ollama:qwen3:8b"},
     set(MACHINES)),
    ("cline observer_as_root", {"runtime": "cline", "observer_as_root": True}, set()),
    ("claude read subset", {**_OP, "runtime": "claude",
                            "access": {"files": {"read": ["src"]}}}, set()),
    ("claude read dot", {**_OP, "runtime": "claude", "access": {"files": {"read": ["."]}}},
     set(MACHINES)),
    ("claude port", {**_OP, "runtime": "claude",
                     "access": {"network": ["a.com:443"], **_NONE}}, set()),
    ("claude comma glob", {**_OP, "runtime": "claude",
                           "access": {"files": {"deny": ["a,b"]}}}, set()),
    ("claude workdir space", {**_OP, "runtime": "claude", "workdir": "/srv/app/a b"},
     set()),
    ("claude list unsandboxed", {**_OP, "runtime": "claude", "access": {"commands": {
        "mode": "list", "allow": ["ls"]}}}, set(MACHINES)),        # a list is guidance
    ("observer shell without bwrap", {"run_as": "deploy"}, {"root"}),
    ("observer none without bwrap", {"run_as": "deploy", "access": _NONE},
     {"root", "root-nobw"}),
    ("privileged runner", {**_OP, "access": {"commands": {"privileged": ["git pull"]}}},
     set()),
    ("claude observer list", {"runtime": "claude", "run_as": "deploy"},
     {"root"}),
    ("claude list + listed", {**_OP, "runtime": "claude", "access": {"commands": {
        "mode": "list", "allow": ["systemctl status"]}},
        "approval": {"policy": "listed", "patterns": ["systemctl restart"]}}, set()),
    ("opencode list + listed", {**_OP, "access": {"commands": {
        "mode": "list", "allow": ["systemctl status"]}},
        "approval": {"policy": "listed", "patterns": ["systemctl restart"]}}, set()),
    ("observer list + listed", {"run_as": "deploy", "access": {"commands": {
        "mode": "list", "allow": ["systemctl status"]}},
        "approval": {"policy": "listed", "patterns": ["systemctl restart"]}},
     {"root"}),
    ("operator listed + always", {**_OP, "approval": {"policy": "always", "patterns": [
        "systemctl restart"]}}, set(MACHINES)),
    ("protected write glob", {**_OP, "access": {"files": {"write": [".git"]}}}, set()),
    ("protected write + always", {**_OP, "access": {"files": {"write": ["**/AGENTS.md"]}},
                                  "approval": {"policy": "always"}}, set()),
    ("eval in a writer's list", {**_OP, "access": {"commands": {
        "mode": "list", "allow": ["eval"]}}}, set(MACHINES)),
    ("privileged + always", {**_OP, "access": {"commands": {"privileged": ["journalctl"]}},
                             "approval": {"policy": "always"}}, {"root", "root-nobw",
                                                                 "root-user"}),
]


@pytest.mark.parametrize("name,over,accepted", _LAUNCHER_RULES,
                         ids=[c[0] for c in _LAUNCHER_RULES])
def test_launcher_rules(name: str, over: dict[str, Any], accepted: set[str]) -> None:
    raw = {**_def(), **over}
    for mname, machine in MACHINES.items():
        try:
            validate_definition(copy.deepcopy(raw), machine)
            ok = True
        except DefinitionError:
            ok = False
        assert ok is (mname in accepted), (name, mname)


def _runner_check(raw: dict[str, Any], machine: Machine) -> Any:
    """The launcher's verdict on ``raw`` for a launcher like ``machine``: its own
    validator (``parse_definition``) plus every check of ``_check_definition`` that does
    not need the machine itself (validate_profile's globs, model and hosts, the runtime
    caps with its sandbox rule, the run-as rules). None: accepted."""
    rdefs = pytest.importorskip("sessionweave_runner.definitions")
    rmode = pytest.importorskip("sessionweave_runner.launcher_mode")
    perms = pytest.importorskip("sessionweave_runner.permissions")
    prereqs = pytest.importorskip("sessionweave_runner.prereqs")
    rsandbox = pytest.importorskip("sessionweave_runner.sandbox")

    sandbox = (rsandbox.SandboxSupport("bwrap", "t", "/usr/bin/bwrap") if machine.sandbox
               else rsandbox.SandboxSupport(None, "t"))
    assert machine.ceiling is not None
    mode = rmode.LauncherMode(machine.mode, Path(machine.ceiling), sandbox, machine.run_as,
                              "linux")
    try:
        p = rdefs.parse_definition(copy.deepcopy(raw), mode)
    except rdefs.DefinitionError as e:
        return str(e)
    if (why := prereqs.run_as_problem(p.run_as)) is not None:
        return why
    if p.run_as == "root":  # resolve_run_as refuses uid 0
        return "run_as root"
    as_root = mode.kind == "root" and mode.can_run_as
    sandboxed = mode.kind == "local" or (
        mode.kind == "root" and p.kind == "observer" and sandbox.kind is not None)
    if p.observer_as_root and not as_root:
        return "observer_as_root needs a launcher running as root"
    if p.kind == "observer" and as_root:
        if p.observer_as_root and not sandboxed:
            return "observer_as_root needs the sandbox"
        if p.observer_as_root and p.runtime == "cline":
            return "observer_as_root: no cline"
        if not p.observer_as_root and p.run_as is None:
            return "an Observer on a --root launcher needs run_as"
    resolved = p.permissions
    if (resolved.commands is not None and resolved.commands.privileged
            and not mode.can_run_as):
        return "privileged needs root"
    for g in [*(p.files_read or []), *(p.files_write or []), *(p.files_deny or [])]:
        if (why := perms.check_glob(g)) is not None:
            return why
    check_model = perms.cline_model_problem if p.runtime == "cline" else perms.model_problem
    if (why := check_model(p.model)) is not None:
        return why
    for h in p.network if isinstance(p.network, list) else []:
        if (why := perms.check_host(h)) is not None:
            return why
    problems: list[str] = perms.check_runtime(p.runtime, resolved, sandboxed=sandboxed)
    return problems[0] if problems else None


def _broker_check(raw: dict[str, Any], machine: Machine) -> str | None:
    try:
        validate_definition(copy.deepcopy(raw), machine)
    except DefinitionError as e:
        return str(e)
    return None


def test_launcher_rules_match_the_launcher() -> None:
    """The table through the Runner's own validator and runtime checks."""
    pytest.importorskip("sessionweave_runner.definitions",
                        reason="sessionweave-runner is not installed (not a broker"
                               " dependency)")
    for name, over, _accepted in _LAUNCHER_RULES:
        raw = {**_def(), **over}
        for mname, machine in MACHINES.items():
            b, r = _broker_check(raw, machine), _runner_check(raw, machine)
            assert (b is None) is (r is None), (name, mname, b, r)


_WORDS = ["git", "/usr/bin/git", "status", "push", "-C", "x", "log", "ls", "-la", "rm",
          "journalctl", "-u", "systemctl", "restart", "env", "sh", "python3.11", "perl5.36",
          "cat", "top", "-b", "node18", "tail", "-f", "docker", "compose", "ps"]
_GLOBS = [".", "**", "src", "src/**", "*.py", "a/*", " a", "a ", "a/~b", "~/x", "a,b",
          "a b", "(x)", "./**", "x?", "[ab]", "../x", "/etc", "a{b}"]
_HOSTS = ["example.com", "Example.com", "*.example.com", "*.com", "a.com:443", "a.com:0",
          "[::1]", "::1", "localhost", "127.0.0.1", "-a.com", "a_b.com", "a.com."]
_MODELS = ["", "sonnet", "-x", "big pickle", "openai:gpt-4", "Open:x", "ollama:qwen3:8b",
           "a:", ":x", "a/b@c+d", "é", "p_x:m", "p-x:-m"]
_PATHS = ["/srv/app", "/srv/app/log", "/srv/app/a*.log", "/srv/app/a[1]", "/srv/app/a b",
          "/srv/app/x?", "/srv/app/(x)"]
_IDS = ["x", ".", "..", "a.", ".a", "a..b", "-"]


def _random_definition(rnd: Any) -> dict[str, Any]:
    def entry() -> str:
        w = [rnd.choice(_WORDS) for _ in range(rnd.randint(1, 3))]
        return " ".join(["ls" if w[0].startswith("-") else w[0], *w[1:]])

    kind = rnd.choice(["operator", "observer"])
    mode = rnd.choice(["list", "shell", "none", None])
    cmds: dict[str, Any] = {} if mode is None else {"mode": mode}
    if mode == "list":
        cmds["allow"] = [entry() for _ in range(rnd.randint(0, 2))]
    if kind == "operator" and rnd.random() < 0.3:
        cmds["privileged"] = [entry()]
    files = {k: [rnd.choice(_GLOBS) for _ in range(rnd.randint(0, 2))]
             for k in ("read", "write", "deny") if rnd.random() < 0.4}
    if kind == "observer" and rnd.random() < 0.9:
        files.pop("write", None)
    access: dict[str, Any] = {"commands": cmds, "files": files}
    r = rnd.random()
    if r < 0.3:
        access["network"] = [rnd.choice(_HOSTS) for _ in range(rnd.randint(0, 2))]
    elif r < 0.6:
        access["network"] = rnd.choice(["none", "any"])
    d: dict[str, Any] = {
        **_def(), "kind": kind, "runtime": rnd.choice(["claude", "opencode", "cline"]),
        "id": rnd.choice(_IDS) if rnd.random() < 0.1 else "x", "model": rnd.choice(_MODELS),
        "workdir": rnd.choice(["/srv/app", "/srv/app/a b", "/srv/app/x,y", "/srv/app/sub"]),
        "access": access,
        "approval": {"policy": rnd.choice(["auto", "listed", "always"]),
                     "patterns": [entry() for _ in range(rnd.randint(0, 2))]},
    }
    if rnd.random() < 0.3:
        d["observe"] = {"sources": [rnd.choice(_PATHS)], "logs": [rnd.choice(_PATHS)]}
    if rnd.random() < 0.25:
        d["run_as"] = rnd.choice(["deploy", "root", "sessionweave"])
    if kind == "observer" and rnd.random() < 0.3:
        d["observer_as_root"] = True
    return d


def test_random_definitions_match_the_launcher() -> None:
    """A seeded random comparison: broker and launcher agree on every definition."""
    import random

    pytest.importorskip("sessionweave_runner.definitions",
                        reason="sessionweave-runner is not installed (not a broker"
                               " dependency)")
    rnd = random.Random(20261127)
    diffs = []
    for _ in range(3000):
        raw = _random_definition(rnd)
        for mname, machine in MACHINES.items():
            b, r = _broker_check(raw, machine), _runner_check(raw, machine)
            if (b is None) is not (r is None):
                diffs.append((mname, raw, b, r))
    assert diffs == []


# --- final review I1: the published-profiles budget -------------------------------------

def test_budget_constants() -> None:
    assert PUBLISHED_PROFILES_BUDGET == 192 * 1024
    assert budget_problem(PUBLISHED_PROFILES_BUDGET) is None
    why = budget_problem(PUBLISHED_PROFILES_BUDGET + 1)
    assert why is not None and why.startswith("machine definitions budget exceeded")
    assert "193 KiB of the 192 KiB" in why


def test_profiles_frame_size_is_the_serialized_frame() -> None:
    profiles = [{"id": "a", "x": "é"}, {"id": "b"}, {"id": "c", "w": ["1", "2"]}]
    for n in range(4):
        frame = {"type": "profiles", "profiles": profiles[:n]}
        assert profiles_frame_size([json_size(p) for p in profiles[:n]]) == json_size(frame)


def test_published_profile_matches_the_launcher() -> None:
    rdefs = pytest.importorskip("sessionweave_runner.definitions",
                                reason="sessionweave-runner is not installed")
    public_profile = pytest.importorskip("sessionweave_runner.launcher_config").public_profile
    rmode = pytest.importorskip("sessionweave_runner.launcher_mode")
    rsandbox = pytest.importorskip("sessionweave_runner.sandbox")

    assert rdefs.PUBLISHED_PROFILES_BUDGET == PUBLISHED_PROFILES_BUDGET
    assert rdefs.BUDGET_PROBLEM.format(size=193, budget=192) == budget_problem(
        PUBLISHED_PROFILES_BUDGET + 1)
    mode = rmode.LauncherMode("local", Path("/srv/app"),
                              rsandbox.SandboxSupport("bwrap", "t", "/x"), False, "linux")
    for raw in (
        _def(),
        _def(kind="operator", description="é" * 1000, capabilities=["c" * 64] * 32,
             limits={"max_duration_s": 600, "max_cost_usd": 1.5},
             access={"network": "any", "files": {"deny": ["x"]}},
             visibility="general", allowed_accounts=["bob", "ann"]),
        _def(limits={"max_idle_s": None}, access={"network": "none"}),
    ):
        d = validate_definition(raw, LOCAL)
        expected = {**public_profile(rdefs.parse_definition(d, mode)), "warnings": ["w"]}
        assert published_profile(d, ["w"]) == expected
        assert json_size(published_profile(d, ["w"])) == json_size(expected)


# --- re-review (Runner fc237d3): protected write globs, listed approvals for writers,
# shell builtins as command runners -----------------------------------------------------

_PROTECTED_REFUSED = [
    ".git", "src/.git/hooks", "**/AGENTS.md", ".claude/settings.json", ".gi?", ".git*",
    "[.]git", "AGENTS.*", "CLAUDE*", "*.md", "docs/*.md", "*.json", ".opencode/plugin",
    "a/.mcp.json", "CLAUDE.local.md", ".cursorrules",
]
_PROTECTED_ACCEPTED = [
    ".", "**", "./**", "*", "src/**", "*.py", ".github/**", ".gitignore", "src", "docs/**",
    "AGENTS", "claude.md", "out/*",
]


@pytest.mark.parametrize("glob", _PROTECTED_REFUSED)
def test_write_globs_naming_protected_files_refused(glob: str) -> None:
    why = protected_write_problem([glob])
    assert why is not None and why.startswith(f"write path {glob!r} would make ")
    for approval in ("auto", "always"):  # approval "always" does not excuse it
        with pytest.raises(DefinitionError) as exc:
            validate_definition(_def(kind="operator", access={"files": {"write": [glob]}},
                                     approval={"policy": approval}), LOCAL)
        assert str(exc.value) == why


@pytest.mark.parametrize("glob", _PROTECTED_ACCEPTED)
def test_ordinary_write_globs_accepted(glob: str) -> None:
    assert protected_write_problem([glob]) is None


def test_protected_write_rule_matches_the_launcher() -> None:
    perms = pytest.importorskip("sessionweave_runner.permissions",
                                reason="sessionweave-runner is not installed")
    assert perms.PROTECTED_WRITE_NAMES == PROTECTED_WRITE_NAMES
    assert perms.PROTECTED_WRITE_PROBLEM == PROTECTED_WRITE_PROBLEM
    assert perms.LISTED_WITH_WRITES == LISTED_WITH_WRITES
    for glob in [*_PROTECTED_REFUSED, *_PROTECTED_ACCEPTED]:
        assert perms.protected_write_problem([glob]) == protected_write_problem([glob])


def test_listed_approvals_refused_for_writers() -> None:
    assert "\u2014" in LISTED_WITH_WRITES
    listed = {"policy": "listed", "patterns": ["systemctl restart"]}
    for cmds in ({"mode": "shell"}, {"mode": "list", "allow": ["systemctl status"]}):
        with pytest.raises(DefinitionError) as exc:
            validate_definition(_def(kind="operator", access={"commands": cmds},
                                     approval=listed), LOCAL)
        assert str(exc.value) == LISTED_WITH_WRITES
    # not a writer, no command, or no pattern: fine
    validate_definition(_def(kind="operator", approval={**listed, "policy": "always"}), LOCAL)
    validate_definition(_def(kind="operator", access={"commands": {"mode": "none"}},
                             approval=listed), LOCAL)
    validate_definition(_def(kind="operator", approval={"policy": "listed", "patterns": []}),
                        LOCAL)
    validate_definition(_def(access={"commands": {"mode": "list", "allow": [
        "systemctl status"]}}, approval=listed), LOCAL)  # an observer


@pytest.mark.parametrize("word", [
    "eval", "source", ".", "builtin", "trap", "coproc", "fc", "alias", "enable", "let",
    "mapfile", "readarray", "hash", "autoload", "zmodload", "noglob", "nocorrect", "sched",
    "zpty", "/usr/bin/eval",
])
def test_shell_builtins_that_run_commands_are_wrappers(word: str) -> None:
    assert is_command_wrapper(word)
    # refused as a privileged entry (swv-root runs it as root outside the sandbox)
    with pytest.raises(DefinitionError, match="can run other commands: swv-root"):
        validate_definition(_def(kind="operator", workdir="/opt", access={"commands": {
            "mode": "list", "allow": ["ls"], "privileged": [word]}}), ROOT)


# --- access model v2 (Runner a085c62): read-only needs the sandbox, access warnings --------

def test_runs_sandboxed() -> None:
    assert runs_sandboxed("observer", LOCAL) and runs_sandboxed("operator", LOCAL)
    assert runs_sandboxed("observer", ROOT) and not runs_sandboxed("operator", ROOT)
    assert not runs_sandboxed("observer", NO_BWRAP_ROOT)
    assert runs_sandboxed("observer", USER_ROOT)


@pytest.mark.parametrize("access,refused", [
    (None, True), ({"commands": {"mode": "shell"}}, True),
    ({"commands": {"mode": "list", "allow": ["ls"]}}, True),
    ({"commands": {"mode": "list"}}, False),    # an empty list runs nothing
    ({"commands": {"mode": "none"}}, False),
])
def test_read_only_without_a_sandbox_refused(access: dict[str, Any] | None,
                                             refused: bool) -> None:
    raw = _def(workdir="/var", run_as="mon", **({"access": access} if access else {}))
    if refused:
        with pytest.raises(DefinitionError) as exc:
            validate_definition(raw, NO_BWRAP_ROOT)
        assert str(exc.value) == READ_ONLY_NEEDS_SANDBOX
    else:
        validate_definition(raw, NO_BWRAP_ROOT)
    validate_definition(raw, ROOT)  # with bubblewrap: fine
    # an operator with approval "always" is read-only but has no commands
    validate_definition(_def(kind="operator", workdir="/var",
                             approval={"policy": "always"}), NO_BWRAP_ROOT)


def test_read_only_sandbox_problem_truth_table() -> None:
    for read_only in (True, False):
        for mode in ("none", "list", "shell"):
            for sandboxed in (True, False):
                want = (READ_ONLY_NEEDS_SANDBOX
                        if read_only and mode != "none" and not sandboxed else None)
                assert read_only_sandbox_problem(read_only=read_only, commands_mode=mode,
                                                 sandboxed=sandboxed) == want


# (definition overrides, expected warnings per machine)
_G, _U, _N = [LIST_IS_GUIDANCE], [UNSANDBOXED_WRITER_WARNING], [READ_ONLY_NETWORK_WARNING]
_NO_WARNINGS: dict[str, list[str]] = {m: [] for m in MACHINES}
_ACCESS_WARNING_CASES: list[tuple[dict[str, Any], dict[str, list[str]]]] = [
    ({"kind": "operator", "access": {"commands": {"mode": "list", "allow": ["git status"]}}},
     {"local": _G, "root": _G + _U, "root-nobw": _G + _U, "root-user": _G + _U}),
    ({"kind": "operator"},                                          # shell
     {"local": [], "root": _U, "root-nobw": _U, "root-user": _U}),
    ({"kind": "operator", "access": {"commands": {"mode": "none"}}}, _NO_WARNINGS),
    ({"kind": "operator", "access": {"commands": {"mode": "list"}}},  # empty list: none
     _NO_WARNINGS),
    ({"kind": "operator", "approval": {"policy": "always"},
      "access": {"commands": {"mode": "list", "allow": ["ls"]}}}, _NO_WARNINGS),
    ({"kind": "observer", "run_as": "deploy",                      # cannot write
      "access": {"commands": {"mode": "list", "allow": ["ls"]}}}, {"root": _N}),
    ({"kind": "observer"}, {"local": _N, "root-user": _N}),          # shell
    ({"kind": "observer", "access": {"commands": {"mode": "none"}}},
     {"local": [], "root-user": []}),
    ({"kind": "operator", "approval": {"policy": "always"}},         # always: no commands
     _NO_WARNINGS),
]


@pytest.mark.parametrize("over,expected", _ACCESS_WARNING_CASES)
def test_access_warnings_at_deploy(over: dict[str, Any], expected: dict[str, list[str]]) -> None:
    for mname, want in expected.items():
        machine = MACHINES[mname]
        d = validate_definition({**_def(), **over}, machine)
        assert definition_warnings(d, machine) == want, mname


def test_access_warnings_match_the_launcher() -> None:
    perms = pytest.importorskip("sessionweave_runner.permissions",
                                reason="sessionweave-runner is not installed")
    for read_only in (True, False):
        for mode in ("none", "list", "shell"):
            for sandboxed in (True, False):
                assert access_warnings(
                    read_only=read_only, commands_mode=mode, sandboxed=sandboxed
                ) == perms.access_warnings(
                    read_only=read_only, commands_mode=mode, sandboxed=sandboxed)
                assert read_only_sandbox_problem(
                    read_only=read_only, commands_mode=mode, sandboxed=sandboxed
                ) == perms.read_only_sandbox_problem(
                    read_only=read_only, commands_mode=mode, sandboxed=sandboxed)


# --- Runner 1edce91: observer_as_root network, read-only covers files only ------------------

@pytest.mark.parametrize("access,refused", [
    ({}, True),                                                    # default commands: shell
    ({"network": "none"}, True),
    ({"commands": {"mode": "list", "allow": ["ls"]}}, True),
    ({"commands": {"mode": "list"}}, False),                       # an empty list: none
    ({"commands": {"mode": "none"}}, False),
    ({"network": "any", "commands": {"mode": "none"}}, False),
])
def test_observer_as_root_needs_commands_none(access: dict[str, Any], refused: bool) -> None:
    raw = _def(workdir="/var", observer_as_root=True, access=access)
    if refused:
        with pytest.raises(DefinitionError) as exc:
            validate_definition(raw, ROOT)
        assert str(exc.value) == OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS
    else:
        validate_definition(raw, ROOT)
    # approval "always" leaves no commands
    validate_definition({**raw, "approval": {"policy": "always"}}, ROOT)


def test_empty_list_is_no_commands() -> None:
    """Runner 7f06d1f/8f6ff56: a list with no allow and no privileged entry runs
    nothing: no read-only-without-sandbox refusal, a host list is fine, listed
    patterns are ignored (no LISTED_WITH_WRITES), no warnings."""
    empty = {"commands": {"mode": "list"}}
    validate_definition(_def(workdir="/var", run_as="mon", access=empty), NO_BWRAP_ROOT)
    validate_definition(_def(kind="operator", access={**empty, "network": ["a.com"]}), LOCAL)
    d = validate_definition(_def(kind="operator", access=empty, approval={
        "policy": "listed", "patterns": ["systemctl restart"]}), LOCAL)
    assert definition_warnings(d, LOCAL) == []
    # privileged entries only: still commands (swv-root)
    with pytest.raises(DefinitionError) as exc:
        validate_definition(_def(kind="operator", workdir="/opt", access={"commands": {
            "mode": "list", "privileged": ["journalctl -u"]}}, approval={
            "policy": "listed", "patterns": ["systemctl restart"]}), ROOT)
    assert str(exc.value) == LISTED_WITH_WRITES


def test_read_only_network_warning() -> None:
    assert access_warnings(read_only=True, commands_mode="shell", sandboxed=True) == [
        READ_ONLY_NETWORK_WARNING]
    assert access_warnings(read_only=True, commands_mode="none", sandboxed=False) == []
