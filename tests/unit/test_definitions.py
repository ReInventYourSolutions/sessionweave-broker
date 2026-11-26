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
    COMMAND_WRAPPERS,
    DEFINITION_MAX_BYTES,
    INSTRUCTIONS_MAX_BYTES,
    KINDS,
    LIMIT_MAX,
    MAX_DEFINITIONS_PER_LAUNCHER,
    METRIC_KINDS,
    READ_COMMANDS,
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
    assert validate_definition(_def(), ROOT)["observer_as_root"] is False


def test_observer_as_root_only_for_observers_on_root_machines() -> None:
    d = validate_definition(_def(observer_as_root=True), ROOT)
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
                                   "/bin/ls", "sh"])
def test_observer_allow_entries_must_be_read_commands(entry: str) -> None:
    with pytest.raises(DefinitionError, match="read command"):
        validate_definition(_def(access={"commands": {"mode": "list", "allow": ["ls", entry]}}),
                            LOCAL)
    # An operator may list it.
    d = validate_definition(
        _def(kind="operator", access={"commands": {"mode": "list", "allow": [entry]}}), LOCAL)
    assert d["access"]["commands"]["allow"] == [entry]


def test_observer_read_command_entries_with_arguments_accepted() -> None:
    allow = ["journalctl -u", "top -b -n1", "systemctl status nginx", "tail -n", "ps aux"]
    d = validate_definition(_def(access={"commands": {"mode": "list", "allow": allow}}), LOCAL)
    assert d["access"]["commands"]["allow"] == allow
    # mode none and the empty default list stay valid
    assert validate_definition(
        _def(access={"commands": {"mode": "none"}}), LOCAL)["access"]["commands"]["mode"] == "none"
    assert validate_definition(_def(), LOCAL)["access"]["commands"]["allow"] == []


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
    validate_definition(_def(observer_as_root=True), ROOT)
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
    ("operator", ["git status", "git log"], [], "git push", True),
    ("operator", ["git status", "git log", "ls -la", "cat"], [], "git push", True),
    ("operator", ["git status -s"], [], "git push", True),
    ("operator", ["git status"], [], "git push --force", True),
    ("operator", ["/usr/bin/git status"], [], "git push", True),
    ("operator", ["docker compose ps", "docker compose logs"], [], "docker compose up", True),
    ("operator", ["ls"], ["systemctl status"], "systemctl restart", True),
    ("observer", ["journalctl -u"], [], "rm", True),
    ("operator", ["git"], [], "git push", False),                  # entry runs the pattern
    ("operator", ["git push origin"], [], "git push", False),      # pattern covers the entry
    ("operator", ["git push --dry-run"], [], "git push", False),
    ("operator", ["git status"], [], "git", False),
    ("operator", ["git --no-pager log"], [], "git --no-pager push", False),  # option first
    ("operator", ["git -C x log"], [], "git -C x push", False),
    ("operator", ["git -C"], [], "git push", False),
    ("operator", ["tail /var/log/syslog"], [], "tail -f", False),  # differ at an option
    ("operator", ["tail -n"], [], "tail -f", False),
    ("operator", ["/usr/bin/git"], [], "git push", False),         # basename
    ("operator", ["ls"], ["systemctl"], "systemctl restart", False),
    ("operator", ["ls"], ["systemctl restart nginx"], "systemctl restart", False),
    ("operator", ["ls"], ["/bin/systemctl"], "systemctl restart", False),
    ("operator", ["ls"], ["systemctl --no-block"], "systemctl restart", False),
    ("observer", [], [], "journalctl --vacuum-size", False),       # the default read list
    ("observer", ["systemctl status"], [], "systemctl status nginx", False),
]


def _listed_def(kind: str, allow: list[str], privileged: list[str], pattern: str,
                ) -> dict[str, Any]:
    return _def(kind=kind, access={"commands": {"mode": "list", "allow": allow,
                                                "privileged": privileged}},
                approval={"policy": "listed", "patterns": [pattern]})


@pytest.mark.parametrize("kind,allow,privileged,pattern,accepted", _LISTED_TABLE)
def test_listed_pattern_vs_allowed_entries(
    kind: str, allow: list[str], privileged: list[str], pattern: str, accepted: bool,
) -> None:
    raw = _listed_def(kind, allow, privileged, pattern)
    if accepted:
        d = validate_definition(raw, ROOT)
        assert d["approval"]["patterns"] == [pattern]
        return
    with pytest.raises(DefinitionError, match="overlaps") as exc:
        validate_definition(raw, ROOT)
    assert exc.value.code is ErrorCode.INVALID_REQUEST
    assert repr(pattern) in str(exc.value)


def test_listed_check_only_with_listed_patterns() -> None:
    for approval in ({"policy": "auto", "patterns": ["git push"]},
                     {"policy": "always", "patterns": ["git push"]},
                     {"policy": "listed", "patterns": []}):
        validate_definition(_def(kind="operator", access={"commands": {
            "mode": "list", "allow": ["git"]}}, approval=approval), LOCAL)
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
    ("operator", "list", ["ls", "git status", "shred"], [], "rm", True),     # not wrappers
    ("operator", "list", ["shx", "bashful", "envoy"], [], "rm", True),
    ("operator", "list", ["ls"], ["systemctl status"], "rm", True),
]


@pytest.mark.parametrize("kind,mode,allow,privileged,pattern,accepted", _LISTED_WRAPPER_TABLE)
def test_listed_refuses_shell_and_wrappers(
    kind: str, mode: str, allow: list[str], privileged: list[str], pattern: str,
    accepted: bool,
) -> None:
    raw = _def(kind=kind, access={"commands": {"mode": mode, "allow": allow,
                                               "privileged": privileged}},
               approval={"policy": "listed", "patterns": [pattern]})
    if accepted:
        assert validate_definition(raw, ROOT)["approval"]["patterns"] == [pattern]
        return
    with pytest.raises(DefinitionError, match=r"'list' until approvals|runs other commands"
                       ) as exc:
        validate_definition(raw, ROOT)
    assert exc.value.code is ErrorCode.INVALID_REQUEST


def test_shell_and_wrappers_fine_without_listed_patterns() -> None:
    for approval in ({"policy": "auto", "patterns": ["git push"]},
                     {"policy": "listed", "patterns": []}):
        validate_definition(_def(kind="operator", approval=approval), LOCAL)  # shell
        validate_definition(_def(kind="operator", access={"commands": {
            "mode": "list", "allow": ["sh", "python3"]}}, approval=approval), LOCAL)


def test_command_wrappers_match_the_launcher() -> None:
    # A copy of the Runner's permissions.COMMAND_WRAPPERS (parity: keep both in step).
    assert len(COMMAND_WRAPPERS) == len(set(COMMAND_WRAPPERS)) == 65
    assert {"sh", "env", "xargs", "python*", "find", "ssh", "man"} <= set(COMMAND_WRAPPERS)
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
