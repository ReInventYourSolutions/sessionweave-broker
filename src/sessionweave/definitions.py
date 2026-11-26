"""Agent definitions: what the Cockpit deploys to a launcher's machine.

``validate_definition`` checks a definition against the schema (unknown keys
are refused at every level) and against the target machine's start-mode
ceiling, and returns it normalised with every default filled, as the broker
stores and sends it. The launcher validates again on its side (realpaths,
sandbox, runtime support); this is the broker's first, purely syntactic line.
"""
from __future__ import annotations

import json
import math
import posixpath
import re
from typing import Any

from sessionweave.errors import ErrorCode
from sessionweave.launchers import (
    _ID_RE,
    _RUNTIMES,
    _VISIBILITIES,
    _account_names,
    _is_int,
    _valid_limits,
    _valid_network,
)
from sessionweave.machines import Machine

DEFINITION_MAX_BYTES = 64 * 1024
INSTRUCTIONS_MAX_BYTES = 16 * 1024
MAX_DEFINITIONS_PER_LAUNCHER = 50
KINDS = ("observer", "operator")
APPROVAL_POLICIES = ("auto", "listed", "always")
COMMAND_MODES = ("none", "list", "shell")
METRIC_KINDS = ("cpu", "memory", "disk", "load")
# Upper bounds of ``limits`` (the launcher's TOML profiles use the same values).
LIMIT_MAX: dict[str, int] = {
    "max_duration_s": 30 * 86400,
    "max_idle_s": 30 * 86400,
    "max_cost_usd": 10000,
}
# The commands an observer may list: each allow entry must start with the words of
# one of them (literal words: ``/bin/ls`` and ``lsblk`` are not ``ls``). A copy of
# the launcher's ``permissions.READ_COMMANDS``: keep both in step.
READ_COMMANDS = (
    "ls", "cat", "head", "tail", "grep", "zcat", "zgrep", "wc", "stat", "file", "du", "df",
    "free", "uptime", "uname", "hostname", "id", "whoami", "date", "ps", "pgrep", "top -b",
    "vmstat", "iostat", "journalctl", "dmesg", "ss", "lsof", "systemctl status",
)
# Commands that run other commands (shells, wrappers, interpreters, ``find -exec``,
# ``awk system()``, ...), compared by the basename of the command word (``python*``
# covers every ``python3.x``). With approval ``listed`` patterns an allow or
# privileged entry starting with one is refused: it would run a listed command
# unchecked. A copy of the launcher's ``permissions.COMMAND_WRAPPERS``: keep both
# in step.
COMMAND_WRAPPERS = (
    "sh", "bash", "dash", "zsh", "ksh", "csh", "tcsh", "fish", "busybox", "env", "command",
    "exec", "xargs", "parallel", "nice", "ionice", "nohup", "timeout", "stdbuf", "setsid",
    "flock", "watch", "script", "taskset", "chrt", "strace", "gdb", "sudo", "su", "doas",
    "pkexec", "runuser", "setpriv", "nsenter", "unshare", "chroot", "systemd-run",
    "python*", "perl", "ruby", "node", "npx", "npm", "bun", "deno", "php", "lua", "tclsh",
    "expect", "find", "awk", "gawk", "mawk", "sed", "make", "ssh", "tar", "rsync",
    "vi", "vim", "nvim", "ex", "less", "more", "man",
)

_TOP_KEYS = frozenset({
    "id", "domain", "role", "capabilities", "description", "runtime", "model", "kind",
    "coordinator_capable", "observer_as_root", "workdir", "run_as", "observe", "access",
    "limits", "approval",
    "instructions", "max_instances", "visibility", "allowed_accounts",
})
_OBSERVE_KEYS = frozenset({"sources", "logs", "processes", "metrics"})
_PROCESS_KEYS = frozenset({"name", "pid"})
_METRICS_KEYS = frozenset({"enabled", "interval_s", "kinds"})
_ACCESS_KEYS = frozenset({"files", "network", "commands"})
_FILES_KEYS = ("read", "write", "deny")
_COMMANDS_KEYS = frozenset({"mode", "allow", "privileged"})
_LIMITS_KEYS = frozenset({"max_duration_s", "max_idle_s", "max_cost_usd"})
_APPROVAL_KEYS = frozenset({"policy", "patterns"})

# A command entry: words of a command prefix, no shell syntax at all.
_COMMAND_RE = re.compile(r"[A-Za-z0-9._/+-]+( [A-Za-z0-9._/=+:-]+){0,7}")
_COMMAND_MAX = 200
_RUN_AS_RE = re.compile(r"[a-z_][a-z0-9_-]{0,31}")
_MAX_PATH = 4096
_MAX_GLOB = 1024
_MAX_LIST = 50  # observe lists
_MAX_GLOBS = 100
_MAX_ALLOW = 100
_MAX_PRIVILEGED = 50
_MAX_PATTERNS = 100
_DEFAULT_INTERVAL_S = 60
# Brace expansion ({..,x}) and escapes (\.\.) could rebuild a '..' that the
# component check does not see, in matchers that support them.
_GLOB_FORBIDDEN = ("{", "}", "\\")


class DefinitionError(ValueError):
    """An invalid definition; ``code`` is the API error to answer with
    (``outside_ceiling`` for a path beyond the machine's ceiling, else
    ``invalid_request``)."""

    def __init__(self, message: str, code: ErrorCode = ErrorCode.INVALID_REQUEST) -> None:
        super().__init__(message)
        self.code = code


def _parts(path: str) -> list[str]:
    return [c for c in posixpath.normpath(path).split("/") if c]


def inside(path: str, root: str) -> bool:
    """Whether absolute ``path`` is ``root`` or below it, compared component
    by component on the normalised paths (``/srv/application`` is not inside
    ``/srv/app``). Purely lexical: the launcher resolves symlinks."""
    if not (path.startswith("/") and root.startswith("/")):
        return False
    p, r = _parts(path), _parts(root)
    return p[: len(r)] == r


def _no_unknown(obj: dict[str, Any], allowed: frozenset[str], where: str) -> None:
    extra = sorted(str(k) for k in obj if k not in allowed)
    if extra:
        raise DefinitionError(f"{where}: unknown keys {extra}")


def _obj(v: Any, where: str, allowed: frozenset[str]) -> dict[str, Any]:
    if v is None:
        return {}
    if not isinstance(v, dict):
        raise DefinitionError(f"{where} must be an object")
    _no_unknown(v, allowed, where)
    return v


def _str_list(v: Any, where: str, limit: int) -> list[str]:
    if v is None:
        return []
    if not isinstance(v, list) or len(v) > limit or not all(isinstance(x, str) for x in v):
        raise DefinitionError(f"{where} must be a list of at most {limit} strings")
    return list(v)


def _path(v: Any, where: str, machine: Machine) -> str:
    if not isinstance(v, str) or not v.startswith("/"):
        raise DefinitionError(f"{where} must be an absolute path")
    if len(v) > _MAX_PATH or not v.isprintable() or v.startswith("//"):
        raise DefinitionError(f"{where} must be a printable path of at most {_MAX_PATH} chars")
    if posixpath.normpath(v) != v:
        raise DefinitionError(f"{where} must be normalised (no '.', '..', '//' or trailing '/')")
    assert machine.ceiling is not None
    if not inside(v, machine.ceiling):
        raise DefinitionError(
            f"{where} {v!r} is outside the machine's ceiling {machine.ceiling!r}",
            ErrorCode.OUTSIDE_CEILING,
        )
    return v


def _glob(v: str, where: str) -> str:
    if (
        not v or len(v) > _MAX_GLOB or not v.isprintable()
        or v.startswith(("/", "~")) or ".." in v.split("/")
        or any(c in v for c in _GLOB_FORBIDDEN)
    ):
        raise DefinitionError(
            f"{where}: {v!r} must be a glob relative to the workdir (no leading '/' or '~',"
            " no '..', no braces or backslashes)"
        )
    return v


def _commands(v: list[str], where: str) -> list[str]:
    for c in v:
        if len(c) > _COMMAND_MAX or not _COMMAND_RE.fullmatch(c):
            raise DefinitionError(
                f"{where}: {c!r} is not a command prefix (words of [A-Za-z0-9._/+-], at most"
                f" 8 words and {_COMMAND_MAX} chars, no shell syntax)"
            )
        if c.startswith("-"):
            raise DefinitionError(f"{where}: {c!r} must start with a command, not an option")
    return v


def _is_read_command(entry: str) -> bool:
    words = entry.split()
    return any(
        (rw := r.split()) and len(rw) <= len(words) and words[: len(rw)] == rw
        for r in READ_COMMANDS
    )


def _norm_words(entry: str) -> list[str]:
    """The entry's words, the command word compared by basename (``/usr/bin/git``
    is ``git``)."""
    words = entry.split()
    return [posixpath.basename(words[0]) or words[0], *words[1:]]


def _distinct_subcommands(entry: list[str], pattern: list[str]) -> bool:
    """Whether an entry and a pattern with the same command word differ at a later
    word that is not an option in either, with no option word before it (``git
    status`` vs ``git push``). False when one is a word prefix of the other (``git``
    vs ``git push``, ``git push origin`` vs ``git push``) or an option comes first
    (``git -C x log`` vs ``git -C x push``). The launcher's rule, word for word."""
    for e, p in zip(entry[1:], pattern[1:], strict=False):
        if e.startswith("-") or p.startswith("-"):
            return False
        if e != p:
            return True
    return False


def is_command_wrapper(word: str) -> bool:
    """Whether the command word (by basename) is in ``COMMAND_WRAPPERS``."""
    name = posixpath.basename(word) or word
    return any(name.startswith(w[:-1]) if w.endswith("*") else name == w
               for w in COMMAND_WRAPPERS)


def _listed_overlap(kind: str, commands: dict[str, Any], approval: dict[str, Any]) -> None:
    """Approval ``listed`` fails closed (its patterns become denies): an allow or
    privileged entry sharing a pattern's command word must differ from it at a
    subcommand word (allow ``git status``, ``git log`` with ``git push``); else the
    entry would run the pattern unasked, always ask, or reach it in another
    spelling. An observer's empty list stands for the default read commands. A
    shell, and any entry that runs other commands (``COMMAND_WRAPPERS``), are
    refused: they could run a listed command under another spelling."""
    patterns = approval["patterns"]
    if approval["policy"] != "listed" or not patterns or commands["mode"] == "none":
        return
    if commands["mode"] == "shell":
        raise DefinitionError(
            "approval.patterns: approval 'listed' with patterns needs access.commands.mode"
            " 'list' until approvals are implemented (a shell can run a listed command"
            " under another spelling)"
        )
    allow = commands["allow"]
    if kind == "observer" and commands["mode"] == "list" and not allow:
        allow = list(READ_COMMANDS)
    for what, entries in (("allow", allow), ("privileged", commands["privileged"])):
        for entry in entries:
            if is_command_wrapper(entry.split()[0]):
                raise DefinitionError(
                    f"access.commands.{what}: {entry!r} runs other commands (shell, wrapper"
                    " or interpreter): with approval for listed commands it could run a"
                    " listed command unchecked; remove it"
                )
        for entry in entries:
            ew = _norm_words(entry)
            for p in patterns:
                pw = _norm_words(p)
                if ew[0] != pw[0] or _distinct_subcommands(ew, pw):
                    continue
                raise DefinitionError(
                    f"approval.patterns: {p!r} shares the command {pw[0]!r} with the"
                    f" access.commands.{what} entry {entry!r} and overlaps it: one is a word"
                    " prefix of the other, or an option comes before they differ; list only"
                    " commands that differ from it at a subcommand word (e.g. 'git status'"
                    " with 'git push'), without options before it"
                )


def _observe(raw: Any, machine: Machine) -> dict[str, Any]:
    obs = _obj(raw, "observe", _OBSERVE_KEYS)
    out: dict[str, Any] = {}
    for key in ("sources", "logs"):
        out[key] = [
            _path(p, f"observe.{key}", machine)
            for p in _str_list(obs.get(key), f"observe.{key}", _MAX_LIST)
        ]
    procs = obs.get("processes", [])
    if not isinstance(procs, list) or len(procs) > _MAX_LIST:
        raise DefinitionError(f"observe.processes must be a list of at most {_MAX_LIST} entries")
    out["processes"] = []
    for p in procs:
        p = _obj(p, "observe.processes[]", _PROCESS_KEYS)
        name = p.get("name")
        if not isinstance(name, str) or not 0 < len(name) <= 64 or not name.isprintable():
            raise DefinitionError("observe.processes[].name must be 1-64 printable chars")
        pid = p.get("pid")
        if pid is not None and not (_is_int(pid) and pid >= 1):
            raise DefinitionError("observe.processes[].pid must be an integer >= 1")
        out["processes"].append({"name": name, "pid": pid})
    met = _obj(obs.get("metrics"), "observe.metrics", _METRICS_KEYS)
    enabled = met.get("enabled", False)
    if not isinstance(enabled, bool):
        raise DefinitionError("observe.metrics.enabled must be a boolean")
    interval = met.get("interval_s", _DEFAULT_INTERVAL_S)
    if not _is_int(interval) or not 5 <= interval <= 3600:
        raise DefinitionError("observe.metrics.interval_s must be an integer from 5 to 3600")
    kinds = _str_list(met.get("kinds", list(METRIC_KINDS)), "observe.metrics.kinds", 4)
    if not set(kinds) <= set(METRIC_KINDS) or len(set(kinds)) != len(kinds):
        raise DefinitionError(
            f"observe.metrics.kinds must be distinct names of {list(METRIC_KINDS)}"
        )
    out["metrics"] = {"enabled": enabled, "interval_s": interval, "kinds": kinds}
    return out


def _access(raw: Any, kind: str, machine: Machine) -> dict[str, Any]:
    acc = _obj(raw, "access", _ACCESS_KEYS)
    files = _obj(acc.get("files"), "access.files", frozenset(_FILES_KEYS))
    out_files = {
        k: [_glob(g, f"access.files.{k}")
            for g in _str_list(files.get(k), f"access.files.{k}", _MAX_GLOBS)]
        for k in _FILES_KEYS
    }
    if kind == "observer" and out_files["write"]:
        raise DefinitionError("an observer never writes files: access.files.write must be empty")
    network = acc.get("network")
    if network is not None and not (
        _valid_network(network)
        and (not isinstance(network, list) or all(h.isprintable() for h in network))
    ):
        raise DefinitionError("access.network must be 'none', 'any' or a list of hosts")
    cmds = _obj(acc.get("commands"), "access.commands", _COMMANDS_KEYS)
    mode = cmds.get("mode", "list" if kind == "observer" else "shell")
    if mode not in COMMAND_MODES:
        raise DefinitionError(f"access.commands.mode must be one of {list(COMMAND_MODES)}")
    if kind == "observer" and mode == "shell":
        raise DefinitionError("an observer runs only listed read commands: mode 'shell' refused")
    allow = _commands(
        _str_list(cmds.get("allow"), "access.commands.allow", _MAX_ALLOW), "access.commands.allow"
    )
    if allow and mode != "list":
        raise DefinitionError("access.commands.allow applies only to mode 'list'")
    if kind == "observer":
        bad = [c for c in allow if not _is_read_command(c)]
        if bad:
            raise DefinitionError(
                f"access.commands.allow: an observer may only run read commands; {bad[0]!r}"
                f" is not one (each entry must start with one of {list(READ_COMMANDS)})"
            )
    privileged = _commands(
        _str_list(cmds.get("privileged"), "access.commands.privileged", _MAX_PRIVILEGED),
        "access.commands.privileged",
    )
    if privileged and kind == "observer":
        raise DefinitionError(
            "an observer gets no privileged (root-delegated) commands:"
            " access.commands.privileged must be empty"
        )
    if privileged and not (machine.mode == "root" and machine.run_as):
        raise DefinitionError(
            "access.commands.privileged needs a machine whose launcher runs with --root"
            " as root (it may run commands as root)",
            ErrorCode.OUTSIDE_CEILING,
        )
    return {
        "files": out_files,
        "network": list(network) if isinstance(network, list) else network,
        "commands": {"mode": mode, "allow": allow, "privileged": privileged},
    }


def _limits(raw: Any) -> dict[str, Any]:
    limits = _obj(raw, "limits", _LIMITS_KEYS)
    try:
        ok = _valid_limits(limits) and all(
            v is None or (math.isfinite(v) and v > 0) for v in limits.values()
        )
    except (OverflowError, ValueError):  # an int too large for a float
        ok = False
    if not ok:
        raise DefinitionError("limits must be positive finite numbers (or null)")
    for key, v in limits.items():
        if v is not None and v > LIMIT_MAX[key]:
            raise DefinitionError(f"limits.{key} must be at most {LIMIT_MAX[key]}")
    return dict(limits)


def _approval(raw: Any) -> dict[str, Any]:
    appr = _obj(raw, "approval", _APPROVAL_KEYS)
    policy = appr.get("policy", "auto")
    if policy not in APPROVAL_POLICIES:
        raise DefinitionError(f"approval.policy must be one of {list(APPROVAL_POLICIES)}")
    patterns = _commands(
        _str_list(appr.get("patterns"), "approval.patterns", _MAX_PATTERNS), "approval.patterns"
    )
    return {"policy": policy, "patterns": patterns}


def _text(raw: dict[str, Any], key: str, limit: int) -> str:
    v = raw.get(key, "")
    if not isinstance(v, str) or len(v) > limit:
        raise DefinitionError(f"{key} must be a string of at most {limit} chars")
    return v


def validate_definition(raw: Any, machine: Machine) -> dict[str, Any]:
    """Return the normalised definition (defaults filled) for ``machine``, or
    raise ``DefinitionError`` with the reason. Paths must be absolute,
    normalised and inside the machine's ceiling; root execution (``run_as``,
    ``privileged``, ``observer_as_root``) needs a root launcher that may run
    agents as other users, and ``observer_as_root`` excludes ``run_as``; an
    observer never writes files nor gets a shell and lists only read commands;
    command entries start with a command, not an option; limits are capped
    (``LIMIT_MAX``). Same rules as the launcher's ``normalise_definition``.
    A ``privileged`` entry is kept as written: the launcher's root helper runs
    only the options it spells."""
    if not isinstance(raw, dict):
        raise DefinitionError("definition must be an object")
    try:
        size = len(json.dumps(raw).encode())
    except (TypeError, ValueError) as exc:
        raise DefinitionError("definition must be plain JSON") from exc
    if size > DEFINITION_MAX_BYTES:
        raise DefinitionError("definition must be at most 64 KiB of JSON")
    _no_unknown(raw, _TOP_KEYS, "definition")
    if machine.ceiling is None:
        raise DefinitionError(
            "the machine's launcher was not started with --local or --root;"
            " it accepts no definitions"
        )
    out: dict[str, Any] = {}
    for key in ("id", "domain", "role"):
        v = raw.get(key)
        if not isinstance(v, str) or not _ID_RE.fullmatch(v):
            raise DefinitionError(f"{key} must be 1-64 chars of [A-Za-z0-9._-]")
        out[key] = v
    caps = raw.get("capabilities", [])
    if (
        not isinstance(caps, list) or len(caps) > 32
        or not all(isinstance(c, str) and 0 < len(c) <= 64 and c.isprintable() for c in caps)
    ):
        raise DefinitionError(
            "capabilities must be at most 32 non-empty printable strings of at most 64 chars"
        )
    out["capabilities"] = list(caps)
    out["description"] = _text(raw, "description", 1000)
    if raw.get("runtime") not in _RUNTIMES:
        raise DefinitionError(f"runtime must be one of {sorted(_RUNTIMES)}")
    out["runtime"] = raw["runtime"]
    out["model"] = _text(raw, "model", 200)
    kind = raw.get("kind")
    if kind not in KINDS:
        raise DefinitionError(f"kind must be one of {list(KINDS)}")
    out["kind"] = kind
    coord = raw.get("coordinator_capable", False)
    if not isinstance(coord, bool):
        raise DefinitionError("coordinator_capable must be a boolean")
    out["coordinator_capable"] = coord
    # An observer that runs as root: it can read every file on the machine,
    # including passwords and keys (still read-only). Root launchers only.
    as_root = raw.get("observer_as_root", False)
    if not isinstance(as_root, bool):
        raise DefinitionError("observer_as_root must be a boolean")
    if as_root and kind != "observer":
        raise DefinitionError("observer_as_root is only valid for an observer")
    if as_root and machine.mode != "root":
        raise DefinitionError(
            "observer_as_root needs a launcher started with --root"
        )
    if as_root and not machine.run_as:
        raise DefinitionError(
            "observer_as_root needs a launcher started with --root running as root",
            ErrorCode.OUTSIDE_CEILING,
        )
    out["observer_as_root"] = as_root
    out["workdir"] = _path(raw.get("workdir"), "workdir", machine)
    run_as = raw.get("run_as")
    if run_as is not None:
        if not isinstance(run_as, str) or not _RUN_AS_RE.fullmatch(run_as):
            raise DefinitionError("run_as must be a machine user name ([a-z_][a-z0-9_-]{0,31})")
        if not (machine.mode == "root" and machine.run_as):
            raise DefinitionError(
                "run_as needs a launcher started with --root that may run agents as other users",
                ErrorCode.OUTSIDE_CEILING,
            )
        if as_root:
            # Two identities for one agent: refused rather than guessed.
            raise DefinitionError(
                "observer_as_root and run_as exclude each other: choose one identity"
                " for the observer"
            )
    out["run_as"] = run_as
    out["observe"] = _observe(raw.get("observe"), machine)
    out["access"] = _access(raw.get("access"), kind, machine)
    out["limits"] = _limits(raw.get("limits"))
    out["approval"] = _approval(raw.get("approval"))
    _listed_overlap(kind, out["access"]["commands"], out["approval"])
    instructions = raw.get("instructions", "")
    try:
        ok = isinstance(instructions, str) and (
            len(instructions.encode()) <= INSTRUCTIONS_MAX_BYTES
        )
    except UnicodeEncodeError:  # lone surrogates
        ok = False
    if not ok:
        raise DefinitionError("instructions must be a string of at most 16 KiB (UTF-8)")
    out["instructions"] = instructions
    mi = raw.get("max_instances")
    if not (isinstance(mi, int) and not isinstance(mi, bool) and 1 <= mi <= 32):
        raise DefinitionError("max_instances must be an integer from 1 to 32")
    out["max_instances"] = mi
    vis = raw.get("visibility")
    if vis is not None and vis not in _VISIBILITIES:
        raise DefinitionError("visibility must be 'personal' or 'general'")
    out["visibility"] = vis
    allowed = raw.get("allowed_accounts")
    if allowed is not None and not (
        _account_names(allowed) and all(a.isprintable() for a in allowed)
    ):
        raise DefinitionError("allowed_accounts must be a list of account names")
    out["allowed_accounts"] = None if allowed is None else list(allowed)
    # The defaults add a few hundred bytes: what is stored and sent obeys the
    # same limit as the input (the launcher re-checks the definition it gets).
    if len(json.dumps(out).encode()) > DEFINITION_MAX_BYTES:
        raise DefinitionError("definition must be at most 64 KiB of JSON (with its defaults)")
    return out
