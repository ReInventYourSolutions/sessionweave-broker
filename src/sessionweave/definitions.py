"""Agent definitions: what the Cockpit deploys to a launcher's machine.

``validate_definition`` checks a definition against the schema (unknown keys
are refused at every level) and against the target machine's start-mode
ceiling, and returns it normalised with every default filled, as the broker
stores and sends it. It applies every rule of the launcher (Runner) that does not
need the machine itself (``_launcher_rules``: host and glob syntax, models, command
runners, what each runtime can enforce, the run-as rules), so a definition the
launcher would always reject is refused before it is stored. The launcher validates
again on its side (realpaths, sandbox, runtime installs and logins).
``published_profile``/``profiles_frame_size`` estimate what a machine publishes,
for the launcher's published-profiles budget (``PUBLISHED_PROFILES_BUDGET``).
"""
from __future__ import annotations

import fnmatch
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
# Suggested read commands (documentation and the Cockpit editor only; access model v2:
# an observer may run any command, its sandbox keeps it read-only). A copy of the
# launcher's ``permissions.READ_COMMANDS``: keep both in step.
READ_COMMANDS = (
    "ls", "cat", "head", "tail", "grep", "zcat", "zgrep", "wc", "stat", "file", "du", "df",
    "free", "uptime", "uname", "hostname", "id", "whoami", "date", "ps", "pgrep", "top -b",
    "vmstat", "iostat", "journalctl", "dmesg", "ss", "lsof", "systemctl status",
)
# Commands that run other commands (shells, wrappers, interpreters, debuggers,
# ``find -exec``, ``awk system()``, ``sed e``, ``make`` recipes, ``ssh``, and ``git``,
# which runs ``core.fsmonitor``, ``diff.external``, hooks, filters and aliases from
# repository config that its own ``--output``/``--format`` options can write), compared
# by ``wrapper_name`` (the basename of the command word with a trailing version suffix
# stripped: ``perl5.36``, ``python3.12``, ``node18`` compare as ``perl``, ``python``,
# ``node``); an entry ending in ``*`` matches by prefix (``python*``, ``ld-linux*``).
# A privileged entry starting with one is refused (swv-root runs it as root outside
# the sandbox), and so is an allow or privileged entry with approval ``listed``
# patterns (it would run a listed command unchecked). An exact copy of the launcher's
# ``permissions.COMMAND_WRAPPERS`` (114 entries, same order): keep both in step.
COMMAND_WRAPPERS = (
    # shells
    "sh", "bash", "dash", "zsh", "ksh", "mksh", "csh", "tcsh", "fish", "ash", "rbash",
    "yash", "busybox", "toybox",
    # wrappers that run their arguments as a command
    "env", "command", "exec", "xargs", "parallel", "nice", "ionice", "nohup", "timeout",
    "time", "stdbuf", "setsid", "flock", "watch", "script", "taskset", "chrt", "socat",
    "sudo", "su", "doas", "pkexec", "runuser", "setpriv", "nsenter", "unshare", "chroot",
    "systemd-run", "ld-linux*", "ld.so", "bwrap", "firejail", "tmux", "screen", "at",
    "batch", "crontab",
    # debuggers and tracers
    "strace", "ltrace", "gdb", "valgrind",
    # interpreters
    "python*", "pypy", "perl", "ruby", "irb", "node", "nodejs", "npx", "npm", "bun", "deno",
    "php", "lua", "luajit", "tclsh", "wish", "expect", "Rscript", "R", "awk", "gawk",
    "mawk", "nawk",
    # shell builtins and keywords that run (or later run) a string or a file as
    # commands: eval, source/., builtin (runs any builtin, eval included), trap and
    # coproc (run a command), fc (re-runs history), alias (redefines a command), enable
    # (loads a builtin from a shared object), let (arithmetic expands command
    # substitutions in array subscripts), mapfile/readarray (-C callback), hash (-p
    # binds a name to any file); zsh: autoload, zmodload, noglob, nocorrect, sched, zpty
    "eval", "source", ".", "builtin", "trap", "coproc", "fc", "alias", "enable", "let",
    "mapfile", "readarray", "hash", "autoload", "zmodload", "noglob", "nocorrect", "sched",
    "zpty",
    # tools with an exec feature
    "find", "sed", "make", "ssh", "scp", "sftp", "tar", "zip", "rsync", "git",
    "vi", "vim", "nvim", "ex", "less", "more", "man",
)
# A trailing version suffix on a command name (the launcher's ``_VERSION_SUFFIX_RE``).
_VERSION_SUFFIX_RE = re.compile(r"-?[0-9][0-9.]*\Z")
# The most a launcher publishes about its agents: the serialized ``profiles`` frame (its
# TOML profiles and applied definitions with their warnings), the same list its hello
# carries. A copy of the launcher's ``definitions.PUBLISHED_PROFILES_BUDGET``: it leaves
# room for the rest of the hello under the broker's 256 KiB frame cap.
PUBLISHED_PROFILES_BUDGET = 192 * 1024
# The launcher's ``launcher_config.DEFAULT_MAX_IDLE_S`` (published for a definition
# without ``limits.max_idle_s``).
DEFAULT_MAX_IDLE_S = 86400.0
# The machine user the launcher reserves for runtime installs (``prereqs``), never an
# agent's ``run_as``.
RESERVED_RUN_AS = ("root", "sessionweave")

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
# Wildcards: observe paths are read literally by some runtimes and as patterns by others.
_GLOB_CHARS = "*?[]{}"
# The launcher's ``permissions`` syntax rules for models and hosts (same expressions).
_MODEL_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:/@+-]{0,199}\Z")
_CLINE_PROVIDER_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}\Z")
_LABEL = r"[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?"
_HOST_RE = re.compile(rf"^(\*\.)?{_LABEL}(\.{_LABEL})*(:(?P<port>\d{{1,5}}))?$")
# Claude's tool-rule parser splits on whitespace and commas (``RUNTIME_CAPS``).
_UNSAFE_PATH_RE = re.compile(r"[\s,()]")


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
        not v or len(v) > _MAX_GLOB or not v.isprintable() or v != v.strip()
        or v.startswith("/") or "~" in v or ".." in v.split("/")
        or any(c in v for c in _GLOB_FORBIDDEN)
    ):
        raise DefinitionError(
            f"{where}: {v!r} must be a glob relative to the workdir (no leading '/',"
            " no '~', no '..', no braces or backslashes, no leading or trailing spaces)"
        )
    return v


def _model(model: str, runtime: str) -> str:
    """The launcher's ``model_problem``/``cline_model_problem``: '' is the runtime's
    default; else it starts with a letter or digit (never read as a flag); Cline's
    ``provider:model`` has a lowercase provider id."""
    if model and not _MODEL_RE.match(model):
        raise DefinitionError(
            f"model {model!r} must start with a letter or digit and use only letters,"
            " digits and . _ : / @ + -"
        )
    if runtime == "cline" and model:
        provider, sep, name = model.partition(":")
        if sep and (not _CLINE_PROVIDER_RE.match(provider) or not _MODEL_RE.match(name)):
            raise DefinitionError(
                f"cline model {model!r} must be 'provider:model' with a lowercase provider"
                " id (letters, digits, _ -) and a model name"
            )
    return model


def _host(host: str) -> str:
    """The launcher's ``check_host``: a lowercase host name, ``*.name`` (not on a
    bare TLD) or ``name:port``; no IPv6 literals."""
    if host != host.lower() or "[" in host or host.count(":") > 1:
        raise DefinitionError(
            f"access.network: malformed host {host!r} (lowercase host name, optional"
            " '*.' prefix and ':port'; no IPv6 literals)"
        )
    m = _HOST_RE.match(host)
    if not m or (m.group("port") is not None and not 1 <= int(m.group("port")) <= 65535):
        raise DefinitionError(f"access.network: malformed host {host!r}")
    name = host.split(":")[0]
    if name.startswith("*.") and "." not in name[2:]:
        raise DefinitionError(f"access.network: wildcard on a bare TLD is not allowed: {host!r}")
    return host


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


def _norm_words(entry: str) -> list[str]:
    """The entry's words, the command word compared by basename (``/usr/bin/git``
    is ``git``)."""
    words = entry.split()
    return [posixpath.basename(words[0]) or words[0], *words[1:]]


def _distinct_subcommands(entry: list[str], pattern: list[str]) -> bool:
    """Whether an entry and a pattern with the same command word differ at a later
    word that is not an option in either, with no option word before it
    (``systemctl status`` vs ``systemctl restart``). False when one is a word prefix
    of the other (``systemctl`` vs ``systemctl restart``, ``systemctl restart nginx``
    vs ``systemctl restart``) or an option comes first (``systemctl -H x status`` vs
    ``systemctl -H x restart``). The launcher's rule, word for word."""
    for e, p in zip(entry[1:], pattern[1:], strict=False):
        if e.startswith("-") or p.startswith("-"):
            return False
        if e != p:
            return True
    return False


def wrapper_name(word: str) -> str:
    """The name ``is_command_wrapper`` compares: the basename of the command word
    with a trailing version suffix (``-``? digits and dots) stripped, so
    ``/usr/bin/perl5.36`` is ``perl`` and ``node18`` is ``node``. A name that is only
    digits stays as it is. The launcher's ``wrapper_name``."""
    name = posixpath.basename(word) or word
    return _VERSION_SUFFIX_RE.sub("", name) or name


def is_command_wrapper(word: str) -> bool:
    """Whether the command word is in ``COMMAND_WRAPPERS``: its ``wrapper_name``
    equals an entry, or starts with an entry ending in ``*``."""
    name = wrapper_name(word)
    return any(name.startswith(w[:-1]) if w.endswith("*") else name == w
               for w in COMMAND_WRAPPERS)


def _listed_overlap(kind: str, commands: dict[str, Any], approval: dict[str, Any]) -> None:
    """Approval ``listed`` fails closed (its patterns become denies): an allow or
    privileged entry sharing a pattern's command word must differ from it at a
    subcommand word (allow ``systemctl status`` with ``systemctl restart``); else the
    entry would run the pattern unasked, always ask, or reach it in another
    spelling. A shell, and any entry that runs other commands (``COMMAND_WRAPPERS``),
    are refused: they could run a listed command under another spelling."""
    patterns = approval["patterns"]
    if approval["policy"] != "listed" or not patterns or _mode_of(commands) == "none":
        return  # an empty list runs nothing: its patterns are ignored
    if commands["mode"] == "shell":
        raise DefinitionError(
            "approval.patterns: approval 'listed' with patterns needs access.commands.mode"
            " 'list' until approvals are implemented (a shell can run a listed command"
            " under another spelling)"
        )
    allow = commands["allow"]
    for what, entries in (("allow", allow), ("privileged", commands["privileged"])):
        for entry in entries:
            if is_command_wrapper(entry.split()[0]):
                raise DefinitionError(
                    f"access.commands.{what}: {entry!r} runs other commands (shell, wrapper,"
                    " interpreter or git, which runs commands from repository config): with"
                    " approval for listed commands it could run a listed command unchecked;"
                    " remove it (git cannot be combined with listed approvals until"
                    " approvals are implemented)"
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
                    " commands that differ from it at a subcommand word (e.g. 'systemctl"
                    " status' with 'systemctl restart'), without options before it"
                )


def _observe(raw: Any, machine: Machine) -> dict[str, Any]:
    obs = _obj(raw, "observe", _OBSERVE_KEYS)
    out: dict[str, Any] = {}
    for key in ("sources", "logs"):
        out[key] = [
            _path(p, f"observe.{key}", machine)
            for p in _str_list(obs.get(key), f"observe.{key}", _MAX_LIST)
        ]
        for p in out[key]:
            if any(c in p for c in _GLOB_CHARS):
                # Claude and opencode would read them as wildcards, Cline and the
                # sandbox literally.
                raise DefinitionError(
                    f"observe.{key}: {p!r} must not contain glob characters (* ? [ ] {{ }})"
                )
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
    if isinstance(network, list):
        for h in network:
            _host(h)
    cmds = _obj(acc.get("commands"), "access.commands", _COMMANDS_KEYS)
    # Access model v2: both kinds default to (and may use) any mode; an observer's
    # sandbox keeps it read-only.
    mode = cmds.get("mode", "shell")
    if mode not in COMMAND_MODES:
        raise DefinitionError(f"access.commands.mode must be one of {list(COMMAND_MODES)}")
    allow = _commands(
        _str_list(cmds.get("allow"), "access.commands.allow", _MAX_ALLOW), "access.commands.allow"
    )
    if allow and mode != "list":
        raise DefinitionError("access.commands.allow applies only to mode 'list'")
    privileged = _commands(
        _str_list(cmds.get("privileged"), "access.commands.privileged", _MAX_PRIVILEGED),
        "access.commands.privileged",
    )
    if privileged and kind == "observer":
        raise DefinitionError(
            "an observer gets no privileged (root-delegated) commands:"
            " access.commands.privileged must be empty"
        )
    if privileged and machine.mode != "root":
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


# --- the published-profiles budget -------------------------------------------------

_LIMIT_KEYS = ("max_duration_s", "max_idle_s", "max_cost_usd")


def published_profile(definition: dict[str, Any], warnings: list[str]) -> dict[str, Any]:
    """What the launcher publishes for an applied (normalised) definition: its
    ``public_profile`` (no workdir, instructions or globs; limits as floats, with the
    default idle limit) plus the warnings it reports with it."""
    files = definition["access"]["files"]
    access: dict[str, Any] = {
        "preset": "reader" if definition["kind"] == "observer" else "operator"}
    network = definition["access"]["network"]
    if network is not None:
        access["network"] = network if isinstance(network, str) else list(network)
    if any(files[k] for k in _FILES_KEYS):
        access["custom_files"] = True
    limits: dict[str, float] = {}
    for key in _LIMIT_KEYS:
        if key in definition["limits"]:
            v = definition["limits"][key]
            value = None if v is None else float(v)
        else:
            value = DEFAULT_MAX_IDLE_S if key == "max_idle_s" else None
        if value is not None:
            limits[key] = value
    out: dict[str, Any] = {
        "id": definition["id"], "domain": definition["domain"], "role": definition["role"],
        "capabilities": list(definition["capabilities"]),
        "description": definition["description"], "runtime": definition["runtime"],
        "model": definition["model"], "access": access, "limits": limits,
        "max_instances": definition["max_instances"], "source": "remote",
        "kind": definition["kind"], "coordinator_capable": definition["coordinator_capable"],
    }
    if definition["visibility"] is not None:
        out["visibility"] = definition["visibility"]
    if definition["allowed_accounts"] is not None:
        out["allowed_accounts"] = list(definition["allowed_accounts"])
    out["warnings"] = list(warnings)
    return out


def json_size(value: Any) -> int:
    """Bytes of ``value`` as the launcher serializes it (``json.dumps`` defaults)."""
    return len(json.dumps(value).encode("utf-8"))


def profiles_frame_size(sizes: list[int]) -> int:
    """Bytes of the ``{"type": "profiles", "profiles": [...]}`` frame whose profiles
    serialize to ``sizes`` bytes each (``json.dumps`` separators)."""
    empty = json_size({"type": "profiles", "profiles": []})
    return empty + sum(sizes) + 2 * max(len(sizes) - 1, 0)


def budget_problem(size: int) -> str | None:
    """The launcher's ``BUDGET_PROBLEM`` when a frame of ``size`` bytes exceeds
    ``PUBLISHED_PROFILES_BUDGET``, else None."""
    if size <= PUBLISHED_PROFILES_BUDGET:
        return None
    return (
        "machine definitions budget exceeded: this machine's published agents would take"
        f" {-(-size // 1024)} KiB of the {PUBLISHED_PROFILES_BUDGET // 1024} KiB a machine"
        " may publish; remove or shorten other definitions (descriptions, capabilities,"
        " allowed_accounts)"
    )


# The runtimes' configuration and instruction files and git's metadata: inside the
# sandbox they stay read-only, and a write glob naming them would bind them read-write.
# The launcher's ``permissions.PROTECTED_WRITE_NAMES`` (``sandbox.PROTECTED_NAMES`` plus
# ``.git``), same order.
PROTECTED_WRITE_NAMES = (
    ".claude", ".mcp.json", "opencode.json", "opencode.jsonc", ".opencode", ".clinerules",
    ".cline", ".agents", ".cursorrules", ".windsurfrules", "CLAUDE.md", "CLAUDE.local.md",
    "AGENTS.md", ".git",
)
# The launcher's ``PROTECTED_WRITE_PROBLEM`` and ``LISTED_WITH_WRITES``, word for word.
PROTECTED_WRITE_PROBLEM = (
    "write path {glob!r} would make {name!r} writable: the runtime configuration, "
    "instruction and git files stay read-only for agents (name the directories to "
    "write instead, e.g. 'src' or 'docs/**'; the default '.' writes the workdir with "
    "them kept read-only)"
)
LISTED_WITH_WRITES = (
    "listed approvals can't protect an agent that can write \u2014 it can run project "
    "code (tests, builds, scripts); use approval 'always' or 'auto' until approvals ship"
)
_GENERIC_GLOB_PARTS = frozenset({"*", "**"})


def protected_write_problem(globs: list[str]) -> str | None:
    """The launcher's ``protected_write_problem``: why one of the write ``globs``
    targets a protected name, else None. Every ``/``-separated part is compared with
    every name at any depth: a literal part equal to one (``.git``,
    ``src/.git/hooks``, ``**/AGENTS.md``) or a wildcard part that matches one
    (``.gi?``, ``AGENTS.*``, ``*.md``; case-sensitive ``fnmatch``) is refused. Parts
    that are only ``*`` or ``**`` (and ``.``) are generic: ``.``, ``**``, ``src/**``
    stay allowed (the sandbox keeps the protected files read-only under them)."""
    for glob in globs:
        for part in glob.split("/"):
            if not part or part == "." or part in _GENERIC_GLOB_PARTS:
                continue
            wild = any(c in part for c in _GLOB_CHARS)
            for name in PROTECTED_WRITE_NAMES:
                if (fnmatch.fnmatchcase(name, part) if wild else part == name):
                    return PROTECTED_WRITE_PROBLEM.format(glob=glob, name=name)
    return None


def _write_rules(d: dict[str, Any]) -> None:
    """The launcher's write checks, in its order (before the listed-approval rules):
    the write globs (an operator's empty list is the default ``.``; approval "always"
    does not excuse a bad glob) never name a protected file, and approval ``listed``
    with patterns is refused for an agent that can write (it can run project code)."""
    observer = d["kind"] == "observer"
    writes = [] if observer else (d["access"]["files"]["write"] or ["."])
    if (why := protected_write_problem(writes)) is not None:
        raise DefinitionError(why)
    approval = d["approval"]
    if (
        approval["policy"] == "listed" and approval["patterns"]
        and _mode_of(d["access"]["commands"]) != "none" and writes
    ):
        raise DefinitionError(LISTED_WITH_WRITES)


# Access model v2 (the launcher's ``permissions`` texts, word for word, ASCII only).
LIST_IS_GUIDANCE = (
    "the command list guides the agent; it is not a security limit - the sandbox and "
    "the writable paths are"
)
UNSANDBOXED_WRITER_WARNING = (
    "this agent runs without the OS sandbox: it can do anything its user can on this "
    "machine (its run_as user, else root), within that user's own permissions"
)
READ_ONLY_NEEDS_SANDBOX = (
    "read-only can't be guaranteed without the OS sandbox: this agent can run commands "
    "and this machine has no usable bubblewrap. Install it and restart the "
    "launcher (Ubuntu 24.04: also enable its AppArmor profile, see the bwrap "
    "prerequisite), or set commands to none"
)
PRIVILEGED_RUNNER_PROBLEM = (
    "privileged entry '{entry}' can run other commands: swv-root would run them as root "
    "outside the sandbox; remove it"
)
READ_ONLY_NETWORK_WARNING = (
    "read-only covers files only: commands can reach the network and call the services "
    "there (including on this machine)"
)
OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS = (
    "observer_as_root can't have commands until network isolation ships: as root its "
    "commands could reach every socket and service on this machine; set commands to none "
    "(its file tools still read everything)"
)
# The launcher's limits-summary and deny texts (shown by the Cockpit's preview; the
# broker does not build the summary). ``DENY_ROOT_OBSERVER_WARNING`` needs the real
# paths of a root observer's workdir and observed paths, so only the launcher reports
# it (in its definition status).
NETWORK_SUMMARY_NONE_WITH_COMMANDS = (
    "- Network: web tools off (commands can still reach the network)"
)
DENY_SUMMARY_SANDBOXED = (
    "- Hidden from you: {globs} (files present when you start; do not try to read or "
    "copy them)"
)
DENY_SUMMARY_ROOT_OBSERVER = (
    "- Hidden from you under {roots} only: {globs} (files present when you start; do not "
    "try to read or copy them); elsewhere on the machine they stay readable"
)
DENY_ROOT_OBSERVER_WARNING = (
    "denied files ({globs}) are hidden under {roots} only; elsewhere on the machine they "
    "stay readable"
)
DENY_SUMMARY_UNSANDBOXED = (
    "- Your file tools can't read: {globs} (commands run with your OS user's permissions)"
)


def runs_sandboxed(kind: str, machine: Machine) -> bool:
    """The launcher's ``runs_sandboxed``: every agent of a ``--local`` launcher runs
    in bubblewrap; on a ``--root`` launcher only an observer, when the machine has a
    usable bubblewrap (``machine.sandbox``)."""
    if machine.mode == "local":
        return True
    return machine.mode == "root" and kind == "observer" and machine.sandbox is not None


def read_only_sandbox_problem(*, read_only: bool, commands_mode: str,
                              sandboxed: bool) -> str | None:
    """The launcher's ``read_only_sandbox_problem``: a read-only agent that can run
    commands needs the sandbox (only its read-only binds keep a command from changing
    files); mode ``none`` is fine."""
    if read_only and commands_mode != "none" and not sandboxed:
        return READ_ONLY_NEEDS_SANDBOX
    return None


def access_warnings(*, read_only: bool, commands_mode: str, sandboxed: bool) -> list[str]:
    """The launcher's ``access_warnings`` (``read_only``: the agent cannot write):
    ``READ_ONLY_NETWORK_WARNING`` for an agent that cannot write but runs commands;
    for one that can write, ``LIST_IS_GUIDANCE`` in list mode and
    ``UNSANDBOXED_WRITER_WARNING`` when it runs commands without the sandbox (a
    ``--root`` operator)."""
    out: list[str] = []
    if read_only:
        if commands_mode != "none":
            out.append(READ_ONLY_NETWORK_WARNING)
        return out
    if commands_mode == "list":
        out.append(LIST_IS_GUIDANCE)
    if commands_mode != "none" and not sandboxed:
        out.append(UNSANDBOXED_WRITER_WARNING)
    return out


def _mode_of(cmds: dict[str, Any]) -> str:
    """The launcher's ``_mode_of``/``effective_commands_mode``: a ``list`` with no allow
    and no privileged entry runs nothing, so it is ``none``."""
    if cmds["mode"] == "list" and not cmds["allow"] and not cmds["privileged"]:
        return "none"
    return str(cmds["mode"])


def _effective_access(d: dict[str, Any]) -> tuple[bool, str]:
    """``(cannot_write, commands_mode)`` as the launcher resolves them
    (``cannot_write(perms)`` and ``commands_of(perms).mode``): observers and approval
    "always" cannot write (a definition's operator never has an empty write list: [] is
    the default "."); "always" leaves no commands; a list with no allow and no
    privileged entry runs nothing."""
    always = d["approval"]["policy"] == "always"
    mode = "none" if always else _mode_of(d["access"]["commands"])
    return d["kind"] == "observer" or always, mode


def definition_warnings(d: dict[str, Any], machine: Machine) -> list[str]:
    """Warnings the launcher will report for the (normalised) definition ``d`` that
    the broker can already tell (shown at deploy, before the launcher answers; the
    launcher's own warnings replace them with its status): its ``access_warnings``."""
    read_only, mode = _effective_access(d)
    return access_warnings(read_only=read_only, commands_mode=mode,
                           sandboxed=runs_sandboxed(d["kind"], machine))


NETWORK_NEEDS_NO_COMMANDS = (
    "access.network: host lists need access.commands.mode 'none' (a shell or a command"
    " list on the shared host network can reach any host; use network 'any' or 'none',"
    " or drop the commands)"
)


def _launcher_rules(d: dict[str, Any], machine: Machine) -> None:
    """The launcher's checks that depend only on the definition, its runtime and the
    machine's start mode (``validate_profile``/``check_runtime`` and the run-as rules of
    ``_check_definition``): a definition failing one is rejected on every machine of
    that mode, so it is refused here before it is stored. Checks that need the machine
    itself (runtime installed, logins, sandbox probes, real paths) stay launcher-side."""
    runtime, observer = d["runtime"], d["kind"] == "observer"
    acc, approval = d["access"], d["approval"]
    files, cmds, network = acc["files"], acc["commands"], acc["network"]
    # What the launcher resolves (``resolve_definition_permissions``): approval
    # "always" leaves no commands and no writes; empty file lists are the defaults.
    # The effective commands mode (the launcher's ``effective_commands_mode``, used by
    # every "has commands" rule): approval "always" leaves none, an empty list runs
    # nothing.
    always = approval["policy"] == "always"
    mode = "none" if always else _mode_of(cmds)
    listed = approval["policy"] == "listed" and mode != "none" and bool(approval["patterns"])
    read = files["read"] or ["."]
    write = [] if observer or always else (files["write"] or ["."])
    hosts = network if isinstance(network, list) else None
    root_launcher = machine.mode == "root" and machine.run_as
    sandboxed = runs_sandboxed(d["kind"], machine)
    # The launcher's interim rule (checked when it resolves the permissions): as uid 0
    # an observer's commands could reach every socket and service on the machine.
    if d["observer_as_root"] and mode != "none":
        raise DefinitionError(OBSERVER_AS_ROOT_NEEDS_NO_COMMANDS)

    if d["run_as"] in RESERVED_RUN_AS:
        raise DefinitionError(
            f"run_as {d['run_as']!r} is reserved (root, or the launcher's install user);"
            " choose a non-root machine user"
        )
    if observer and root_launcher and d["run_as"] is None and not d["observer_as_root"]:
        raise DefinitionError(
            "an observer on a --root launcher needs run_as (a non-root machine user) or"
            " observer_as_root"
        )
    if d["observer_as_root"]:
        if machine.sandbox is None:
            raise DefinitionError(
                "observer_as_root needs the bubblewrap sandbox on the launcher's machine"
                " (install bubblewrap and restart the launcher)",
                ErrorCode.OUTSIDE_CEILING,
            )
        if runtime == "cline":
            raise DefinitionError(
                "observer_as_root does not support the cline runtime (Cline keeps its"
                " settings in a host directory)"
            )
    if not always:
        # The launcher's ``privileged_runner_problem``: swv-root runs a privileged
        # entry as root outside the sandbox, so none may run other commands (git
        # included). A writer's command list itself is guidance (access model v2).
        for entry in cmds["privileged"]:
            words = entry.split()
            if words and is_command_wrapper(words[0]):
                raise DefinitionError(PRIVILEGED_RUNNER_PROBLEM.format(entry=entry))
    # Access model v2: read-only is the sandbox's read-only binds; a read-only agent
    # with commands is refused without one (observer_as_root has its own check above).
    read_only, _effective = _effective_access(d)
    if not d["observer_as_root"] and (why := read_only_sandbox_problem(
            read_only=read_only, commands_mode=mode, sandboxed=sandboxed)) is not None:
        raise DefinitionError(why)
    if cmds["privileged"] and not always and not root_launcher:
        # (approval "always" leaves no commands: its privileged entries never run)
        raise DefinitionError(
            "access.commands.privileged needs a machine whose launcher runs with --root"
            " as root (it may run commands as root)",
            ErrorCode.OUTSIDE_CEILING,
        )
    if hosts is not None and mode != "none":
        raise DefinitionError(NETWORK_NEEDS_NO_COMMANDS)
    # The runtime's enforcement limits (the launcher's RUNTIME_CAPS).
    if runtime == "claude":
        if read != ["."]:
            raise DefinitionError(
                "claude cannot restrict reads to part of the workdir: leave"
                " access.files.read empty (or '.') and use deny paths"
            )
        if hosts is not None and any(":" in h for h in hosts):
            raise DefinitionError("claude cannot restrict a host to one port; drop ':port'")
        bad = [v for v in [d["workdir"], *read, *write, *files["deny"]]
               if _UNSAFE_PATH_RE.search(v)]
        if bad:
            raise DefinitionError(
                "claude cannot enforce paths with whitespace, ',' or parentheses:"
                f" {bad[0]!r}"
            )
        if mode == "list" and listed:
            raise DefinitionError(
                "claude cannot hold a command list next to approval for listed commands:"
                " its built-in read-only commands (git status, git log, ...) run without a"
                " rule and git runs commands from repository config, so a listed command"
                " could run unasked; use opencode or cline, or approval 'always' or 'auto'"
            )
    elif runtime == "opencode":
        if any("*" in g.replace("**", "") for g in [*read, *write]):
            raise DefinitionError(
                "opencode widens a single '*' in read/write globs to any depth; use 'dir'"
                " or 'dir/**'"
            )
        if hosts is not None and any(h.startswith("*.") for h in hosts):
            raise DefinitionError("opencode cannot enforce wildcard hosts; list each host")
    elif runtime == "cline" and hosts is not None:
        raise DefinitionError(
            "cline cannot restrict network hosts (its web fetch follows redirects); use"
            " network 'none' or 'any'"
        )


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
    observer never writes files (access model v2: it may run any command, its
    sandbox keeps it read-only; without one it gets commands "none" only);
    command entries start with a command, not an option; limits are capped
    (``LIMIT_MAX``). The launcher's ``normalise_definition`` plus the checks of its
    ``_check_definition`` that do not need the machine (``_launcher_rules``).
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
            raise DefinitionError(f"{key} must be 1-64 chars of [A-Za-z0-9._-], not only dots")
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
    out["model"] = _model(_text(raw, "model", 200), out["runtime"])
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
    _write_rules(out)
    _listed_overlap(kind, out["access"]["commands"], out["approval"])
    _launcher_rules(out, machine)
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
