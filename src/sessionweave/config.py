from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, fields, replace
from typing import Any

from sessionweave.errors import ErrorCode, ToolError


@dataclass(slots=True, frozen=True)
class SessionConfig:
    max_participants: int = 16
    max_payload_bytes: int = 262_144
    idle_ttl_seconds: int = 1800
    default_command_timeout_seconds: int = 60
    max_command_timeout_seconds: int = 300
    offline_grace_seconds: int = 300
    max_pending_notifications_per_participant: int = 1000
    max_wait_seconds: int = 600
    max_concurrent_waits_total: int = 1000
    max_wait_seconds_mcp: int = 20


@dataclass(slots=True, frozen=True)
class ConfigBounds:
    max_participants_min: int = 2
    max_participants_max: int = 256
    max_payload_bytes_min: int = 1024
    max_payload_bytes_max: int = 4_194_304
    idle_ttl_seconds_min: int = 60
    idle_ttl_seconds_max: int = 86_400
    default_command_timeout_seconds_min: int = 1
    max_command_timeout_seconds_min: int = 1
    max_command_timeout_seconds_max: int = 3600
    offline_grace_seconds_min: int = 0
    offline_grace_seconds_max: int = 3600
    max_pending_notifications_per_participant_min: int = 0
    max_pending_notifications_per_participant_max: int = 100_000
    max_wait_seconds_min: int = 1
    max_wait_seconds_max: int = 3600
    max_concurrent_waits_total_min: int = 0
    max_concurrent_waits_total_max: int = 100_000
    max_wait_seconds_mcp_min: int = 1
    max_wait_seconds_mcp_max: int = 60

    @staticmethod
    def builtin() -> ConfigBounds:
        return ConfigBounds()


@dataclass(slots=True, frozen=True)
class DeploymentDefaults:
    defaults: SessionConfig
    bounds: ConfigBounds


_INT_FIELDS: tuple[str, ...] = tuple(f.name for f in fields(SessionConfig))


def apply_overrides(
    defaults: SessionConfig,
    bounds: ConfigBounds,
    overrides: Mapping[str, Any],
) -> SessionConfig:
    candidate: dict[str, Any] = {}
    for key, value in overrides.items():
        if key not in _INT_FIELDS:
            raise ToolError(ErrorCode.INVALID_CONFIG, f"unknown config key: {key}")
        if not isinstance(value, int) or isinstance(value, bool):
            raise ToolError(
                ErrorCode.INVALID_CONFIG, f"{key} must be int, got {type(value).__name__}"
            )
        candidate[key] = value

    merged = replace(defaults, **candidate)
    _validate(merged, bounds, overridden=set(candidate))
    return merged


def _validate(cfg: SessionConfig, b: ConfigBounds, *, overridden: set[str] | None = None) -> None:
    """Validate cfg against bounds.

    When *overridden* is provided, only fields in that set are range-checked
    against the operator bounds.  This lets deployment defaults be set outside
    the client-facing range (e.g. a very short idle_ttl for testing) while
    still enforcing bounds on client-supplied values.
    The cross-field constraint ``default_command_timeout_seconds <=
    max_command_timeout_seconds`` is enforced whenever either field is touched.
    """
    def check(field: str, value: int, lo: int, hi: int | None) -> None:
        if overridden is not None and field not in overridden:
            return
        if value < lo or (hi is not None and value > hi):
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"{field}={value} outside bounds [{lo}, {hi}]",
            )

    check("max_participants", cfg.max_participants, b.max_participants_min, b.max_participants_max)
    check(
        "max_payload_bytes",
        cfg.max_payload_bytes,
        b.max_payload_bytes_min,
        b.max_payload_bytes_max,
    )
    check(
        "idle_ttl_seconds",
        cfg.idle_ttl_seconds,
        b.idle_ttl_seconds_min,
        b.idle_ttl_seconds_max,
    )
    check(
        "max_command_timeout_seconds",
        cfg.max_command_timeout_seconds,
        b.max_command_timeout_seconds_min,
        b.max_command_timeout_seconds_max,
    )
    # Per-field bounds check for default_command_timeout_seconds (subject to override guard).
    check(
        "default_command_timeout_seconds",
        cfg.default_command_timeout_seconds,
        b.default_command_timeout_seconds_min,
        None,
    )
    # Cross-field invariant: enforce when either timeout field is overridden.
    if (
        overridden is None
        or "default_command_timeout_seconds" in overridden
        or "max_command_timeout_seconds" in overridden
    ):
        if cfg.default_command_timeout_seconds > cfg.max_command_timeout_seconds:
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"default_command_timeout_seconds={cfg.default_command_timeout_seconds} "
                f"exceeds max_command_timeout_seconds={cfg.max_command_timeout_seconds}",
            )
    # Cross-field invariant: max_wait_seconds_mcp must never exceed
    # max_wait_seconds. Otherwise the tools.py clamp would report
    # effective_timeout_seconds=max_wait_seconds_mcp while
    # Session.wait_for_event silently re-clamps to the smaller REST cap.
    if (
        overridden is None
        or "max_wait_seconds_mcp" in overridden
        or "max_wait_seconds" in overridden
    ):
        if cfg.max_wait_seconds_mcp > cfg.max_wait_seconds:
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"max_wait_seconds_mcp={cfg.max_wait_seconds_mcp} "
                f"exceeds max_wait_seconds={cfg.max_wait_seconds}",
            )
    check(
        "offline_grace_seconds",
        cfg.offline_grace_seconds,
        b.offline_grace_seconds_min,
        b.offline_grace_seconds_max,
    )
    check(
        "max_pending_notifications_per_participant",
        cfg.max_pending_notifications_per_participant,
        b.max_pending_notifications_per_participant_min,
        b.max_pending_notifications_per_participant_max,
    )
    check(
        "max_wait_seconds",
        cfg.max_wait_seconds,
        b.max_wait_seconds_min,
        b.max_wait_seconds_max,
    )
    check(
        "max_concurrent_waits_total",
        cfg.max_concurrent_waits_total,
        b.max_concurrent_waits_total_min,
        b.max_concurrent_waits_total_max,
    )
    check(
        "max_wait_seconds_mcp",
        cfg.max_wait_seconds_mcp,
        b.max_wait_seconds_mcp_min,
        b.max_wait_seconds_mcp_max,
    )


def load_deployment_defaults(env: Mapping[str, str]) -> DeploymentDefaults:
    def env_int(name: str, default: int) -> int:
        raw = env.get(name, str(default))
        try:
            return int(raw)
        except (TypeError, ValueError) as e:
            raise ToolError(
                ErrorCode.INVALID_CONFIG,
                f"{name}={raw!r} is not an integer: {e}",
            ) from e

    defaults = SessionConfig(
        max_participants=env_int("SW_DEFAULT_MAX_PARTICIPANTS", 16),
        max_payload_bytes=env_int("SW_DEFAULT_MAX_PAYLOAD_BYTES", 262_144),
        idle_ttl_seconds=env_int("SW_DEFAULT_IDLE_TTL_SECONDS", 1800),
        default_command_timeout_seconds=env_int("SW_DEFAULT_COMMAND_TIMEOUT_SECONDS", 60),
        max_command_timeout_seconds=env_int("SW_DEFAULT_MAX_COMMAND_TIMEOUT_SECONDS", 300),
        offline_grace_seconds=env_int("SW_DEFAULT_OFFLINE_GRACE_SECONDS", 300),
        max_pending_notifications_per_participant=env_int(
            "SW_DEFAULT_MAX_PENDING_NOTIFICATIONS", 1000
        ),
        max_wait_seconds=env_int("SW_DEFAULT_MAX_WAIT_SECONDS", 600),
        max_concurrent_waits_total=env_int("SW_DEFAULT_MAX_CONCURRENT_WAITS_TOTAL", 1000),
        max_wait_seconds_mcp=env_int("SW_DEFAULT_MAX_WAIT_SECONDS_MCP", 20),
    )
    bounds = ConfigBounds(
        max_participants_min=env_int("SW_MIN_MAX_PARTICIPANTS", 2),
        max_participants_max=env_int("SW_MAX_MAX_PARTICIPANTS", 256),
        max_payload_bytes_min=env_int("SW_MIN_MAX_PAYLOAD_BYTES", 1024),
        max_payload_bytes_max=env_int("SW_MAX_MAX_PAYLOAD_BYTES", 4_194_304),
        idle_ttl_seconds_min=env_int("SW_MIN_IDLE_TTL_SECONDS", 60),
        idle_ttl_seconds_max=env_int("SW_MAX_IDLE_TTL_SECONDS", 86_400),
        default_command_timeout_seconds_min=env_int("SW_MIN_DEFAULT_COMMAND_TIMEOUT_SECONDS", 1),
        max_command_timeout_seconds_min=env_int("SW_MIN_MAX_COMMAND_TIMEOUT_SECONDS", 1),
        max_command_timeout_seconds_max=env_int("SW_MAX_MAX_COMMAND_TIMEOUT_SECONDS", 3600),
        offline_grace_seconds_min=env_int("SW_MIN_OFFLINE_GRACE_SECONDS", 0),
        offline_grace_seconds_max=env_int("SW_MAX_OFFLINE_GRACE_SECONDS", 3600),
        max_pending_notifications_per_participant_min=env_int(
            "SW_MIN_MAX_PENDING_NOTIFICATIONS", 0
        ),
        max_pending_notifications_per_participant_max=env_int(
            "SW_MAX_MAX_PENDING_NOTIFICATIONS", 100_000
        ),
        max_wait_seconds_min=env_int("SW_MIN_MAX_WAIT_SECONDS", 1),
        max_wait_seconds_max=env_int("SW_MAX_MAX_WAIT_SECONDS", 3600),
        max_concurrent_waits_total_min=env_int("SW_MIN_MAX_CONCURRENT_WAITS_TOTAL", 0),
        max_concurrent_waits_total_max=env_int("SW_MAX_MAX_CONCURRENT_WAITS_TOTAL", 100_000),
        max_wait_seconds_mcp_min=env_int("SW_MIN_MAX_WAIT_SECONDS_MCP", 1),
        max_wait_seconds_mcp_max=env_int("SW_MAX_MAX_WAIT_SECONDS_MCP", 60),
    )
    # Operator misconfiguration: defaults must fit inside bounds at startup
    # so sessions created without client config overrides don't silently use
    # out-of-range values. _validate with overridden=None range-checks all
    # fields against the bounds.
    _validate(defaults, bounds, overridden=None)

    return DeploymentDefaults(defaults=defaults, bounds=bounds)
