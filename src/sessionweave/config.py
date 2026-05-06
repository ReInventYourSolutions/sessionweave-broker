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
    _validate(merged, bounds)
    return merged


def _validate(cfg: SessionConfig, b: ConfigBounds) -> None:
    def check(field: str, value: int, lo: int, hi: int | None) -> None:
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
    check(
        "default_command_timeout_seconds",
        cfg.default_command_timeout_seconds,
        b.default_command_timeout_seconds_min,
        cfg.max_command_timeout_seconds,
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


def load_deployment_defaults(env: Mapping[str, str]) -> DeploymentDefaults:
    def env_int(name: str, default: int) -> int:
        return int(env.get(name, default))

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
    )
    return DeploymentDefaults(defaults=defaults, bounds=bounds)
