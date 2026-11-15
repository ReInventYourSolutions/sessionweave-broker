from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class ErrorCode(StrEnum):
    NOT_IN_SESSION = "not_in_session"
    ALREADY_IN_SESSION = "already_in_session"
    SESSION_NOT_FOUND = "session_not_found"
    SESSION_FULL = "session_full"
    PAYLOAD_TOO_LARGE = "payload_too_large"
    INVALID_DISPATCH_FILTERS = "invalid_dispatch_filters"
    INVALID_TARGETS = "invalid_targets"
    COMMAND_NOT_FOUND = "command_not_found"
    NOT_A_RECIPIENT = "not_a_recipient"
    ALREADY_RESPONDED = "already_responded"
    PARTICIPANT_NOT_FOUND = "participant_not_found"
    INVALID_CONFIG = "invalid_config"
    INVALID_DELIVERY_MODE = "invalid_delivery_mode"
    WAIT_ALREADY_IN_FLIGHT = "wait_already_in_flight"
    PARTICIPANT_EVICTED = "participant_evicted"
    TOO_MANY_WAITS = "too_many_waits"
    PERMISSION_DENIED = "permission_denied"
    NO_HISTORY = "no_history"
    PERSISTENCE_FAILED = "persistence_failed"
    PERSISTENCE_CORRUPTED = "persistence_corrupted"
    TOKEN_NOT_FOUND = "token_not_found"
    RATE_LIMITED = "rate_limited"
    PARTICIPANT_PAUSED = "participant_paused"
    PARTICIPANT_REMOVED = "participant_removed"
    INVALID_REQUEST = "invalid_request"
    NOTE_NOT_FOUND = "note_not_found"
    ACCOUNT_TOKEN_REQUIRED = "account_token_required"
    INVALID_ACCOUNT_TOKEN = "invalid_account_token"
    ACCOUNT_NOT_FOUND = "account_not_found"
    ACCOUNT_NAME_TAKEN = "account_name_taken"
    ACCOUNTS_UNAVAILABLE = "accounts_unavailable"
    SESSION_SUSPENDED = "session_suspended"


@dataclass(slots=True)
class ToolError(Exception):
    code: ErrorCode
    message: str

    def __str__(self) -> str:
        return f"{self.code.value}: {self.message}"

    def to_dict(self) -> dict[str, str]:
        return {"code": self.code.value, "message": self.message}
