from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass

from sessionweave.errors import ErrorCode, ToolError
from sessionweave.models import Participant
from sessionweave.schemas import DispatchMode, Target


@dataclass(slots=True, frozen=True)
class DispatchDecision:
    recipients: list[str]


class RoundRobinPicker:
    """Stateful selector for `dispatch=any`. Cycles through matched candidates
    sorted by participant_id to keep behavior deterministic in tests.
    """

    __slots__ = ("_index",)

    def __init__(self) -> None:
        self._index: int = 0

    def pick(self, candidates: list[str]) -> str | None:
        if not candidates:
            return None
        ordered = sorted(candidates)
        choice = ordered[self._index % len(ordered)]
        self._index += 1
        return choice


def decide_dispatch(
    *,
    roster: Mapping[str, Participant],
    sender_id: str,
    dispatch: DispatchMode,
    target: Target,
    picker: RoundRobinPicker,
) -> DispatchDecision:
    if dispatch is DispatchMode.SPECIFIC:
        decision = _specific(roster, target.participant_ids or [])
        if any(_is_paused(roster[pid]) for pid in decision.recipients):
            raise _paused_error()
        return decision
    if dispatch not in (DispatchMode.ALL, DispatchMode.ANY):
        raise ToolError(ErrorCode.INVALID_TARGETS, f"unknown dispatch={dispatch}")
    # Paused participants are never matched by role / capability. A match
    # set made only of paused participants is refused rather than closed as
    # ``no_recipients``: the sender must learn that the supervisor stopped
    # them, not that nobody fits.
    matched = _filter_role_capability(roster, sender_id, target)
    active = [pid for pid in matched if not _is_paused(roster[pid])]
    if matched and not active:
        raise _paused_error()
    if dispatch is DispatchMode.ALL:
        return DispatchDecision(active)
    chosen = picker.pick([pid for pid in active if roster[pid].online])
    return DispatchDecision([chosen] if chosen else [])


def _is_paused(p: Participant) -> bool:
    return p.supervision_state == "paused"


def _paused_error() -> ToolError:
    return ToolError(ErrorCode.PARTICIPANT_PAUSED, "participant is paused by the supervisor")


def _specific(
    roster: Mapping[str, Participant], pids: list[str]
) -> DispatchDecision:
    unknown = [p for p in pids if p not in roster]
    if unknown:
        raise ToolError(
            ErrorCode.INVALID_TARGETS,
            f"unknown participant_id(s): {','.join(unknown)}",
        )
    # Dedup while preserving first-occurrence order so callers that submit
    # ["pid-1","pid-1","pid-2"] don't trigger double delivery.
    return DispatchDecision(list(dict.fromkeys(pids)))


def _filter_role_capability(
    roster: Mapping[str, Participant],
    sender_id: str,
    target: Target,
) -> list[str]:
    out: list[str] = []
    for pid, p in roster.items():
        if pid == sender_id:
            continue
        if target.role is not None and p.role != target.role:
            continue
        if target.capability is not None and target.capability not in p.capabilities:
            continue
        out.append(pid)
    return out
