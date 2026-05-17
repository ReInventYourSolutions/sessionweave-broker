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
        return _specific(roster, target.participant_ids or [])
    if dispatch is DispatchMode.ALL:
        return DispatchDecision(_filter_role_capability(roster, sender_id, target))
    if dispatch is DispatchMode.ANY:
        candidates = _filter_role_capability(roster, sender_id, target, online_only=True)
        chosen = picker.pick(candidates)
        return DispatchDecision([chosen] if chosen else [])
    raise ToolError(ErrorCode.INVALID_TARGETS, f"unknown dispatch={dispatch}")


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
    online_only: bool = False,
) -> list[str]:
    out: list[str] = []
    for pid, p in roster.items():
        if pid == sender_id:
            continue
        if target.role is not None and p.role != target.role:
            continue
        if target.capability is not None and target.capability not in p.capabilities:
            continue
        if online_only and not p.online:
            continue
        out.append(pid)
    return out
