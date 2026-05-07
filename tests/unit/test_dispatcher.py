import pytest

from sessionweave.dispatcher import (
    DispatchDecision,  # noqa: F401
    RoundRobinPicker,
    decide_dispatch,
)
from sessionweave.errors import ErrorCode, ToolError
from sessionweave.models import Participant
from sessionweave.schemas import DispatchMode, Target


def _p(pid: str, role: str | None, caps: list[str], online: bool = True) -> Participant:
    p = Participant.create(role=role, capabilities=caps)
    p.participant_id = pid
    p.online = online
    return p


def test_specific_includes_offline_in_grace() -> None:
    roster = {
        "a": _p("a", "x", ["c1"], online=True),
        "b": _p("b", "x", ["c1"], online=False),
    }
    decision = decide_dispatch(
        roster=roster,
        sender_id="x",
        dispatch=DispatchMode.SPECIFIC,
        target=Target(participant_ids=["a", "b"]),
        picker=RoundRobinPicker(),
    )
    assert sorted(decision.recipients) == ["a", "b"]


def test_specific_excludes_unknown_ids() -> None:
    roster = {"a": _p("a", "x", ["c1"], online=True)}
    with pytest.raises(ToolError) as exc:
        decide_dispatch(
            roster=roster,
            sender_id="x",
            dispatch=DispatchMode.SPECIFIC,
            target=Target(participant_ids=["a", "ghost"]),
            picker=RoundRobinPicker(),
        )
    assert exc.value.code is ErrorCode.INVALID_TARGETS


def test_all_excludes_sender_self() -> None:
    roster = {
        "a": _p("a", "reviewer", ["sec"], online=True),
        "b": _p("b", "reviewer", ["sec"], online=True),
    }
    decision = decide_dispatch(
        roster=roster,
        sender_id="a",
        dispatch=DispatchMode.ALL,
        target=Target(role="reviewer"),
        picker=RoundRobinPicker(),
    )
    assert decision.recipients == ["b"]


def test_all_filters_by_role_and_capability() -> None:
    roster = {
        "a": _p("a", "reviewer", ["sec"]),
        "b": _p("b", "reviewer", ["other"]),
        "c": _p("c", "executor", ["sec"]),
    }
    d = decide_dispatch(
        roster=roster,
        sender_id="x",
        dispatch=DispatchMode.ALL,
        target=Target(role="reviewer", capability="sec"),
        picker=RoundRobinPicker(),
    )
    assert d.recipients == ["a"]


def test_all_includes_offline_in_grace() -> None:
    roster = {
        "a": _p("a", "reviewer", ["sec"], online=True),
        "b": _p("b", "reviewer", ["sec"], online=False),
    }
    d = decide_dispatch(
        roster=roster,
        sender_id="x",
        dispatch=DispatchMode.ALL,
        target=Target(role="reviewer"),
        picker=RoundRobinPicker(),
    )
    assert sorted(d.recipients) == ["a", "b"]


def test_any_only_picks_online() -> None:
    roster = {
        "a": _p("a", "reviewer", ["sec"], online=False),
        "b": _p("b", "reviewer", ["sec"], online=True),
    }
    d = decide_dispatch(
        roster=roster,
        sender_id="x",
        dispatch=DispatchMode.ANY,
        target=Target(role="reviewer"),
        picker=RoundRobinPicker(),
    )
    assert d.recipients == ["b"]


def test_any_returns_empty_when_no_online_match() -> None:
    roster = {"a": _p("a", "reviewer", ["sec"], online=False)}
    d = decide_dispatch(
        roster=roster,
        sender_id="x",
        dispatch=DispatchMode.ANY,
        target=Target(role="reviewer"),
        picker=RoundRobinPicker(),
    )
    assert d.recipients == []


def test_round_robin_distributes_over_time() -> None:
    roster = {
        "a": _p("a", "r", []),
        "b": _p("b", "r", []),
        "c": _p("c", "r", []),
    }
    picker = RoundRobinPicker()
    picks: list[str] = []
    for _ in range(6):
        d = decide_dispatch(
            roster=roster,
            sender_id="x",
            dispatch=DispatchMode.ANY,
            target=Target(role="r"),
            picker=picker,
        )
        picks.extend(d.recipients)
    assert sorted(set(picks)) == ["a", "b", "c"]
    assert picks.count("a") == 2
    assert picks.count("b") == 2
    assert picks.count("c") == 2
