from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class ConnectionHandle(Protocol):
    def send(
        self, name: str, payload: dict[str, Any], *, seq: int | None = None
    ) -> None: ...
    def is_alive(self) -> bool: ...


class ConnectionManager:
    """Maps (session_id, participant_id) ↔ ConnectionHandle.

    A handle may be bound to at most one (session_id, participant_id).
    Re-binding the same key to a new handle returns the prior handle so the
    caller can release it (last-resume-wins).
    """

    __slots__ = ("_by_handle", "_by_key")

    def __init__(self) -> None:
        self._by_handle: dict[int, tuple[str, str]] = {}
        self._by_key: dict[tuple[str, str], ConnectionHandle] = {}

    def bind(
        self,
        handle: ConnectionHandle,
        session_id: str,
        participant_id: str,
    ) -> ConnectionHandle | None:
        existing = self._by_handle.get(id(handle))
        if existing is not None and existing != (session_id, participant_id):
            raise ValueError(
                f"handle already bound to {existing}, cannot re-bind to "
                f"({session_id}, {participant_id})"
            )
        key = (session_id, participant_id)
        prior = self._by_key.get(key)
        if prior is not None and prior is not handle:
            self._by_handle.pop(id(prior), None)
        self._by_key[key] = handle
        self._by_handle[id(handle)] = key
        return prior if prior is not handle else None

    def unbind(self, handle: ConnectionHandle) -> None:
        key = self._by_handle.pop(id(handle), None)
        if key is not None and self._by_key.get(key) is handle:
            self._by_key.pop(key, None)

    def session_for(self, handle: ConnectionHandle) -> tuple[str, str] | None:
        return self._by_handle.get(id(handle))

    def handle_for(
        self, session_id: str, participant_id: str
    ) -> ConnectionHandle | None:
        return self._by_key.get((session_id, participant_id))

    def send(
        self,
        session_id: str,
        participant_id: str,
        name: str,
        payload: dict[str, Any],
        *,
        seq: int | None = None,
    ) -> bool:
        handle = self._by_key.get((session_id, participant_id))
        if handle is None or not handle.is_alive():
            return False
        handle.send(name, payload, seq=seq)
        return True
