from typing import Any


class FakeHandle:
    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self._alive = True

    def send(self, name: str, payload: dict[str, Any]) -> None:
        self.sent.append((name, payload))

    def is_alive(self) -> bool:
        return self._alive

    def close(self) -> None:
        self._alive = False
