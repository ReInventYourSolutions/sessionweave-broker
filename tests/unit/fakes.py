from typing import Any


class FakeHandle:
    def __init__(self) -> None:
        # ``sent`` preserves the legacy (name, payload) shape so older
        # tests that destructure as 2-tuples keep working. Tests that
        # care about the dedup ``seq`` envelope can read ``sent_seqs``
        # parallel array (indexed in lockstep with ``sent``).
        self.sent: list[tuple[str, dict[str, Any]]] = []
        self.sent_seqs: list[int | None] = []
        self._alive = True

    def send(
        self, name: str, payload: dict[str, Any], *, seq: int | None = None
    ) -> None:
        self.sent.append((name, payload))
        self.sent_seqs.append(seq)

    def is_alive(self) -> bool:
        return self._alive

    def close(self) -> None:
        self._alive = False
