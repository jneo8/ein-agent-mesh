"""``InMemoryBus``: topics as lists, for unit tests and evals. No broker, deterministic clock."""

from __future__ import annotations

from dataclasses import dataclass, field

from ein_mesh.bus.base import MessageBus, Record


@dataclass
class InMemoryBus(MessageBus):
    """Topics as lists. ``poll`` hands out unread records from the subscribed topics; ``commit`` is a
    no-op because nothing is ever re-read. ``clock`` lets tests drive time-based batching."""

    topics: dict[str, list[Record]] = field(default_factory=dict)
    cursor: dict[str, int] = field(default_factory=dict)
    subscribed: tuple[str, ...] = ()
    clock: float = 0.0

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def _produce(self, topic: str, key: str | None, value: dict) -> None:
        lst = self.topics.setdefault(topic, [])
        lst.append(Record(topic, key, value, offset=len(lst)))

    async def read_to_end(self, topic: str) -> list[Record]:
        return list(self.topics.get(topic, []))

    def subscribe(self, group: str | None, *topics: str, **consumer_options) -> None:
        self.subscribed = topics

    async def poll(self, timeout_ms: int, max_records: int | None = None) -> list[Record]:
        out: list[Record] = []
        for t in self.subscribed:
            lst = self.topics.get(t, [])
            pos = self.cursor.get(t, 0)
            take = lst[pos:] if max_records is None else lst[pos:pos + max(0, max_records - len(out))]
            out.extend(take)
            self.cursor[t] = pos + len(take)
        if not out:
            self.clock += timeout_ms / 1000
        return out

    async def commit(self) -> None:
        pass

    def now(self) -> float:
        return self.clock

    def pending(self) -> bool:
        return any(self.cursor.get(t, 0) < len(self.topics.get(t, [])) for t in self.subscribed)

    # -- test helpers --
    def values(self, topic: str) -> list[dict]:
        return [r.value for r in self.topics.get(topic, [])]
