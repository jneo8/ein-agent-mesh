"""The message bus boundary: ``Record`` and the ``MessageBus`` ABC every role and integrator sees.

Transport-specific code lives beside this module: ``kafka.py`` (the only place aiokafka is
imported) and ``memory.py`` (tests and evals, no broker).
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from dataclasses import dataclass


@dataclass(frozen=True)
class Record:
    topic: str
    key: str | None
    value: dict
    offset: int = 0


class MessageBus(ABC):
    """What a role or integrator may do with the bus. Concrete methods are the shared behaviour
    (value normalisation, the default clock); everything transport-specific is abstract."""

    @abstractmethod
    async def start(self) -> None: ...

    @abstractmethod
    async def stop(self) -> None: ...

    @abstractmethod
    async def _produce(self, topic: str, key: str | None, value: dict) -> None: ...

    @abstractmethod
    async def read_to_end(self, topic: str) -> list[Record]:
        """Everything currently on ``topic``; the restore step for compacted state topics."""

    @abstractmethod
    def subscribe(self, group: str | None, *topics: str, **consumer_options) -> None: ...

    @abstractmethod
    async def poll(self, timeout_ms: int, max_records: int | None = None) -> list[Record]: ...

    @abstractmethod
    async def commit(self) -> None: ...

    async def produce(self, topic: str, key: str | None, value) -> None:
        """``value`` is a pydantic model or a JSON-able dict; it reaches the transport as a dict."""
        if hasattr(value, "model_dump"):
            value = value.model_dump(by_alias=True, mode="json")
        await self._produce(topic, key, value)

    def now(self) -> float:
        """Monotonic seconds. The in-memory bus replaces this with a fake clock so time-based
        batching can be tested."""
        return time.monotonic()

    def pending(self) -> bool:
        """True when the bus still holds unread records for the subscription. Only the in-memory
        bus can know; Kafka answers False and the run loop never ends on its own."""
        return False
