"""Base class for every role: one consumer loop, a restore step, and the event helper.

A role owns one state transition of the incident lifecycle. Subclasses declare the topics they
consume, rebuild in-memory state in ``restore``, and implement ``handle`` for each poll. The loop,
the commit-after-produce rule and the event envelope live here so a role's own code is only the
decision logic, which is what unit tests and evals exercise through an ``InMemoryBus``.
"""

from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from typing import ClassVar

from ein_mesh.bus import MessageBus, Record
from ein_mesh.config import Topics
from ein_mesh.schemas import Event

log = logging.getLogger(__name__)


class Role(ABC):
    name: ClassVar[str]
    poll_timeout_ms: ClassVar[int] = 1000

    def __init__(self, bus: MessageBus, topics: Topics) -> None:
        self.bus, self.topics = bus, topics

    # -- what a role declares --

    @property
    @abstractmethod
    def inputs(self) -> list[str]:
        """Topics this role consumes, with consumer group ``<prefix><name>``."""

    def consumer_options(self) -> dict:
        return {}

    def poll_max_records(self) -> int | None:
        return None

    async def restore(self) -> None:
        """Rebuild in-memory state from compacted topics. Default: stateless."""

    @abstractmethod
    async def handle(self, records: list[Record]) -> bool:
        """Process one poll (possibly empty). Return True when offsets may be committed, i.e. every
        record received so far has had its outputs produced."""

    # -- shared helpers --

    async def event(self, root_id: str, kind: str, body: dict) -> None:
        await self.bus.produce(self.topics.events, root_id,
                               Event(root_id=root_id, role=self.name, kind=kind, body=body))

    # -- the loop --

    async def run(self) -> None:
        self.bus.subscribe(self.name, *self.inputs, **self.consumer_options())
        await self.bus.start()
        try:
            await self.restore()
            log.info("%s consuming %s", self.name, self.inputs)
            while True:
                records = await self.bus.poll(self.poll_timeout_ms, self.poll_max_records())
                if await self.handle(records):
                    await self.bus.commit()
        finally:
            await self.bus.stop()

    def idle(self) -> bool:
        """True when nothing is buffered inside the role. Roles that batch override this."""
        return True

    async def run_until_idle(self) -> None:
        """Test helper: restore, then poll until the bus and the role both have nothing left."""
        self.bus.subscribe(self.name, *self.inputs, **self.consumer_options())
        await self.restore()
        while True:
            records = await self.bus.poll(self.poll_timeout_ms, self.poll_max_records())
            if await self.handle(records):
                await self.bus.commit()
            if not records and self.idle() and not self.bus.pending():
                break
