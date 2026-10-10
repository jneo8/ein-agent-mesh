"""Orchestrator role, PoC slice: own every incident, move it to ``investigating``.

Reads ``incidents.critical`` and ``incidents.normal``, keeps one ``InvestigationState`` per
``root_id`` in memory, checkpoints it to the compacted ``investigation.state`` topic, and records
``investigation.started`` on the event log. No contracts, no LLM, no questions yet: this is the
first transition after ``open`` and nothing more.
"""

from __future__ import annotations

import logging
from typing import ClassVar

from pydantic import ValidationError

from ein_mesh.bus import MessageBus, Record
from ein_mesh.config import Topics
from ein_mesh.roles.base import Role
from ein_mesh.schemas import IncidentMessage, InvestigationState

log = logging.getLogger(__name__)


class Orchestrator(Role):
    name: ClassVar[str] = "orchestrator"

    def __init__(self, bus: MessageBus, topics: Topics) -> None:
        super().__init__(bus, topics)
        self.state: dict[str, InvestigationState] = {}  # root_id -> state

    @property
    def inputs(self) -> list[str]:
        return [self.topics.incidents("critical"), self.topics.incidents("normal")]

    async def restore(self) -> None:
        """Rebuild the per-investigation table from the compacted state topic."""
        for r in await self.bus.read_to_end(self.topics.investigation_state):
            st = InvestigationState.model_validate(r.value)
            self.state[st.root_id] = st
        log.info("restored state: %d investigations", len(self.state))

    async def handle(self, records: list[Record]) -> bool:
        for r in records:
            try:
                inc = IncidentMessage.model_validate(r.value)
            except ValidationError as e:
                log.warning("bad incident at %s offset %s: %s", r.topic, r.offset, e)
                continue
            await self.on_incident(inc)
        return bool(records)

    async def _write_state(self, st: InvestigationState) -> None:
        self.state[st.root_id] = st
        await self.bus.produce(self.topics.investigation_state, st.root_id, st)

    async def on_incident(self, inc: IncidentMessage) -> None:
        if inc.root_id in self.state:
            log.info("incident %s already known (%s), skipping", inc.root_id, self.state[inc.root_id].status)
            return
        st = InvestigationState(root_id=inc.root_id, cluster=inc.cluster, priority=inc.priority,
                                title=inc.title, status="investigating", opened_at=inc.opened_at)
        await self._write_state(st)
        await self.event(inc.root_id, "investigation.started",
                         {"priority": inc.priority, "title": inc.title, "alerts": [a.id for a in inc.alerts]})
        log.info("investigating %s [%s] %s (%d alerts)", inc.root_id, inc.priority, inc.title, len(inc.alerts))
