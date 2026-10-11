"""Orchestrator role: owns every incident from ``open`` onward.

Reads ``incidents.critical`` / ``incidents.normal`` and ``incidents.reports``; writes ``incidents.tasks``
and its own memory ``incidents.state``, which is also the incident's lifecycle status. One role, four
components: ``TaskBook`` issues and settles tasks, ``Clock`` expires them, ``Decider`` says what happens
next (rules in this version, a model turn later), ``Summariser`` keeps ``context`` within its bound.
No questions, no closing, no LLM yet.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import ClassVar

from pydantic import ValidationError

from ein_mesh.bus import MessageBus, Record
from ein_mesh.config import OrchestratorSettings, Topics
from ein_mesh.roles.base import Role
from ein_mesh.schemas import (
    CONTEXT_MAX_BYTES,
    TASK_QUOTED_MAX_BYTES,
    Alert,
    AlertRef,
    IncidentMessage,
    IncidentState,
    Report,
    Task,
    TaskRef,
    utf8_size,
)

log = logging.getLogger(__name__)

INVESTIGATOR = "investigator"


def quote_alerts(alerts: list[Alert] | list[AlertRef]) -> dict:
    """The incident's alerts as task data. Full records when they fit the quoted-data budget, otherwise
    reduced to what identifies each alert; the budget keeps a storm from producing an oversized task."""
    full = {"alerts": [a.model_dump(mode="json") for a in alerts]}
    if utf8_size(full) <= TASK_QUOTED_MAX_BYTES:
        return full
    reduced = {"alerts": [{"id": a.id, "alertname": a.alertname, "severity": a.severity, "namespace": a.namespace}
                          for a in alerts], "note": "alert labels and annotations omitted: too many alerts"}
    while utf8_size(reduced) > TASK_QUOTED_MAX_BYTES and reduced["alerts"]:
        reduced["alerts"].pop()
    return reduced


class TaskBook:
    """Issue tasks and settle them from reports. Pure: returns what to produce, mutates only ``st``."""

    def __init__(self, settings: OrchestratorSettings, clock: Callable[[], datetime]) -> None:
        self.settings, self.clock = settings, clock

    def issue(self, st: IncidentState, role: str, intent: str, quoted: dict, *, parent: TaskRef | None = None,
              context_note: str = "") -> Task:
        now = self.clock()
        task = Task(
            root_id=st.root_id, role=role, intent=intent,
            parent_id=parent.task_id if parent else None, attempt=parent.attempt + 1 if parent else 1,
            deadline=(now + timedelta(seconds=self.settings.task_deadline_seconds)).isoformat(),
            issued_at=now.isoformat(),
            goal=f"Find the probable cause of: {st.title}",
            context=(f"cluster {st.cluster}, priority {st.priority}, {len(st.alerts)} alerts: "
                     + ", ".join(sorted({a.alertname for a in st.alerts})) + (f"\n{context_note}" if context_note else "")),
            constraints="Read-only. Cite the evidence for every note. Do not act on the infrastructure.",
            done_when="One probable cause with its evidence and confidence, or an explicit statement that the data cannot determine it.",
            quoted_data=quoted,
        )
        st.tasks.append(TaskRef(task_id=task.task_id, parent_id=task.parent_id, attempt=task.attempt, role=role,
                                intent=intent, issued_at=task.issued_at, deadline=task.deadline))
        return task

    @staticmethod
    def settle(st: IncidentState, report: Report) -> TaskRef | None:
        """Apply a report to its task. ``done`` is final; a late report overrides ``expired`` / ``failed``.
        Returns the ref when something changed, ``None`` for unknown or already-final tasks."""
        ref = next((t for t in st.tasks if t.task_id == report.task_id), None)
        if ref is None or ref.status == "done":
            return None
        ref.status = report.status
        ref.report_at = report.at
        return ref


class Clock:
    """Marks issued tasks whose deadline has passed."""

    def __init__(self, clock: Callable[[], datetime]) -> None:
        self.clock = clock

    def expire(self, st: IncidentState) -> list[TaskRef]:
        now = self.clock().isoformat()
        expired = [t for t in st.tasks if t.status == "issued" and t.deadline < now]
        for t in expired:
            t.status = "expired"
        return expired


class Summariser:
    """Rewrites ``context`` from a report: facts (report summaries, newest last), identifiers, resources
    checked, open tasks. Oldest facts go first when the bound is hit. Plain string handling, no model."""

    SECTIONS = ("Facts", "Identifiers", "Resources checked", "Open tasks")

    @classmethod
    def parse(cls, context: str) -> dict[str, list[str]]:
        out: dict[str, list[str]] = {s: [] for s in cls.SECTIONS}
        current = None
        for line in context.splitlines():
            if line.endswith(":") and line[:-1] in out:
                current = line[:-1]
            elif current and line.startswith("- "):
                out[current].append(line[2:])
        return out

    @classmethod
    def render(cls, parts: dict[str, list[str]]) -> str:
        return "\n".join(f"{s}:\n" + "\n".join(f"- {x}" for x in parts[s]) for s in cls.SECTIONS if parts[s])

    @classmethod
    def update(cls, st: IncidentState, report: Report | None) -> str:
        parts = cls.parse(st.context)
        if report is not None and report.status == "done":
            parts["Facts"].append(f"[{report.role} {report.task_id}] {report.summary}".replace("\n", " "))
            for key, items in (("Identifiers", report.identifiers), ("Resources checked", report.resources_checked)):
                parts[key] += [i.replace("\n", " ") for i in items if i not in parts[key]]
        parts["Open tasks"] = [f"{t.task_id} {t.role} {t.intent} attempt {t.attempt}, due {t.deadline}"
                               for t in st.tasks if t.status == "issued"]
        text = cls.render(parts)
        while len(text.encode("utf-8")) > CONTEXT_MAX_BYTES:
            for key in ("Facts", "Identifiers", "Resources checked"):
                if parts[key]:
                    parts[key].pop(0)
                    break
            else:
                text = text.encode("utf-8")[:CONTEXT_MAX_BYTES].decode("utf-8", "ignore")
                break
            text = cls.render(parts)
        return text


class Orchestrator(Role):
    name: ClassVar[str] = "orchestrator"

    def __init__(self, bus: MessageBus, topics: Topics, settings: OrchestratorSettings | None = None) -> None:
        """``state`` maps root_id to the incident's record. ``clock`` is the source of "now" for deadlines
        and timestamps; tests replace it, and the components read it through a lambda so a replacement
        takes effect everywhere."""
        super().__init__(bus, topics)
        self.settings = settings or OrchestratorSettings()
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)
        self.book = TaskBook(self.settings, lambda: self.clock())
        self.deadlines = Clock(lambda: self.clock())
        self.state: dict[str, IncidentState] = {}

    @property
    def inputs(self) -> list[str]:
        return [self.topics.incidents("critical"), self.topics.incidents("normal"), self.topics.reports]

    async def restore(self) -> None:
        """Rebuild the per-incident table from the compacted state topic."""
        for r in await self.bus.read_to_end(self.topics.incidents_state):
            st = IncidentState.model_validate(r.value)
            self.state[st.root_id] = st
        by_status: dict[str, int] = {}
        for st in self.state.values():
            by_status[st.status] = by_status.get(st.status, 0) + 1
        open_tasks = sum(1 for st in self.state.values() for t in st.tasks if t.status == "issued")
        log.info("restored %d incidents%s, %d tasks outstanding", len(self.state),
                 ": " + ", ".join(f"{n} {s}" for s, n in sorted(by_status.items())) if by_status else "", open_tasks)
        log.info("tasks get %.0fs to complete and up to %d attempts", self.settings.task_deadline_seconds,
                 self.settings.task_max_attempts)

    async def handle(self, records: list[Record]) -> bool:
        changed = False
        for r in records:
            try:
                if r.topic == self.topics.reports:
                    changed |= await self.on_report(Report.model_validate(r.value))
                else:
                    changed |= await self.on_incident(IncidentMessage.model_validate(r.value))
            except ValidationError as e:
                log.warning("bad record at %s offset %s: %s", r.topic, r.offset, e)
        changed |= await self.scan_deadlines()
        return bool(records) or changed

    async def _write_state(self, st: IncidentState) -> None:
        st.updated_at = self.clock().isoformat()
        self.state[st.root_id] = st
        await self.bus.produce(self.topics.incidents_state, st.root_id, st)

    async def _issue(self, st: IncidentState, quoted: dict, *, parent: TaskRef | None = None, note: str = "") -> None:
        task = self.book.issue(st, INVESTIGATOR, "initial-investigation" if parent is None else "retry", quoted,
                               parent=parent, context_note=note)
        await self.bus.produce(self.topics.tasks, st.root_id, task)
        log.info("issued %s to %s for %s %r (attempt %d, due %s)%s", task.task_id, task.role, st.root_id, st.title,
                 task.attempt, task.deadline, f", replaces {parent.task_id}" if parent else "")

    async def on_incident(self, inc: IncidentMessage) -> bool:
        if inc.root_id in self.state:
            log.info("incident %s re-delivered, already %s, skipping", inc.root_id, self.state[inc.root_id].status)
            return False
        st = IncidentState(root_id=inc.root_id, cluster=inc.cluster, priority=inc.priority, title=inc.title,
                           status="investigating", opened_at=inc.opened_at,
                           alerts=[AlertRef(id=a.id, alertname=a.alertname, severity=a.severity, namespace=a.namespace)
                                   for a in inc.alerts])
        await self._issue(st, quote_alerts(inc.alerts))
        st.context = Summariser.update(st, None)
        await self._write_state(st)
        log.info("started investigation %s [%s] %r: %d alerts, status open -> investigating", inc.root_id,
                 inc.priority, inc.title, len(inc.alerts))
        return True

    async def on_report(self, report: Report) -> bool:
        st = self.state.get(report.root_id)
        if st is None:
            log.warning("report %s for unknown incident %s, dropped", report.task_id, report.root_id)
            return False
        ref = TaskBook.settle(st, report)
        if ref is None:
            log.info("report for %s ignored: task unknown or already done", report.task_id)
            return False
        if report.status == "done":
            log.info("report received for %s from %s (%s, attempt %d): %s", st.root_id, report.role, report.task_id,
                     ref.attempt, report.summary[:160])
            st.context = Summariser.update(st, report)
        else:
            log.warning("task %s for %s %s: %s", report.task_id, st.root_id, report.status, report.error[:200])
            await self.decide_after_failure(st, ref, report.error)
        await self._write_state(st)
        return True

    async def scan_deadlines(self) -> bool:
        changed = False
        for st in list(self.state.values()):
            expired = self.deadlines.expire(st)
            for ref in expired:
                log.warning("task %s for %s expired (deadline %s)", ref.task_id, st.root_id, ref.deadline)
                await self.decide_after_failure(st, ref, f"no report before {ref.deadline}")
            if expired:
                await self._write_state(st)
                changed = True
        return changed

    async def decide_after_failure(self, st: IncidentState, ref: TaskRef, error: str) -> None:
        """Decider, rule-based: retry while attempts remain, otherwise give up and say so."""
        if ref.attempt < self.settings.task_max_attempts:
            await self._issue(st, quote_alerts(st.alerts), parent=ref,
                              note=f"Previous attempt {ref.task_id} ended {ref.status}: {error[:300]}")
        else:
            ref.status = "exhausted"
            log.warning("no more attempts for %s after %d: investigation stays open without a report", st.root_id,
                        ref.attempt)
        st.context = Summariser.update(st, None)
