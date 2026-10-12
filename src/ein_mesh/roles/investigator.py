"""Investigator role: one task in, one report out."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import Counter
from collections.abc import Callable
from datetime import UTC, datetime
from string import Template
from typing import ClassVar

from pydantic import ValidationError

from ein_mesh.bus import MessageBus, Record
from ein_mesh.config import InvestigatorSettings, Topics, ToolSettings
from ein_mesh.prompts import load_prompt
from ein_mesh.roles.base import Role
from ein_mesh.roles.toolloop import SUBMIT, ToolLoop
from ein_mesh.schemas import REPORT_MAX_BYTES, REPORT_SUMMARY_MAX_BYTES, Findings, Report, Task
from ein_utcp import Toolbox

log = logging.getLogger(__name__)

def system_prompt(template: str, toolbox: Toolbox) -> str:
    """The role's prompt with the toolbox's own guide and catalogs filled in."""
    return Template(template).safe_substitute(submit=SUBMIT, tools=toolbox.guide())


class Investigator(Role):
    """A pure function over ``incidents.tasks``: no memory, no state, no delegation. It refuses work that
    arrived too late or too deep, otherwise lets the model investigate with read-only tools and returns
    structured ``Findings``; what was actually fetched goes into the report as its evidence. Offsets commit
    after the report is produced, so a crash replays the task and the orchestrator deduplicates by
    ``task_id``."""

    name: ClassVar[str] = "investigator"

    def __init__(self, bus: MessageBus, topics: Topics, settings: InvestigatorSettings,
                 toolbox: Toolbox | None = None) -> None:
        """``clock`` is the source of "now" for deadline checks; tests replace it. ``toolbox`` is built from
        ``ToolSettings`` at ``restore`` when not given. The prompt comes from ``settings.prompt`` or the
        shipped file."""
        super().__init__(bus, topics)
        self.settings = settings
        self.toolbox = toolbox
        self.prompt = load_prompt("investigator", settings.prompt)
        self.clock: Callable[[], datetime] = lambda: datetime.now(UTC)

    @property
    def inputs(self) -> list[str]:
        return [self.topics.tasks]

    def consumer_options(self) -> dict:
        """One task can take as long as the model timeout; the group must not consider us dead before."""
        return {"max_poll_interval_ms": int((self.settings.timeout + 60) * 1000)}

    def poll_max_records(self) -> int | None:
        return 1

    async def restore(self) -> None:
        if self.toolbox is None:
            tools = ToolSettings()
            self.toolbox = Toolbox.from_configs(tools.sources(), limits=tools.limits(), allow_http=tools.allow_http)
        if self.toolbox.sources:
            counts = ", ".join(f"{s} {len(self.toolbox.catalog(s))} operations" for s in self.toolbox.sources)
            log.info("tools: %s", counts)
        else:
            log.info("no tools configured: reasoning over quoted data only")
        log.info("model=%s, task timeout %.0fs (%.0fs of it for looking), max depth %d, %d tokens per task; "
                 "one task at a time", self.settings.model, self.settings.timeout, self.settings.investigating_time,
                 self.settings.max_depth, self.settings.token_budget)

    async def handle(self, records: list[Record]) -> bool:
        for r in records:
            try:
                task = Task.model_validate(r.value)
            except ValidationError as e:
                log.warning("bad task at offset %s: %s", r.offset, e)
                continue
            if task.role != self.name:
                log.info("task %s is for role %s, not for me, skipping", task.task_id, task.role)
                continue
            report = await self.run_task(task)
            await self.bus.produce(self.topics.reports, task.root_id, report)
        return bool(records)

    async def run_task(self, task: Task) -> Report:
        """Refuse what is too deep or too late without a model call; otherwise investigate, and turn any
        failure into a ``failed`` report so the orchestrator always hears back."""
        now = self.clock().isoformat()
        header = dict(task_id=task.task_id, root_id=task.root_id, role=self.name)
        if task.depth > self.settings.max_depth:
            log.warning("rejected %s for %s: depth %d exceeds %d", task.task_id, task.root_id, task.depth,
                        self.settings.max_depth)
            return Report(**header, status="failed", error=f"depth {task.depth} exceeds {self.settings.max_depth}")
        if task.deadline < now:
            log.warning("rejected %s for %s: deadline %s already passed", task.task_id, task.root_id, task.deadline)
            return Report(**header, status="expired", error=f"deadline {task.deadline} passed before the task was picked up")
        log.info("accepted %s for %s (%s, attempt %d): %r", task.task_id, task.root_id, task.intent, task.attempt,
                 task.goal[:100])
        t0 = time.monotonic()
        try:
            findings = await asyncio.wait_for(self.investigate(task), timeout=self.settings.timeout)
        except Exception as e:
            log.warning("task %s for %s failed after %.1fs: %s: %s", task.task_id, task.root_id,
                        time.monotonic() - t0, type(e).__name__, e)
            return Report(**header, status="failed", error=f"{type(e).__name__}: {e}"[:2000])
        calls = self.toolbox.calls_made() if self.toolbox else []
        log.info("report for %s (%s): %s | %d notes, %d identifiers, %d tool calls (%s) in %.1fs", task.root_id,
                 task.task_id, findings.summary[:120], len(findings.notes), len(findings.identifiers), len(calls),
                 _call_summary(calls), time.monotonic() - t0)
        return Report(**header, status="done", **findings.model_dump())

    async def investigate(self, task: Task) -> Findings:
        """The model's conversation with the tools over the task body, fitted into a report's bounds."""
        if self.toolbox is None:
            self.toolbox = Toolbox([])
        self.toolbox.reset()
        loop = ToolLoop(self.settings.model, self.toolbox, Findings, token_budget=self.settings.token_budget,
                        time_budget=self.settings.investigating_time)
        findings = await loop.run(system_prompt(self.prompt, self.toolbox), task.render())
        return fit(findings, self.toolbox.calls_made())


def fit(findings: Findings, calls: list[str]) -> Findings:
    """What was actually fetched leads ``resources_checked``; then the findings are trimmed from the oldest
    note until a report built from them fits its size limit."""
    checked = list(dict.fromkeys(calls + [r for r in findings.resources_checked if r not in calls]))
    summary = findings.summary.encode()[:REPORT_SUMMARY_MAX_BYTES].decode(errors="ignore")
    notes, identifiers = list(findings.notes), list(findings.identifiers)
    while True:
        candidate = Findings(summary=summary, notes=notes, identifiers=identifiers, resources_checked=checked)
        if len(candidate.model_dump_json().encode()) <= REPORT_MAX_BYTES - 1024 or not (notes or identifiers):
            return candidate
        if len(notes) >= len(identifiers):
            notes.pop(0)
        else:
            identifiers.pop(0)


def _call_summary(calls: list[str]) -> str:
    counts = Counter(c.split(" ", 1)[0] for c in calls)
    return ", ".join(f"{op} x{n}" if n > 1 else op for op, n in counts.items()) or "none"
