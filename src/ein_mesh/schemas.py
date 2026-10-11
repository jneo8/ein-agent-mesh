"""The messages on the mesh: flow records (Alert, IncidentMessage, Task, Report), the two role states
(AlertsState, IncidentState) and the triage decision schema."""

from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

CONTEXT_MAX_BYTES = 16 * 1024
"""Upper bound on ``IncidentState.context`` in UTF-8 bytes. The field is a checkpoint of what the
orchestrator needs to resume, not a transcript."""

TASK_QUOTED_MAX_BYTES = 64 * 1024
"""Upper bound on ``Task.quoted_data`` as JSON. The orchestrator reduces alerts to id / alertname /
severity / namespace when the full records would exceed it."""

REPORT_SUMMARY_MAX_BYTES = 4 * 1024
REPORT_MAX_BYTES = 64 * 1024


def utf8_size(value) -> int:
    return len(json.dumps(value, ensure_ascii=False).encode("utf-8"))

How = Literal["llm", "rule", "fallback"]
"""Who made a triage decision: the model, a rule that never asked it, or the fallback after a bad answer."""


def now_iso() -> str:
    return datetime.now(UTC).isoformat()


def new_root_id(prefix: str = "inc") -> str:
    """Identity of one investigation, e.g. ``inc-20261010T074252Z-3f9a1c2b7d``. The UTC timestamp
    makes ids sort and read by time; the random tail keeps two incidents opened in the same second
    distinct. Every ``incidents.*`` topic uses it as the Kafka key."""
    return f"{prefix}-{datetime.now(UTC):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:10]}"


class Alert(BaseModel):
    """One deduplicated alert on the alerts topic. Key = cluster."""

    id: str = Field(description="fingerprint:startsAt, unique per alert instance")
    cluster: str
    namespace: str = ""
    alertname: str
    severity: str = "warning"
    status: Literal["firing", "resolved"] = "firing"
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    starts_at: str = ""
    group_key: str = Field(default="", description="Alertmanager groupKey when the webhook had one, else derived from GROUP_BY labels")
    group_labels: dict[str, str] = Field(default_factory=dict)

    @property
    def fingerprint(self) -> str:
        """The label-set hash Alertmanager assigns; stable across re-sends of the same alert."""
        return self.id.split(":", 1)[0]


class Attachment(BaseModel):
    """Alerts folded into an open incident after it was opened, and why. Kept on the incident inside
    ``alerts.state`` so the decision survives without a separate log."""

    alerts: list[str]
    how: How
    reason: str
    at: str = Field(default_factory=now_iso)


class Incident(BaseModel):
    """An open incident as triage knows it: the summary that goes back into the prompt, plus the
    decisions that built it (``how`` / ``reason`` for the opening, ``attached`` for later alerts)."""

    id: str
    cluster: str
    namespace: str = ""
    title: str
    priority: Literal["critical", "normal"]
    alert_ids: list[str] = Field(default_factory=list)
    alertnames: list[str] = Field(default_factory=list)
    opened_at: str = Field(default_factory=now_iso)
    group_key: str = ""
    how: How = "llm"
    reason: str = ""
    attached: list[Attachment] = Field(default_factory=list, description="most recent attachments; bounded")


class Attach(BaseModel):
    action: Literal["attach"]
    alerts: list[str]
    incident: str
    reason: str


class Open(BaseModel):
    action: Literal["open"]
    alerts: list[str]
    priority: Literal["critical", "normal"]
    title: str
    reason: str


class Ignore(BaseModel):
    action: Literal["ignore"]
    alerts: list[str]
    reason: str


Decision = Annotated[Attach | Open | Ignore, Field(discriminator="action")]


class TriageOutput(BaseModel):
    decisions: list[Decision]


class Ignored(BaseModel):
    """One fingerprint judged noise: the alert instances it covered, who decided and why. Re-sends refresh
    ``at``; entries older than the TTL are dropped at snapshot time."""

    alerts: list[str]
    how: How
    reason: str
    at: str = Field(default_factory=now_iso)


class AlertsState(BaseModel):
    """The memory of the ``alerts`` consumer (triage) on the compacted ``alerts.state`` topic, one record
    per cluster: open incidents with the decisions behind them, and the fingerprints judged noise."""

    schema_version: int = 1
    cluster: str
    incidents: list[Incident]
    ignored: dict[str, Ignored] = Field(default_factory=dict, description="fingerprint -> why it is ignored")


class IncidentMessage(BaseModel):
    """What goes on the incidents.<priority> topics for the orchestrator."""

    root_id: str
    cluster: str
    priority: Literal["critical", "normal"]
    title: str
    reason: str
    alerts: list[Alert]
    opened_at: str = Field(default_factory=now_iso)


class AlertRef(BaseModel):
    """Enough of an alert for the orchestrator to re-issue work after a restart without the full record."""

    id: str
    alertname: str
    severity: str
    namespace: str = ""


TaskStatus = Literal["issued", "done", "failed", "expired", "exhausted"]


class TaskRef(BaseModel):
    """One task as the orchestrator remembers it. ``done`` is the only final state: a late report still
    overrides ``expired`` or ``failed``. ``exhausted`` means no more attempts will be issued."""

    task_id: str
    parent_id: str | None = None
    attempt: int = 1
    role: str
    intent: str
    issued_at: str
    deadline: str
    status: TaskStatus = "issued"
    report_at: str | None = None


class IncidentState(BaseModel):
    """The memory of the ``incidents.*`` consumer (the orchestrator) on the compacted ``incidents.state``
    topic, one record per root_id. Because the orchestrator owns the lifecycle, this is also where an
    incident's lifecycle status lives; ``status`` / ``title`` / ``priority`` / ``updated_at`` are the
    fields a future reader may rely on, the rest is the orchestrator's own."""

    schema_version: int = 1
    root_id: str
    cluster: str
    priority: Literal["critical", "normal"]
    title: str
    status: Literal["investigating", "waiting", "closed"]
    opened_at: str
    updated_at: str = Field(default_factory=now_iso)
    alerts: list[AlertRef] = Field(default_factory=list, description="the incident's alerts, reduced")
    tasks: list[TaskRef] = Field(default_factory=list, description="every task issued for this incident, oldest first")
    context: str = Field(
        default="",
        description="Bounded summary the orchestrator rewrites every turn so a restart resumes from the "
        "state alone: confirmed facts, current hypothesis and plan, conclusions of past questions. "
        "Never a transcript.",
    )

    @field_validator("context")
    @classmethod
    def context_within_limit(cls, v: str) -> str:
        size = len(v.encode("utf-8"))
        if size > CONTEXT_MAX_BYTES:
            raise ValueError(f"context is {size} bytes, limit is {CONTEXT_MAX_BYTES}; summarise before writing state")
        return v


class Task(BaseModel):
    """Work the orchestrator hands to a worker role on ``incidents.tasks``. Immutable: a retry or a change
    of direction is a new task whose ``parent_id`` points at this one. Headers are what the runtime needs
    to route and time it; the body is what the role reads. ``quoted_data`` is data, never instructions."""

    schema_version: int = 1
    root_id: str
    task_id: str = Field(default_factory=lambda: new_root_id("task"))
    parent_id: str | None = None
    attempt: int = 1
    depth: int = 1
    role: str
    intent: str
    deadline: str = Field(description="ISO 8601 UTC; the role refuses work it receives after this")
    issued_at: str = Field(default_factory=now_iso)
    goal: str
    context: str = ""
    constraints: str = ""
    done_when: str = ""
    quoted_data: dict = Field(default_factory=dict)

    @field_validator("quoted_data")
    @classmethod
    def quoted_within_limit(cls, v: dict) -> dict:
        size = utf8_size(v)
        if size > TASK_QUOTED_MAX_BYTES:
            raise ValueError(f"quoted_data is {size} bytes, limit is {TASK_QUOTED_MAX_BYTES}; reduce the alerts")
        return v

    def render(self) -> str:
        """The body as the role's prompt. Quoted data is kept as one JSON block so a reader (or the eval
        harness) can parse it back."""
        return (
            f"## Goal\n{self.goal}\n\n## Context\n{self.context}\n\n## Constraints\n{self.constraints}\n\n"
            f"## Done when\n{self.done_when}\n\n## Quoted data\n{json.dumps(self.quoted_data, ensure_ascii=False)}"
        )


class Findings(BaseModel):
    """What an investigation produced: the shape a worker role's model must return."""

    summary: str = ""
    notes: list[str] = Field(default_factory=list, description="observations with the evidence behind them")
    identifiers: list[str] = Field(default_factory=list, description="'<name> - <what it locates>'")
    resources_checked: list[str] = Field(default_factory=list)

    @field_validator("summary")
    @classmethod
    def summary_within_limit(cls, v: str) -> str:
        size = len(v.encode("utf-8"))
        if size > REPORT_SUMMARY_MAX_BYTES:
            raise ValueError(f"summary is {size} bytes, limit is {REPORT_SUMMARY_MAX_BYTES}")
        return v


class Report(Findings):
    """A worker role's answer to one task, on ``incidents.reports``. ``done`` carries findings; ``failed``
    and ``expired`` carry ``error`` and no findings."""

    schema_version: int = 1
    task_id: str
    root_id: str
    role: str
    status: Literal["done", "failed", "expired"]
    error: str = ""
    at: str = Field(default_factory=now_iso)

    @model_validator(mode="after")
    def within_limit(self) -> Report:
        size = len(self.model_dump_json().encode("utf-8"))
        if size > REPORT_MAX_BYTES:
            raise ValueError(f"report is {size} bytes, limit is {REPORT_MAX_BYTES}; shorten notes")
        return self
