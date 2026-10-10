"""Canonical events on the mesh and the triage decision schema."""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field, field_validator

CONTEXT_MAX_BYTES = 16 * 1024
"""Upper bound on ``InvestigationState.context`` in UTF-8 bytes. The field is a checkpoint of what
the orchestrator needs to resume, not a transcript; the raw history stays on investigation.events."""


def new_root_id(prefix: str = "inc") -> str:
    """Identity of one investigation, e.g. ``inc-20261010T074252Z-3f9a1c2b7d``. The UTC timestamp
    makes ids sort and read by time; the random tail keeps two incidents opened in the same second
    distinct. Every lifecycle topic uses it as the Kafka key."""
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


class Incident(BaseModel):
    """An open investigation as triage knows it; the summary sent back into the prompt."""

    id: str
    cluster: str
    namespace: str = ""
    title: str
    priority: Literal["critical", "normal"]
    alert_ids: list[str] = Field(default_factory=list)
    alertnames: list[str] = Field(default_factory=list)
    opened_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    group_key: str = ""


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


class TriageState(BaseModel):
    """Compacted snapshot on the triage.state topic, one record per cluster."""

    cluster: str
    incidents: list[Incident]
    ignored: list[str] = Field(default_factory=list, description="fingerprints the model told us to ignore; re-sends are ignored by rule")


class IncidentMessage(BaseModel):
    """What goes on the incidents.<priority> topics for the orchestrator."""

    root_id: str
    cluster: str
    priority: Literal["critical", "normal"]
    title: str
    reason: str
    alerts: list[Alert]
    opened_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


class InvestigationState(BaseModel):
    """Compacted snapshot on the investigation.state topic, one record per root_id. Owned by the
    orchestrator; the outward-facing picture of where an investigation is."""

    root_id: str
    cluster: str
    priority: Literal["critical", "normal"]
    title: str
    status: Literal["investigating", "waiting", "closed"]
    opened_at: str
    updated_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    context: str = Field(
        default="",
        description="Bounded summary the orchestrator rewrites every turn so a restart resumes without "
        "replaying events: confirmed facts, current hypothesis and plan, conclusions of past "
        "questions. Never the transcript.",
    )

    @field_validator("context")
    @classmethod
    def context_within_limit(cls, v: str) -> str:
        size = len(v.encode("utf-8"))
        if size > CONTEXT_MAX_BYTES:
            raise ValueError(f"context is {size} bytes, limit is {CONTEXT_MAX_BYTES}; summarise before writing state")
        return v


class Event(BaseModel):
    """Audit record on the investigation.events topic."""

    root_id: str
    role: str = "triage"
    kind: str
    at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    body: dict
