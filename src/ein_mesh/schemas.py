"""Canonical events on the mesh and the triage decision schema."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Annotated, Literal

from pydantic import BaseModel, Field


class Trigger(BaseModel):
    """One deduplicated alert on ``ops.triggers``. Key = cluster."""

    id: str = Field(description="fingerprint:startsAt, unique per alert instance")
    cluster: str
    namespace: str = ""
    alertname: str
    severity: str = "warning"
    status: Literal["firing", "resolved"] = "firing"
    labels: dict[str, str] = Field(default_factory=dict)
    annotations: dict[str, str] = Field(default_factory=dict)
    starts_at: str = ""


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


class Merge(BaseModel):
    action: Literal["merge"]
    from_: str = Field(alias="from")
    into: str
    reason: str

    model_config = {"populate_by_name": True}


class Ignore(BaseModel):
    action: Literal["ignore"]
    alerts: list[str]
    reason: str


Decision = Annotated[Attach | Open | Merge | Ignore, Field(discriminator="action")]


class TriageOutput(BaseModel):
    decisions: list[Decision]


class TriageState(BaseModel):
    """Compacted snapshot on ``ops.triage.state``, one record per cluster."""

    cluster: str
    incidents: list[Incident]


class IncidentMessage(BaseModel):
    """What goes on ``ops.incidents.<priority>`` for the orchestrator."""

    root_id: str
    cluster: str
    priority: Literal["critical", "normal"]
    title: str
    reason: str
    alerts: list[Trigger]
    opened_at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())


class Event(BaseModel):
    """Audit record on ``ops.investigation.events``."""

    root_id: str
    role: str = "triage"
    kind: str
    at: str = Field(default_factory=lambda: datetime.now(UTC).isoformat())
    body: dict
