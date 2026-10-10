"""Process settings, all from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default)


@dataclass(frozen=True)
class KafkaSettings:
    bootstrap: str = field(default_factory=lambda: _env("KAFKA_BOOTSTRAP", "127.0.0.1:9092"))
    username: str = field(default_factory=lambda: _env("KAFKA_USERNAME"))
    password: str = field(default_factory=lambda: _env("KAFKA_PASSWORD"))
    group_prefix: str = field(default_factory=lambda: _env("KAFKA_GROUP_PREFIX", "ein-"))

    def client_kwargs(self) -> dict:
        kw: dict = {"bootstrap_servers": self.bootstrap}
        if self.username:
            kw.update(
                security_protocol="SASL_PLAINTEXT",
                sasl_mechanism="SCRAM-SHA-512",
                sasl_plain_username=self.username,
                sasl_plain_password=self.password,
            )
        return kw


@dataclass(frozen=True)
class Topics:
    """Topic names. ``TOPIC_PREFIX`` sets the family; ``TOPIC_<NAME>`` overrides one topic outright.

    The Terraform data-integrator grants PREFIXED ACLs on ``TOPIC_PREFIX``, so an override that
    leaves the prefix needs its own ACL.
    """

    prefix: str = field(default_factory=lambda: _env("TOPIC_PREFIX", "ein."))
    alerts: str = field(default_factory=lambda: _env("TOPIC_ALERTS"))
    triage_state: str = field(default_factory=lambda: _env("TOPIC_TRIAGE_STATE"))
    incidents_critical: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_CRITICAL"))
    incidents_normal: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_NORMAL"))
    events: str = field(default_factory=lambda: _env("TOPIC_EVENTS"))
    investigation_state: str = field(default_factory=lambda: _env("TOPIC_INVESTIGATION_STATE"))

    def __post_init__(self) -> None:
        defaults = {
            "alerts": "alerts",
            "triage_state": "triage.state",
            "incidents_critical": "incidents.critical",
            "incidents_normal": "incidents.normal",
            "events": "investigation.events",
            "investigation_state": "investigation.state",
        }
        for attr, suffix in defaults.items():
            if not getattr(self, attr):
                object.__setattr__(self, attr, f"{self.prefix}{suffix}")

    def by_suffix(self) -> dict[str, str]:
        """Short name -> full topic name, e.g. ``incidents.critical`` -> ``ein.incidents.critical``."""
        return {
            "alerts": self.alerts,
            "triage.state": self.triage_state,
            "incidents.critical": self.incidents_critical,
            "incidents.normal": self.incidents_normal,
            "investigation.events": self.events,
            "investigation.state": self.investigation_state,
        }

    def resolve(self, name: str) -> str:
        """A short name from ``by_suffix`` or a full topic name, returned as the full name."""
        return self.by_suffix().get(name, name)

    def incidents(self, priority: str) -> str:
        return {"critical": self.incidents_critical, "normal": self.incidents_normal}[priority]

    def all(self) -> dict[str, dict]:
        """Topic name -> extra config, for the init step."""
        return {
            self.alerts: {},
            self.triage_state: {"cleanup.policy": "compact"},
            self.incidents_critical: {},
            self.incidents_normal: {},
            self.events: {"retention.ms": str(30 * 24 * 3600 * 1000)},
            self.investigation_state: {"cleanup.policy": "compact"},
        }


@dataclass(frozen=True)
class GroupingSettings:
    """How the gateway derives an alert's group key when the webhook does not carry one."""

    group_by: tuple[str, ...] = field(
        default_factory=lambda: tuple(k.strip() for k in _env("GROUP_BY", "cluster,namespace").split(",") if k.strip()))


@dataclass(frozen=True)
class TriageSettings:
    model: str = field(default_factory=lambda: _env("EIN_AGENT_MODEL", "gemini/gemini-3.7-flash"))
    batch_size: int = field(default_factory=lambda: int(_env("TRIAGE_BATCH_SIZE", "50")))
    batch_seconds: float = field(default_factory=lambda: float(_env("TRIAGE_BATCH_SECONDS", "30")))
    llm_timeout: float = field(default_factory=lambda: float(_env("TRIAGE_LLM_TIMEOUT", "120")))
