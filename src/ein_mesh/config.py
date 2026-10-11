"""Process settings, all from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import ClassVar


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

    Two kinds of topic, nothing else. A *flow* topic carries one message type from one role to the
    next (``alerts``, ``incidents.critical`` / ``incidents.normal``, ``incidents.tasks``,
    ``incidents.reports``). A *state* topic is the memory of exactly one role, keyed by that role's
    unit of work and compacted, read back only by that role when it restarts: ``alerts.state`` is
    what the consumer of ``alerts`` (triage) remembers per cluster, ``incidents.state`` what the
    consumer of ``incidents.*`` (the orchestrator) remembers per root_id. A topic exists only when
    something consumes it.

    The Terraform data-integrator grants PREFIXED ACLs on ``TOPIC_PREFIX``, so an override that
    leaves the prefix needs its own ACL.

    ``TOPIC_PARTITIONS`` (default 12) is the partition count of every mesh topic. One number for all
    of them because the topics keyed by ``root_id`` must be co-partitioned: a record's partition is
    ``hash(key) % partitions``, so the instance holding an incident's state only receives its later
    records when every topic has the same count. The number is fixed once topics exist; changing it
    moves live keys to other partitions. ``TOPIC_REPLICATION`` (default 1) must not exceed the
    broker count.
    """

    prefix: str = field(default_factory=lambda: _env("TOPIC_PREFIX", "ein."))
    alerts: str = field(default_factory=lambda: _env("TOPIC_ALERTS"))
    alerts_state: str = field(default_factory=lambda: _env("TOPIC_ALERTS_STATE"))
    incidents_critical: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_CRITICAL"))
    incidents_normal: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_NORMAL"))
    incidents_state: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_STATE"))
    tasks: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_TASKS"))
    reports: str = field(default_factory=lambda: _env("TOPIC_INCIDENTS_REPORTS"))
    partitions: int = field(default_factory=lambda: int(_env("TOPIC_PARTITIONS", "12")))
    replication: int = field(default_factory=lambda: int(_env("TOPIC_REPLICATION", "1")))

    def __post_init__(self) -> None:
        if self.partitions < 1:
            raise ValueError(f"TOPIC_PARTITIONS must be >= 1, got {self.partitions}")
        for attr, suffix in self.SUFFIXES.items():
            if not getattr(self, attr):
                object.__setattr__(self, attr, f"{self.prefix}{suffix}")

    SUFFIXES: ClassVar[dict[str, str]] = {
        "alerts": "alerts",
        "alerts_state": "alerts.state",
        "incidents_critical": "incidents.critical",
        "incidents_normal": "incidents.normal",
        "incidents_state": "incidents.state",
        "tasks": "incidents.tasks",
        "reports": "incidents.reports",
    }

    def by_suffix(self) -> dict[str, str]:
        """Short name -> full topic name, e.g. ``incidents.critical`` -> ``ein.incidents.critical``."""
        return {suffix: getattr(self, attr) for attr, suffix in self.SUFFIXES.items()}

    def resolve(self, name: str) -> str:
        """A short name from ``by_suffix`` or a full topic name, returned as the full name."""
        return self.by_suffix().get(name, name)

    def incidents(self, priority: str) -> str:
        return {"critical": self.incidents_critical, "normal": self.incidents_normal}[priority]

    def all(self) -> dict[str, dict]:
        """Topic name -> extra config, for the init step. State topics are compacted; flow topics keep
        the broker's default retention."""
        return {
            self.alerts: {},
            self.alerts_state: {"cleanup.policy": "compact"},
            self.incidents_critical: {},
            self.incidents_normal: {},
            self.incidents_state: {"cleanup.policy": "compact"},
            self.tasks: {},
            self.reports: {},
        }

    def stale(self, existing: list[str]) -> list[str]:
        """Topics on the broker that carry this prefix but are not part of the layout: left over from
        an earlier topic model. ``ein-mesh reset --prune`` deletes them."""
        return sorted(t for t in existing if t.startswith(self.prefix) and t not in self.all())

    def config_drift(self, name: str, current: dict[str, str]) -> dict[str, tuple[str | None, str | None]]:
        """How a topic's dynamic config on the broker differs from ``all()[name]``. ``current`` is the
        topic-level config actually set (broker defaults excluded). Returns ``key -> (broker value,
        wanted value)`` with ``None`` for absent; empty when the topic already matches. Keys the
        broker has but this layout does not are drift too: init owns the whole topic config."""
        wanted = self.all()[name]
        drift: dict[str, tuple[str | None, str | None]] = {}
        for key in sorted(set(wanted) | set(current)):
            if wanted.get(key) != current.get(key):
                drift[key] = (current.get(key), wanted.get(key))
        return drift

    def layout_errors(self, counts: dict[str, int | None]) -> list[str]:
        """Why the broker's topics do not match this layout. ``counts`` maps each mesh topic to its
        partition count on the broker, ``None`` when it does not exist. Empty means everything is
        co-partitioned as configured; a non-empty result is a reason to refuse to start."""
        errors = []
        for name in self.all():
            n = counts.get(name)
            if n is None:
                errors.append(f"{name}: missing, run `ein-mesh init`")
            elif n != self.partitions:
                errors.append(f"{name}: {n} partitions, TOPIC_PARTITIONS is {self.partitions}; "
                              "topics must be co-partitioned, recreate the mesh topics or fix the setting")
        return errors


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
    # bounds on alerts.state: it is one record per cluster, rewritten whole on every change
    max_attachments: int = field(default_factory=lambda: int(_env("ALERTS_STATE_MAX_ATTACHMENTS", "20")))
    ignored_ttl_days: float = field(default_factory=lambda: float(_env("IGNORED_TTL_DAYS", "7")))
