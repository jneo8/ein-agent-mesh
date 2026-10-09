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
    prefix: str = field(default_factory=lambda: _env("TOPIC_PREFIX", "ops."))

    @property
    def triggers(self) -> str:
        return f"{self.prefix}triggers"

    @property
    def triage_state(self) -> str:
        return f"{self.prefix}triage.state"

    def incidents(self, priority: str) -> str:
        return f"{self.prefix}incidents.{priority}"

    @property
    def events(self) -> str:
        return f"{self.prefix}investigation.events"

    def all(self) -> dict[str, dict]:
        """Topic name -> extra config, for the init step."""
        return {
            self.triggers: {},
            self.triage_state: {"cleanup.policy": "compact"},
            self.incidents("critical"): {},
            self.incidents("normal"): {},
            self.events: {"retention.ms": str(30 * 24 * 3600 * 1000)},
        }


@dataclass(frozen=True)
class TriageSettings:
    model: str = field(default_factory=lambda: _env("EIN_AGENT_MODEL", "gemini/gemini-3.7-flash"))
    batch_size: int = field(default_factory=lambda: int(_env("TRIAGE_BATCH_SIZE", "50")))
    batch_seconds: float = field(default_factory=lambda: float(_env("TRIAGE_BATCH_SECONDS", "30")))
    llm_timeout: float = field(default_factory=lambda: float(_env("TRIAGE_LLM_TIMEOUT", "60")))
