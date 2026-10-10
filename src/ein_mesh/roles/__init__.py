"""Roles: each owns one transition of the incident lifecycle. All are ``Role`` subclasses on a
``MessageBus``; see ``base.py``. Integrators (gateway) live outside this package."""

from ein_mesh.roles.base import Role
from ein_mesh.roles.orchestrator import Orchestrator
from ein_mesh.roles.triage import Triage

__all__ = ["Orchestrator", "Role", "Triage"]
