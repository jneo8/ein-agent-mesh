"""Integrators bridge an external system onto a topic and make no decisions: the gateway
(Alertmanager webhook in) and tail (stdout out). Roles live in ``ein_mesh.roles``."""

from ein_mesh.integrators.gateway import derive_group, serve, to_alert
from ein_mesh.integrators.tail import tail

__all__ = ["derive_group", "serve", "tail", "to_alert"]
