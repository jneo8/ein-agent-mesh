"""Alertmanager."""

from __future__ import annotations

from typing import Any

from ein_utcp.source import HttpSource


class AlertmanagerSource(HttpSource):
    name = "alertmanager"
    spec_version = "0.34.1"

    def records(self, result: Any) -> list[dict] | None:
        if isinstance(result, list) and result and all(isinstance(x, dict) for x in result):
            return result
        return super().records(result)
