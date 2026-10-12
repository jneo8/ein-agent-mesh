"""Loki."""

from __future__ import annotations

from typing import Any

from ein_utcp.source import HttpSource


class LokiSource(HttpSource):
    name = "loki"
    spec_version = "3"
    query_validator = "loki.format-query"

    def records(self, result: Any) -> list[dict] | None:
        data = result.get("data") if isinstance(result, dict) else None
        inner = data.get("result") if isinstance(data, dict) else data
        if isinstance(inner, list) and inner and all(isinstance(x, dict) for x in inner):
            return inner
        return super().records(result)
