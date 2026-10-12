"""The sources this package knows."""

from __future__ import annotations

from ein_utcp.source import HttpSource, Source, SourceConfig
from ein_utcp.sources.alertmanager import AlertmanagerSource
from ein_utcp.sources.loki import LokiSource
from ein_utcp.sources.prometheus import PrometheusSource

SOURCES: dict[str, type[HttpSource]] = {cls.name: cls for cls in (PrometheusSource, LokiSource, AlertmanagerSource)}


def make_source(config: SourceConfig, *, allow_http: bool = False) -> Source:
    """The source for a config, by name. Raises ``ValueError`` for an unknown name."""
    cls = SOURCES.get(config.name)
    if cls is None:
        raise ValueError(f"unknown source {config.name!r}; known: {', '.join(sorted(SOURCES))}")
    return cls(config, allow_http=allow_http)


__all__ = ["SOURCES", "AlertmanagerSource", "LokiSource", "PrometheusSource", "make_source"]
