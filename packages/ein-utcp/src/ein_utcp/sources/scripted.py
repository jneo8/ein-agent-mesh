"""A source without a network, for tests and evals."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from ein_utcp.source import Source
from ein_utcp.tools import Operation, parse_catalog_line


class ScriptedSource(Source):
    """``catalog`` is the source's catalog lines; ``responses`` is a list of ``{operation, arguments?, result}``.
    A response matches on operation (short or full name), then on the subset of arguments it names; the
    first match wins. With none, ``default`` is returned, or ``LookupError`` is raised when there is no
    default. Every dispatch is recorded in ``calls``."""

    def __init__(self, name: str, catalog: list[str], responses: list[dict[str, Any]] | None = None, *,
                 default: Any = None) -> None:
        super().__init__()
        self.name = name  # instance attribute: a scripted source is named by its fixture
        self._lines = list(catalog)
        self.responses = list(responses or [])
        self.default = default
        self.calls: list[tuple[str, dict[str, Any]]] = []

    @classmethod
    def from_json(cls, path: Path) -> list[ScriptedSource]:
        """``{"<source>": {"catalog": [...], "responses": [...], "default": ...}, ...}``; a missing file is no source."""
        if not path.exists():
            return []
        data = json.loads(path.read_text())
        return [cls(name, spec.get("catalog", []), spec.get("responses", []), default=spec.get("default"))
                for name, spec in data.items()]

    def load_operations(self) -> dict[str, Operation]:
        return {op.name: op for op in (parse_catalog_line(line) for line in self._lines)}

    async def dispatch(self, op: Operation, arguments: dict[str, Any]) -> Any:
        self.calls.append((op.name, dict(arguments)))
        short = op.name.removeprefix(f"{self.name}.")
        for response in self.responses:
            if response.get("operation") not in (op.name, short):
                continue
            wanted = response.get("arguments") or {}
            if all(arguments.get(k) == v for k, v in wanted.items()):
                return response.get("result")
        if self.default is not None:
            return self.default
        raise LookupError(f"no scripted response for {op.name} {arguments}")
