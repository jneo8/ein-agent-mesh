"""What a source is: one system to query, owning how its calls are built, sent and read."""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Any, ClassVar
from urllib.parse import urlparse

import yaml
from utcp_http.openapi_converter import OpenApiConverter

from ein_utcp.query import (
    HttpDispatch,
    allow_plain_http_for_url,
    install_plain_http_override,
    is_loopback_host,
    reject_invalid_query,
    validate_arguments,
)
from ein_utcp.results import find_records
from ein_utcp.tools import Operation, build_catalog, filter_readonly_operations, resolve_server_url

log = logging.getLogger(__name__)

SPECS_DIR = Path(__file__).parent / "specs"


@dataclass(frozen=True)
class SourceConfig:
    """How to reach one source: its name, the base URL of its API, and an optional bearer token."""

    name: str
    url: str
    token: str = ""
    version: str = ""  # which shipped spec; empty means the source's default

    def __post_init__(self) -> None:
        parsed = urlparse(self.url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            raise ValueError(f"{self.name}: url must be http(s)://host[:port][/prefix], got {self.url!r}")

    @property
    def is_plain_http_remote(self) -> bool:
        parsed = urlparse(self.url)
        return parsed.scheme == "http" and not is_loopback_host(parsed.hostname or "")

    @property
    def headers(self) -> dict[str, str] | None:
        return {"Authorization": f"Bearer {self.token}"} if self.token else None


class Source(ABC):
    """One queryable system. Subclasses set ``name`` and implement the stages."""

    name: ClassVar[str]

    def __init__(self) -> None:
        self._operations: dict[str, Operation] | None = None
        self._catalog: list[str] | None = None

    @abstractmethod
    def load_operations(self) -> dict[str, Operation]:
        """Every operation the model may call, keyed by full name (``<source>.<operation>``)."""

    def operations(self) -> dict[str, Operation]:
        if self._operations is None:
            self._operations = self.load_operations()
            self._catalog = build_catalog(list(self._operations.values()))
        return self._operations

    def catalog(self) -> list[str]:
        self.operations()
        return list(self._catalog or [])

    def operation(self, name: str) -> Operation | None:
        return self.operations().get(name)

    async def preflight(self, op: Operation, arguments: dict[str, Any]) -> dict[str, Any] | None:
        """An error payload that stops the call, or None to proceed. The default checks parameter names."""
        return validate_arguments(op.name, arguments, op.schema)

    @abstractmethod
    async def dispatch(self, op: Operation, arguments: dict[str, Any]) -> Any:
        """Perform the call and return the decoded body; may raise."""

    def decode(self, result: Any) -> Any:
        """Shape a result before it is snapshotted. The default leaves it alone."""
        return result

    def records(self, result: Any) -> list[dict] | None:
        """Where the record list sits in a result; None when there is none. The default looks for the
        largest list of dicts within reach."""
        return find_records(result)


class HttpSource(Source):
    """A shipped OpenAPI spec, loaded GET-only, dispatched over utcp-http.

    Specs live under ``specs/<name>/<version>.yaml``. A subclass sets ``name`` and ``spec_version`` (the
    default when the config names none) and may set ``query_validator`` (an operation that validates a
    ``query`` argument with the server's own parser).
    """

    spec_version: ClassVar[str]
    query_validator: ClassVar[str | None] = None

    def __init__(self, config: SourceConfig, *, allow_http: bool = False) -> None:
        super().__init__()
        if config.name != self.name:
            raise ValueError(f"config is for {config.name!r}, source is {self.name!r}")
        if config.is_plain_http_remote:
            if not allow_http:
                raise ValueError(f"{self.name}: {config.url} is plain HTTP to a non-loopback host; "
                                 "allow_http must be set to accept it")
            if not install_plain_http_override():
                raise ValueError(f"{self.name}: plain HTTP cannot be allowed on this utcp-http version")
            allow_plain_http_for_url(config.url)
        self.config = config
        self._dispatch = HttpDispatch()

    @property
    def version(self) -> str:
        return self.config.version or self.spec_version

    def spec_path(self) -> Path:
        return SPECS_DIR / self.name / f"{self.version}.yaml"

    def load_spec(self) -> dict:
        path = self.spec_path()
        try:
            text = path.read_text()
            return yaml.safe_load(text) if path.suffix in (".yaml", ".yml") else json.loads(text)
        except (OSError, json.JSONDecodeError, yaml.YAMLError) as e:
            raise ValueError(f"{self.name}: cannot read spec {path}: {e}") from e

    def prepare_spec(self, spec: dict) -> dict:
        """What the spec looks like before conversion. The default keeps GET only."""
        return filter_readonly_operations(spec, self.name)

    def load_operations(self) -> dict[str, Operation]:
        spec = self.prepare_spec(self.load_spec())
        base_url = resolve_server_url(spec, self.config.url)
        try:
            manual = OpenApiConverter(spec, spec_url=f"file://{self.spec_path()}", call_template_name=self.name,
                                      base_url=base_url).convert()
        except Exception as e:
            raise ValueError(f"{self.name}: cannot convert spec {self.spec_path()}: {e}") from e
        operations: dict[str, Operation] = {}
        for tool in manual.tools:
            name = tool.name if tool.name.startswith(f"{self.name}.") else f"{self.name}.{tool.name}"
            if self.config.headers:
                tool.tool_call_template.headers = dict(self.config.headers)  # the converter never fills headers
            inputs = tool.inputs
            operations[name] = Operation(
                name=name, path=urlparse(tool.tool_call_template.url).path, description=tool.description or "",
                properties=inputs.properties or None,
                schema=inputs.model_dump(exclude_none=True, by_alias=True) if inputs.properties else None,
                transport=tool.tool_call_template)
        log.info("%s: %d read-only operations from spec %s at %s", self.name, len(operations), self.version, base_url)
        return operations

    def http_methods(self) -> set[str]:
        """Every HTTP method among the operations; the read-only guarantee is ``{"GET"}``."""
        return {op.transport.http_method for op in self.operations().values()}

    async def preflight(self, op: Operation, arguments: dict[str, Any]) -> dict[str, Any] | None:
        invalid = validate_arguments(op.name, arguments, op.schema)
        if invalid is not None or self.query_validator is None:
            return invalid
        return await reject_invalid_query(self._dispatch_by_name, self.query_validator, op.name, arguments)

    async def dispatch(self, op: Operation, arguments: dict[str, Any]) -> Any:
        return await self._dispatch(op, arguments)

    async def _dispatch_by_name(self, name: str, arguments: dict[str, Any]) -> Any:
        op = self.operation(name)
        if op is None:
            raise LookupError(f"{self.name}: no operation {name!r}")
        return await self._dispatch(op, arguments)
