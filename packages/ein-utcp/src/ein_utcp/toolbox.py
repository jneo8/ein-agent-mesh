"""What the model holds: its tools, over the configured sources."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ein_utcp.query import arguments_from, coerce_arguments, describe_failure
from ein_utcp.results import (
    CACHE_MAX_BYTES,
    DIGEST_MAX_BYTES,
    RESULT_MAX_BYTES,
    SNAPSHOT_REFRESHED_BANNER,
    CallRecord,
    Register,
    Snapshot,
    call_fingerprint,
)
from ein_utcp.source import Source, SourceConfig
from ein_utcp.tools import Operation, grep_catalog, tool_specs

log = logging.getLogger(__name__)


class ToolError(Exception):
    """A failure the model should read; ``call`` turns it into the error payload."""

    def __init__(self, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.payload = {"error": message, **extra}


@dataclass(frozen=True)
class Limits:
    """Sizes and timeouts enforced per task."""

    call_timeout: float = 60.0
    digest_max_bytes: int = DIGEST_MAX_BYTES
    result_max_bytes: int = RESULT_MAX_BYTES
    cache_max_bytes: int = CACHE_MAX_BYTES


class Toolbox:
    """The tools the model calls, over the given sources; ``call`` routes by name and never raises.

    For ``call_operation`` the source's steps run in order: the operation is looked up, the arguments are
    coerced and checked (``source.preflight``), a repeat of a registered call is refused in favour of its
    handle, the call is dispatched under the call timeout (``source.dispatch``), and the decoded result is
    snapshotted under a handle and returned as a digest. The other tools look into registered snapshots.
    Every failure is an error payload the model can read. ``reset`` starts a new task.
    """

    def __init__(self, sources: list[Source], limits: Limits | None = None) -> None:
        self.limits = limits or Limits()
        self._sources = {s.name: s for s in sources}
        self.reset()

    @classmethod
    def from_configs(cls, configs: list[SourceConfig], *, limits: Limits | None = None,
                     allow_http: bool = False) -> Toolbox:
        """The real thing: one known source per config (see ``ein_utcp.sources``)."""
        from ein_utcp.sources import make_source

        return cls([make_source(c, allow_http=allow_http) for c in configs], limits)

    @classmethod
    def scripted(cls, path: Path, *, limits: Limits | None = None) -> Toolbox:
        """Scripted sources from a JSON fixture (see ``ScriptedSource.from_json``); a missing file means none."""
        from ein_utcp.sources.scripted import ScriptedSource

        return cls(ScriptedSource.from_json(path), limits)

    def reset(self) -> None:
        self.register = Register(self.limits.cache_max_bytes)

    @property
    def sources(self) -> list[str]:
        return sorted(self._sources)

    def source(self, name: str) -> Source:
        return self._sources[name]

    def catalog(self, source: str) -> list[str]:
        return self._sources[source].catalog()

    def calls_made(self) -> list[str]:
        """Every call actually dispatched this task, in order: ``source.operation key=value ...``."""
        return list(self.register.trail)

    def tool_specs(self) -> list[dict[str, Any]]:
        """OpenAI-style function schemas for the tools; empty when no source is configured."""
        return tool_specs(self.sources)

    async def call(self, name: str, arguments: dict[str, Any] | None) -> str:
        """Route one tool call by name. Never raises."""
        args = arguments or {}
        tools = {"grep_operations": self._grep_operations, "call_operation": self._call_operation,
                 "read_result": self._read_result, "grep_result": self._grep_result,
                 "refresh_result": self._refresh_result, "list_results": self._list_results}
        try:
            if not isinstance(args, dict):
                raise ToolError("tool arguments must be an object")
            if name not in tools:
                raise ToolError(f"unknown tool {name!r}", tools=list(tools))
            return await tools[name](args)
        except ToolError as e:
            return _json(e.payload)
        except Exception as e:  # the loop must go on; the model sees what went wrong
            log.exception("tool %s failed", name)
            return _error(f"{type(e).__name__}: {e}")

    async def _grep_operations(self, args: dict[str, Any]) -> str:
        source = self._require_source(args)
        return grep_catalog(source.catalog(), _require_str(args, "pattern"), limit=_int(args.get("limit"), 20))

    async def _call_operation(self, args: dict[str, Any]) -> str:
        source = self._require_source(args)
        raw = _require_str(args, "operation", "operation must be an operation name from the catalog")
        name = raw if raw.startswith(f"{source.name}.") else f"{source.name}.{raw}"
        op = source.operation(name)
        if op is None:
            raise ToolError(f"unknown operation {raw!r} for source {source.name!r}; find the name with grep_operations")
        pairs = arguments_from(args.get("arguments"))
        if pairs is None:
            raise ToolError("arguments must be a list of {name, value} pairs")
        arguments = coerce_arguments(pairs, op.properties)
        rejected = await source.preflight(op, arguments)
        if rejected is not None:
            return _json(rejected)

        key = call_fingerprint(source.name, name, arguments)
        if self.register.is_repeat(key):
            handle = self.register.by_key[key].handle
            raise ToolError(f"this exact call is already registered as {handle}; use read_result('{handle}', ...) "
                            f"or grep_result('{handle}', ...) to look at it, or refresh_result('{handle}') "
                            "for a fresh snapshot")
        self.register.count(key)
        record = self.register.by_key.get(key) or self.register.new_record(source.name, name, arguments, key)
        snapshot = await self._fetch(source, op, record)
        changed = self.register.store(record, snapshot)
        return self._digest(record, snapshot, changed)

    async def _read_result(self, args: dict[str, Any]) -> str:
        record, snapshot, banner = await self._snapshot(args.get("handle"))
        return banner + snapshot.read(offset=_int(args.get("offset"), 0), limit=_int(args.get("limit"), 100),
                                      handle=record.handle, fields=_fields(args), max_bytes=self.limits.result_max_bytes)

    async def _grep_result(self, args: dict[str, Any]) -> str:
        pattern = _require_str(args, "pattern")
        record, snapshot, banner = await self._snapshot(args.get("handle"))
        return banner + snapshot.grep(pattern, before=_int(args.get("before"), 0), after=_int(args.get("after"), 0),
                                      max_matches=_int(args.get("max_matches"), 50), fields=_fields(args),
                                      max_bytes=self.limits.result_max_bytes)

    async def _refresh_result(self, args: dict[str, Any]) -> str:
        record = self._require_record(args.get("handle"))
        snapshot = await self._refetch(record, what=f"refresh of {record.handle}")
        changed = self.register.store(record, snapshot, note=" (refreshed)")
        text = self._digest(record, snapshot, changed)
        return text if changed else f"[unchanged - the source returned the same snapshot as before]\n{text}"

    async def _list_results(self, args: dict[str, Any]) -> str:
        records = self.register.records.values()
        if not records:
            return "(no results registered yet)"
        return "\n".join(f"{r.handle}: {r.describe()} ({r.item_count} items, {r.byte_size} bytes)"
                         if r.item_count is not None else f"{r.handle}: {r.describe()} ({r.byte_size} bytes)"
                         for r in records)

    def _require_source(self, args: dict[str, Any]) -> Source:
        name = args.get("source")
        if not isinstance(name, str) or name not in self._sources:
            raise ToolError("unknown or missing source", sources=self.sources)
        return self._sources[name]

    def _require_record(self, handle: Any) -> CallRecord:
        record = self.register.records.get(handle) if isinstance(handle, str) else None
        if record is None:
            raise ToolError(f"unknown handle {handle!r}", handles=list(self.register.records))
        return record

    def _digest(self, record: CallRecord, snapshot: Snapshot, changed: bool) -> str:
        text = snapshot.digest(handle=record.handle, tool_name=record.operation, max_bytes=self.limits.digest_max_bytes)
        return f"{SNAPSHOT_REFRESHED_BANNER}\n{text}" if changed else text

    async def _snapshot(self, handle: Any) -> tuple[CallRecord, Snapshot, str]:
        """The record and its snapshot, re-fetching on a cache miss; the banner says when the re-fetch differed."""
        record = self._require_record(handle)
        snapshot = self.register.cache.get(record.fingerprint)
        if snapshot is not None:
            return record, snapshot, ""
        snapshot = await self._refetch(record, what=f"{record.handle} is not cached and re-fetching it")
        changed = self.register.store(record, snapshot, note=" (re-fetched)")
        return record, snapshot, f"{SNAPSHOT_REFRESHED_BANNER}\n" if changed else ""

    async def _refetch(self, record: CallRecord, *, what: str) -> Snapshot:
        source = self._sources[record.source]
        op = source.operation(record.operation)
        if op is None:
            raise ToolError(f"{what} failed: operation {record.operation!r} is gone")
        return await self._fetch(source, op, record, what=what)

    async def _fetch(self, source: Source, op: Operation, record: CallRecord, *, what: str = "") -> Snapshot:
        """Dispatch under the call timeout and snapshot the decoded result; any failure is a ``ToolError``."""
        try:
            result = await asyncio.wait_for(source.dispatch(op, record.arguments), timeout=self.limits.call_timeout)
        except Exception as e:
            raise ToolError(f"{what} failed: {describe_failure(e)}" if what else describe_failure(e)) from e
        result = source.decode(result)
        return Snapshot(result, record.arguments, records=source.records(result))


def _json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, ensure_ascii=False, default=str)


def _error(message: str, **extra: Any) -> str:
    return _json({"error": message, **extra})


def _require_str(args: dict[str, Any], key: str, message: str = "") -> str:
    value = args.get(key)
    if not isinstance(value, str) or not value:
        raise ToolError(message or f"{key} must be a string")
    return value


def _int(value: Any, default: int) -> int:
    return int(value) if isinstance(value, int | float) and not isinstance(value, bool) else default


def _fields(args: dict[str, Any]) -> list[str] | None:
    fields = args.get("fields")
    return fields if isinstance(fields, list) and fields and all(isinstance(f, str) for f in fields) else None
