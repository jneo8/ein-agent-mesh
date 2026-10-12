"""How a source's operations become something the model can discover and call."""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Any

log = logging.getLogger(__name__)

_HTTP_METHODS = {"get", "put", "post", "delete", "options", "head", "patch", "trace"}


@dataclass(frozen=True)
class Operation:
    """One callable operation of a source.

    ``name`` is the full name the model uses (``prometheus.query``). ``properties`` and ``schema`` describe
    the parameters (None when the spec declares none). ``transport`` is whatever the source's dispatch
    needs to perform the call (a utcp-http call template, or nothing for a scripted source).
    """

    name: str
    path: str = ""
    description: str = ""
    properties: dict[str, Any] | None = None
    schema: dict[str, Any] | None = None
    transport: Any = field(default=None, compare=False, repr=False)


def filter_readonly_operations(spec: dict, source: str) -> dict:
    """Keep only GET operations (Swagger 2.0 or OpenAPI 3.x); paths left without one are dropped."""
    if "paths" not in spec:
        return spec
    kept_paths: dict = {}
    total = kept = 0
    for path, item in spec["paths"].items():
        if not isinstance(item, dict):
            continue
        kept_item = {}
        for key, value in item.items():
            if key.lower() not in _HTTP_METHODS:
                kept_item[key] = value  # 'parameters', 'servers': apply to the kept GETs
                continue
            total += 1
            if key.lower() == "get":
                kept_item[key] = value
                kept += 1
        if "get" in {k.lower() for k in kept_item}:
            kept_paths[path] = kept_item
    spec["paths"] = kept_paths
    log.info("%s: kept %d GET operations, dropped %d write operations", source, kept, total - kept)
    return spec


def resolve_server_url(spec: dict, api_base_url: str) -> str:
    """The configured base URL plus the spec's own prefix: Swagger ``basePath``, or a relative
    ``servers[0].url`` (an absolute one in a shipped spec says nothing about this deployment)."""
    base = api_base_url.rstrip("/")
    if "basePath" in spec:
        return f"{base}{spec['basePath']}".rstrip("/")
    servers = spec.get("servers") or []
    url = servers[0].get("url", "") if servers and isinstance(servers[0], dict) else ""
    return f"{base}{url}".rstrip("/") if url and not url.startswith("http") else base


BOILERPLATE_PARAMS = frozenset({"pretty"})  # request-shaping parameters, stripped from every catalog line
DESC_MAX = 60  # the description column is cut here at build time
MAX_PATTERN_LENGTH = 400  # longer patterns are rejected before compilation


GREP_TOOL_DESCRIPTION = r"""Search the operation catalog with a case-insensitive regex.

Each operation is one line:
    <operation name> | <path> | (<params>, * = required) | <description>
Example (one catalog line):
    prometheus.query | /api/v1/query | (query*, time, timeout) | Evaluate an instant query

Columns are separated by ' | ', so a pattern like '/query$' matches nothing; anchor a column end with
'/query \|' instead. A returned line is directly callable: pass its operation name and parameters to
call_operation. Zero hits means the pattern is too narrow: widen it and grep again."""


def catalog_line(op: Operation) -> str:
    """One operation as a catalog line."""
    required = set((op.schema or {}).get("required") or ())
    params = [(0 if p in required else 1, f"{p}{'*' if p in required else ''}")
              for p in (op.properties or {}) if p not in BOILERPLATE_PARAMS]
    params.sort(key=lambda entry: entry[0])  # required first; stable, so spec order survives within groups
    desc = " ".join(op.description.replace("|", " ").split())[:DESC_MAX]  # '|' is the column separator
    return f"{op.name} | {op.path} | ({', '.join(p for _, p in params)}) | {desc}"


_PARAMS = re.compile(r"\((.*?)\)")


def parse_catalog_line(line: str) -> Operation:
    """A catalog line back into an operation (the scripted source is defined by its catalog)."""
    columns = [c.strip() for c in line.split(" | ")]
    name, path = columns[0], columns[1] if len(columns) > 1 else ""
    params = _PARAMS.search(columns[2]) if len(columns) > 2 else None
    names = [p.strip() for p in params.group(1).split(",") if p.strip()] if params else []
    properties = {p.rstrip("*"): {"type": "string"} for p in names}
    required = [p.rstrip("*") for p in names if p.endswith("*")]
    schema = {"type": "object", "properties": properties, "required": required} if properties else None
    return Operation(name=name, path=path, description=columns[3] if len(columns) > 3 else "",
                     properties=properties or None, schema=schema)


def build_catalog(operations: list[Operation]) -> list[str]:
    """The sorted grep corpus."""
    return sorted(catalog_line(op) for op in operations)


def grep_catalog(lines: list[str], pattern: str, limit: int = 20) -> str:
    """Match a case-insensitive regex against the corpus; only hits are returned.

    Every outcome is actionable text, never a dead end: zero hits say to widen, an overflow says to
    narrow, and a bad pattern comes back as a correctable message rather than an exception.
    """
    if len(pattern) > MAX_PATTERN_LENGTH:
        return f"error: pattern longer than {MAX_PATTERN_LENGTH} chars - simplify it"
    try:
        rx = re.compile(pattern, re.IGNORECASE)
    except re.error as exc:
        return f"error: bad pattern: {exc}"

    hits = [line for line in lines if rx.search(line)]
    shown = hits[:limit]
    if len(hits) > limit:
        note = f"\n[{len(hits)} matches, showing first {limit} - narrow your pattern]"
    else:
        note = f"\n[{len(hits)} matches]"
    return ("\n".join(shown) or "(no matches - widen your pattern)") + note


def tool_specs(sources: list[str]) -> list[dict[str, Any]]:
    """OpenAI-style function schemas for the tools; empty when no source is configured."""
    if not sources:
        return []
    source = {"type": "string", "enum": sources, "description": "Which source to query."}
    handle = {"type": "string", "description": "A result handle from a call_operation digest, e.g. call-3."}
    fields = {"type": "array", "items": {"type": "string"},
              "description": 'Dotted paths to project each record down to, e.g. ["labels.alertname", "status.state"].'}
    pattern = {"type": "string", "description": "Case-insensitive regex."}

    def fn(name: str, description: str, properties: dict, required: list[str]) -> dict:
        return {"type": "function", "function": {"name": name, "description": description, "parameters": {
            "type": "object", "properties": properties, "required": required}}}

    return [
        fn("grep_operations", GREP_TOOL_DESCRIPTION,
           {"source": source, "pattern": pattern, "limit": {"type": "integer", "description": "Max lines (default 20)."}},
           ["source", "pattern"]),
        fn("call_operation",
           "Call one read-only operation of a source. 'operation' is the operation name exactly as a catalog "
           "line shows it. 'arguments' is one {name, value} pair per parameter, values as strings; give an "
           "array-valued parameter once per element. A large result comes back as a digest with a handle; "
           "a small one comes back whole. Errors come back as {\"error\": ...}: read them, correct the call.",
           {"source": source,
            "operation": {"type": "string", "description": "Operation name from the catalog, e.g. prometheus.query."},
            "arguments": {"type": "array", "description": "Parameters as {name, value} pairs.",
                          "items": {"type": "object", "properties": {"name": {"type": "string"}, "value": {"type": "string"}},
                                    "required": ["name", "value"]}}},
           ["source", "operation"]),
        fn("read_result",
           "Read a window of a registered result by handle. To LIST records, pass fields=[...] and get one "
           "small line per record; to inspect one record whole, pass offset and limit=1.",
           {"handle": handle, "offset": {"type": "integer", "description": "First line (default 0)."},
            "limit": {"type": "integer", "description": "Lines to return (default 100)."}, "fields": fields},
           ["handle"]),
        fn("grep_result",
           "Search a registered result by handle with a case-insensitive regex; only matching records come "
           "back. Add fields=[...] to get just the columns you need from each hit.",
           {"handle": handle, "pattern": pattern, "before": {"type": "integer"}, "after": {"type": "integer"},
            "max_matches": {"type": "integer", "description": "Default 50."}, "fields": fields},
           ["handle", "pattern"]),
        fn("refresh_result", "Re-run a registered call at the source and get a fresh digest under the same "
           "handle. Use this instead of repeating the original call.", {"handle": handle}, ["handle"]),
        fn("list_results", "The handles registered so far, one line each.", {}, []),
    ]


USAGE_GUIDE = """## Tools
Operations of the sources below are called with call_operation(source, operation, arguments); arguments are
one {name, value} pair per parameter, values as strings. A small result comes back whole; a large one as
a digest: a header with the item count and a handle such as call-3, the distributions of its fields
computed over every item, a few sample records, and a footer naming the tools that reach the rest. The
full rows are not in the digest. To LIST records, read_result(handle, fields=["path.to.field", ...])
returns one small line per record; grep_result(handle, pattern, fields=[...]) finds specific records;
read_result(handle, offset, limit=1) shows one record whole; refresh_result(handle) re-runs the call when
the digest may be stale. The digest already counts the whole set. grep_operations(source, pattern)
searches the catalog when you need an operation not listed here. An {"error": ...} result tells you
what to correct; a repeated identical call is refused in favour of its handle.

Each catalog line is: <operation> | <path> | (<parameters>, * = required) | <description>
"""

NO_TOOLS = """## Tools
None in this deployment. Reason over the quoted data alone and do not pretend to have run anything."""


def usage_guide(catalogs: dict[str, list[str]]) -> str:
    """What a model needs to know to use the tools over these sources, catalogs included; or the one line
    that says there are none."""
    if not catalogs:
        return NO_TOOLS
    return USAGE_GUIDE + "\n".join(f"### {source}\n" + "\n".join(lines) for source, lines in sorted(catalogs.items()))
