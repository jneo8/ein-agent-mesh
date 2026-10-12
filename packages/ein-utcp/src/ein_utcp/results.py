"""What happens to a result once it is back: kept under a handle and looked at in bounded views."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from collections import Counter, OrderedDict
from dataclasses import dataclass
from typing import Any

import regex

log = logging.getLogger(__name__)

_RECORD_SEARCH_DEPTH = 4  # how deep to look for the record list inside an envelope ($.data.result, a root array)
MAX_PATTERN_LENGTH = 400  # longer grep patterns are rejected before compilation
GREP_TIMEOUT_SECONDS = 1.0  # per-line budget for a model-supplied regex, the ReDoS backstop
RESULT_MAX_BYTES = 16 * 1024  # default ceiling on one grep/read response; wide records should be projected
CACHE_MAX_BYTES = 64 * 1024 * 1024
SNAPSHOT_REFRESHED_BANNER = (  # prepended when a re-fetch differs from what the register recorded
    "[SNAPSHOT REFRESHED - the source changed since this handle was captured; "
    "counts and records below are the current values, not the earlier ones.]"
)
_TRUNCATED = "\n\n... [TRUNCATED - response too large. Use more specific filters to narrow results.] ..."
DIGEST_MAX_BYTES = 8 * 1024  # default ceiling on a digest, and below it a result is returned whole
_LOW_CARDINALITY = 20  # a field with more distinct values than this is not aggregated
_SMALL_ENUM = 8  # up to this many distinct values a field shows its whole distribution, above it the top few
_SAMPLE = 3  # sample records in a digest
_SAMPLE_LINE_BYTES = 200  # each sample line is trimmed to this
_FLATTEN_DEPTH = 3  # how deep nested records are flattened to dotted paths
_MAX_PATHS = 200  # ceiling on distinct paths tracked (bounds open-ended label maps)
_PAGINATION_KEYS = ("limit", "continue", "marker", "offset", "page", "per_page")  # echoed in the completeness note
_ISO_TS = re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}")  # timestamps are not aggregated
_OPAQUE_HEX = re.compile(r"[0-9a-f]{32,}", re.IGNORECASE)  # opaque identifiers are not aggregated



def canonical_json(result: Any) -> bytes:
    """Sorted keys, compact, stable across runs."""
    return json.dumps(result, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str).encode()


def content_hash(canonical: bytes) -> str:
    return hashlib.sha256(canonical).hexdigest()[:16]


def find_records(result: Any, max_depth: int = _RECORD_SEARCH_DEPTH) -> list[dict] | None:
    """The largest list-of-dicts within reach, or None. Structural, not name-driven."""
    best: list[dict] | None = None
    frontier: list[tuple[Any, int]] = [(result, 0)]
    while frontier:
        node, depth = frontier.pop()
        is_records = isinstance(node, list) and node and all(isinstance(x, dict) for x in node)
        if is_records and (best is None or len(node) > len(best)):
            best = node
        if depth < max_depth and isinstance(node, dict | list):
            children = node.values() if isinstance(node, dict) else node
            frontier.extend((child, depth + 1) for child in children)
    return best


def render(result: Any, max_chars: int | None = None) -> str:
    """Pretty JSON (or ``str``), head-truncated to ``max_chars`` when given. A cut at an arbitrary character
    leaves a JSON fragment, so this is only the fallback for a result the digest cannot aggregate."""
    text = json.dumps(result, indent=2, ensure_ascii=False, default=str) if isinstance(result, dict | list) else str(result)
    if max_chars is None or len(text) <= max_chars:
        return text
    if max_chars <= len(_TRUNCATED):
        return text[:max_chars]
    return text[: max_chars - len(_TRUNCATED)] + _TRUNCATED


def _is_short_scalar(value: Any) -> bool:
    if isinstance(value, bool | int | float):
        return True
    return isinstance(value, str) and len(value) <= 64


def _looks_like_timestamp(value: Any) -> bool:
    return isinstance(value, str) and _ISO_TS.match(value) is not None


def _human_size(n: int) -> str:
    size = float(n)
    for unit in ("B", "KB", "MB"):
        if size < 1024:
            return f"{int(size)} {unit}" if unit == "B" else f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} GB"


def _flatten(record: dict, depth: int, prefix: str, out: dict[str, Any]) -> None:
    """Collect a record's short, informative scalars as dotted paths."""
    for key, value in record.items():
        path = f"{prefix}.{key}" if prefix else str(key)
        if isinstance(value, dict) and depth > 1:
            _flatten(value, depth - 1, path, out)
        elif (_is_short_scalar(value) and not _looks_like_timestamp(value)
              and not (isinstance(value, str) and _OPAQUE_HEX.search(value))):
            out[path] = value


def _aggregate(records: list[dict]) -> tuple[dict[str, Counter | None], dict[str, int]]:
    """Count each path's values over the full set; abandon high-cardinality paths (bucket becomes None)."""
    buckets: dict[str, Counter | None] = {}
    present: dict[str, int] = {}
    for record in records:
        flat: dict[str, Any] = {}
        _flatten(record, _FLATTEN_DEPTH, "", flat)
        for path, value in flat.items():
            if path not in buckets:
                if len(buckets) >= _MAX_PATHS:
                    continue
                buckets[path] = Counter()
                present[path] = 0
            present[path] += 1
            counter = buckets[path]
            if counter is None:
                continue
            counter[json.dumps(value, default=str)] += 1
            if len(counter) > _LOW_CARDINALITY:
                buckets[path] = None
    return buckets, present


def _pretty(json_key: str) -> str:
    try:
        value = json.loads(json_key)
    except ValueError:
        return json_key
    return value.replace("\n", " ") if isinstance(value, str) else json_key


def _field_line(path: str, counter: Counter, present: int, item_count: int) -> str:
    pairs = counter.most_common()
    distinct = len(counter)
    if distinct <= _SMALL_ENUM:
        body = ", ".join(f"{_pretty(k)} {c}" for k, c in pairs)
    else:
        top = ", ".join(f"{_pretty(k)} {c}" for k, c in pairs[:_SMALL_ENUM])
        body = f"{distinct} distinct values (top: {top}, ...)"
    coverage = f"  (n={present})" if present < item_count else ""
    return f"  {path}: {body}{coverage}"


def _select_fields(buckets: dict[str, Counter | None], present: dict[str, int],
                   item_count: int) -> list[tuple[str, Counter, int]]:
    """Keep the aggregatable paths, lowest distinct count first; drop abandoned and all-unique fields."""
    kept: list[tuple[int, str, Counter]] = []
    for path, counter in buckets.items():
        if counter is None or not counter:
            continue
        distinct = len(counter)
        if distinct == item_count and item_count > 1:
            continue
        kept.append((distinct, path, counter))
    kept.sort(key=lambda t: (t[0], t[1]))
    return [(path, counter, present[path]) for _, path, counter in kept]


def _sample_lines(lines: list[str]) -> list[str]:
    trimmed = []
    for line in lines[:_SAMPLE]:
        if len(line.encode()) > _SAMPLE_LINE_BYTES:
            line = line.encode()[:_SAMPLE_LINE_BYTES].decode(errors="ignore") + "..."
        trimmed.append(f"  {line}")
    return trimmed


def _walk_path(node: Any, path: str) -> Any:
    """A dotted path through a record. An exact key match of the whole remaining path wins before splitting
    on dots (label keys contain dots); a list projects the rest over its elements; missing paths are None."""
    if not path:
        return node
    if isinstance(node, list):
        return [_walk_path(element, path) for element in node]
    if not isinstance(node, dict):
        return None
    if path in node:
        return node[path]
    head, _, rest = path.partition(".")
    return _walk_path(node[head], rest) if head in node else None


def project_line(line: str, fields: list[str]) -> str:
    """One record line down to the named dotted paths; a non-JSON line passes through."""
    try:
        record = json.loads(line)
    except ValueError:
        return line
    if not isinstance(record, dict):
        return line
    return json.dumps({field: _walk_path(record, field) for field in fields}, ensure_ascii=False, default=str)


class Snapshot:
    """One captured result and the three ways the model looks at it.

    Measured once over the whole result: ``content_hash`` (identity), ``byte_size``, ``item_count`` (None
    when there is no record list) and ``lines`` (one compact JSON line per record, or the pretty-printed
    text split into lines). ``arguments`` are the coerced ones the call was dispatched with, so a replay
    reuses the exact form. Three views: ``digest`` first, then ``read`` (a window) and ``grep`` (hits).
    """

    def __init__(self, result: Any, arguments: dict[str, Any] | None = None,
                 records: list[dict] | None = None) -> None:
        canonical = canonical_json(result)
        self.result = result
        self.arguments = arguments or {}
        self.content_hash = content_hash(canonical)
        self.byte_size = len(canonical)
        self.records = records if records is not None else find_records(result)
        self.item_count = len(self.records) if self.records is not None else None
        self.lines = ([canonical_json(r).decode() for r in self.records] if self.records is not None
                      else render(result).splitlines() or [""])

    def digest(self, *, handle: str, tool_name: str, max_bytes: int = DIGEST_MAX_BYTES) -> str:
        """The first view of a result: a small one whole; a large one as a header (count, size, identity),
        the distributions of its low-cardinality fields computed over every record, a few trimmed samples,
        and a footer naming the tools that reach the rest by ``handle``."""
        rendered = render(self.result)
        if len(rendered.encode()) <= max_bytes:
            return rendered

        if self.records is None:
            # No record list to aggregate (one oversized blob, or a flat list of scalars like a label-values
            # response). Head-truncate, but still name the handle: the snapshot is registered and the tools
            # reach it. The footer rides outside the truncation budget.
            fallback_footer = (
                f"[Truncated. Full result reachable: read_result('{handle}', offset, limit) for a window, "
                f"grep_result('{handle}', pattern) to search, refresh_result('{handle}') for the current snapshot.]"
            )
            body = render(self.result, max_chars=max(max_bytes - len(fallback_footer) - 1, 64))
            return f"{body}\n{fallback_footer}"

        item_count = self.item_count or len(self.records)
        header = f"Result {handle}: {tool_name} - {item_count} items, {_human_size(self.byte_size)}, hash {self.content_hash[:8]}..."
        paged = [f"{k}={self.arguments[k]}" for k in _PAGINATION_KEYS if k in self.arguments]
        note = f"  [call was paginated ({', '.join(paged)}); more may exist beyond these {item_count}.]" if paged else ""
        sample = _sample_lines(self.lines)
        footer = (
            f"[Full result reachable: to LIST records, read_result('{handle}', fields=[\"path.to.field\", ...]) "
            f"projects just the fields you name - never page whole records for a listing. "
            f"grep_result('{handle}', pattern, fields=[...]) to search, read_result('{handle}', offset, limit) "
            f"for whole records, refresh_result('{handle}') for the current snapshot if this may be stale.]"
        )

        fixed = [header] + ([note] if note else []) + [f"First {len(sample)} records (trimmed):", *sample, footer]
        budget = max_bytes - len("\n".join(fixed).encode())
        buckets, present = _aggregate(self.records)
        field_lines: list[str] = []
        used = len(f"Computed over all {item_count} items:\n".encode())
        for path, counter, seen in _select_fields(buckets, present, item_count):
            line = _field_line(path, counter, present=seen, item_count=item_count)
            cost = len((line + "\n").encode())
            if field_lines and used + cost > budget:
                break
            field_lines.append(line)
            used += cost

        parts = [header] + ([note] if note else [])
        if field_lines:
            parts += [f"Computed over all {item_count} items:", *field_lines]
        parts += [f"First {len(sample)} records (trimmed):", *sample, footer]
        return "\n".join(parts)


    def grep(self, pattern: str, *, before: int = 0, after: int = 0, max_matches: int = 50,
             fields: list[str] | None = None, max_bytes: int = RESULT_MAX_BYTES) -> str:
        """Only the matching records. Zero hits say to widen, an overflow says to narrow, a bad pattern comes
        back correctable, a pathological one is stopped by the timeout. ``fields`` projects each hit."""
        if len(pattern) > MAX_PATTERN_LENGTH:
            return f"error: pattern longer than {MAX_PATTERN_LENGTH} chars - simplify it"
        try:
            rx = regex.compile(pattern, regex.IGNORECASE)
        except regex.error as exc:
            return f"error: bad pattern: {exc}"

        matches: list[int] = []
        try:
            for i, line in enumerate(self.lines):
                if rx.search(line, timeout=GREP_TIMEOUT_SECONDS):
                    matches.append(i)
                    if len(matches) > max_matches:
                        break
        except TimeoutError:
            return "error: pattern took too long to evaluate (possible catastrophic backtracking) - simplify it"

        overflow = len(matches) > max_matches
        shown = matches[:max_matches]
        if not shown:
            return "(no matches - widen your pattern)\n[0 matches]"

        def line(j: int) -> str:
            return project_line(self.lines[j], fields) if fields else self.lines[j]

        windows = self._windows(shown, before, after)
        if before or after:
            body = "\n--\n".join("\n".join(line(j) for j in w) for w in windows)
        else:
            body = "\n".join(line(j) for w in windows for j in w)
        footer = (f"\n[{max_matches}+ matches, showing first {max_matches} - narrow your pattern]" if overflow
                  else f"\n[{len(shown)} matches]")
        return _cap(body + footer, max_bytes)

    def read(self, *, offset: int = 0, limit: int = 100, handle: str = "", fields: list[str] | None = None,
             max_bytes: int = RESULT_MAX_BYTES) -> str:
        """A window of lines with a footer naming the next window. ``fields`` projects each record."""
        total = len(self.lines)
        offset = max(offset, 0)
        if offset >= total:
            return f"[offset {offset} is past the end - {total} line(s) total]"
        end = min(total, offset + limit) if limit > 0 else total
        window = [project_line(line, fields) for line in self.lines[offset:end]] if fields else self.lines[offset:end]
        if end < total:
            cont = f"read_result('{handle}', offset={end})" if handle else f"offset={end}"
            footer = f"\n[lines {offset}-{end - 1} of {total}; {cont} to continue]"
        else:
            footer = f"\n[lines {offset}-{end - 1} of {total} - end of result]"
        return _cap("\n".join(window) + footer, max_bytes)

    def _windows(self, indices: list[int], before: int, after: int) -> list[range]:
        """Each match's context window, overlaps coalesced."""
        total = len(self.lines)
        windows: list[range] = []
        for i in indices:
            lo, hi = max(0, i - before), min(total, i + after + 1)
            if windows and lo <= windows[-1].stop:
                windows[-1] = range(windows[-1].start, max(windows[-1].stop, hi))
            else:
                windows.append(range(lo, hi))
        return windows


def _cap(text: str, max_bytes: int) -> str:
    """Trim to the byte ceiling on a whole-line boundary."""
    encoded = text.encode()
    if len(encoded) <= max_bytes:
        return text
    clipped = encoded[:max_bytes].decode(errors="ignore")
    head, _, _ = clipped.rpartition("\n")
    return (head or clipped) + ('\n[... output truncated at the size ceiling - narrow the query, or project just '
                                'the fields you need: fields=["labels.alertname", ...].]')


class SnapshotCache:
    """Byte-capped LRU of snapshots. The most-recently-used entry is kept even when it alone exceeds the
    cap: evicting the snapshot a grep is about to read would defeat the point."""

    def __init__(self, max_bytes: int = CACHE_MAX_BYTES) -> None:
        self._max_bytes = max_bytes
        self._store: OrderedDict[str, Snapshot] = OrderedDict()
        self._total = 0

    def get(self, key: str) -> Snapshot | None:
        snapshot = self._store.get(key)
        if snapshot is not None:
            self._store.move_to_end(key)
        return snapshot

    def put(self, key: str, snapshot: Snapshot) -> None:
        existing = self._store.pop(key, None)
        if existing is not None:
            self._total -= existing.byte_size
        self._store[key] = snapshot
        self._total += snapshot.byte_size
        while self._total > self._max_bytes and len(self._store) > 1:
            _, dropped = self._store.popitem(last=False)
            self._total -= dropped.byte_size

    def clear(self) -> None:
        self._store.clear()
        self._total = 0


_MAX_IDENTICAL_CALLS = 2  # a call repeated this often with identical arguments is refused
_TRAIL_VALUE_CHARS = 80  # argument values in the evidence trail are cut here


@dataclass
class CallRecord:
    """How to reproduce one registered result, and its identity the last time it was fetched."""

    handle: str
    source: str
    operation: str
    arguments: dict[str, Any]
    fingerprint: str
    content_hash: str = ""
    byte_size: int = 0
    item_count: int | None = None

    def measure(self, snapshot: Snapshot) -> bool:
        """Take the snapshot's identity; True when it differs from the recorded one."""
        changed = bool(self.content_hash) and snapshot.content_hash != self.content_hash
        self.content_hash, self.byte_size, self.item_count = snapshot.content_hash, snapshot.byte_size, snapshot.item_count
        return changed

    def describe(self) -> str:
        rendered = " ".join(f"{k}={_short(v)}" for k, v in self.arguments.items())
        return f"{self.operation} {rendered}".rstrip()


class Register:
    """Handles, snapshots and the evidence trail for one task.

    Every dispatched call gets a handle (``call-1``, ``call-2``, ...), a cached snapshot and a record of how
    to reproduce it. ``trail`` lists what was actually dispatched, so a report's evidence comes from the
    runtime rather than the model's account of itself.
    """

    def __init__(self, cache_max_bytes: int) -> None:
        self.records: dict[str, CallRecord] = {}  # by handle
        self.by_key: dict[str, CallRecord] = {}  # by call fingerprint
        self.cache = SnapshotCache(cache_max_bytes)
        self.trail: list[str] = []
        self.last_key: str | None = None
        self.key_counts: Counter[str] = Counter()

    def is_repeat(self, key: str) -> bool:
        """The same call as the previous one, or the same call for the third time."""
        return key in self.by_key and (key == self.last_key or self.key_counts[key] >= _MAX_IDENTICAL_CALLS)

    def count(self, key: str) -> None:
        self.last_key = key
        self.key_counts[key] += 1

    def store(self, record: CallRecord, snapshot: Snapshot, *, note: str = "") -> bool:
        """Keep a fetched result's snapshot under its handle; True when its identity changed."""
        changed = record.measure(snapshot)
        self.records.setdefault(record.handle, record)
        self.by_key.setdefault(record.fingerprint, record)
        self.cache.put(record.fingerprint, snapshot)
        self.trail.append(record.describe() + note)
        return changed

    def new_record(self, source: str, operation: str, arguments: dict[str, Any], key: str) -> CallRecord:
        return CallRecord(handle=f"call-{len(self.records) + 1}", source=source, operation=operation,
                          arguments=arguments, fingerprint=key)


def call_fingerprint(source: str, operation: str, arguments: dict[str, Any]) -> str:
    """A stable key for one call, independent of its handle: the same call shares one snapshot."""
    return content_hash(canonical_json([source, operation, arguments]))


def _short(value: Any) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False, default=str)
    return text if len(text) <= _TRAIL_VALUE_CHARS else text[: _TRAIL_VALUE_CHARS - 3] + "..."
