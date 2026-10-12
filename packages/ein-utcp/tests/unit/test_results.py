"""A captured result: identity, the digest, read / grep / projection, the cache and the register."""

import json

import pytest

from ein_utcp.results import (
    DIGEST_MAX_BYTES,
    CallRecord,
    Register,
    Snapshot,
    SnapshotCache,
    call_fingerprint,
    canonical_json,
    content_hash,
    find_records,
    render,
)

RECORDS = [{"name": f"r{i}", "kind": "a" if i % 2 else "b", "meta": {"k.v": i}} for i in range(10)]


def test_identity_is_stable_across_key_order_and_finds_the_record_list():
    a, b = {"x": 1, "y": [1, 2]}, {"y": [1, 2], "x": 1}
    assert canonical_json(a) == canonical_json(b) and content_hash(canonical_json(a)) == content_hash(canonical_json(b))
    assert find_records({"data": {"result": RECORDS}}) == RECORDS  # Prometheus envelope
    assert find_records(RECORDS) == RECORDS  # bare root array
    assert find_records({"status": "ok", "data": ["a", "b"]}) is None  # scalars are not records
    snap = Snapshot({"data": RECORDS}, {"query": "up"})
    assert snap.item_count == 10 and snap.arguments == {"query": "up"} and len(snap.content_hash) == 16
    assert Snapshot(["a", "b"]).item_count is None
    assert Snapshot({"wrapped": RECORDS[:2]}, records=RECORDS).item_count == 10  # a source may say where records are
    assert call_fingerprint("p", "p.query", {"a": 1, "b": 2}) == call_fingerprint("p", "p.query", {"b": 2, "a": 1})
    assert call_fingerprint("p", "p.query", {"a": 1}) != call_fingerprint("p", "p.query", {"a": 2})


def test_render_truncates_only_when_asked_and_always_fits():
    text = render({"blob": "x" * 500})
    assert len(text) > 500 and render({"blob": "x" * 500}, max_chars=None) == text
    assert len(render({"blob": "x" * 500}, max_chars=200)) <= 200 and "TRUNCATED" in render({"blob": "x" * 500}, 200)
    assert render({"blob": "x" * 500}, max_chars=10) == text[:10]  # below the suffix length: plain cut


def test_snapshot_renders_one_line_per_record_else_pretty_lines():
    snap = Snapshot({"data": {"result": RECORDS}})
    assert len(snap.lines) == 10 and json.loads(snap.lines[0]) == RECORDS[0]
    assert Snapshot({"a": {"b": 1}}).lines == ["{", '  "a": {', '    "b": 1', "  }", "}"]
    assert Snapshot("").lines == [""]


def test_grep_outcomes_are_all_actionable():
    snap = Snapshot(RECORDS)
    assert snap.grep("R3").count("\n") == 1 and "[1 matches]" in snap.grep("R3")  # case-insensitive, whole record
    assert "widen" in snap.grep("nothing") and "[0 matches]" in snap.grep("nothing")
    assert "narrow" in snap.grep("r", max_matches=3) and snap.grep("r", max_matches=3).count("\n") == 3
    assert snap.grep("(").startswith("error: bad pattern")
    assert snap.grep("x" * 401).startswith("error: pattern longer")
    with_context = snap.grep("r5", before=1, after=1)
    assert "r4" in with_context and "r6" in with_context and "--" not in with_context  # one window, no separator
    assert snap.grep("r[12]", fields=["name"]).splitlines()[:2] == ['{"name": "r1"}', '{"name": "r2"}']


def test_read_windows_by_line_and_projects_fields():
    snap = Snapshot(RECORDS)
    first = snap.read(limit=3, handle="call-1")
    assert first.count("\n") == 3 and "read_result('call-1', offset=3)" in first
    assert "end of result" in snap.read(offset=8)
    assert "past the end" in snap.read(offset=99)
    projected = snap.read(limit=1, fields=["meta.k.v", "missing", "name"]).splitlines()[0]
    assert json.loads(projected) == {"meta.k.v": 0, "missing": None, "name": "r0"}  # dotted key matched whole first


def test_responses_are_capped_on_a_line_boundary_and_teach_projection():
    snap = Snapshot([{"n": i, "pad": "x" * 100} for i in range(50)])
    capped = snap.read(limit=50, max_bytes=600)
    assert len(capped.encode()) < 800 and "fields=" in capped
    assert all(line.startswith("{") and line.endswith("}") for line in capped.splitlines()[:-1])


def test_cache_is_an_lru_that_keeps_the_newest_entry():
    snaps = [Snapshot(RECORDS[:i + 1] * 2) for i in range(3)]
    cache = SnapshotCache(max_bytes=snaps[1].byte_size + snaps[2].byte_size)
    for i, s in enumerate(snaps):
        cache.put(f"k{i}", s)
    assert cache.get("k0") is None and cache.get("k2") is snaps[2]  # oldest evicted, newest kept
    huge = Snapshot([{"pad": "x" * 10_000}])
    cache.put("huge", huge)
    assert cache.get("huge") is huge  # over the cap alone, still kept
    cache.clear()
    assert cache.get("huge") is None


@pytest.mark.parametrize("result", [[], {}, "", None, 0, [1, 2], {"a": [{"b": 1}]}])
def test_every_shape_renders_to_at_least_one_line(result):
    assert Snapshot(result).lines


def test_register_hands_out_handles_detects_repeats_and_keeps_the_trail():
    reg = Register(cache_max_bytes=1 << 20)
    key = call_fingerprint("am", "am.getAlerts", {"filter": 'ns="x"'})
    assert not reg.is_repeat(key)
    record = reg.new_record("am", "am.getAlerts", {"filter": 'ns="x"'}, key)
    assert record.handle == "call-1" and record.describe() == 'am.getAlerts filter=ns="x"'
    assert reg.store(record, Snapshot(RECORDS, record.arguments)) is False  # first measurement: not a change
    reg.count(key)
    assert reg.is_repeat(key)  # same as the previous call
    reg.count(call_fingerprint("am", "am.getStatus", {}))
    assert not reg.is_repeat(key)
    reg.count(key)
    assert reg.is_repeat(key)  # third time, whatever came between
    assert reg.store(record, Snapshot(RECORDS[:3], record.arguments), note=" (refreshed)") is True
    assert reg.trail == ['am.getAlerts filter=ns="x"', 'am.getAlerts filter=ns="x" (refreshed)']
    assert reg.by_key[key] is record and reg.records["call-1"].item_count == 3
    assert CallRecord("h", "s", "s.op", {"long": "x" * 100}, "k").describe().endswith("...")


# --- digest ---


def alerts(n: int = 300) -> dict:
    return {"status": "success", "data": [
        {"labels": {"alertname": f"A{i % 3}", "severity": "critical" if i % 5 else "warning", "pod": f"pod-{i}",
                    "namespace": "ceph" if i < 200 else "temporal"},
         "status": {"state": "active"}, "startsAt": "2026-10-12T03:55:18Z", "fingerprint": f"{i:040x}",
         "annotations": {"summary": "disk " * 30}} for i in range(n)]}


def digest(result, arguments=None, **kw):
    return Snapshot(result, arguments).digest(handle="call-7", tool_name="alertmanager.getAlerts", **kw)


def test_small_results_come_back_whole_and_large_ones_as_a_digest():
    small = alerts(2)
    assert digest(small) == render(small)
    assert len(render(alerts()).encode()) > DIGEST_MAX_BYTES
    text = digest(alerts())
    assert text.startswith("Result call-7: alertmanager.getAlerts - 300 items")
    assert "hash " in text and "First 3 records (trimmed):" in text and text.count("...") >= 3
    assert "read_result('call-7', fields=" in text and "refresh_result('call-7')" in text
    assert len(text.encode()) <= DIGEST_MAX_BYTES + 200


def test_aggregation_picks_low_cardinality_fields_and_orders_them():
    section = digest(alerts()).split("Computed over all 300 items:")[1].split("First 3")[0]
    lines = [line.strip() for line in section.strip().splitlines()]
    assert lines[0].startswith("status.state: active 300")  # one value first
    assert any(line.startswith("labels.namespace: ceph 200, temporal 100") for line in lines)
    assert any(line.startswith("labels.alertname: A0 100, A1 100, A2 100") for line in lines)
    assert not any(line.startswith(("labels.pod", "fingerprint", "startsAt")) for line in lines)  # unique, hex, timestamp


def test_partial_fields_are_annotated_and_paginated_calls_are_flagged():
    result = alerts()
    for a in result["data"][:50]:
        a["labels"]["team"] = "storage"
    text = digest(result, arguments={"limit": "300"})
    assert "labels.team: storage 50  (n=50)" in text
    assert "[call was paginated (limit=300); more may exist beyond these 300.]" in text


def test_a_blob_without_records_is_truncated_under_the_handle():
    text = digest({"blob": "x" * (DIGEST_MAX_BYTES + 1000)})
    assert "TRUNCATED" in text and text.endswith("refresh_result('call-7') for the current snapshot.]")
    assert len(text.encode()) <= DIGEST_MAX_BYTES + 100


def test_the_footer_survives_a_field_flood():
    result = [{f"field{j}": f"v{i % 2}" for j in range(300)} for i in range(40)]
    text = digest(result, max_bytes=1500)
    assert text.endswith("if this may be stale.]") and len(text.encode()) < 1500 + 800
