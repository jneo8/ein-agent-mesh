"""The tools over a scripted source: every outcome is data the model can act on."""

import asyncio
import json

import aiohttp
import pytest

from ein_utcp import Limits, ScriptedSource, Toolbox
from ein_utcp.results import SNAPSHOT_REFRESHED_BANNER

AM = ["alertmanager.getAlerts | /api/v2/alerts | (active, filter) | Get a list of alerts",
      "alertmanager.getStatus | /api/v2/status | () | Get current status"]
PROM = ["prometheus.query | /api/v1/query | (query*, time) | Evaluate an instant query",
        "prometheus.format-query | /api/v1/format_query | (query*) | Format a PromQL query"]
LIMITS = Limits(digest_max_bytes=1500, result_max_bytes=1200)


def alerts(n, state="active"):
    return [{"labels": {"alertname": f"A{i % 3}", "pod": f"pod-{i}"}, "status": {"state": state}} for i in range(n)]


def box(responses=None, default=None, limits=LIMITS):
    am = ScriptedSource("alertmanager", AM, responses, default=default)
    return Toolbox([am, ScriptedSource("prometheus", PROM, default={"data": {"result": []}})], limits)


def get_alerts(**extra):
    return {"source": "alertmanager", "operation": "getAlerts", "arguments": [{"name": "filter", "value": 'ns="ceph"'}], **extra}


def test_tool_specs_follow_the_configured_sources():
    assert box().tool_specs()[0]["function"]["parameters"]["properties"]["source"]["enum"] == ["alertmanager", "prometheus"]
    assert Toolbox([]).tool_specs() == [] and Toolbox([]).sources == []


@pytest.mark.parametrize("name, args, needle", [
    ("nope", {}, "unknown tool"),
    ("call_operation", {"source": "grafana", "operation": "x"}, "unknown or missing source"),
    ("call_operation", {"source": "prometheus", "operation": "nope"}, "find the name with grep_operations"),
    ("call_operation", {"source": "prometheus", "operation": "query", "arguments": "up"}, "list of {name, value}"),
    ("call_operation", {"source": "prometheus", "operation": "query", "arguments": [{"name": "bogus", "value": "1"}]},
     "missing required parameter(s): query"),
    ("grep_operations", {"source": "prometheus"}, "pattern must be a string"),
    ("read_result", {"handle": "call-9"}, "unknown handle 'call-9'"),
    ("grep_result", {"handle": "call-1"}, "pattern must be a string"),
])
async def test_bad_calls_are_error_payloads(name, args, needle):
    assert needle in json.loads(await box().call(name, args))["error"]


async def test_grep_operations_searches_the_catalog():
    text = await box().call("grep_operations", {"source": "prometheus", "pattern": "format"})
    assert text.startswith("prometheus.format-query") and text.endswith("[1 matches]")


async def test_a_small_result_comes_back_whole_and_the_trail_records_the_call():
    b = box([{"operation": "getAlerts", "result": alerts(2)}])
    assert json.loads(await b.call("call_operation", get_alerts())) == alerts(2)
    assert b.calls_made() == ['alertmanager.getAlerts filter=ns="ceph"']
    assert b.source("alertmanager").calls == [("alertmanager.getAlerts", {"filter": 'ns="ceph"'})]
    assert await b.call("call_operation", {"source": "alertmanager", "operation": "alertmanager.getStatus"}) == json.dumps(
        {"error": "LookupError: no scripted response for alertmanager.getStatus {}"})  # a dispatch failure is data


async def test_a_large_result_is_digested_then_read_grepped_and_listed_from_the_cache():
    b = box([{"operation": "getAlerts", "result": alerts(80)}])
    digest = await b.call("call_operation", get_alerts())
    assert digest.startswith("Result call-1: alertmanager.getAlerts - 80 items") and "labels.alertname: A0 27" in digest
    window = await b.call("read_result", {"handle": "call-1", "limit": 2, "fields": ["labels.alertname"]})
    assert window.splitlines()[:2] == ['{"labels.alertname": "A0"}', '{"labels.alertname": "A1"}']
    hits = await b.call("grep_result", {"handle": "call-1", "pattern": "pod-7\"", "fields": ["labels.pod"]})
    assert hits == '{"labels.pod": "pod-7"}\n[1 matches]'
    assert (await b.call("list_results", {})).startswith("call-1: alertmanager.getAlerts filter=ns=\"ceph\" (80 items")
    assert len(b.source("alertmanager").calls) == 1  # read and grep hit the cache
    assert b.calls_made() == ['alertmanager.getAlerts filter=ns="ceph"']


async def test_repeating_a_call_is_refused_but_a_different_call_is_not():
    b = box(default=alerts(1))
    assert "Result" not in await b.call("call_operation", get_alerts())
    assert "already registered as call-1" in await b.call("call_operation", get_alerts())
    other = {"source": "prometheus", "operation": "query", "arguments": [{"name": "query", "value": "up"}]}
    assert "error" not in await b.call("call_operation", other)
    assert "error" not in await b.call("call_operation", get_alerts())  # second time, after another call: allowed
    assert "already registered" in await b.call("call_operation", get_alerts())  # third time: refused
    assert len(b.source("alertmanager").calls) == 2 and len(b.source("prometheus").calls) == 1


async def test_refresh_refetches_and_flags_a_changed_snapshot():
    state = {"n": 3}
    b = box()
    src = b.source("alertmanager")
    src.dispatch = _produce(src, lambda: alerts(state["n"]))
    assert "A0" in await b.call("call_operation", get_alerts())
    assert (await b.call("refresh_result", {"handle": "call-1"})).startswith("[unchanged")
    state["n"] = 90
    changed = await b.call("refresh_result", {"handle": "call-1"})
    assert changed.startswith(SNAPSHOT_REFRESHED_BANNER) and "90 items" in changed
    assert b.calls_made()[1:] == ['alertmanager.getAlerts filter=ns="ceph" (refreshed)'] * 2
    assert (await b.call("read_result", {"handle": "call-1", "limit": 1})).count("\n") == 1  # cache holds the new one


async def test_a_cache_miss_refetches_and_reports_a_change():
    b = box(default=alerts(5))
    await b.call("call_operation", get_alerts())
    b.register.cache.clear()
    b.source("alertmanager").default = alerts(6)
    text = await b.call("read_result", {"handle": "call-1", "limit": 1})
    assert text.startswith(SNAPSHOT_REFRESHED_BANNER) and b.calls_made()[-1].endswith("(re-fetched)")


async def test_a_source_preflight_can_stop_the_call():
    b = box()
    src = b.source("prometheus")

    async def preflight(op, arguments):
        return {"error": f"parser said no to {arguments['query']}"} if "{" in arguments.get("query", "") else None

    src.preflight = preflight
    bad = {"source": "prometheus", "operation": "query", "arguments": [{"name": "query", "value": "up{"}]}
    assert json.loads(await b.call("call_operation", bad)) == {"error": "parser said no to up{"}
    assert src.calls == []


async def test_timeouts_and_http_errors_are_described_not_raised():
    b = box(limits=Limits(call_timeout=0.01))
    src = b.source("alertmanager")

    async def slow(op, arguments):
        await asyncio.sleep(1)

    src.dispatch = slow
    assert json.loads(await b.call("call_operation", get_alerts()))["error"] == "timed out waiting for the source"

    async def forbidden(op, arguments):
        raise aiohttp.ClientResponseError(None, (), status=403, message="Forbidden: no token")

    src.dispatch = forbidden
    assert json.loads(await b.call("call_operation", {"source": "alertmanager", "operation": "getStatus"}))["error"] == "HTTP 403 Forbidden: no token"
    assert b.calls_made() == []


async def test_reset_and_scripted_fixture(tmp_path):
    b = box(default=alerts(1))
    await b.call("call_operation", get_alerts())
    b.reset()
    assert b.calls_made() == [] and "unknown handle" in await b.call("read_result", {"handle": "call-1"})

    path = tmp_path / "tools.json"
    path.write_text(json.dumps({"prometheus": {"catalog": PROM, "responses": [
        {"operation": "query", "arguments": {"query": "up"}, "result": {"data": 1}}]}}))
    loaded = Toolbox.scripted(path)
    assert loaded.sources == ["prometheus"]
    assert await loaded.call("call_operation", {"source": "prometheus", "operation": "query", "arguments": [{"name": "query", "value": "up"}]}) == '{\n  "data": 1\n}'
    assert Toolbox.scripted(tmp_path / "missing.json").sources == []


def _produce(src, produce):
    async def dispatch(op, arguments):
        src.calls.append((op.name, arguments))
        return produce()
    return dispatch
