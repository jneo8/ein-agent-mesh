"""Arguments, the checks before dispatch (with every real call from a recorded investigation), and plain HTTP."""

import json
from pathlib import Path

import aiohttp
import pytest
import yaml
from utcp_http import _security

from ein_utcp import query
from ein_utcp.query import Argument, arguments_from, coerce_arguments, reject_invalid_query, validate_arguments
from ein_utcp.source import SPECS_DIR
from ein_utcp.sources import LokiSource, PrometheusSource

SCHEMA = {"properties": {"query": {"type": "string"}, "match[]": {"type": "array"}, "n": {"type": ["integer", "string"]}},
          "required": ["query"]}


def test_arguments_accept_pairs_or_a_mapping_and_stringify_values():
    assert arguments_from(None) == []
    assert arguments_from([{"name": "q", "value": "up"}, {"name": "n", "value": 5}]) == [Argument("q", "up"), Argument("n", "5")]
    assert arguments_from({"q": ["a", 1]}) == [Argument("q", '["a", 1]')]
    assert arguments_from("q=up") is None and arguments_from([{"value": "x"}]) is None


def test_coercion_only_touches_arrays():
    pairs = [Argument("query", 'up{a="1"}'), Argument("match[]", "up"), Argument("match[]", '["x","y"]'),
             Argument("n", "true"), Argument("n", "7")]
    assert coerce_arguments(pairs, SCHEMA["properties"]) == {"query": 'up{a="1"}', "match[]": ["up", "x", "y"], "n": "7"}
    assert coerce_arguments([Argument("a", "[not json")], None) == {"a": "[not json"}


@pytest.mark.parametrize("arguments, expected", [
    ({"query": "up"}, None),
    ({"query": "up", "n": "1"}, None),
    ({}, "missing required parameter(s): query"),
    ({"query": "up", "bogus": "1", "zzz": "2"}, "unknown parameter(s): bogus, zzz"),
])
def test_validation_names_the_problem_and_carries_the_schema(arguments, expected):
    verdict = validate_arguments("prometheus.query", arguments, SCHEMA)
    if expected is None:
        assert verdict is None
    else:
        assert expected in verdict["error"] and verdict["parameters"] is SCHEMA
    assert validate_arguments("x", {"anything": "1"}, {"properties": {}}) is None  # undeclared parameters: not judged


def shipped_schemas() -> dict[str, dict]:
    """``{operation: schema}`` for every GET operation in the shipped Prometheus and Loki specs."""
    schemas = {}
    for cls in (PrometheusSource, LokiSource):
        spec = yaml.safe_load((SPECS_DIR / cls.name / f"{cls.spec_version}.yaml").read_text())
        for operations in spec["paths"].values():
            shared = [p for p in operations.get("parameters", []) if isinstance(p, dict)]
            get = operations.get("get")
            if not isinstance(get, dict) or not get.get("operationId"):
                continue
            declared = shared + [p for p in get.get("parameters", []) if isinstance(p, dict)]
            name = get["operationId"] if get["operationId"].startswith(f"{cls.name}.") else f"{cls.name}.{get['operationId']}"
            schemas[name] = {"properties": {p["name"]: p.get("schema", {}) for p in declared},
                             "required": [p["name"] for p in declared if p.get("required")]}
    return schemas


def test_every_recorded_real_call_passes_validation_against_the_shipped_specs():
    calls = json.loads((Path(__file__).parent / "known_good_calls.json").read_text())
    schemas = shipped_schemas()
    assert len(calls) >= 40 and {c["tool_name"] for c in calls} <= set(schemas)
    assert [c for c in calls if validate_arguments(c["tool_name"], c["arguments"], schemas[c["tool_name"]])] == []


class Dispatcher:
    def __init__(self, raise_with=None):
        self.calls, self.raise_with = [], raise_with

    async def __call__(self, operation, arguments):
        self.calls.append((operation, arguments))
        if self.raise_with:
            raise self.raise_with
        return {"status": "success"}


def http_error(status, message=""):
    return aiohttp.ClientResponseError(None, (), status=status, message=message)


async def test_query_check_stops_only_on_a_parser_no():
    bad = Dispatcher(http_error(400, "parse error at 1:9"))
    verdict = await reject_invalid_query(bad, "prometheus.format-query", "prometheus.query", {"query": "up{"})
    assert "rejected by the server's parser" in verdict["error"] and "parse error at 1:9" in verdict["error"]
    assert bad.calls == [("prometheus.format-query", {"query": "up{"})]

    for d in (Dispatcher(), Dispatcher(http_error(503)), Dispatcher(ConnectionError("down"))):
        assert await reject_invalid_query(d, "loki.format-query", "loki.instant-query", {"query": "{app=x"}) is None
        assert d.calls[0][0] == "loki.format-query"  # fail-open, but the parser was asked

    skip = Dispatcher()
    assert await reject_invalid_query(skip, "prometheus.format-query", "prometheus.format-query", {"query": "up"}) is None
    assert await reject_invalid_query(skip, "prometheus.format-query", "prometheus.labels", {"match[]": ["up"]}) is None
    assert skip.calls == []  # the validator itself, or no query: no round trip


# --- plain HTTP ---


@pytest.fixture
def clean_allowlist():
    query.uninstall_plain_http_override()
    query.clear_plain_http_allowlist()
    yield
    query.uninstall_plain_http_override()
    query.clear_plain_http_allowlist()


def test_allowlist_is_keyed_by_host_and_port_with_scheme_defaults(clean_allowlist):
    assert query.allow_plain_http_for_url("http://10.0.0.5/cos-prometheus-0") == ("10.0.0.5", 80)
    assert query.allow_plain_http_for_url("http://h:9090/x") == ("h", 9090)
    assert query.allow_plain_http_for_url("file:///spec.yaml") is None
    assert query.is_plain_http_allowed("http://10.0.0.5/anything")
    assert not query.is_plain_http_allowed("http://10.0.0.5:8080/")  # port mismatch
    assert not query.is_plain_http_allowed("http://10.0.0.6/")
    assert not query.is_plain_http_allowed("https://10.0.0.5/")  # https never consults the allowlist


def test_override_reaches_ensure_secure_url_and_can_be_undone(clean_allowlist):
    url, other = "http://10.0.0.5/cos-prometheus-0", "http://10.0.0.6/"
    with pytest.raises(ValueError):
        _security.ensure_secure_url(url)
    assert query.install_plain_http_override() and query.install_plain_http_override()  # idempotent
    query.allow_plain_http_for_url(url)
    _security.ensure_secure_url(url)  # the protocols call this; it resolves is_secure_url via module globals
    with pytest.raises(ValueError):
        _security.ensure_secure_url(other)
    assert _security.is_secure_url("https://anything/")
    query.uninstall_plain_http_override()
    with pytest.raises(ValueError):
        _security.ensure_secure_url(url)


def test_install_fails_closed_without_the_patch_point(clean_allowlist, monkeypatch):
    monkeypatch.delattr(_security, "is_secure_url")
    assert query.install_plain_http_override() is False


def test_drift_guard_the_protocols_still_consult_the_module_global():
    from utcp_http import http_communication_protocol

    assert http_communication_protocol.ensure_secure_url is _security.ensure_secure_url
    assert "is_secure_url" in _security.ensure_secure_url.__code__.co_names
