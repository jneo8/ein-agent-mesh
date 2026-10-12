"""The three shipped sources load as read-only operations; each reads its own results."""

import pytest

from ein_utcp import SourceConfig, Toolbox, make_source
from ein_utcp.sources import SOURCES, AlertmanagerSource, LokiSource, PrometheusSource

READS = {"prometheus": ["prometheus.query", "prometheus.query-range", "prometheus.series", "prometheus.label-values",
                        "prometheus.alerts", "prometheus.get-targets", "prometheus.rules"],
         "loki": ["loki.instant-query", "loki.range-query", "loki.list-labels", "loki.label-values", "loki.series"],
         "alertmanager": ["alertmanager.getAlerts", "alertmanager.getAlertGroups", "alertmanager.getSilences",
                          "alertmanager.getSilence", "alertmanager.getReceivers", "alertmanager.getStatus"]}
WRITES = {"prometheus": ["delete-series", "snapshot", "clean-tombstones", "write"],
          "alertmanager": ["postAlerts", "postSilences", "deleteSilence"], "loki": []}
PREFIX = {"prometheus": "/cos-prometheus-0/api/v1/", "loki": "/cos-loki-0/", "alertmanager": "/cos-alertmanager/api/v2/"}


@pytest.fixture(scope="module")
def box():
    return Toolbox.from_configs([SourceConfig(name, f"http://localhost:1/cos-{name}{'' if name == 'alertmanager' else '-0'}")
                                 for name in READS])


@pytest.mark.parametrize("name", list(READS))
def test_reads_are_offered_writes_are_not_and_the_proxy_prefix_is_kept(box, name):
    source = box.source(name)
    names = set(source.operations())
    assert set(READS[name]) <= names
    assert not any(w in n for n in names for w in WRITES[name])
    assert source.http_methods() == {"GET"}
    paths = [line.split(" | ")[1] for line in source.catalog()]
    assert all(p.startswith(PREFIX[name]) for p in paths), paths
    assert source.operation(READS[name][0]).name == READS[name][0] and source.operation("nope") is None


async def test_a_dead_endpoint_is_an_error_payload_not_an_exception(box):
    assert box.sources == ["alertmanager", "loki", "prometheus"]
    assert [len(box.catalog(s)) for s in box.sources] == [6, 8, 25]
    text = await box.call("call_operation", {"source": "alertmanager", "operation": "getAlerts"})
    assert text.startswith('{"error": "ClientConnectorError') and box.calls_made() == []


def test_plain_http_to_a_remote_host_needs_allow_http():
    with pytest.raises(ValueError, match="allow_http"):
        make_source(SourceConfig("loki", "http://10.0.0.5/loki"))
    source = make_source(SourceConfig("loki", "http://10.0.0.5:1/loki", token="t0k"), allow_http=True)
    assert source.operation("loki.ready").transport.headers == {"Authorization": "Bearer t0k"}


@pytest.mark.parametrize("name, url", [("grafana", "http://h/"), ("loki", "h:3100"), ("loki", "ftp://h/")])
def test_unknown_sources_and_bad_urls_are_refused(name, url):
    with pytest.raises(ValueError):
        make_source(SourceConfig(name, url))
    assert set(SOURCES) == {"prometheus", "loki", "alertmanager"}


def test_a_config_may_pick_a_shipped_spec_version():
    assert LokiSource(SourceConfig("loki", "http://localhost:1")).spec_path().name == "3.yaml"
    with pytest.raises(ValueError, match="cannot read spec"):
        LokiSource(SourceConfig("loki", "http://localhost:1", version="99")).operations()


def test_each_source_knows_where_its_records_are():
    rows = [{"metric": {"job": "x"}, "value": [1, "1"]}, {"metric": {"job": "y"}, "value": [1, "0"]}]
    prom = PrometheusSource(SourceConfig("prometheus", "http://localhost:1"))
    assert prom.records({"status": "success", "data": {"resultType": "vector", "result": rows}}) == rows
    assert prom.records({"status": "success", "data": ["up", "node_load1"]}) is None  # label values: scalars
    loki = LokiSource(SourceConfig("loki", "http://localhost:1"))
    assert loki.records({"status": "success", "data": {"resultType": "streams", "result": rows}}) == rows
    assert loki.records({"status": "success", "data": [{"app": "a"}, {"app": "b"}]}) == [{"app": "a"}, {"app": "b"}]  # series
    am = AlertmanagerSource(SourceConfig("alertmanager", "http://localhost:1"))
    assert am.records([{"labels": {}}, {"labels": {}}]) == [{"labels": {}}, {"labels": {}}]
    assert am.records({"cluster": {"status": "ready"}}) is None
    assert prom.decode({"x": 1}) == {"x": 1}
