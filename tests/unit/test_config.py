"""Topic layout settings: one partition count for every topic, and the check roles run at startup."""

import pytest

from ein_mesh.config import ToolSettings, Topics


def test_defaults():
    t = Topics(prefix="t.")
    assert t.partitions == 12 and t.replication == 1


def test_env_overrides(monkeypatch):
    monkeypatch.setenv("TOPIC_PARTITIONS", "6")
    monkeypatch.setenv("TOPIC_REPLICATION", "3")
    t = Topics(prefix="t.")
    assert t.partitions == 6 and t.replication == 3
    monkeypatch.setenv("TOPIC_PARTITIONS", "0")
    with pytest.raises(ValueError):
        Topics(prefix="t.")


def test_layout_errors_name_missing_and_mismatched_topics():
    t = Topics(prefix="t.", partitions=12)
    counts = {name: 12 for name in t.all()}
    assert t.layout_errors(counts) == []

    counts[t.incidents_state] = 8
    counts[t.alerts] = None
    errors = t.layout_errors(counts)
    assert len(errors) == 2
    assert any(e.startswith("t.alerts: missing") for e in errors)
    assert any(e.startswith("t.incidents.state: 8 partitions") for e in errors)


def test_layout_is_two_kinds_of_topic():
    """Flow topics are plain; state topics are compacted; short names resolve; stale topics are those
    under the prefix that the layout does not name."""
    t = Topics(prefix="t.")
    assert set(t.all()) == {"t.alerts", "t.alerts.state", "t.incidents.critical", "t.incidents.normal",
                            "t.incidents.state", "t.incidents.tasks", "t.incidents.reports"}
    assert {n for n, c in t.all().items() if c.get("cleanup.policy") == "compact"} == {"t.alerts.state", "t.incidents.state"}
    assert t.resolve("incidents.tasks") == "t.incidents.tasks" and t.resolve("other") == "other"
    assert t.stale(["t.triage.state", "t.alerts", "x.alerts", "t.investigation.events"]) == ["t.investigation.events", "t.triage.state"]


def test_config_drift_compares_against_the_layout():
    """Drift is a value that differs, a wanted key that is missing, or a key set on the broker that the
    layout does not want."""
    t = Topics(prefix="t.")
    compact = {"cleanup.policy": "compact"}
    assert t.config_drift(t.alerts_state, compact) == {}
    assert t.config_drift(t.alerts, {}) == {}

    assert t.config_drift(t.alerts_state, {"cleanup.policy": "delete"}) == {"cleanup.policy": ("delete", "compact")}
    assert t.config_drift(t.alerts_state, {}) == {"cleanup.policy": (None, "compact")}
    assert t.config_drift(t.alerts, {"retention.ms": "1000"}) == {"retention.ms": ("1000", None)}
    assert t.config_drift(t.incidents_state, {"cleanup.policy": "compact", "segment.bytes": "5"}) == {
        "segment.bytes": ("5", None),
    }



def test_tool_settings_offer_only_the_sources_with_a_url(monkeypatch):
    assert ToolSettings().sources() == [] and ToolSettings().limits().call_timeout == 60
    monkeypatch.setenv("UTCP_ALERTMANAGER_URL", "http://127.0.0.1:9093")
    monkeypatch.setenv("UTCP_PROMETHEUS_URL", "https://cos.example/cos-prometheus-0")
    monkeypatch.setenv("UTCP_PROMETHEUS_TOKEN", "t0k")
    monkeypatch.setenv("TOOL_CALL_TIMEOUT", "5")
    monkeypatch.setenv("UTCP_ALLOW_HTTP", "true")
    settings = ToolSettings()
    assert [(s.name, s.url, s.token) for s in settings.sources()] == [
        ("prometheus", "https://cos.example/cos-prometheus-0", "t0k"), ("alertmanager", "http://127.0.0.1:9093", "")]
    assert settings.allow_http and settings.limits().call_timeout == 5
    monkeypatch.setenv("UTCP_LOKI_URL", "loki:3100")
    with pytest.raises(ValueError):
        ToolSettings().sources()
