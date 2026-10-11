"""Topic layout settings: one partition count for every topic, and the check roles run at startup."""

import pytest

from ein_mesh.config import Topics


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

    counts[t.events] = 8
    counts[t.alerts] = None
    errors = t.layout_errors(counts)
    assert len(errors) == 2
    assert any(e.startswith("t.alerts: missing") for e in errors)
    assert any(e.startswith("t.investigation.events: 8 partitions") for e in errors)


def test_config_drift_compares_against_the_layout():
    t = Topics(prefix="t.")
    compact = {"cleanup.policy": "compact"}
    assert t.config_drift(t.triage_state, compact) == {}
    assert t.config_drift(t.alerts, {}) == {}

    # value differs, key missing, key set on the broker that the layout does not want
    assert t.config_drift(t.triage_state, {"cleanup.policy": "delete"}) == {"cleanup.policy": ("delete", "compact")}
    assert t.config_drift(t.triage_state, {}) == {"cleanup.policy": (None, "compact")}
    assert t.config_drift(t.alerts, {"retention.ms": "1000"}) == {"retention.ms": ("1000", None)}
    assert t.config_drift(t.events, {"retention.ms": "1000", "segment.bytes": "5"}) == {
        "retention.ms": ("1000", str(30 * 24 * 3600 * 1000)),
        "segment.bytes": ("5", None),
    }

