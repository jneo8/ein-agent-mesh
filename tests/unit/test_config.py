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
