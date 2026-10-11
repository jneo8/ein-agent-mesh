"""Investigator through the InMemoryBus: no Kafka, no LLM (investigate is replaced per test)."""

from datetime import UTC, datetime, timedelta

import pytest

from ein_mesh.config import InvestigatorSettings
from ein_mesh.roles import Investigator
from ein_mesh.schemas import REPORT_SUMMARY_MAX_BYTES, TASK_QUOTED_MAX_BYTES, Findings, Report, Task

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


def make(bus, topics, **kw):
    inv = Investigator(bus, topics, InvestigatorSettings(model="fake", timeout=kw.pop("timeout", 5), max_depth=kw.pop("max_depth", 3)))
    inv.clock = lambda: NOW
    return inv


def task(**kw) -> Task:
    base = dict(root_id="inc-1", role="investigator", intent="initial-investigation",
                deadline=(NOW + timedelta(minutes=15)).isoformat(), goal="Find the probable cause of: ceph down",
                quoted_data={"alerts": [{"id": "a1:t0", "alertname": "CephOSDDown"}]})
    return Task(**{**base, **kw})


async def test_task_becomes_a_done_report(bus, topics):
    inv = make(bus, topics)
    seen = []

    async def investigate(t):
        seen.append(t.render())
        return Findings(summary="osd.3 is down; PGs degraded follow", notes=["two OSD alerts - evidence: CephOSDDown x2"],
                        identifiers=["osd.3 - ceph node 2"], resources_checked=["quoted alerts"])

    inv.investigate = investigate
    t = task()
    await bus.produce(topics.tasks, t.root_id, t)
    await inv.run_until_idle()

    [rec] = bus.topics[topics.reports]
    assert rec.key == "inc-1"
    r = rec.value
    assert (r["status"], r["task_id"], r["root_id"], r["role"]) == ("done", t.task_id, "inc-1", "investigator")
    assert r["summary"].startswith("osd.3") and r["identifiers"] == ["osd.3 - ceph node 2"]
    assert "## Quoted data" in seen[0] and "CephOSDDown" in seen[0]


async def test_late_task_is_expired_without_a_model_call(bus, topics):
    inv = make(bus, topics)

    async def investigate(t):
        raise AssertionError("the model must not be asked about an expired task")

    inv.investigate = investigate
    await bus.produce(topics.tasks, "inc-1", task(deadline=(NOW - timedelta(seconds=1)).isoformat()))
    await inv.run_until_idle()
    [r] = bus.values(topics.reports)
    assert r["status"] == "expired" and "deadline" in r["error"] and r["summary"] == ""


async def test_model_failure_is_a_failed_report(bus, topics):
    inv = make(bus, topics)

    async def investigate(t):
        raise TimeoutError("model down")

    inv.investigate = investigate
    await bus.produce(topics.tasks, "inc-1", task())
    await inv.run_until_idle()
    [r] = bus.values(topics.reports)
    assert r["status"] == "failed" and r["error"].startswith("TimeoutError")


async def test_too_deep_is_refused(bus, topics):
    inv = make(bus, topics, max_depth=2)
    await bus.produce(topics.tasks, "inc-1", task(depth=3))
    await inv.run_until_idle()
    [r] = bus.values(topics.reports)
    assert r["status"] == "failed" and "depth 3 exceeds 2" in r["error"]


async def test_other_roles_tasks_are_skipped(bus, topics):
    inv = make(bus, topics)
    await bus.produce(topics.tasks, "inc-1", task(role="consultant"))
    await inv.run_until_idle()
    assert bus.values(topics.reports) == [] and not bus.pending()


async def test_bounds_on_task_and_report():
    with pytest.raises(ValueError, match="quoted_data"):
        task(quoted_data={"blob": "x" * (TASK_QUOTED_MAX_BYTES + 1)})
    with pytest.raises(ValueError, match="summary"):
        Findings(summary="x" * (REPORT_SUMMARY_MAX_BYTES + 1))
    with pytest.raises(ValueError, match="shorten notes"):
        Report(task_id="t", root_id="r", role="investigator", status="done", notes=["x" * 70_000])
