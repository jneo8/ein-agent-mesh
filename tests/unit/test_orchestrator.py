from datetime import UTC, datetime, timedelta

import pytest

from ein_mesh.config import OrchestratorSettings
from ein_mesh.roles import Orchestrator
from ein_mesh.schemas import CONTEXT_MAX_BYTES, IncidentMessage, IncidentState, Report

from .conftest import make_alert

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


def incident(root="inc-abc", priority="critical"):
    return IncidentMessage(root_id=root, cluster="dev", priority=priority, title="ceph down", reason="r",
                           alerts=[make_alert("a1")])


def make(bus, topics, attempts=2, deadline=900):
    o = Orchestrator(bus, topics, OrchestratorSettings(task_deadline_seconds=deadline, task_max_attempts=attempts))
    o.clock = lambda: NOW
    return o


def report(task_id, status="done", **kw):
    return Report(task_id=task_id, root_id="inc-abc", role="investigator", status=status, **kw)


def last_state(bus, topics, root="inc-abc"):
    return [s for s in bus.values(topics.incidents_state) if s["root_id"] == root][-1]


async def test_incident_becomes_investigating(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    [state] = bus.values(topics.incidents_state)
    assert state["root_id"] == "inc-abc" and state["status"] == "investigating"
    assert (state["cluster"], state["priority"], state["title"], state["schema_version"]) == ("dev", "critical", "ceph down", 1)
    assert state["alerts"] == [{"id": "a1:t0", "alertname": "CephOSDDown", "severity": "critical", "namespace": "ceph"}]


async def test_replayed_incident_is_skipped(bus, topics):
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await Orchestrator(bus, topics).run_until_idle()
    assert len(bus.values(topics.incidents_state)) == 1


async def test_restore_then_skip(bus, topics):
    """A new process on the same topics restores the table and treats the replayed incident as known."""
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    o = Orchestrator(bus, topics)
    await o.restore()
    assert o.state["inc-abc"].status == "investigating"
    await o.on_incident(incident())
    assert len(bus.values(topics.incidents_state)) == 1, "a replay after restore writes nothing"
    assert [t.status for t in o.state["inc-abc"].tasks] == ["issued"], "tasks survive the restore"


async def test_new_incident_issues_one_investigator_task(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await make(bus, topics).run_until_idle()

    [rec] = bus.topics[topics.tasks]
    t = rec.value
    assert rec.key == "inc-abc" and t["role"] == "investigator" and t["intent"] == "initial-investigation"
    assert (t["attempt"], t["parent_id"], t["depth"]) == (1, None, 1)
    assert t["quoted_data"]["alerts"][0]["id"] == "a1:t0" and t["goal"].endswith("ceph down")
    assert t["deadline"] == (NOW + timedelta(seconds=900)).isoformat()
    st = last_state(bus, topics)
    assert [(x["task_id"], x["status"]) for x in st["tasks"]] == [(t["task_id"], "issued")]
    assert "Open tasks:" in st["context"] and t["task_id"] in st["context"]


async def test_report_settles_the_task_and_fills_context(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    o = make(bus, topics)
    await o.run_until_idle()
    task_id = bus.values(topics.tasks)[0]["task_id"]

    await bus.produce(topics.reports, "inc-abc", report(task_id, summary="osd.3 down, PGs degraded; high confidence",
                                                        identifiers=["osd.3 - ceph node 2"], resources_checked=["quoted alerts"]))
    await o.run_until_idle()
    st = last_state(bus, topics)
    assert st["tasks"][0]["status"] == "done" and st["tasks"][0]["report_at"]
    assert "osd.3 down" in st["context"] and "osd.3 - ceph node 2" in st["context"] and "Open tasks:" not in st["context"]
    assert len(st["context"].encode()) <= CONTEXT_MAX_BYTES and st["status"] == "investigating"

    writes = len(bus.values(topics.incidents_state))
    await bus.produce(topics.reports, "inc-abc", report(task_id, summary="again"))
    await o.run_until_idle()
    assert len(bus.values(topics.incidents_state)) == writes, "a duplicate report changes nothing"


async def test_expired_task_is_reissued_then_exhausted(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    o = make(bus, topics, attempts=2, deadline=60)
    await o.run_until_idle()
    first = bus.values(topics.tasks)[0]

    o.clock = lambda: NOW + timedelta(seconds=61)
    assert await o.handle([]) is True, "an empty poll that expires a task is a change worth committing"
    tasks = bus.values(topics.tasks)
    assert len(tasks) == 2 and tasks[1]["parent_id"] == first["task_id"] and tasks[1]["attempt"] == 2
    assert "Previous attempt" in tasks[1]["context"] and tasks[1]["intent"] == "retry"
    st = last_state(bus, topics)
    assert [x["status"] for x in st["tasks"]] == ["expired", "issued"]

    o.clock = lambda: NOW + timedelta(seconds=200)
    await o.handle([])
    st = last_state(bus, topics)
    assert [x["status"] for x in st["tasks"]] == ["expired", "exhausted"] and len(bus.values(topics.tasks)) == 2


async def test_late_report_overrides_expired(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    o = make(bus, topics, deadline=60)
    await o.run_until_idle()
    first = bus.values(topics.tasks)[0]["task_id"]
    o.clock = lambda: NOW + timedelta(seconds=61)
    await o.handle([])
    assert last_state(bus, topics)["tasks"][0]["status"] == "expired"

    await bus.produce(topics.reports, "inc-abc", report(first, summary="late but useful"))
    await o.run_until_idle()
    st = last_state(bus, topics)
    assert st["tasks"][0]["status"] == "done" and "late but useful" in st["context"]


async def test_failed_report_triggers_a_retry(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    o = make(bus, topics)
    await o.run_until_idle()
    first = bus.values(topics.tasks)[0]["task_id"]
    await bus.produce(topics.reports, "inc-abc", report(first, status="failed", error="TimeoutError: model down"))
    await o.run_until_idle()
    tasks = bus.values(topics.tasks)
    assert len(tasks) == 2 and tasks[1]["parent_id"] == first and "model down" in tasks[1]["context"]


async def test_context_is_bounded():
    """The limit is in UTF-8 bytes, not characters."""
    base = dict(root_id="inc-x", cluster="dev", priority="critical", title="t", status="investigating", opened_at="0")
    ok = IncidentState(**base, context="a" * CONTEXT_MAX_BYTES)
    assert len(ok.context) == CONTEXT_MAX_BYTES
    with pytest.raises(ValueError, match="summarise"):
        IncidentState(**base, context="a" * (CONTEXT_MAX_BYTES + 1))
    with pytest.raises(ValueError):
        IncidentState(**base, context="\u6e2c" * (CONTEXT_MAX_BYTES // 3 + 1))
