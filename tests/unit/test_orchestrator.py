import pytest

from ein_mesh.roles import Orchestrator
from ein_mesh.schemas import IncidentMessage

from .conftest import make_alert

pytestmark = pytest.mark.asyncio


def incident(root="inc-abc", priority="critical"):
    return IncidentMessage(root_id=root, cluster="dev", priority=priority, title="ceph down", reason="r",
                           alerts=[make_alert("a1")])


async def test_incident_becomes_investigating(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    [state] = bus.values(topics.investigation_state)
    assert state["root_id"] == "inc-abc" and state["status"] == "investigating"
    [ev] = bus.values(topics.events)
    assert ev["kind"] == "investigation.started" and ev["role"] == "orchestrator" and ev["body"]["alerts"] == ["a1:t0"]


async def test_replayed_incident_is_skipped(bus, topics):
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await Orchestrator(bus, topics).run_until_idle()
    assert len(bus.values(topics.investigation_state)) == 1
    assert len(bus.values(topics.events)) == 1


async def test_restore_then_skip(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    fresh_bus_view = bus  # same topics, new process
    o = Orchestrator(fresh_bus_view, topics)
    await o.restore()
    assert o.state["inc-abc"].status == "investigating"
    await o.on_incident(incident())
    assert len(bus.values(topics.events)) == 1
