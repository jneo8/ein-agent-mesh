import pytest

from ein_mesh.roles import Orchestrator
from ein_mesh.schemas import CONTEXT_MAX_BYTES, IncidentMessage, IncidentState

from .conftest import make_alert

pytestmark = pytest.mark.asyncio


def incident(root="inc-abc", priority="critical"):
    return IncidentMessage(root_id=root, cluster="dev", priority=priority, title="ceph down", reason="r",
                           alerts=[make_alert("a1")])


async def test_incident_becomes_investigating(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    [state] = bus.values(topics.incidents_state)
    assert state["root_id"] == "inc-abc" and state["status"] == "investigating"
    assert (state["cluster"], state["priority"], state["title"], state["schema_version"]) == ("dev", "critical", "ceph down", 1)


async def test_replayed_incident_is_skipped(bus, topics):
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await bus.produce(topics.incidents("normal"), "inc-abc", incident(priority="normal"))
    await Orchestrator(bus, topics).run_until_idle()
    assert len(bus.values(topics.incidents_state)) == 1


async def test_restore_then_skip(bus, topics):
    await bus.produce(topics.incidents("critical"), "inc-abc", incident())
    await Orchestrator(bus, topics).run_until_idle()

    fresh_bus_view = bus  # same topics, new process
    o = Orchestrator(fresh_bus_view, topics)
    await o.restore()
    assert o.state["inc-abc"].status == "investigating"
    await o.on_incident(incident())
    assert len(bus.values(topics.incidents_state)) == 1, "a replay after restore writes nothing"


async def test_context_is_bounded():
    base = dict(root_id="inc-x", cluster="dev", priority="critical", title="t", status="investigating", opened_at="0")
    ok = IncidentState(**base, context="a" * CONTEXT_MAX_BYTES)
    assert len(ok.context) == CONTEXT_MAX_BYTES
    with pytest.raises(ValueError, match="summarise"):
        IncidentState(**base, context="a" * (CONTEXT_MAX_BYTES + 1))
    with pytest.raises(ValueError):  # the limit is in bytes, not characters
        IncidentState(**base, context="\u6e2c" * (CONTEXT_MAX_BYTES // 3 + 1))
