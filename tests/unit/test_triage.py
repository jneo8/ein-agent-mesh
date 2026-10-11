"""Triage through the InMemoryBus: no Kafka, no LLM (classify is replaced per test). Decisions and their
reasons are read back from alerts.state, the role's memory."""

import re
from datetime import UTC, datetime, timedelta

import pytest

from ein_mesh.config import TriageSettings
from ein_mesh.schemas import Attach, Ignore, Open, TriageOutput
from ein_mesh.roles import Triage

from .conftest import make_alert

pytestmark = pytest.mark.asyncio


def make(bus, topics, **ts):
    return Triage(bus, topics, TriageSettings(model="fake", batch_size=ts.pop("batch_size", 3), batch_seconds=30, **ts))


def state(bus, topics, cluster="dev"):
    """The latest alerts.state record of a cluster."""
    return [s for s in bus.values(topics.alerts_state) if s["cluster"] == cluster][-1]


async def test_fallback_opens_one_incident_per_namespace(bus, topics):
    t = make(bus, topics)

    async def boom(cluster, alerts):
        raise TimeoutError("llm down")

    t.classify = boom
    for a in (make_alert("a1"), make_alert("a2", ns="temporal", name="KubePodCrashLooping"), make_alert("a3", ns="ceph")):
        await bus.produce(topics.alerts, "dev", a)
    await t.run_until_idle()

    incidents = bus.values(topics.incidents("critical"))
    assert len(incidents) == 2
    assert sorted(i["cluster"] + "/" + i["alerts"][0]["namespace"] for i in incidents) == ["dev/ceph", "dev/temporal"]
    assert all(re.fullmatch(r"inc-\d{8}T\d{6}Z-[0-9a-f]{10}", i["root_id"]) for i in incidents)
    st = state(bus, topics)
    assert len(st["incidents"]) == 2 and st["schema_version"] == 1
    assert all(i["how"] == "fallback" and i["reason"].startswith("fallback") for i in st["incidents"])


async def test_llm_decisions_are_applied_and_recorded(bus, topics):
    t = make(bus, topics)

    async def classify(cluster, alerts):
        ids = [a.id for a in alerts]
        return TriageOutput(decisions=[
            Open(action="open", alerts=ids[:2], priority="normal", title="ceph degraded", reason="same osd"),
            Ignore(action="ignore", alerts=ids[2:], reason="watchdog"),
        ])

    t.classify = classify
    for a in (make_alert("a1"), make_alert("a2"), make_alert("w", ns="cos", name="Watchdog", severity="none")):
        await bus.produce(topics.alerts, "dev", a)
    await t.run_until_idle()

    assert [i["title"] for i in bus.values(topics.incidents("normal"))] == ["ceph degraded"]
    st = state(bus, topics)
    [inc] = st["incidents"]
    assert (inc["how"], inc["reason"]) == ("llm", "same osd")
    assert list(st["ignored"]) == ["w"] and st["ignored"]["w"]["alerts"] == ["w:t0"]
    assert (st["ignored"]["w"]["how"], st["ignored"]["w"]["reason"]) == ("llm", "watchdog")


async def test_invalid_llm_output_falls_back(bus, topics):
    """An attach toward an incident that does not exist fails validation and the fallback decides."""
    t = make(bus, topics)

    async def classify(cluster, alerts):
        return TriageOutput(decisions=[Attach(action="attach", alerts=[a.id for a in alerts], incident="inc-nope", reason="x")])

    t.classify = classify
    for a in (make_alert("a1"), make_alert("a2"), make_alert("a3")):
        await bus.produce(topics.alerts, "dev", a)
    await t.run_until_idle()
    assert [i["how"] for i in state(bus, topics)["incidents"]] == ["fallback"]


async def test_time_based_flush_and_resolved_skipped(bus, topics):
    """Empty polls advance the fake clock past ``batch_seconds``, so one alert is flushed alone; the
    resolved alert never reaches the model."""
    t = make(bus, topics, batch_size=50)
    seen = []

    async def classify(cluster, alerts):
        seen.append([a.id for a in alerts])
        return TriageOutput(decisions=[Open(action="open", alerts=[a.id for a in alerts], priority="normal", title="x", reason="r")])

    t.classify = classify
    await bus.produce(topics.alerts, "dev", make_alert("a1"))
    await bus.produce(topics.alerts, "dev", make_alert("r1", status="resolved"))
    await t.run_until_idle()
    assert seen == [["a1:t0"]]


async def test_same_group_attaches_by_rule_without_the_model(bus, topics):
    """Second poll: an alert whose group already has one open incident (a4, group dev/ceph) never reaches
    classify; only the new temporal group does."""
    t = make(bus, topics)
    calls = []

    async def classify(cluster, alerts):
        calls.append([a.id for a in alerts])
        return TriageOutput(decisions=[Open(action="open", alerts=[a.id for a in alerts], priority="critical", title="ceph", reason="r")])

    t.classify = classify
    for a in (make_alert("a1"), make_alert("a2"), make_alert("a3")):
        await bus.produce(topics.alerts, "dev", a)
    await t.run_until_idle()
    root = bus.values(topics.incidents("critical"))[0]["root_id"]

    await bus.produce(topics.alerts, "dev", make_alert("a4"))
    await bus.produce(topics.alerts, "dev", make_alert("b1", ns="temporal", name="KubePodCrashLooping"))
    await bus.produce(topics.alerts, "dev", make_alert("b2", ns="temporal", name="KubePodCrashLooping"))
    await bus.produce(topics.alerts, "dev", make_alert("b3", ns="temporal", name="KubePodCrashLooping"))
    await t.run_until_idle()

    assert calls == [["a1:t0", "a2:t0", "a3:t0"], ["b1:t0", "b2:t0", "b3:t0"]], "a4 was attached by rule, only the new group was classified"
    incidents = {i["id"]: i for i in state(bus, topics)["incidents"]}
    [att] = incidents[root]["attached"]
    assert (att["how"], att["alerts"], att["reason"]) == ("rule", ["a4:t0"], "same group as the open incident")
    assert sorted(i["group_key"] for i in incidents.values()) == ["cluster=dev|namespace=ceph", "cluster=dev|namespace=temporal"]


async def test_model_may_attach_only_among_candidates(bus, topics):
    """Two open incidents in one group: new same-group alerts go to the model with exactly those two as
    candidates; attaching to anything else is rejected and falls back. The first batch splits one group
    into two incidents to create that situation."""
    t = make(bus, topics, batch_size=2)
    prompts = []

    async def classify(cluster, alerts):
        prompts.append(sorted(t.candidate_incidents(cluster, alerts)))
        ids = [a.id for a in alerts]
        if len(ids) == 2 and ids[0].startswith("n1:"):
            return TriageOutput(decisions=[Open(action="open", alerts=[ids[0]], priority="critical", title="node-7", reason="r"),
                                           Open(action="open", alerts=[ids[1]], priority="normal", title="node-3", reason="r")])
        return TriageOutput(decisions=[Attach(action="attach", alerts=ids, incident="inc-not-a-candidate", reason="x")])

    t.classify = classify
    await bus.produce(topics.alerts, "dev", make_alert("n1", ns="kube-system", name="KubeNodeNotReady"))
    await bus.produce(topics.alerts, "dev", make_alert("n2", ns="kube-system", name="NodeClockSkewDetected", severity="warning"))
    await t.run_until_idle()
    opened = sorted(i["root_id"] for i in bus.values(topics.incidents("critical")) + bus.values(topics.incidents("normal")))

    await bus.produce(topics.alerts, "dev", make_alert("n3", ns="kube-system", name="KubeNodeUnreachable"))
    await bus.produce(topics.alerts, "dev", make_alert("n4", ns="kube-system", name="KubeNodeUnreachable"))
    await t.run_until_idle()

    assert prompts[0] == [] and prompts[1] == opened, "ambiguous group: both incidents offered as candidates"
    attached = [(i["id"], a) for i in state(bus, topics)["incidents"] for a in i["attached"]]
    [(target, att)] = attached
    assert target in opened and att["how"] == "fallback" and sorted(att["alerts"]) == ["n3:t0", "n4:t0"]


async def test_restore_rebuilds_open_table(bus, topics):
    t = make(bus, topics)

    async def classify(cluster, alerts):
        return TriageOutput(decisions=[Open(action="open", alerts=[a.id for a in alerts], priority="normal", title="first", reason="r")])

    t.classify = classify
    for a in (make_alert("a1"), make_alert("a2"), make_alert("a3")):
        await bus.produce(topics.alerts, "dev", a)
    await t.run_until_idle()
    root = bus.values(topics.incidents("normal"))[0]["root_id"]

    fresh = make(bus, topics)
    await fresh.restore()
    assert list(fresh.open["dev"]) == [root]


async def test_ignored_entries_expire_and_attachments_are_bounded(bus, topics):
    """alerts.state is one record per cluster rewritten whole, so both tables are bounded. Three more
    same-group alerts attach by rule one poll each and only the last two attachments survive; a noise
    entry aged past the TTL is dropped at the next snapshot and a restart no longer knows it."""
    t = make(bus, topics, batch_size=1, max_attachments=2, ignored_ttl_days=1)

    async def classify(cluster, alerts):
        ids = [a.id for a in alerts]
        if alerts[0].alertname == "Watchdog":
            return TriageOutput(decisions=[Ignore(action="ignore", alerts=ids, reason="heartbeat")])
        return TriageOutput(decisions=[Open(action="open", alerts=ids, priority="normal", title="x", reason="r")])

    t.classify = classify
    await bus.produce(topics.alerts, "dev", make_alert("w", ns="cos", name="Watchdog", severity="none"))
    await bus.produce(topics.alerts, "dev", make_alert("a1"))
    await t.run_until_idle()
    assert "w" in state(bus, topics)["ignored"]

    for n in ("a2", "a3", "a4"):
        await bus.produce(topics.alerts, "dev", make_alert(n))
        await t.run_until_idle()
    [inc] = state(bus, topics)["incidents"]
    assert len(inc["alert_ids"]) == 4 and [a["alerts"] for a in inc["attached"]] == [["a3:t0"], ["a4:t0"]]

    t.ignored["dev"]["w"].at = (datetime.now(UTC) - timedelta(days=2)).isoformat()
    await t.snapshot("dev")
    assert state(bus, topics)["ignored"] == {}
    fresh = make(bus, topics)
    await fresh.restore()
    assert fresh.ignored["dev"] == {}
