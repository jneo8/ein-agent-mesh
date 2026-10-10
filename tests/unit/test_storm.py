"""The recorded storm end to end: Alertmanager payload -> gateway mapping -> triage with the LLM
mocked at the litellm boundary -> incidents, events, state. No broker, no network."""

import json
from types import SimpleNamespace

import pytest

from ein_mesh.config import TriageSettings
from ein_mesh.integrators import to_alert
from ein_mesh.roles import Orchestrator, Triage
from ein_mesh.roles import triage as triage_mod

from .conftest import storm_payload

pytestmark = pytest.mark.asyncio

# What a well-behaved triage says about this storm under the under-merge policy: Ceph and temporal stay
# separate (no label ties them), TargetDown is its own normal incident, noise is ignored.
def model_answer(prompt_user: str) -> str:
    """Decisions for whatever subset of the storm is in this batch; empty groups are left out."""
    batch = json.loads(prompt_user.split("## Quoted data: alerts in this batch\n", 1)[1])
    by_name: dict[str, list[str]] = {}
    for a in batch:
        by_name.setdefault(a["alertname"], []).append(a["id"])
    ceph = sum((by_name.get(n, []) for n in ("CephOSDDown", "CephPGDegraded", "CephClusterWarningState")), [])
    temporal = sum((by_name.get(n, []) for n in ("KubePersistentVolumeFillingUp", "KubePodCrashLooping")), [])
    decisions = []
    if ceph:
        decisions.append({"action": "open", "alerts": ceph, "priority": "critical", "title": "ceph-2 OSDs down, RBD degraded",
                          "reason": "two OSDs on ceph-2 down; PG degradation follows"})
    if temporal:
        decisions.append({"action": "open", "alerts": temporal, "priority": "critical", "title": "temporal pods crash-looping, PVC full",
                          "reason": "same namespace; database volume and dependent pods"})
    if by_name.get("TargetDown"):
        decisions.append({"action": "open", "alerts": by_name["TargetDown"], "priority": "normal", "title": "node-exporter targets down",
                          "reason": "unrelated scrape failures"})
    noise = by_name.get("Watchdog", []) + by_name.get("KubeHpaMaxedOut", [])
    if noise:
        decisions.append({"action": "ignore", "alerts": noise, "reason": "noise"})
    return json.dumps({"decisions": decisions})


@pytest.fixture
def fake_llm(monkeypatch):
    calls = []

    async def acompletion(model, messages, **kw):
        calls.append(messages)
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=model_answer(messages[1]["content"])))])

    monkeypatch.setattr(triage_mod.litellm, "acompletion", acompletion)
    return calls


async def test_storm_through_gateway_mapping_triage_and_orchestrator(bus, topics, fake_llm):
    for alert in storm_payload()["alerts"]:
        t = to_alert(alert)
        await bus.produce(topics.alerts, t.cluster, t)

    await Triage(bus, topics, TriageSettings(model="fake", batch_size=50, batch_seconds=30)).run_until_idle()

    assert len(fake_llm) == 1, "one LLM call for the whole batch"
    assert fake_llm[0][0]["role"] == "system" and "Quoted data" in fake_llm[0][1]["content"]
    critical = bus.values(topics.incidents("critical"))
    normal = bus.values(topics.incidents("normal"))
    assert len(critical) == 2 and sorted(len(c["alerts"]) for c in critical) == [3, 4]
    assert len(normal) == 1 and normal[0]["title"] == "node-exporter targets down"
    kinds = sorted(e["kind"] for e in bus.values(topics.events))
    assert kinds == ["alerts.ignored", "incident.opened", "incident.opened", "incident.opened"]
    assert all(e["body"]["how"] == "llm" for e in bus.values(topics.events))

    await Orchestrator(bus, topics).run_until_idle()
    states = {s["root_id"]: s["status"] for s in bus.values(topics.investigation_state)}
    assert states == {c["root_id"]: "investigating" for c in critical + normal}


async def test_second_wave_attaches_by_rule_after_a_restart(bus, topics, monkeypatch):
    """Replay the same storm after triage already opened incidents, with a fresh Triage that restores from
    triage.state: re-sent incident alerts attach by rule (their group has exactly one open incident), re-sent
    noise is ignored by rule (fingerprint remembered in the state), and the model is not called at all."""
    calls = []

    async def acompletion(model, messages, **kw):
        calls.append(messages[1]["content"])
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=model_answer(messages[1]["content"])))])

    monkeypatch.setattr(triage_mod.litellm, "acompletion", acompletion)
    ts = TriageSettings(model="fake", batch_size=50, batch_seconds=30)
    for alert in storm_payload()["alerts"]:
        t = to_alert(alert)
        await bus.produce(topics.alerts, t.cluster, t)
    await Triage(bus, topics, ts).run_until_idle()
    assert len(calls) == 1
    before = {i["id"]: len(i["alert_ids"]) for i in bus.values(topics.triage_state)[-1]["incidents"]}

    wave2 = storm_payload()
    for alert in wave2["alerts"]:
        alert["startsAt"] = "2026-10-10T00:05:00+00:00"  # new instances, same fingerprints
        t = to_alert(alert)
        await bus.produce(topics.alerts, t.cluster, t)
    await Triage(bus, topics, ts).run_until_idle()  # a fresh process: restores from triage.state

    assert len(calls) == 1, "wave 2 needs no model call: attach and ignore both happen by rule"
    ignored = [e for e in bus.values(topics.events) if e["kind"] == "alerts.ignored"]
    assert [e["body"]["how"] for e in ignored] == ["llm", "rule"] and len(ignored[1]["body"]["alerts"]) == 2
    after = {i["id"]: len(i["alert_ids"]) for i in bus.values(topics.triage_state)[-1]["incidents"]}
    assert set(after) == set(before) and all(after[k] == 2 * before[k] for k in before), "every incident doubled, none opened"
    hows = {e["body"]["how"] for e in bus.values(topics.events) if e["kind"] == "alerts.attached"}
    assert hows == {"rule"}
