"""The recorded storm end to end: Alertmanager payload -> gateway mapping -> triage with the LLM
mocked at the litellm boundary -> incidents and alerts.state -> orchestrator and incidents.state.
No broker, no network."""

import json
from types import SimpleNamespace

import pytest

from ein_mesh.config import InvestigatorSettings, TriageSettings
from ein_mesh.integrators import to_alert
from ein_mesh.roles import Investigator, Orchestrator, Triage
from ein_mesh.roles import investigator as investigator_mod
from ein_mesh.roles import triage as triage_mod

from .conftest import storm_payload

pytestmark = pytest.mark.asyncio

def model_answer(prompt_user: str) -> str:
    """What a well-behaved triage says about this storm under the under-merge policy: Ceph and temporal
    stay separate (no label ties them), TargetDown is its own normal incident, noise is ignored.
    Decisions cover whatever subset of the storm is in this batch; empty groups are left out."""
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
    st = bus.values(topics.alerts_state)[-1]
    assert len(st["incidents"]) == 3 and all(i["how"] == "llm" for i in st["incidents"])
    assert len(st["ignored"]) == 2 and all(e["how"] == "llm" and e["reason"] == "noise" for e in st["ignored"].values())

    await Orchestrator(bus, topics).run_until_idle()
    states = {s["root_id"]: s["status"] for s in bus.values(topics.incidents_state)}
    assert states == {c["root_id"]: "investigating" for c in critical + normal}
    assert {t["root_id"] for t in bus.values(topics.tasks)} == set(states), "one task per incident"


async def test_storm_tasks_are_investigated_and_folded_into_context(bus, topics, fake_llm, monkeypatch):
    """triage -> orchestrator -> investigator (model mocked) -> orchestrator: every incident ends with one done
    task and a context that holds the investigator's summary."""
    for alert in storm_payload()["alerts"]:
        t = to_alert(alert)
        await bus.produce(topics.alerts, t.cluster, t)
    await Triage(bus, topics, TriageSettings(model="fake", batch_size=50, batch_seconds=30)).run_until_idle()
    await Orchestrator(bus, topics).run_until_idle()

    async def acompletion(model, messages, **kw):
        quoted = json.loads(messages[1]["content"].split("## Quoted data\n", 1)[1])
        names = sorted({a["alertname"] for a in quoted["alerts"]})
        body = {"summary": f"probable cause behind {', '.join(names)}", "notes": [f"{n} firing - evidence: alert" for n in names],
                "identifiers": [a["id"] for a in quoted["alerts"]], "resources_checked": ["quoted alerts"]}
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(body)))])

    monkeypatch.setattr(investigator_mod.litellm, "acompletion", acompletion)
    await Investigator(bus, topics, InvestigatorSettings(model="fake", timeout=5)).run_until_idle()
    reports = bus.values(topics.reports)
    assert len(reports) == 3 and all(r["status"] == "done" for r in reports)

    await Orchestrator(bus, topics).run_until_idle()
    latest = {s["root_id"]: s for s in bus.values(topics.incidents_state)}
    for st in latest.values():
        assert [t["status"] for t in st["tasks"]] == ["done"]
        assert "probable cause behind" in st["context"] and "Open tasks:" not in st["context"]


async def test_second_wave_attaches_by_rule_after_a_restart(bus, topics, monkeypatch):
    """Replay the same storm after triage already opened incidents, with a fresh Triage that restores from
    alerts.state: re-sent incident alerts attach by rule (their group has exactly one open incident), re-sent
    noise is ignored by rule (fingerprint remembered in the state), and the model is not called at all.
    Wave 2 changes only ``startsAt``, so the alerts are new instances with the same fingerprints."""
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
    before = {i["id"]: len(i["alert_ids"]) for i in bus.values(topics.alerts_state)[-1]["incidents"]}

    wave2 = storm_payload()
    for alert in wave2["alerts"]:
        alert["startsAt"] = "2026-10-10T00:05:00+00:00"
        t = to_alert(alert)
        await bus.produce(topics.alerts, t.cluster, t)
    await Triage(bus, topics, ts).run_until_idle()

    assert len(calls) == 1, "wave 2 needs no model call: attach and ignore both happen by rule"
    st = bus.values(topics.alerts_state)[-1]
    assert len(st["ignored"]) == 2 and all(len(e["alerts"]) == 2 and e["how"] == "llm" for e in st["ignored"].values()), \
        "the re-sent noise joined the entries the model created; the fingerprints are the same"
    after = {i["id"]: len(i["alert_ids"]) for i in st["incidents"]}
    assert set(after) == set(before) and all(after[k] == 2 * before[k] for k in before), "every incident doubled, none opened"
    assert {a["how"] for i in st["incidents"] for a in i["attached"]} == {"rule"}
