"""The eval harness is itself under test: with the scripted model every scenario and every stage must be
clean; a lumping model must fail triage on false merges, a splitting model must only lose recall, a dead
model must show up as fallback; stage control (until / start-at from a dump) must work so a stage can be
debugged alone."""

import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from .harness import run_scenario, scenarios
from .report import dump, load_dump
from .score import score

pytestmark = pytest.mark.asyncio


def _batch(messages):
    return json.loads(messages[1]["content"].split("## Quoted data: alerts in this batch\n", 1)[1])


def _answer(out):
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(out)))])


@pytest.mark.parametrize("scenario", scenarios(), ids=lambda s: s.name)
async def test_scripted_model_is_clean_at_every_stage(scenario):
    s = score(await run_scenario(scenario))
    assert s.violations == []
    assert s.gateway.ok() and s.triage.ok() and s.orchestrator.ok() and s.investigator.ok()
    assert s.perfect, s
    groups = s.triage.metrics["produced_groups"]
    assert groups <= s.llm_calls <= len(scenario.waves) + groups, \
        "triage: at most one call per wave (a wave of pure re-sends needs none); investigator: one call per incident"
    assert s.orchestrator.metrics["investigating"] == groups
    assert s.investigator.metrics["reports_done"] == s.orchestrator.metrics["tasks_done"] == groups


async def test_waves_attach_through_a_restarted_triage():
    """Three waves, three fresh Triage instances: later waves attach to incidents restored from alerts.state,
    the orchestrator sees each incident once, and the state ends with every alert placed. A wave may write
    the state more than once (rule attaches and re-sent noise in one poll, then the model's batch)."""
    [scenario] = scenarios(["rolling-ceph-storm"])
    run = await run_scenario(scenario)
    s = score(run)
    assert s.violations == [] and s.perfect, s
    assert s.llm_calls == 2 + 2, "triage: wave 1 (ceph), wave 2 (new temporal group), wave 3 all rule attaches; investigator: one per incident"
    snapshots = run.bus.values(run.topics.alerts_state)
    assert len(snapshots) >= 3 and all(sn["cluster"] == "dev" for sn in snapshots), "at least one write per wave"
    final = snapshots[-1]["incidents"]
    attachments = [a for i in final for a in i["attached"]]
    assert len(final) == 2 and len(attachments) >= 3 and all(a["how"] == "rule" for a in attachments)
    assert len({s["root_id"] for s in run.bus.values(run.topics.incidents_state)}) == 2, "orchestrator skipped replays"
    assert sum(len(i["alert_ids"]) for i in final) == 9


async def test_lumping_model_fails_triage_only():
    [scenario] = scenarios(["ceph-osd-storm"])

    async def lump(model, messages, **kw):
        return _answer({"decisions": [{"action": "open", "alerts": [a["id"] for a in _batch(messages)], "priority": "normal",
                                       "title": "everything", "reason": "lazy"}]})

    with patch("tests.eval.harness.scripted_completion", lambda run: lump):
        s = score(await run_scenario(scenario))
    assert s.violations == []
    assert s.gateway.ok() and s.orchestrator.ok(), "the stages around triage are fine; only triage is wrong"
    assert not s.triage.ok(0.0), "false merges must fail even with no recall threshold"
    assert s.false_merges > 0 and s.exact_groups == 0 and s.ignore_recall == 0.0


async def test_splits_only_cost_recall():
    [scenario] = scenarios(["ceph-osd-storm"])

    async def split(model, messages, **kw):
        noise = {"Watchdog", "KubeHpaMaxedOut"}
        batch = _batch(messages)
        decisions = [{"action": "open", "alerts": [a["id"]], "priority": "critical" if a["alertname"] != "TargetDown" else "normal",
                      "title": a["alertname"], "reason": "one each"} for a in batch if a["alertname"] not in noise]
        decisions.append({"action": "ignore", "alerts": [a["id"] for a in batch if a["alertname"] in noise], "reason": "noise"})
        return _answer({"decisions": decisions})

    with patch("tests.eval.harness.scripted_completion", lambda run: split):
        s = score(await run_scenario(scenario))
    assert s.violations == [] and s.false_merges == 0 and s.splits > 0
    assert s.pair_recall < 1.0 and s.ok(0.0) and not s.ok(1.0)


async def test_dead_model_shows_as_fallback():
    """With the triage model down, every incident is opened by the rule fallback and the score says so. The
    fallback may well be perfect on a label-clean storm; what must not happen is a silent pass as `llm`."""
    [scenario] = scenarios(["ceph-osd-storm"])

    async def broken(model, messages, **kw):
        raise TimeoutError("no model")

    with patch("tests.eval.harness.scripted_completion", lambda run: broken):
        s = score(await run_scenario(scenario))
    assert s.violations == []
    assert s.fallback_opens == s.triage.metrics["produced_groups"] >= 1, "every incident came from the fallback"
    assert s.investigator.ok(), "the investigator does not depend on triage's model"


async def test_until_stops_after_a_stage():
    [scenario] = scenarios(["ceph-osd-storm"])
    run = await run_scenario(scenario, until="triage")
    assert run.stages == ["gateway", "triage"]
    assert run.bus.values(run.topics.incidents_state) == []
    s = score(run)
    assert s.orchestrator.skipped and s.ok()

    run = await run_scenario(scenario, until="gateway")
    assert run.stages == ["gateway"]
    assert len(run.bus.values(run.topics.alerts)) == len(scenario.alerts)
    assert score(run).triage.skipped


async def test_orchestrator_can_be_replayed_from_a_dump(tmp_path):
    """Run through triage once, dump, then run the orchestrator alone on the dump: no second LLM call."""
    [scenario] = scenarios(["ceph-osd-storm"])
    first = await run_scenario(scenario, until="triage")
    dump(first, tmp_path)

    seeded = load_dump(tmp_path, scenario.name)
    second = await run_scenario(scenario, start_at="orchestrator", until="orchestrator", bus=seeded)
    assert second.stages == ["orchestrator"] and second.llm_calls == 0
    s = score(second)
    assert s.gateway.skipped and s.triage.skipped and s.orchestrator.ok()
    assert s.orchestrator.metrics["investigating"] == len(first.bus.values(first.topics.incidents("critical"))
                                                           + first.bus.values(first.topics.incidents("normal")))


async def test_start_at_without_a_bus_is_an_error():
    [scenario] = scenarios(["ceph-osd-storm"])
    with pytest.raises(ValueError):
        await run_scenario(scenario, start_at="orchestrator")
