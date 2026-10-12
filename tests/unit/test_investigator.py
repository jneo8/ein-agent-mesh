"""Investigator through the InMemoryBus: no Kafka, no LLM (investigate is replaced per test)."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest

from litellm.exceptions import BadRequestError

from ein_mesh.config import InvestigatorSettings
from ein_mesh.roles import Investigator, toolloop
from ein_mesh.prompts import load_prompt
from ein_mesh.roles.investigator import fit, system_prompt
from ein_mesh.schemas import REPORT_MAX_BYTES, REPORT_SUMMARY_MAX_BYTES, TASK_QUOTED_MAX_BYTES, Findings, Report, Task
from ein_utcp import ScriptedSource, Toolbox

from .conftest import scripted_model, tool_call, turn

pytestmark = pytest.mark.asyncio

NOW = datetime(2026, 10, 11, 12, 0, tzinfo=UTC)


def make(bus, topics, toolbox=None, **kw):
    settings = InvestigatorSettings(model="fake", timeout=kw.pop("timeout", 5), submit_reserve=1, max_depth=kw.pop("max_depth", 3))
    inv = Investigator(bus, topics, settings, toolbox=toolbox if toolbox is not None else Toolbox([]))
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


@pytest.mark.parametrize("error, prefix", [
    (TimeoutError("model down"), "TimeoutError"),
    (BadRequestError("bad tool schema", model="fake", llm_provider="gemini"), "BadRequestError"),  # not an APIError subclass
])
async def test_any_model_failure_is_a_failed_report(bus, topics, error, prefix):
    inv = make(bus, topics)

    async def investigate(t):
        raise error

    inv.investigate = investigate
    await bus.produce(topics.tasks, "inc-1", task())
    await inv.run_until_idle()
    [r] = bus.values(topics.reports)
    assert r["status"] == "failed" and r["error"].startswith(prefix)


async def test_the_tool_loop_runs_and_what_was_fetched_leads_the_evidence(bus, topics):
    """The real investigate: a scripted source and a scripted model that calls one tool, then submits."""
    source = ScriptedSource("alertmanager", ["alertmanager.getAlerts | /api/v2/alerts | (filter) | alerts"],
                            default=[{"labels": {"alertname": "CephOSDDown"}}])
    inv = make(bus, topics, toolbox=Toolbox([source]))
    calls = [{"name": "call_operation", "arguments": {"source": "alertmanager", "operation": "getAlerts",
                                                     "arguments": [{"name": "filter", "value": 'cluster="dev"'}]}},
             {"name": "submit_report", "arguments": {"summary": "osd down", "notes": ["osd.3 down - evidence: call-1"],
                                                    "resources_checked": ["quoted alerts"]}}]
    acompletion, requests = scripted_model([turn([tool_call(c["name"], c["arguments"], f"c{i}")]) for i, c in enumerate(calls)])
    with patch.object(toolloop.litellm, "acompletion", acompletion):
        await bus.produce(topics.tasks, "inc-1", task())
        await inv.run_until_idle()
    [r] = bus.values(topics.reports)
    assert r["status"] == "done" and r["resources_checked"] == ['alertmanager.getAlerts filter=cluster="dev"', "quoted alerts"]
    assert source.calls == [("alertmanager.getAlerts", {"filter": 'cluster="dev"'})]
    prompt = requests[0]["messages"][0]["content"]
    assert "### alertmanager" in prompt and "alertmanager.getAlerts | /api/v2/alerts" in prompt and "submit_report" in prompt


async def test_the_prompt_comes_from_a_file_and_names_the_tools(tmp_path, monkeypatch):
    shipped = load_prompt("investigator")
    assert "submit_report" in system_prompt(shipped, Toolbox([])) and "$submit" not in system_prompt(shipped, Toolbox([]))
    assert "None in this deployment" in system_prompt(shipped, Toolbox([]))
    source = ScriptedSource("alertmanager", ["alertmanager.getAlerts | /api/v2/alerts | (filter) | alerts"])
    assert "### alertmanager\nalertmanager.getAlerts | /api/v2/alerts" in system_prompt(shipped, Toolbox([source]))
    custom = tmp_path / "mine.md"
    custom.write_text("Be terse. $tools")
    assert InvestigatorSettings(model="fake", prompt=str(custom)).prompt == str(custom)
    monkeypatch.setenv("INVESTIGATOR_PROMPT", str(custom))
    assert load_prompt("investigator", InvestigatorSettings(model="fake").prompt).startswith("Be terse.")
    with pytest.raises(ValueError, match="cannot read the investigator prompt"):
        load_prompt("investigator", tmp_path / "missing.md")


async def test_the_gates_are_settings_and_fit_keeps_the_report_within_bounds():
    assert InvestigatorSettings(timeout=600).investigating_time == 480
    assert InvestigatorSettings(timeout=100, submit_reserve=30).investigating_time == 70
    with pytest.raises(ValueError, match="SUBMIT_RESERVE"):
        InvestigatorSettings(timeout=60, submit_reserve=60)
    big = Findings(summary="s", notes=["n" * 1000] * 80, identifiers=["i"] * 5, resources_checked=["model says"])
    fitted = fit(big, ["prometheus.query query=up", "model says"])
    assert fitted.resources_checked == ["prometheus.query query=up", "model says"]
    assert len(fitted.notes) < 80 and fitted.identifiers == ["i"] * 5
    assert len(Report(task_id="t", root_id="r", role="investigator", status="done", **fitted.model_dump()).model_dump_json()) <= REPORT_MAX_BYTES


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
