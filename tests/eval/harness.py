"""Load a scenario and run it through the mesh on an InMemoryBus, one stage at a time.

Stages, in order: ``gateway`` (payload -> alert records), ``triage`` (alerts -> incidents and
``alerts.state``), ``orchestrator`` (incidents -> ``incidents.state`` and one task each), ``investigator``
(tasks -> reports, scripted from the quoted alerts, then the orchestrator again to fold them in). Each stage
reads only what the previous one left on the bus, so a run can stop after any stage (``until``) or start at
a later one from a saved dump (``start_at`` + a pre-loaded bus) to debug that stage alone.

A scenario's ``alerts.json`` is one Alertmanager payload, or ``{"waves": [payload, ...]}`` for alerts that
keep arriving: the gateway and triage stages then run once per wave, with a fresh ``Triage`` each time
(so it restores from ``alerts.state`` like a restarted process), and the orchestrator after every wave.
Matchers may carry ``"_wave": n`` (1-based) to name one wave's instance of a repeated alert."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ein_mesh.bus import InMemoryBus
from ein_mesh.config import InvestigatorSettings, Topics, TriageSettings
from ein_mesh.integrators import to_alert
from ein_mesh.roles import Investigator, Orchestrator, Triage
from ein_mesh.roles import investigator as investigator_mod
from ein_mesh.roles import triage as triage_mod

SCENARIOS = Path(__file__).resolve().parent / "scenarios"
STAGES = ("gateway", "triage", "orchestrator", "investigator")

Matcher = dict[str, str]


WAVE_KEY = "_wave"


def matches(matcher: Matcher, labels: dict[str, str], wave: int) -> bool:
    if WAVE_KEY in matcher and int(matcher[WAVE_KEY]) != wave:
        return False
    return all(labels.get(k) == v for k, v in matcher.items() if k != WAVE_KEY)


@dataclass
class Scenario:
    """One scenario directory loaded: ``waves`` holds one Alertmanager payload per wave, ``alerts`` the
    Alert records of every wave in arrival order, ``wave_of`` maps alert id to its 1-based wave."""

    name: str
    path: Path
    waves: list[dict]
    expected: dict
    model: dict | None
    alerts: list = field(default_factory=list)
    wave_of: dict[str, int] = field(default_factory=dict)

    @classmethod
    def load(cls, path: Path) -> Scenario:
        raw = json.loads((path / "alerts.json").read_text())
        waves = raw["waves"] if "waves" in raw else [raw]
        expected = json.loads((path / "expected.json").read_text())
        model_file = path / "model.json"
        model = json.loads(model_file.read_text()) if model_file.exists() else None
        sc = cls(path.name, path, waves, expected, model)
        for n, payload in enumerate(waves, start=1):
            for a in payload["alerts"]:
                alert = to_alert(a)
                sc.alerts.append(alert)
                sc.wave_of[alert.id] = n
        return sc

    @property
    def payload_alerts(self) -> int:
        return sum(len(w["alerts"]) for w in self.waves)

    def wave_alerts(self, n: int) -> list:
        return [a for a in self.alerts if self.wave_of[a.id] == n]

    def resolve(self, matcher: Matcher) -> str:
        """The one alert id a matcher names. A matcher that hits zero or several alerts is a scenario bug;
        a repeated alert needs ``_wave`` to say which instance."""
        hits = [a.id for a in self.alerts if matches(matcher, a.labels, self.wave_of[a.id])]
        if len(hits) != 1:
            raise ValueError(f"{self.name}: matcher {matcher} matched {len(hits)} alerts, want 1")
        return hits[0]

    def resolve_all(self, matchers: list[Matcher]) -> list[str]:
        return [self.resolve(m) for m in matchers]


def scripted_completion(run: Run):
    """A litellm.acompletion stand-in that answers from model.json, resolving matchers against the
    alerts actually present in the prompt's batch (so it also works across waves and split batches).

    An ``attach`` decision names its target as ``"incident": <matcher>``: the open incident that already
    holds that alert, looked up in the latest ``alerts.state`` snapshot, since the script cannot know ids."""
    scenario = run.scenario
    assert scenario.model is not None, f"{scenario.name} has no model.json; use --llm"

    def incident_holding(alert_id: str) -> str:
        latest: dict[str, dict] = {}
        for snap in run.bus.values(run.topics.alerts_state):
            latest[snap["cluster"]] = snap
        for snap in latest.values():
            for inc in snap["incidents"]:
                if alert_id in inc["alert_ids"]:
                    return inc["id"]
        raise ValueError(f"{scenario.name}: no open incident holds {alert_id}; model.json attach target is wrong")

    async def acompletion(model, messages, **kw):
        user = messages[1]["content"]
        batch = json.loads(user.split("## Quoted data: alerts in this batch\n", 1)[1])
        in_batch = {a["id"] for a in batch}
        decisions = []
        for d in scenario.model["decisions"]:
            ids = [i for i in scenario.resolve_all(d["alerts"]) if i in in_batch]
            if not ids:
                continue
            out = {k: v for k, v in d.items() if k != "alerts"}
            out["alerts"] = ids
            if d["action"] == "attach" and isinstance(d["incident"], dict):
                out["incident"] = incident_holding(scenario.resolve(d["incident"]))
            decisions.append(out)
        content = json.dumps({"decisions": decisions})
        return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=content))])

    return acompletion


async def scripted_investigation(model, messages, **kw):
    """A litellm.acompletion stand-in for the investigator: findings built from the task's quoted alerts, so
    the plumbing (task -> report -> context) is exercised without a model."""
    quoted = json.loads(messages[1]["content"].split("## Quoted data\n", 1)[1])
    alerts = quoted.get("alerts", [])
    names = sorted({a["alertname"] for a in alerts})
    body = {"summary": f"scripted: {len(alerts)} alerts ({', '.join(names)}); cause to be confirmed with tools",
            "notes": [f"{n} firing - evidence: quoted alert" for n in names],
            "identifiers": [f"{a['id']} - alert instance" for a in alerts], "resources_checked": ["quoted alerts"]}
    return SimpleNamespace(choices=[SimpleNamespace(message=SimpleNamespace(content=json.dumps(body)))])


@dataclass
class Usage:
    """Token usage summed over every LLM call of a run. Zero for the scripted model; ``cost_usd`` is
    None when litellm cannot price the model."""

    calls: int = 0
    prompt_tokens: int = 0
    completion_tokens: int = 0
    cost_usd: float | None = None

    def add(self, response) -> None:
        self.calls += 1
        u = getattr(response, "usage", None)
        if u is None:
            return
        self.prompt_tokens += getattr(u, "prompt_tokens", 0) or 0
        self.completion_tokens += getattr(u, "completion_tokens", 0) or 0
        try:
            cost = triage_mod.litellm.completion_cost(completion_response=response)
        except Exception:  # noqa: BLE001 - unknown model, no price table
            return
        self.cost_usd = (self.cost_usd or 0.0) + cost

    @property
    def total_tokens(self) -> int:
        return self.prompt_tokens + self.completion_tokens


@dataclass
class Run:
    """One scenario run: its bus, topics, usage and the stages executed, in order."""

    scenario: Scenario
    bus: InMemoryBus
    topics: Topics
    usage: Usage
    stages: list[str] = field(default_factory=list)

    @property
    def llm_calls(self) -> int:
        return self.usage.calls

    def ran(self, stage: str) -> bool:
        return stage in self.stages


def _span(start_at: str, until: str) -> list[str]:
    i, j = STAGES.index(start_at), STAGES.index(until)
    if i > j:
        raise ValueError(f"start_at {start_at!r} is after until {until!r}")
    return list(STAGES[i:j + 1])


async def stage_gateway(run: Run, wave: int | None = None) -> None:
    """What the gateway does to a webhook payload, minus HTTP: one Alert record per alert. ``wave`` limits
    it to one wave; None produces every wave at once (used when triage is not part of the run)."""
    alerts = run.scenario.wave_alerts(wave) if wave else run.scenario.alerts
    for a in alerts:
        await run.bus.produce(run.topics.alerts, a.cluster, a)


async def stage_triage(run: Run, llm: bool, ts: TriageSettings) -> None:
    """One fresh Triage over whatever is unread on the alerts topic; it restores from alerts.state first."""
    inner = triage_mod.litellm.acompletion if llm else scripted_completion(run)

    async def counting(*a, **kw):
        response = await inner(*a, **kw)
        run.usage.add(response)
        return response

    with patch.object(triage_mod.litellm, "acompletion", counting):
        await Triage(run.bus, run.topics, ts).run_until_idle()


async def stage_orchestrator(run: Run) -> None:
    await Orchestrator(run.bus, run.topics).run_until_idle()


async def stage_investigator(run: Run, llm: bool) -> None:
    """One Investigator over whatever tasks are unread, then the orchestrator once more to fold the reports in."""
    inner = investigator_mod.litellm.acompletion if llm else scripted_investigation

    async def counting(*a, **kw):
        response = await inner(*a, **kw)
        run.usage.add(response)
        return response

    with patch.object(investigator_mod.litellm, "acompletion", counting):
        await Investigator(run.bus, run.topics, InvestigatorSettings(timeout=120)).run_until_idle()
    await stage_orchestrator(run)


async def run_scenario(scenario: Scenario, llm: bool = False, ts: TriageSettings | None = None, *,
                       until: str = STAGES[-1], start_at: str = "gateway", bus: InMemoryBus | None = None) -> Run:
    """Run the stages from ``start_at`` to ``until`` inclusive. ``bus`` seeds the run (a loaded dump) when
    starting after the gateway; without it, a later ``start_at`` has nothing to read and is an error.
    When the run includes both gateway and triage, alerts keep arriving: each wave is ingested and
    classified in turn, with the orchestrator (and investigator) after every wave."""
    if start_at != "gateway" and bus is None:
        raise ValueError(f"start_at={start_at!r} needs a pre-loaded bus (see report.load_dump)")
    run = Run(scenario, bus or InMemoryBus(), Topics(prefix="eval."), Usage())
    ts = ts or TriageSettings(model=TriageSettings().model if llm else "scripted", batch_size=50, batch_seconds=30)
    span = _span(start_at, until)
    if "gateway" in span and "triage" in span:
        for n in range(1, len(scenario.waves) + 1):
            await stage_gateway(run, n)
            await stage_triage(run, llm, ts)
            if "orchestrator" in span:
                await stage_orchestrator(run)
            if "investigator" in span:
                await stage_investigator(run, llm)
        run.stages.extend(span)
        return run
    for stage in span:
        if stage == "gateway":
            await stage_gateway(run)
        elif stage == "triage":
            await stage_triage(run, llm, ts)
        elif stage == "orchestrator":
            await stage_orchestrator(run)
        elif stage == "investigator":
            await stage_investigator(run, llm)
        run.stages.append(stage)
    return run


def scenarios(names: list[str] | None = None) -> list[Scenario]:
    paths = sorted(p for p in SCENARIOS.iterdir() if (p / "alerts.json").exists())
    if names:
        paths = [p for p in paths if p.name in names]
    return [Scenario.load(p) for p in paths]
