"""Score one run, stage by stage, so a failure points at the stage that caused it.

- gateway: every payload alert became exactly one record, ids unique, cluster/namespace mapped.
- triage: grouping against the expected incidents under the under-merge policy (a false merge fails,
  a split only costs recall), ignore precision/recall, priority, and triage's own invariants. Decisions
  are read from the latest ``alerts.state`` per cluster, the role's memory.
- orchestrator: every incident has an investigating ``incidents.state`` record and one task; after the
  investigator stage, that task is done and the context holds its findings.
- investigator: every issued task got exactly one done report.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations

from .harness import Run


@dataclass
class StageScore:
    """One stage's metrics and violations; ``skipped`` when the stage was not part of this run."""

    stage: str
    metrics: dict = field(default_factory=dict)
    violations: list[str] = field(default_factory=list)
    skipped: bool = False

    def ok(self, threshold: float = 1.0) -> bool:
        if self.skipped:
            return True
        if self.violations:
            return False
        m = self.metrics
        if self.stage == "triage":
            return (m["false_merges"] == 0 and m["ignore_precision"] == m["ignore_recall"] == 1.0
                    and m["priority_hits"] == m["exact_groups"] and m["pair_recall"] >= threshold)
        return True

    def detail(self) -> str:
        if self.skipped:
            return "skipped"
        m = self.metrics
        if self.stage == "gateway":
            return f"{m['records']}/{m['payload_alerts']} alerts in {m['waves']} wave(s), {m['unique_ids']} unique ids, clusters={m['clusters']}"
        if self.stage == "triage":
            return (f"groups={m['produced_groups']}/{m['expected_groups']} exact={m['exact_groups']} "
                    f"false_merge={m['false_merges']} splits={m['splits']} pair_r={m['pair_recall']} "
                    f"ign={m['ignore_precision']}/{m['ignore_recall']} prio={m['priority_hits']}/{m['exact_groups']} "
                    f"attached={m['attached']}/{m['expected_attached']} fallback={m['fallback_opens']}")
        if self.stage == "orchestrator":
            return f"{m['investigating']}/{m['incidents']} investigating, {m['tasks_done']} tasks done"
        return f"{m['reports_done']}/{m['tasks']} tasks reported done, {m['reports_failed']} failed"


@dataclass
class Score:
    scenario: str
    llm_calls: int
    prompt_tokens: int
    completion_tokens: int
    cost_usd: float | None
    gateway: StageScore
    triage: StageScore
    orchestrator: StageScore
    investigator: StageScore

    @property
    def stages(self) -> list[StageScore]:
        return [self.gateway, self.triage, self.orchestrator, self.investigator]

    def ok(self, threshold: float = 1.0) -> bool:
        return all(st.ok(threshold) for st in self.stages)

    @property
    def violations(self) -> list[str]:
        return [f"{st.stage}: {v}" for st in self.stages for v in st.violations]

    @property
    def perfect(self) -> bool:
        t = self.triage.metrics
        return self.ok(1.0) and (self.triage.skipped or t["exact_groups"] == t["expected_groups"] == t["produced_groups"])

    def __getattr__(self, name):
        """Convenience accessor for the table and the tests: any metric of any stage that ran."""
        for st in (self.triage, self.gateway, self.orchestrator, self.investigator):
            if not st.skipped and name in st.metrics:
                return st.metrics[name]
        raise AttributeError(name)


def _pairs(groups: list[set[str]]) -> set[frozenset[str]]:
    return {frozenset(p) for g in groups for p in combinations(sorted(g), 2)}


def _ratio(num: int, den: int) -> float:
    return 1.0 if den == 0 else num / den


def latest_alerts_state(run: Run) -> dict[str, dict]:
    """cluster -> the last alerts.state record for it: what triage would restore on restart."""
    latest: dict[str, dict] = {}
    for s in run.bus.values(run.topics.alerts_state):
        latest[s["cluster"]] = s
    return latest


def latest_incidents_state(run: Run) -> dict[str, dict]:
    """root_id -> the last incidents.state record for it: what the orchestrator would restore."""
    latest: dict[str, dict] = {}
    for s in run.bus.values(run.topics.incidents_state):
        latest[s["root_id"]] = s
    return latest


def score_gateway(run: Run) -> StageScore:
    if not run.ran("gateway"):
        return StageScore("gateway", skipped=True)
    sc, t = run.scenario, run.topics
    records = run.bus.values(t.alerts)
    ids = [r["id"] for r in records]
    violations = []
    if len(records) != sc.payload_alerts:
        violations.append(f"{len(records)} records for {sc.payload_alerts} payload alerts")
    if len(set(ids)) != len(ids):
        violations.append("duplicate alert ids")
    unknown = [r["id"] for r in records if r["cluster"] == "unknown"]
    if unknown:
        violations.append(f"cluster 'unknown' for {len(unknown)} alert(s)")
    return StageScore("gateway", {"payload_alerts": sc.payload_alerts, "records": len(records), "waves": len(sc.waves),
                                  "unique_ids": len(set(ids)), "clusters": sorted({r["cluster"] for r in records})},
                      violations)


def score_triage(run: Run) -> StageScore:
    if not run.ran("triage"):
        return StageScore("triage", skipped=True)
    sc, bus, t = run.scenario, run.bus, run.topics
    all_ids = {a.id for a in sc.alerts}
    exp_groups = [(set(sc.resolve_all(g["alerts"])), g["priority"]) for g in sc.expected["incidents"]]
    exp_ignored = set(sc.resolve_all(sc.expected.get("ignored", [])))

    incidents = bus.values(t.incidents("critical")) + bus.values(t.incidents("normal"))
    state_incidents = [i for s in latest_alerts_state(run).values() for i in s["incidents"]]
    final = {i["id"]: (set(i["alert_ids"]), i["priority"]) for i in state_incidents}
    prod_groups = list(final.values()) if final else [(set(a["id"] for a in i["alerts"]), i["priority"]) for i in incidents]
    prod_ignored = {aid for s in latest_alerts_state(run).values() for e in s["ignored"].values() for aid in e["alerts"]}

    exact = prio_hits = 0
    for g, prio in exp_groups:
        hit = next((pp for pg, pp in prod_groups if pg == g), None)
        if hit is not None:
            exact += 1
            prio_hits += hit == prio
    ep, pp = _pairs([g for g, _ in exp_groups]), _pairs([g for g, _ in prod_groups])
    ig_tp = len(exp_ignored & prod_ignored)

    violations = []
    placed = [aid for g, _ in prod_groups for aid in g] + sorted(prod_ignored)
    if sorted(placed) != sorted(all_ids):
        missing = all_ids - set(placed)
        dup = {x for x in placed if placed.count(x) > 1}
        violations.append(f"alerts not placed exactly once: missing={sorted(missing)} duplicated={sorted(dup)}")
    inc_ids = {i["root_id"] for i in incidents}
    if set(final) != inc_ids:
        violations.append(f"alerts.state incidents {sorted(final)} != incidents {sorted(inc_ids)}")
    exp_attached = set(sc.resolve_all(sc.expected.get("attached", [])))
    attached = {aid for i in state_incidents for a in i["attached"] for aid in a["alerts"]}
    not_attached = exp_attached - attached
    if not_attached:
        violations.append(f"{len(not_attached)} alert(s) expected to attach to an open incident were not attached")

    return StageScore("triage", {
        "expected_groups": len(exp_groups), "produced_groups": len(prod_groups), "exact_groups": exact,
        "false_merges": len(pp - ep), "splits": len(ep - pp), "pair_recall": round(_ratio(len(ep & pp), len(ep)), 3),
        "ignore_precision": round(_ratio(ig_tp, len(prod_ignored)), 3),
        "ignore_recall": round(_ratio(ig_tp, len(exp_ignored)), 3), "priority_hits": prio_hits,
        "fallback_opens": sum(1 for i in state_incidents if i["how"] == "fallback"),
        "attached": len(attached), "expected_attached": len(exp_attached),
    }, violations)


def score_orchestrator(run: Run) -> StageScore:
    if not run.ran("orchestrator"):
        return StageScore("orchestrator", skipped=True)
    bus, t = run.bus, run.topics
    incidents = {i["root_id"]: i for i in bus.values(t.incidents("critical")) + bus.values(t.incidents("normal"))}
    states = latest_incidents_state(run)
    investigating = {rid for rid, s in states.items() if s["status"] == "investigating"}
    violations = []
    if investigating != set(incidents):
        violations.append(f"investigating {sorted(investigating)} != incidents {sorted(incidents)}")
    tasks_done = 0
    for rid, st in states.items():
        inc = incidents.get(rid)
        if inc and (st["priority"], st["title"], st["cluster"]) != (inc["priority"], inc["title"], inc["cluster"]):
            violations.append(f"state {rid} disagrees with its incident on priority/title/cluster")
        if not st["tasks"]:
            violations.append(f"{rid} has no task: the orchestrator issued nothing")
        done = [x for x in st["tasks"] if x["status"] == "done"]
        tasks_done += len(done)
        if run.ran("investigator"):
            if not done:
                violations.append(f"{rid} has no done task after the investigator ran: {[x['status'] for x in st['tasks']]}")
            if not st["context"]:
                violations.append(f"{rid} has an empty context after a report")
    return StageScore("orchestrator", {"incidents": len(incidents), "investigating": len(investigating),
                                       "tasks_done": tasks_done}, violations)


def score_investigator(run: Run) -> StageScore:
    if not run.ran("investigator"):
        return StageScore("investigator", skipped=True)
    bus, t = run.bus, run.topics
    issued = {x["task_id"] for st in latest_incidents_state(run).values() for x in st["tasks"]}
    reports = bus.values(t.reports)
    done = [r["task_id"] for r in reports if r["status"] == "done"]
    violations = []
    unknown = {r["task_id"] for r in reports} - issued
    if unknown:
        violations.append(f"reports for tasks nobody issued: {sorted(unknown)}")
    if len(done) != len(set(done)):
        violations.append("a task reported done more than once")
    missing = issued - set(done)
    if missing:
        violations.append(f"{len(missing)} issued task(s) without a done report")
    return StageScore("investigator", {"tasks": len(issued), "reports_done": len(done),
                                       "reports_failed": sum(1 for r in reports if r["status"] != "done")}, violations)


def score(run: Run) -> Score:
    return Score(scenario=run.scenario.name, llm_calls=run.llm_calls, prompt_tokens=run.usage.prompt_tokens,
                 completion_tokens=run.usage.completion_tokens, cost_usd=run.usage.cost_usd,
                 gateway=score_gateway(run), triage=score_triage(run), orchestrator=score_orchestrator(run),
                 investigator=score_investigator(run))
