"""Human-readable view of one run, one section per stage that ran, plus dump/load of the bus so a later
stage can be replayed on its own. Alerts are shown by alertname plus whichever labels tell same-named
alerts apart, never by id."""

from __future__ import annotations

import json
from pathlib import Path

from ein_mesh.bus import InMemoryBus, Record

from .harness import Run
from .score import latest_alerts_state, latest_incidents_state


def _short_names(run: Run) -> dict[str, str]:
    """id -> 'CephOSDDown{osd=osd.3}'. Labels are added only when needed to disambiguate; the same alert
    in several waves gets a ``@w<n>`` suffix."""
    alerts = run.scenario.alerts
    by_name: dict[str, list] = {}
    for a in alerts:
        by_name.setdefault(a.alertname, []).append(a)
    skip = {"alertname", "severity", "cluster", "namespace"}
    out = {}
    for name, group in by_name.items():
        keys = sorted({k for a in group for k in a.labels if k not in skip}
                      & {k for k in group[0].labels if any(a.labels.get(k) != group[0].labels.get(k) for a in group)})
        for a in group:
            label = name + ("{" + ",".join(f"{k}={a.labels.get(k)}" for k in keys) + "}" if keys else "")
            same = [b for b in group if all(b.labels.get(k) == a.labels.get(k) for k in keys)]
            if len(same) > 1:
                label += f"@w{run.scenario.wave_of[a.id]}"
            out[a.id] = label
    return out


def _gateway_section(run: Run, names: dict[str, str]) -> list[str]:
    lines = [f"[gateway] alert records ({len(run.scenario.waves)} wave(s)):"]
    for r in run.bus.values(run.topics.alerts):
        lines.append(f"  w{run.scenario.wave_of.get(r['id'], '?')} {names.get(r['id'], r['id']):48} "
                     f"cluster={r['cluster']} ns={r['namespace'] or '-'} sev={r['severity']}")
    return lines


def _triage_section(run: Run, names: dict[str, str]) -> list[str]:
    sc = run.scenario
    fmt = lambda ids: ", ".join(sorted(names.get(i, i) for i in ids))  # noqa: E731
    lines = ["[triage] expected:"]
    for g in sc.expected["incidents"]:
        lines.append(f"  [{g['priority']:8}] {fmt(sc.resolve_all(g['alerts']))}")
    if sc.expected.get("ignored"):
        lines.append(f"  [ignore  ] {fmt(sc.resolve_all(sc.expected['ignored']))}")
    if sc.expected.get("attached"):
        lines.append(f"  [attach  ] {fmt(sc.resolve_all(sc.expected['attached']))}")
    lines += ["[triage] produced (latest alerts.state per cluster):"]
    for snap in latest_alerts_state(run).values():
        for inc in snap["incidents"]:
            lines.append(f"  [{inc['priority']:8}] {fmt(inc['alert_ids'])}   ({inc['id']}, by {inc['how']})")
            lines.append(f"             title:  {inc['title']}")
            if inc.get("reason"):
                lines.append(f"             reason: {inc['reason']}")
            for a in inc["attached"]:
                lines.append(f"             attached by {a['how']}: {fmt(a['alerts'])}  ({a['reason']})")
        for entry in snap["ignored"].values():
            lines.append(f"  [ignore  ] {fmt(entry['alerts'])}   (by {entry['how']})")
            lines.append(f"             reason: {entry['reason']}")
    return lines


def _orchestrator_section(run: Run) -> list[str]:
    lines = ["[orchestrator] incidents.state (latest per incident):"]
    for s in latest_incidents_state(run).values():
        tasks = ", ".join(f"{x['role']}#{x['attempt']} {x['status']}" for x in s["tasks"]) or "no tasks"
        lines.append(f"  {s['root_id']}  {s['status']:14} [{s['priority']}] {s['title']}")
        lines.append(f"             tasks: {tasks}; context {len(s['context'].encode())} bytes")
    return lines


def _investigator_section(run: Run) -> list[str]:
    lines = ["[investigator] incidents.reports:"]
    for r in run.bus.values(run.topics.reports):
        lines.append(f"  {r['root_id']}  {r['task_id']}  {r['status']:8} {(r['summary'] or r['error'])[:90]}")
        for checked in r["resources_checked"]:
            lines.append(f"      checked: {checked[:100]}")
    return lines


def report(run: Run) -> str:
    sc = run.scenario
    names = _short_names(run)
    lines = [f"== {sc.name}: {sc.expected.get('description', '')}".rstrip(), f"   stages: {' -> '.join(run.stages)}", ""]
    if run.ran("gateway"):
        lines += _gateway_section(run, names) + [""]
    if run.ran("triage"):
        lines += _triage_section(run, names) + [""]
    if run.ran("orchestrator"):
        lines += _orchestrator_section(run) + [""]
    if run.ran("investigator"):
        lines += _investigator_section(run) + [""]
    return "\n".join(lines).rstrip()


def dump(run: Run, directory: Path) -> list[Path]:
    """Every topic on the bus as <dir>/<scenario>/<topic>.json, for inspection by hand."""
    out = directory / run.scenario.name
    out.mkdir(parents=True, exist_ok=True)
    written = []
    for topic, records in run.bus.topics.items():
        p = out / f"{topic}.json"
        p.write_text(json.dumps([{"key": r.key, "value": r.value} for r in records], indent=2, ensure_ascii=False) + "\n")
        written.append(p)
    return written


def load_dump(directory: Path, scenario_name: str) -> InMemoryBus:
    """Rebuild a bus from ``dump`` output so a run can start at a later stage. Every topic's records are
    unread, so the stage that runs next consumes them; compacted-state restores read them all anyway."""
    folder = directory / scenario_name
    bus = InMemoryBus()
    for p in sorted(folder.glob("*.json")):
        topic = p.stem
        bus.topics[topic] = [Record(topic, r["key"], r["value"], offset=i) for i, r in enumerate(json.loads(p.read_text()))]
    return bus
