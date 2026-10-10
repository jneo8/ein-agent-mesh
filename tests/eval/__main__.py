"""``python -m tests.eval [options] [scenario ...]``: run every scenario stage by stage, print a table, exit 1
when any stage fails.

  --until STAGE        stop after this stage (gateway | triage | orchestrator)
  --start-at STAGE     begin at this stage, reading the earlier stages' output from --from-dump
  --from-dump DIR      a directory written by --dump
  --dump DIR           write every bus topic as DIR/<scenario>/<topic>.json after the run
  --show               print, per stage, what the run produced (and for triage, what was expected)
  --llm                use the real model instead of model.json
  --threshold F        minimum triage pair recall; false merges and invariant violations always fail
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys
from pathlib import Path

from .harness import STAGES, run_scenario, scenarios
from .report import dump, load_dump, report
from .score import Score, score


def _cost(c: float | None) -> str:
    return "-" if c is None else f"${c:.4f}"


def _flag(st, threshold) -> str:
    return "skip" if st.skipped else ("yes" if st.ok(threshold) else "NO")


def table(scores: list[Score], threshold: float) -> str:
    cols = ["scenario", "gw", "llm", "tok_in", "tok_out", "cost", "groups", "exact", "false_merge", "splits",
            "pair_r", "ign_p", "ign_r", "prio", "fallback", "orch", "ok"]
    rows = []
    for s in scores:
        t = s.triage.metrics if not s.triage.skipped else {}
        g = lambda k, d="": t.get(k, d)  # noqa: E731
        rows.append([s.scenario, _flag(s.gateway, threshold), s.llm_calls, s.prompt_tokens, s.completion_tokens,
                     _cost(s.cost_usd),
                     f"{g('produced_groups')}/{g('expected_groups')}" if t else "", g("exact_groups"), g("false_merges"),
                     g("splits"), g("pair_recall"), g("ignore_precision"), g("ignore_recall"),
                     f"{g('priority_hits')}/{g('exact_groups')}" if t else "", g("fallback_batches"),
                     _flag(s.orchestrator, threshold), "yes" if s.ok(threshold) else "NO"])
    ts = [s.triage.metrics for s in scores if not s.triage.skipped]
    costs = [s.cost_usd for s in scores if s.cost_usd is not None]
    total = ["TOTAL", f"{sum(s.gateway.ok(threshold) for s in scores)}/{len(scores)}",
             sum(s.llm_calls for s in scores), sum(s.prompt_tokens for s in scores),
             sum(s.completion_tokens for s in scores), _cost(sum(costs) if costs else None),
             f"{sum(m['produced_groups'] for m in ts)}/{sum(m['expected_groups'] for m in ts)}" if ts else "",
             sum(m["exact_groups"] for m in ts), sum(m["false_merges"] for m in ts), sum(m["splits"] for m in ts),
             "", "", "", f"{sum(m['priority_hits'] for m in ts)}/{sum(m['exact_groups'] for m in ts)}" if ts else "",
             sum(m["fallback_batches"] for m in ts),
             f"{sum(s.orchestrator.ok(threshold) for s in scores)}/{len(scores)}",
             f"{sum(s.ok(threshold) for s in scores)}/{len(scores)}"]
    rows.append(total)
    widths = [max(len(str(x)) for x in col) for col in zip(cols, *rows)]
    fmt = "  ".join(f"{{:<{w}}}" for w in widths)
    body = [fmt.format(*map(str, r)) for r in rows]
    return "\n".join([fmt.format(*cols)] + body[:-1] + ["-" * len(body[-1]), body[-1]])


def explain(s: Score, threshold: float) -> list[str]:
    out = [f"{s.scenario}: VIOLATION {v}" for v in s.violations]
    t = s.triage
    if not t.skipped and not t.violations and not t.ok(threshold):
        m = t.metrics
        if m["false_merges"]:
            out.append(f"{s.scenario}: triage: {m['false_merges']} false-merge pair(s): alerts grouped that must stay apart")
        if m["pair_recall"] < threshold:
            out.append(f"{s.scenario}: triage: pair recall {m['pair_recall']} < {threshold}")
        if m["ignore_precision"] < 1.0 or m["ignore_recall"] < 1.0:
            out.append(f"{s.scenario}: triage: ignore precision {m['ignore_precision']} recall {m['ignore_recall']}")
        if m["priority_hits"] != m["exact_groups"]:
            out.append(f"{s.scenario}: triage: priority wrong on {m['exact_groups'] - m['priority_hits']} exact group(s)")
    return out


async def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="python -m tests.eval")
    p.add_argument("scenario", nargs="*", help="scenario directory names; default all")
    p.add_argument("--llm", action="store_true", help="use the real model (EIN_AGENT_MODEL) instead of model.json")
    p.add_argument("--threshold", type=float, default=1.0, help="minimum triage pair recall; false merges always fail")
    p.add_argument("--until", choices=STAGES, default="orchestrator", help="stop after this stage")
    p.add_argument("--start-at", choices=STAGES, default="gateway", help="begin at this stage (needs --from-dump)")
    p.add_argument("--from-dump", type=Path, metavar="DIR", help="seed the bus from a --dump directory")
    p.add_argument("--show", action="store_true", help="print what each stage produced")
    p.add_argument("--dump", type=Path, metavar="DIR", help="write every bus topic as JSON under DIR/<scenario>/")
    args = p.parse_args(argv)
    logging.getLogger("LiteLLM").setLevel(logging.ERROR)
    if args.start_at != "gateway" and not args.from_dump:
        p.error("--start-at after gateway needs --from-dump DIR")

    scores = []
    for sc in scenarios(args.scenario or None):
        seed = load_dump(args.from_dump, sc.name) if args.from_dump else None
        run = await run_scenario(sc, llm=args.llm, until=args.until, start_at=args.start_at, bus=seed)
        scores.append(score(run))
        if args.show:
            print(report(run), end="\n\n")
        if args.dump:
            for path in dump(run, args.dump):
                print(f"wrote {path}", file=sys.stderr)
    print(table(scores, args.threshold))
    bad = [s for s in scores if not s.ok(args.threshold)]
    for s in bad:
        for line in explain(s, args.threshold):
            print(line, file=sys.stderr)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
