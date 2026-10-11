# Evals

`python -m tests.eval` (`just eval`) runs every scenario under `tests/eval/scenarios` on an in-memory
bus and scores each stage. How to use it: [run and write evals](../how-to/evals.md).

## Scenario files

| File | Content |
| --- | --- |
| `alerts.json` | one Alertmanager webhook payload, or `{"waves": [payload, ...]}` |
| `expected.json` | `description`; `incidents`: `[{priority, alerts: [matcher]}]`; `ignored`: `[matcher]`; optional `attached`: `[matcher]` for alerts that must join an existing incident |
| `model.json` | `decisions`: triage decisions with matchers instead of ids; an `attach` names its target as `"incident": matcher` (the open incident holding that alert) |

A matcher is a dict of labels that selects exactly one alert; `"_wave": n` picks one wave's instance
of a repeated alert.

## Stages

| Stage | Does | Scored on |
| --- | --- | --- |
| `gateway` | `to_alert` on every payload alert → `alerts` | one record per alert, unique ids, no `unknown` cluster |
| `triage` | a fresh `Triage` per wave, model patched to `model.json` (`--llm` keeps the real one) | grouping, ignore, priority, invariants |
| `orchestrator` | `Orchestrator.run_until_idle` after every wave | one `investigating` record per incident, agreeing with it, one task issued |
| `investigator` | an `Investigator` with the model patched to a scripted answer built from the task's quoted alerts (`--llm` keeps the real one), then the orchestrator again | one `done` report per issued task; every incident has a done task and a non-empty `context` |

## Flags

| Flag | Effect |
| --- | --- |
| `SCENARIO...` | directory names; default all |
| `--llm` | real model from `EIN_AGENT_MODEL` |
| `--threshold F` | minimum triage pair recall, default `1.0`; a false merge fails regardless |
| `--until STAGE` | stop after that stage; default the last one |
| `--start-at STAGE --from-dump DIR` | seed the bus from a dump, run from that stage |
| `--dump DIR` | write every topic as `DIR/<scenario>/<topic>.json` |
| `--show` | print expected vs produced per stage |

## Scorer columns

| Column | Meaning |
| --- | --- |
| `gw`, `orch`, `inv` | stage result: `yes`, `NO`, `skip` |
| `llm`, `tok_in`, `tok_out`, `cost` | model calls (triage and investigator), tokens, litellm price (`-` when unknown) |
| `groups` | produced / expected incidents, from the latest `alerts.state` per cluster |
| `exact` | expected groups reproduced exactly |
| `false_merge` | alert pairs grouped together that should be apart; any value above 0 fails |
| `splits` | alert pairs apart that should be together; costs recall only |
| `pair_r` | 1 − splits / expected pairs; compared with `--threshold` |
| `ign_p`, `ign_r` | ignore precision and recall |
| `prio` | exact groups with the expected priority |
| `fallback` | incidents opened by the rule fallback |
| `ok` | no violation; triage: no false merge, ignore P = R = 1, priorities right, `pair_r` ≥ threshold |

`--show` also reports `attached` vs expected attached; a shortfall is a violation.

## Invariants

- triage: every alert in exactly one place (an incident or ignored); the incidents in `alerts.state`
  are exactly those on `incidents.*`.
- orchestrator: every incident `investigating` with a task issued, priority / title / cluster match;
  after the investigator stage, a `done` task and a non-empty `context`.
- investigator: a `done` report for every issued task, none for unknown tasks, none twice.

## Scenarios

| Scenario | Alerts | Expected | Probes |
| --- | --- | --- | --- |
| `ceph-osd-storm` | 10 | 3 incidents, 2 ignored | cross-layer cause (Ceph → PVC → pods) is not triage's to infer |
| `same-namespace-two-faults` | 8 | 3 incidents, 2 ignored | same namespace is not evidence; group by application |
| `noise-storm-one-real` | 11 | 2 incidents, 8 ignored | ignore precision; node-7 NotReady/Unreachable is one incident |
| `rolling-ceph-storm` | 11 in 3 waves | 2 incidents, 2 ignored, 6 attaches | later waves attach through a restarted triage; one triage call per wave plus one investigator call per incident; no re-open on replay |

## Harness self-tests

Part of `just test` (`tests/eval/test_eval.py`): scripted model perfect at every stage; a lumping model
fails triage only; a splitting model passes at a low threshold and fails at 1.0; a dead model shows as
fallback; `--until` stops where told; the orchestrator replays from a dump with no model call.
