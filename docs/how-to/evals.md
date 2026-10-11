# Run and write evals

Scenario evals run a recorded alert payload through the mesh on an in-memory bus and score each
stage. File formats and scorer columns: [evals reference](../reference/evals.md).

## Run the baseline

```bash
just eval             # every scenario, scripted model, must be perfect
just eval ceph-osd-storm --show
```

- `--show` prints expected and produced grouping side by side, with titles and reasons.
- The scripted model (`model.json`) is the baseline; it must score perfect or the harness is wrong.

## Compare the real model

```bash
just eval-llm                 # EIN_AGENT_MODEL from .env, spends tokens
just eval-llm --threshold 0.8 # tolerate splits down to 80% pair recall
```

A false merge fails at any threshold; only splits are tolerated.

## Debug one stage

```bash
just eval ceph-osd-storm --until triage --dump /tmp/run
just eval ceph-osd-storm --start-at orchestrator --from-dump /tmp/run
```

The dump holds every topic as JSON; the second run replays the orchestrator on it with no model call.

## Add a scenario

1. Create `tests/eval/scenarios/<name>/`.
2. `alerts.json`: an Alertmanager webhook payload, or `{"waves": [payload, ...]}` for alerts that
   arrive over time.
3. `expected.json`: the incidents and ignores you expect, each alert named by labels that select
   exactly one alert (`"_wave": n` for a repeated alert).
4. `model.json`: the perfect answer in the same matcher form. This makes the scenario a harness test.
5. `just eval <name> --show` until the scripted run is perfect; then `just eval-llm <name> --show`.

## Keep the harness honest

```bash
just eval-test        # harness self-tests: scripted perfect, lumping fails, splitting costs recall, dead model shows as fallback
just gates            # lint, unit tests, harness tests, scripted evals
```
