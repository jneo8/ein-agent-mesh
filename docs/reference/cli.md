# Commands

`ein-mesh <cmd>` (also `python -m ein_mesh <cmd>`) reads its settings from the environment. Every
command has a `just` recipe that loads `.env` first. Settings: [settings](settings.md).

## Recipes

| Recipe | Runs |
| --- | --- |
| `deploy-init`, `deploy-plan`, `deploy-apply` | `terraform -chdir=deploy/terraform init` / `plan -out=ein-mesh.tfplan` / `apply ein-mesh.tfplan` |
| `deploy-creds` | `juju run ein-mesh-client/leader get-credentials` |
| `kafka-init` | `ein-mesh init` |
| `kafka-reset *args` | `ein-mesh reset` |
| `kafka-topics *args` | `ein-mesh topics` |
| `kafka-roles` | `ein-mesh roles` |
| `triage`, `orchestrator`, `investigator` | the role |
| `gateway *args`, `tail *topics` | the integrator |
| `gateway-fire cluster='dev'` | `curl` the recorded storm at `http://127.0.0.1:8088/alertmanager` with `startsAt` set to now |
| `lint`, `fmt`, `test *args` | `ruff check`, `ruff format`, `pytest tests/unit tests/eval` (unit tests and harness self-tests; no broker, no model) |
| `eval *args`, `eval-llm *args` | `python -m tests.eval` (scripted model, prints the score table), the same with `--llm --show` |
| `gates` | `lint`, `test`, `eval` |

Recipe names carry their group as prefix (`deploy-`, `kafka-`, `gateway-`); roles and integrators
keep their process name.

## `init`

Creates every topic with `TOPIC_PARTITIONS` partitions and `TOPIC_REPLICATION` replicas; state
topics compacted. Safe to re-run.

- Existing topic: config compared and altered when the user may (`DescribeConfigs`; the
  data-integrator user may not, so a warning is logged and the step skipped).
- Partition count differs from the setting: exit 1, names the topic. Partitions are never altered.

## `reset [--yes] [--prune]`

Development only. Deletes every topic and runs `init`. Records and committed offsets are gone.

| Flag | Effect |
| --- | --- |
| `--yes` | no confirmation prompt |
| `--prune` | also delete topics under `TOPIC_PREFIX` that the layout does not name |

- Refuses while any `ein-*` group has a member.
- Needs `DELETE` on the topics (granted by the Terraform module's `entity-permissions`).

## `topics [--watch SECONDS]`

One row per topic: `TOPIC`, `PARTITIONS`, `RECORDS`.

- `RECORDS` = end offset − beginning offset, summed; on a compacted topic, what compaction kept.
- `12!=24` marks a partition count that differs from the setting; `missing` a topic that does not exist.
- `--watch 5` clears and refreshes every 5 s.

## `roles`

Every consumer group with prefix `KAFKA_GROUP_PREFIX`: state, member count, lag per topic, then one
line per member with client id, host and assigned partitions.

- No members: the role is stopped; offsets live for `offsets.retention.minutes` (7 days default).
- Readers without a group (`tail`, state restores) do not appear.

## `gateway [--host 0.0.0.0] [--port 8088]`

Integrator. No LLM, no auth, no TLS.

| Method | Path | Returns |
| --- | --- | --- |
| `POST` | `/alertmanager` | `{"received": n, "produced": m}`; `m` excludes ids already seen |
| `GET` | `/healthz` | `ok` |

- Writes `alerts`, key = cluster.
- Dedup set (`fingerprint:startsAt`) is in memory; lost on restart.
- Field mapping: [topics › alerts](topics.md#alerts).

## `triage`

Role, group `ein-triage`.

| | |
| --- | --- |
| Reads | `alerts`; `alerts.state` at startup |
| Writes | `incidents.critical` / `incidents.normal`, `alerts.state` |
| Memory | per cluster: open incidents, ignored fingerprints |
| Done when | every alert of a poll is in an incident or ignored, and the state is written |
| On failure | model timeout / bad output / failed check → rule fallback (`how=fallback`); broker error → nothing commits, poll replayed |

Loop: poll → rules (known noise → ignore; one open incident in the group → attach) → batch the rest
per cluster → at 50 alerts or 30 s, one model call per cluster → validate → apply → write state →
commit. Settings: [triage](settings.md#triage).

## `orchestrator`

Role, group `ein-orchestrator`.

| | |
| --- | --- |
| Reads | `incidents.critical`, `incidents.normal`, `incidents.reports`; `incidents.state` at startup |
| Writes | `incidents.tasks`, `incidents.state` |
| Memory | per `root_id`: `IncidentState` with its tasks and `context` |
| Done when | a new incident has an `investigating` record and one task issued; a report has settled its task and `context` is rewritten |
| On failure | task `failed` or past its deadline → re-issued with `parent_id` while attempts remain, then `exhausted`; broker error → nothing commits, poll replayed; known `root_id` and already-done tasks skipped |

Loop: poll → incidents: new `root_id` → write state, issue one `investigator` task → reports: settle
the task (`done` is final; a late report overrides `expired` / `failed`) → rewrite `context` → every
poll, including empty ones: expire tasks past their deadline and decide on retries → commit.
Settings: [orchestrator](settings.md#orchestrator). No model turn yet.

## `investigator`

Role, group `ein-investigator`.

| | |
| --- | --- |
| Reads | `incidents.tasks` |
| Writes | `incidents.reports` |
| Memory | none |
| Done when | one report per task, produced before the offset commits |
| On failure | depth over the limit → `failed`; deadline passed → `expired`, no model call; model timeout / error / bad JSON → `failed` with the error |

One task at a time; `max_poll_interval_ms` is the task timeout plus 60 s. A task for another `role`
is skipped. No tools yet: one model call over the task body. Settings:
[investigator](settings.md#investigator).

## `tail [TOPIC...] [--from-start]`

Integrator, no group. Prints `[<topic>] key=<key> <json>` for every record.

- `TOPIC`: a suffix (`alerts`, `incidents.state`, ...) or a full name; none means every topic.
- `--from-start`: earliest offset instead of new records only.
