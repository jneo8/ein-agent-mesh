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
| `triage`, `orchestrator` | the role |
| `gateway *args`, `tail *topics` | the integrator |
| `gateway-fire cluster='dev'` | `curl` the recorded storm at `http://127.0.0.1:8088/alertmanager` with `startsAt` set to now |
| `lint`, `fmt`, `unit-test *args` | `ruff check`, `ruff format`, `pytest tests/unit` |
| `eval *args`, `eval-llm *args`, `eval-test *args` | `python -m tests.eval`, the same with `--llm --show`, `pytest tests/eval` |
| `gates` | `lint`, `unit-test`, `eval-test`, `eval` |

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
| Reads | `incidents.critical`, `incidents.normal`; `incidents.state` at startup |
| Writes | `incidents.state` |
| Memory | per `root_id`: `IncidentState` |
| Done when | the incident has an `investigating` record |
| On failure | broker error → nothing commits, poll replayed; known `root_id` skipped |

Tasks, reports and the model turn arrive with the work layer.

## `tail [TOPIC...] [--from-start]`

Integrator, no group. Prints `[<topic>] key=<key> <json>` for every record.

- `TOPIC`: a suffix (`alerts`, `incidents.state`, ...) or a full name; none means every topic.
- `--from-start`: earliest offset instead of new records only.
