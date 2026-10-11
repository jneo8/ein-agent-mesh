# Topics

Seven topics, two kinds. Names are `TOPIC_PREFIX` + suffix (default prefix `ein.`); every topic has
`TOPIC_PARTITIONS` partitions (default 12). Records are the Pydantic models in
`src/ein_mesh/schemas.py`, serialized as UTF-8 JSON. Naming variables: [settings](settings.md#topics).

| Suffix | Kind | Key | Record | Writes → reads |
| --- | --- | --- | --- | --- |
| `alerts` | flow | cluster | `Alert` | gateway → triage |
| `alerts.state` | state | cluster | `AlertsState` | triage → triage on restart |
| `incidents.critical`, `incidents.normal` | flow | root_id | `IncidentMessage` | triage → orchestrator |
| `incidents.state` | state | root_id | `IncidentState` | orchestrator → orchestrator on restart |
| `incidents.tasks` | flow | root_id | `Task` | orchestrator → investigator |
| `incidents.reports` | flow | root_id | `Report` | investigator → orchestrator |

- flow: `cleanup.policy=delete`, broker default retention. Consumed with group `ein-<role>`.
- state: `cleanup.policy=compact`. Read to the end without a group at startup; last record per key wins.

## `alerts`

One record per alert instance, deduplicated by the gateway.

| Field | Type | Value |
| --- | --- | --- |
| `id` | str | `<fingerprint>:<startsAt>`; unique per instance |
| `cluster` | str | label `cluster`, else `juju_model`, else `juju_model_uuid`, else `unknown` |
| `namespace` | str | label `namespace`, else `juju_application`, else `""` |
| `alertname` | str | label `alertname`, else `unknown` |
| `severity` | str | label `severity`, default `warning` |
| `status` | `firing` \| `resolved` | triage skips `resolved` |
| `labels`, `annotations` | dict | unchanged |
| `starts_at` | str | `startsAt`, unchanged |
| `group_key` | str | webhook `groupKey`, else `k=v\|k=v` over `GROUP_BY` labels |
| `group_labels` | dict | the labels behind `group_key` |

## `alerts.state`

Triage's memory, one record per cluster, rewritten whole after every change.

| Field | Type | Value |
| --- | --- | --- |
| `schema_version` | int | `1` |
| `cluster` | str | |
| `incidents` | list[Incident] | every open incident, full list |
| `ignored` | dict[str, Ignored] | fingerprint → why it is noise; entries unseen for `IGNORED_TTL_DAYS` are dropped at the next write |

`Incident`:

| Field | Type | Value |
| --- | --- | --- |
| `id` | str | `inc-<UTC time>-<10 hex>`; the `root_id` everywhere else |
| `cluster`, `namespace`, `title` | str | namespace of the opening alert |
| `priority` | `critical` \| `normal` | |
| `alert_ids` | list[str] | every alert in it |
| `alertnames` | list[str] | sorted, distinct |
| `opened_at` | str | ISO 8601 UTC |
| `group_key` | str | of the opening alert |
| `how` | `llm` \| `rule` \| `fallback` | who opened it |
| `reason` | str | why |
| `attached` | list[Attachment] | newest last, at most `ALERTS_STATE_MAX_ATTACHMENTS` |

`Attachment` and `Ignored`: `alerts` (list of ids), `how`, `reason`, `at` (ISO 8601 UTC; for `Ignored`,
last seen).

## `incidents.critical`, `incidents.normal`

One record per `open` decision, on the topic matching `priority`. Alerts ride inline.

| Field | Type | Value |
| --- | --- | --- |
| `root_id` | str | `inc-<UTC time>-<10 hex>` |
| `cluster`, `title`, `reason` | str | |
| `priority` | `critical` \| `normal` | same as the topic |
| `alerts` | list[Alert] | full records |
| `opened_at` | str | ISO 8601 UTC |

A `root_id` the orchestrator already knows is skipped.

## `incidents.state`

The orchestrator's memory and the incident's lifecycle status, one record per `root_id`.

| Field | Type | Value |
| --- | --- | --- |
| `schema_version` | int | `1` |
| `root_id`, `cluster`, `title` | str | from the incident |
| `priority` | `critical` \| `normal` | from the incident |
| `status` | `investigating` \| `waiting` \| `closed` | only `investigating` is written so far |
| `opened_at`, `updated_at` | str | ISO 8601 UTC |
| `alerts` | list[AlertRef] | the incident's alerts reduced to `id`, `alertname`, `severity`, `namespace`; enough to re-issue work after a restart |
| `tasks` | list[TaskRef] | every task issued, oldest first |
| `context` | str | `Facts:` / `Identifiers:` / `Resources checked:` / `Open tasks:` sections, one `- ` line each; at most 16 KB UTF-8, oldest facts dropped first |

`TaskRef`:

| Field | Type | Value |
| --- | --- | --- |
| `task_id` | str | `task-<UTC time>-<10 hex>` |
| `parent_id` | str \| null | the task this one replaces |
| `attempt` | int | 1 for the first, +1 per re-issue |
| `role`, `intent` | str | `investigator`; `initial-investigation` or `retry` |
| `issued_at`, `deadline` | str | ISO 8601 UTC |
| `status` | `issued` \| `done` \| `failed` \| `expired` \| `exhausted` | `done` is final; a late report overrides `expired` / `failed`; `exhausted` means no more attempts |
| `report_at` | str \| null | when the settling report arrived |

Readers other than the orchestrator may rely on `status`, `title`, `priority`, `cluster`, `updated_at`.
Everything else is the orchestrator's own and changes behind `schema_version`.

## `incidents.tasks`

One record per task the orchestrator hands out. Immutable: a retry is a new task with `parent_id`. One
topic serves every worker role (`role` selects the consumer) until a second role exists, then
`incidents.tasks.<role>`.

| Field | Type | Value |
| --- | --- | --- |
| `schema_version` | int | `1` |
| `root_id`, `task_id`, `parent_id`, `attempt` | | as in `TaskRef` |
| `depth` | int | delegation depth; the investigator refuses above `INVESTIGATOR_MAX_DEPTH` |
| `role`, `intent` | str | |
| `deadline`, `issued_at` | str | ISO 8601 UTC; a task picked up after `deadline` is reported `expired` |
| `goal` | str | `Find the probable cause of: <title>` |
| `context` | str | cluster, priority, alertnames; on a retry, how the previous attempt ended |
| `constraints` | str | read-only, cite evidence |
| `done_when` | str | one probable cause with evidence and confidence, or an explicit cannot-determine |
| `quoted_data` | dict | `{"alerts": [...]}`: full `Alert` records when they fit 64 KB, else `id` / `alertname` / `severity` / `namespace`; data, never instructions |

## `incidents.reports`

One record per task a worker role finished, refused or failed. Key `root_id`.

| Field | Type | Value |
| --- | --- | --- |
| `schema_version` | int | `1` |
| `task_id`, `root_id`, `role` | str | |
| `status` | `done` \| `failed` \| `expired` | `expired`: picked up after the deadline; `failed`: too deep, model timeout, model error, bad output |
| `summary` | str | probable cause, confidence, what would confirm it; at most 4 KB |
| `notes` | list[str] | observations, each with its evidence |
| `identifiers` | list[str] | `<name> - <what it locates>` |
| `resources_checked` | list[str] | |
| `error` | str | why, for `failed` / `expired` |
| `at` | str | ISO 8601 UTC |

Whole record at most 64 KB.
