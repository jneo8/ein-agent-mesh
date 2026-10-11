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
| `incidents.tasks` | flow | root_id | `Task` (next) | orchestrator → investigator |
| `incidents.reports` | flow | root_id | `Report` (next) | investigator → orchestrator |

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
| `context` | str | bounded summary, at most 16 KB UTF-8; a larger value fails validation. Empty until reports exist |

Readers other than the orchestrator may rely on `status`, `title`, `priority`, `cluster`, `updated_at`.
Everything else is the orchestrator's own and changes behind `schema_version`.

## `incidents.tasks`, `incidents.reports`

Created by `init`; records and roles arrive with the work layer. One tasks topic serves every worker
role (`role` field) until a second role exists, then `incidents.tasks.<role>`.
