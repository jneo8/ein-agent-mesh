# Kafka topics reference

Every topic the mesh creates, in one template each: what it is for, which kind it is, key, record,
who writes, who reads, retention, what a duplicate delivery means and what happens on restart.
Record schemas are the Pydantic models in `src/ein_mesh/schemas.py`; every value is serialized with
`model_dump(by_alias=True)` as UTF-8 JSON, keys are UTF-8 strings. Which process runs what is in the
[roles reference](roles.md).

## Two kinds of topic

| Kind | Definition | Cleanup | Examples |
| --- | --- | --- | --- |
| flow | Carries exactly one message type from one role to the next. Exists only because something consumes it. | `delete`, broker default retention | `alerts`, `incidents.critical` / `incidents.normal`, `incidents.tasks`, `incidents.reports` |
| state | The memory of exactly one role: one record per unit of work, rewritten whole, read back only by that role when it restarts. Named after the topic the role consumes plus `.state`. | `compact` | `alerts.state` (the `alerts` consumer, triage), `incidents.state` (the `incidents.*` consumer, the orchestrator) |

There is no third kind. Nothing is written by everybody and read by nobody; reasons for a decision
live in the state of the role that made it. The orchestrator owns the incident lifecycle, so its state
record is also where an incident's lifecycle status lives; a public view for other readers is derived
from it when such a reader exists (design note: 2026-10-11 計畫).

## Naming

Topic names are resolved by `Topics` in `src/ein_mesh/config.py` from the environment.

| Variable | Default | Effect |
| --- | --- | --- |
| `TOPIC_PREFIX` | `ein.` | Prepended to every default name below. Must equal Terraform's `topic_prefix`, which issues the PREFIXED ACL. |
| `TOPIC_ALERTS` | `${TOPIC_PREFIX}alerts` | Full name of the alerts topic. Ignores the prefix. |
| `TOPIC_ALERTS_STATE` | `${TOPIC_PREFIX}alerts.state` | Full name of triage's state topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_CRITICAL` | `${TOPIC_PREFIX}incidents.critical` | Full name of the critical incidents topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_NORMAL` | `${TOPIC_PREFIX}incidents.normal` | Full name of the normal incidents topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_STATE` | `${TOPIC_PREFIX}incidents.state` | Full name of the orchestrator's state topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_TASKS` | `${TOPIC_PREFIX}incidents.tasks` | Full name of the tasks topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_REPORTS` | `${TOPIC_PREFIX}incidents.reports` | Full name of the reports topic. Ignores the prefix. |
| `TOPIC_PARTITIONS` | `12` | Partition count of every mesh topic. One number for all so the `root_id`-keyed topics are co-partitioned. Fixed once topics exist. |
| `TOPIC_REPLICATION` | `1` | Replication factor at creation. Must not exceed the broker count (Terraform `kafka_units`). |

A name that does not start with `TOPIC_PREFIX` is outside the data-integrator's ACL and needs its
own grant. Short names (`alerts.state`, `incidents.tasks`, ...) are accepted wherever a topic is named
on the command line and resolved through `Topics.resolve`.

`ein-mesh init` creates all seven topics with `TOPIC_PARTITIONS` partitions and `TOPIC_REPLICATION`
replicas. For a topic that already exists it compares the topic-level config (cleanup policy) with
the table below and alters it to match, logging each change; a key set on the broker that the table
does not list falls back to the broker default. This needs `DescribeConfigs` and `AlterConfigs` on
the topics, which the data-integrator's `producer,consumer` roles do not grant (the Kafka charm only
hands out Create/Write/Read/Describe/Delete); with that user init logs a warning and skips the
comparison, and topic config is changed with the charm's own tooling inside the broker container
instead. It then checks that every mesh topic has exactly `TOPIC_PARTITIONS` partitions and fails on
a mismatch instead of altering a topic. Every role runs the partition check at startup and refuses to
start on a mismatch. Consumer groups are `${KAFKA_GROUP_PREFIX}<name>`, default prefix `ein-`.

`ein-mesh reset` (development only) deletes every mesh topic and runs init again, so all records and
every group's committed offsets are gone and the next role start begins from an empty mesh. It
refuses while any `${KAFKA_GROUP_PREFIX}*` group has members. `--prune` also deletes every topic under
`TOPIC_PREFIX` that the layout does not name, which is how topics from an earlier topic model
(`triage.state`, `investigation.events`, `investigation.state`) leave a development broker. Both need
`DELETE` on the topics, which the Terraform module requests through the data-integrator's
`entity-permissions` (see [Terraform](#terraform)).

## Partitions

- Every mesh topic has the same partition count, default 12.
- Why the same: a record lands on `hash(key) % partitions`. `incidents.*`, `incidents.state`,
  `incidents.tasks` and `incidents.reports` are keyed by `root_id`, so with equal counts one
  incident's records all land on the same partition number, and the orchestrator instance that owns
  that partition across the topics holds the state for every record it receives. `alerts` and
  `alerts.state` are the same pair keyed by `cluster`. Kafka Streams enforces this for joins; the
  mesh enforces it in `init` and at role startup.
- Why 12: divisible by 1, 2, 3, 4, 6 and 12, so the range assignor gives every instance the same
  share for any likely process count; far more processes than the mesh will run, since parallelism
  inside a process is per `root_id`; and the cost of empty partitions is negligible at this volume.
- Why fixed: adding partitions changes the hash mapping, so an in-flight `root_id` would start
  landing on a partition whose owner has no state for it. Change the number only with no open
  incidents, by recreating the topics.
- Capacity: with classic consumer groups, instances beyond the partition count sit idle. Share
  groups (Kafka 4.x) lift that limit but drop key affinity, so they are a future option for the
  work topics (`incidents.tasks`), never for the state topics.

## Topics

Headings give the suffix; the full name is `TOPIC_PREFIX` + suffix unless the topic's own variable
overrides it.

### `alerts`

| | |
| --- | --- |
| Definition | Incoming alerts, one record per alert instance, deduplicated. |
| Kind | flow |
| Record | `Alert` |
| Key | `cluster` |
| Writes | `ein-mesh gateway` |
| Reads | `ein-mesh triage`, group `ein-triage` |
| Cleanup | `delete` (broker default retention) |
| Duplicates | The gateway drops repeats of the same `id`; a repeat that gets through is a re-send and triage settles it by rule (attach or ignore). |
| On restart | Nothing to restore; a consumer resumes from its committed offset. |

`Alert` fields:

| Field | Type | Description |
| --- | --- | --- |
| `id` | `str` | `<fingerprint>:<startsAt>` from the Alertmanager alert. Unique per alert instance; the gateway drops repeats of the same `id`. |
| `cluster` | `str` | First non-empty of the labels `cluster`, `juju_model`, `juju_model_uuid`; `unknown` if none. |
| `namespace` | `str` | Label `namespace`, else `juju_application`, else `""`. |
| `alertname` | `str` | Label `alertname`, else `unknown`. |
| `severity` | `str` | Label `severity`, default `warning`. |
| `status` | `"firing" \| "resolved"` | Alert `status`. Triage ignores `resolved`. |
| `labels` | `dict[str, str]` | All alert labels, unchanged. |
| `annotations` | `dict[str, str]` | All alert annotations, unchanged. |
| `starts_at` | `str` | Alert `startsAt`, unchanged. |
| `group_key` | `str` | Alertmanager `groupKey` from the webhook when present, else `k=v|k=v` over the `GROUP_BY` labels. Triage attaches by rule when a group has exactly one open incident. |
| `group_labels` | `dict[str, str]` | The labels behind `group_key`. |

### `alerts.state`

| | |
| --- | --- |
| Definition | The memory of the `alerts` consumer: per cluster, the open incidents with the decisions that built them, and the fingerprints judged noise. |
| Kind | state |
| Record | `AlertsState` |
| Key | `cluster` |
| Writes | `ein-mesh triage`, one record after every change to a cluster's table |
| Reads | `ein-mesh triage` at startup, no consumer group, read to the end offsets |
| Cleanup | `compact` |
| Duplicates | Later record for a key wins; a replayed batch rewrites the same table. |
| On restart | Last record per cluster is the table. Nothing else should read it: its fields are shaped for the prompt, not as a contract. |

`AlertsState` fields:

| Field | Type | Description |
| --- | --- | --- |
| `schema_version` | `int` | `1`. |
| `cluster` | `str` | The cluster this record belongs to. |
| `incidents` | `list[Incident]` | Every open incident for that cluster. The full list, not a delta. |
| `ignored` | `dict[str, Ignored]` | Fingerprint → why it is noise. Re-sends of a known fingerprint are ignored by rule and refresh the entry; entries not refreshed within `IGNORED_TTL_DAYS` (default 7) are dropped when the record is next written. |

`Incident` fields:

| Field | Type | Description |
| --- | --- | --- |
| `id` | `str` | `inc-<UTC time>-<10 hex>`, e.g. `inc-20261010T074252Z-3f9a1c2b7d`; also the `root_id` on every `incidents.*` topic. |
| `cluster` | `str` | Cluster. |
| `namespace` | `str` | Namespace of the first alert that opened it, or `""`. |
| `title` | `str` | Title chosen by triage. |
| `priority` | `"critical" \| "normal"` | Priority chosen by triage. |
| `alert_ids` | `list[str]` | `Alert.id` of every alert in it. |
| `alertnames` | `list[str]` | Sorted distinct `alertname` values across its alerts. |
| `opened_at` | `str` | ISO 8601 UTC timestamp. |
| `group_key` | `str` | `group_key` of the alert that opened it. |
| `how` | `"llm" \| "rule" \| "fallback"` | Who opened it: the model, or the rule-based fallback after a bad answer. |
| `reason` | `str` | The reason given for opening it. |
| `attached` | `list[Attachment]` | Alerts folded in after the opening, newest last; only the last `ALERTS_STATE_MAX_ATTACHMENTS` (default 20) are kept. |

`Attachment`: `alerts` (list of `Alert.id`), `how` (`rule` when the group had exactly one open incident,
`llm` or `fallback` when a batch decided it), `reason`, `at`. `Ignored`: `alerts` (every instance of the
fingerprint seen), `how`, `reason`, `at` (last seen).

### `incidents.critical`, `incidents.normal`

| | |
| --- | --- |
| Definition | A newly opened incident awaiting an owner, with its alerts inline. |
| Kind | flow |
| Record | `IncidentMessage` |
| Key | `root_id` |
| Writes | `ein-mesh triage`, one record per `open` decision, on the topic matching `priority` |
| Reads | `ein-mesh orchestrator`, group `ein-orchestrator` |
| Cleanup | `delete` (broker default retention) |
| Duplicates | A `root_id` the orchestrator already knows is skipped. |
| On restart | Nothing to restore; the orchestrator resumes from its committed offset and skips what its state already has. |

`IncidentMessage` fields:

| Field | Type | Description |
| --- | --- | --- |
| `root_id` | `str` | Incident id, `inc-<UTC time>-<10 hex>`. |
| `cluster` | `str` | Cluster. |
| `priority` | `"critical" \| "normal"` | Same as the topic suffix. |
| `title` | `str` | Title chosen by triage. |
| `reason` | `str` | Reason given by the LLM, or the fallback rule. |
| `alerts` | `list[Alert]` | Full records of the alerts grouped into this incident. |
| `opened_at` | `str` | ISO 8601 UTC timestamp. |

Only `open` decisions produce a record here. `attach` and `ignore` change `alerts.state` only.

### `incidents.state`

| | |
| --- | --- |
| Definition | The memory of the `incidents.*` consumer: per `root_id`, where the incident is in its lifecycle and what the orchestrator knows about it. |
| Kind | state |
| Record | `IncidentState` |
| Key | `root_id` |
| Writes | `ein-mesh orchestrator`, one record per change |
| Reads | `ein-mesh orchestrator` at startup, no consumer group, read to the end offsets; `ein-mesh tail` on request |
| Cleanup | `compact` |
| Duplicates | Later record for a key wins. |
| On restart | Last record per `root_id` is the table; nothing is replayed. `status`, `title`, `priority`, `cluster` and `updated_at` are the fields a later reader (bot, cluster status view) may rely on; the rest is the orchestrator's own and evolves behind `schema_version`. |

`IncidentState` fields:

| Field | Type | Description |
| --- | --- | --- |
| `schema_version` | `int` | `1`. |
| `root_id` | `str` | Incident id, `inc-<UTC time>-<10 hex>`. |
| `cluster` | `str` | Cluster. |
| `priority` | `"critical" \| "normal"` | From the incident. |
| `title` | `str` | From the incident. |
| `status` | `"investigating" \| "waiting" \| "closed"` | Current lifecycle state. Only `investigating` is written so far. |
| `opened_at` | `str` | `IncidentMessage.opened_at`. |
| `updated_at` | `str` | ISO 8601 UTC timestamp of this record. |
| `context` | `str` | Bounded summary the orchestrator rewrites every turn: confirmed facts, current hypothesis and plan, conclusions of past questions. At most 16 KB UTF-8 (`CONTEXT_MAX_BYTES`); a larger value fails validation and is not written. Never a transcript. Empty until the work layer writes reports into it. |

### `incidents.tasks`, `incidents.reports`

| | `incidents.tasks` | `incidents.reports` |
| --- | --- | --- |
| Definition | Work handed by the orchestrator to a worker role (`Task`, with a `role` field). | A worker role's result handed back (`Report`). |
| Kind | flow | flow |
| Key | `root_id` | `root_id` |
| Writes | `ein-mesh orchestrator` | `ein-mesh investigator` |
| Reads | `ein-mesh investigator`, group `ein-investigator` | `ein-mesh orchestrator`, group `ein-orchestrator` |
| Cleanup | `delete` | `delete` |

Created by `init` now so the layout is complete; the records and the roles that use them arrive with
the work layer (design note: 2026-10-11 計畫). One tasks topic serves every worker role until a second
role exists, then it splits into `incidents.tasks.<role>` under the same prefix.

## Terraform

`deploy/terraform` creates no topics. It deploys `kafka-k8s` and a `data-integrator` whose user
gets `producer,consumer` on `${topic_prefix}*` (PREFIXED) and on consumer groups
`${consumer_group_prefix}*`, plus `DELETE` on `${topic_prefix}*` through `entity-permissions` for
`ein-mesh reset`. Variables and defaults are in `deploy/terraform/variables.tf`.

The Kafka charm writes a client's ACLs once, when the integration is created, and ignores later
changes to `topic-name`, `extra-user-roles` or `entity-permissions`. To apply a change, replace the
integration: `terraform apply -replace=juju_integration.kafka_client`. That mints a new user, so
refresh `KAFKA_USERNAME` / `KAFKA_PASSWORD` (`just deploy-creds`) afterwards.
