# Kafka topics reference

Every topic the v1 mesh creates: key, retention, and the JSON record on it. Which process reads
or writes each one is in the [roles reference](roles.md). Record schemas are the Pydantic models in `src/ein_mesh/schemas.py`; every value is
serialized with `model_dump(by_alias=True)` as UTF-8 JSON, keys are UTF-8 strings.

## Naming

Topic names are resolved by `Topics` in `src/ein_mesh/config.py` from the environment.

| Variable | Default | Effect |
| --- | --- | --- |
| `TOPIC_PREFIX` | `ein.` | Prepended to every default name below. Must equal Terraform's `topic_prefix`, which issues the PREFIXED ACL. |
| `TOPIC_ALERTS` | `${TOPIC_PREFIX}alerts` | Full name of the alerts topic. Ignores the prefix. |
| `TOPIC_TRIAGE_STATE` | `${TOPIC_PREFIX}triage.state` | Full name of the triage state topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_CRITICAL` | `${TOPIC_PREFIX}incidents.critical` | Full name of the critical incidents topic. Ignores the prefix. |
| `TOPIC_INCIDENTS_NORMAL` | `${TOPIC_PREFIX}incidents.normal` | Full name of the normal incidents topic. Ignores the prefix. |
| `TOPIC_EVENTS` | `${TOPIC_PREFIX}investigation.events` | Full name of the events topic. Ignores the prefix. |
| `TOPIC_INVESTIGATION_STATE` | `${TOPIC_PREFIX}investigation.state` | Full name of the investigation state topic. Ignores the prefix. |
| `TOPIC_PARTITIONS` | `12` | Partition count of every mesh topic. One number for all so the `root_id`-keyed topics are co-partitioned. Fixed once topics exist. |
| `TOPIC_REPLICATION` | `1` | Replication factor at creation. Must not exceed the broker count (Terraform `kafka_units`). |

A name that does not start with `TOPIC_PREFIX` is outside the data-integrator's ACL and needs its
own grant.

`ein-mesh init` creates all six topics with `TOPIC_PARTITIONS` partitions and `TOPIC_REPLICATION`
replicas. For a topic that already exists it compares the topic-level config (cleanup policy,
retention) with the table below and alters it to match, logging each change; a key set on the
broker that the table does not list falls back to the broker default. This needs `DescribeConfigs`
and `AlterConfigs` on the topics, which the data-integrator's `producer,consumer` roles do not grant
(the Kafka charm only hands out Create/Write/Read/Describe); with that user init logs a warning and
skips the comparison, and topic config is changed with the charm's own tooling inside the broker
container instead. It then checks that every mesh topic has exactly `TOPIC_PARTITIONS` partitions
and fails on a mismatch instead of altering a topic. Every role runs the partition check at startup and refuses to start on a mismatch. Consumer
groups are `${KAFKA_GROUP_PREFIX}<name>`, default prefix `ein-`.

`ein-mesh reset` (development only) deletes every mesh topic and runs init again, so all records
and every group's committed offsets are gone and the next role start begins from an empty mesh. It
refuses while any `${KAFKA_GROUP_PREFIX}*` group has members. It needs `DELETE` on the topics,
which the Terraform module requests through the data-integrator's `entity-permissions` (see
[Terraform](#terraform)).

## Partitions

- Every mesh topic has the same partition count, default 12.
- Why the same: a record lands on `hash(key) % partitions`. `incidents.*`, `investigation.events`
  and `investigation.state` are keyed by `root_id`, so with equal counts one investigation's records
  all land on the same partition number, and the orchestrator instance that owns that partition
  across the topics holds the state for every record it receives. `alerts` and `triage.state` are
  the same pair keyed by `cluster`. Kafka Streams enforces this for joins; the mesh enforces it in
  `init` and at role startup.
- Why 12: divisible by 1, 2, 3, 4, 6 and 12, so the range assignor gives every instance the same
  share for any likely process count; far more processes than the mesh will run, since parallelism
  inside a process is per `root_id`; and the cost of empty partitions is negligible at this volume.
- Why fixed: adding partitions changes the hash mapping, so an in-flight `root_id` would start
  landing on a partition whose owner has no state for it. Change the number only with no open
  investigations, by recreating the topics.
- Capacity: with classic consumer groups, instances beyond the partition count sit idle. Share
  groups (Kafka 4.x) lift that limit but drop key affinity, so they are for the future per-role work
  topics, not for the lifecycle topics.

## Topics

All six v1 topics are lifecycle topics except `triage.state`, which is triage's private checkpoint.
Per-role work topics (`contracts.<role>`, `dlq.<role>`) do not exist yet; see
[Mesh design](../explanation/mesh-design.md#two-layers-of-topics).

Headings give the suffix; the full name is `TOPIC_PREFIX` + suffix unless the topic's own
variable overrides it.

### `alerts`

| | |
| --- | --- |
| Record | `Alert` |
| Key | `cluster` |
| Cleanup | `delete` (broker default retention) |
| Producers | `ein-mesh gateway` |
| Consumers | `ein-mesh triage`, group `ein-triage` |

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

### `triage.state`

| | |
| --- | --- |
| Record | `TriageState` |
| Key | `cluster` |
| Cleanup | `compact` |
| Producers | `ein-mesh triage`, one record after every applied batch |
| Consumers | `ein-mesh triage` at startup, no consumer group, read to the end offsets |

`TriageState` fields:

| Field | Type | Description |
| --- | --- | --- |
| `cluster` | `str` | The cluster this snapshot belongs to. |
| `incidents` | `list[Incident]` | Every open incident for that cluster. The full list, not a delta. |
| `ignored` | `list[str]` | Fingerprints the model judged noise; re-sends are ignored by rule. |

`Incident` fields:

| Field | Type | Description |
| --- | --- | --- |
| `id` | `str` | `inc-<UTC time>-<10 hex>`, e.g. `inc-20261010T074252Z-3f9a1c2b7d`; also the `root_id` on incidents and events. |
| `cluster` | `str` | Cluster. |
| `namespace` | `str` | Namespace of the first alert that opened it, or `""`. |
| `title` | `str` | Title chosen by triage. |
| `priority` | `"critical" \| "normal"` | Priority chosen by triage. |
| `alert_ids` | `list[str]` | `Alert.id` of every attached alert. |
| `alertnames` | `list[str]` | Sorted distinct `alertname` values across attached alerts. |
| `opened_at` | `str` | ISO 8601 UTC timestamp. |
| `group_key` | `str` | `group_key` of the alert that opened it. |

### `incidents.critical`, `incidents.normal`

| | |
| --- | --- |
| Record | `IncidentMessage` |
| Key | `root_id` |
| Cleanup | `delete` (broker default retention) |
| Producers | `ein-mesh triage`, one record per `open` decision, on the topic matching `priority` |
| Consumers | `ein-mesh orchestrator`, group `ein-orchestrator`; `ein-mesh tail` |

`IncidentMessage` fields:

| Field | Type | Description |
| --- | --- | --- |
| `root_id` | `str` | Incident id, `inc-<UTC time>-<10 hex>`, e.g. `inc-20261010T074252Z-3f9a1c2b7d`. |
| `cluster` | `str` | Cluster. |
| `priority` | `"critical" \| "normal"` | Same as the topic suffix. |
| `title` | `str` | Title chosen by triage. |
| `reason` | `str` | Reason given by the LLM, or the fallback rule. |
| `alerts` | `list[Alert]` | Full records of the alerts grouped into this incident. |
| `opened_at` | `str` | ISO 8601 UTC timestamp. |

Only `open` decisions produce a record here. `attach` and `ignore` are recorded on
`investigation.events` only.

### `investigation.events`

| | |
| --- | --- |
| Record | `Event` |
| Key | `root_id`; `triage-<cluster>` for `alerts.ignored` |
| Cleanup | `delete`, `retention.ms` = 30 days |
| Producers | `ein-mesh triage`, one record per decision; `ein-mesh orchestrator`, one record per transition |
| Consumers | `ein-mesh tail` in v1; the orchestrator and the Mattermost bot in the design |

`Event` fields:

| Field | Type | Description |
| --- | --- | --- |
| `root_id` | `str` | Incident id the event belongs to, or `triage-<cluster>` when there is none. |
| `role` | `str` | Producing role: `triage` or `orchestrator`. |
| `kind` | `str` | One of the kinds below. |
| `at` | `str` | ISO 8601 UTC timestamp. |
| `body` | `dict` | Kind-specific payload, see below. |

Every triage `body` carries `how`, which is `rule` when no model was involved (re-send of ignored noise,
or the group's single open incident), `llm` when the decision came from the model, and `fallback` when
the rule-based fallback ran, and `reason`, the reason text.

| `kind` | Extra `body` fields | Emitted on |
| --- | --- | --- |
| `incident.opened` | `title`, `priority`, `alerts` (list of `Alert.id`) | `open` decision |
| `alerts.attached` | `alerts` (list of `Alert.id`) | rule attach or `attach` decision; `root_id` is the target incident |
| `alerts.ignored` | `alerts` (list of `Alert.id`) | `ignore` decision; `root_id` is `triage-<cluster>` |
| `investigation.started` | `priority`, `title`, `alerts` (list of `Alert.id`); `role` is `orchestrator` | orchestrator takes an incident from `incidents.*` |

The design reserves further kinds for later roles, including `report` for sub-agent reports; see
[Mesh design](../explanation/mesh-design.md#reports-go-on-the-event-log).

### `investigation.state`

| | |
| --- | --- |
| Record | `InvestigationState` |
| Key | `root_id` |
| Cleanup | `compact` |
| Producers | `ein-mesh orchestrator`, one record per state change |
| Consumers | `ein-mesh orchestrator` at startup, no consumer group, read to the end offsets; `ein-mesh tail` on request |

`InvestigationState` fields:

| Field | Type | Description |
| --- | --- | --- |
| `root_id` | `str` | Incident id, `inc-<UTC time>-<10 hex>`, e.g. `inc-20261010T074252Z-3f9a1c2b7d`. |
| `cluster` | `str` | Cluster. |
| `priority` | `"critical" \| "normal"` | From the incident. |
| `title` | `str` | From the incident. |
| `status` | `"investigating" \| "waiting" \| "closed"` | Current lifecycle state. The PoC only writes `investigating`. |
| `opened_at` | `str` | `IncidentMessage.opened_at`. |
| `updated_at` | `str` | ISO 8601 UTC timestamp of this record. |
| `context` | `str` | Bounded summary the orchestrator rewrites every turn: confirmed facts, current hypothesis and plan, conclusions of past questions. At most 16 KB UTF-8 (`CONTEXT_MAX_BYTES`); a larger value fails validation and is not written. Never the transcript, which stays on `investigation.events`. The PoC writes `""`. |

## Terraform

`deploy/terraform` creates no topics. It deploys `kafka-k8s` and a `data-integrator` whose user
gets `producer,consumer` on `${topic_prefix}*` (PREFIXED) and on consumer groups
`${consumer_group_prefix}*`, plus `DELETE` on `${topic_prefix}*` through `entity-permissions` for
`ein-mesh reset`. Variables and defaults are in `deploy/terraform/variables.tf`.

The Kafka charm writes a client's ACLs once, when the integration is created, and ignores later
changes to `topic-name`, `extra-user-roles` or `entity-permissions`. To apply a change, replace the
integration: `terraform apply -replace=juju_integration.kafka_client`. That mints a new user, so
refresh `KAFKA_USERNAME` / `KAFKA_PASSWORD` (`just deploy-creds`) afterwards.
