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

A name that does not start with `TOPIC_PREFIX` is outside the data-integrator's ACL and needs its
own grant.

`ein-mesh init` creates all six topics with 1 partition and replication factor 1, and is safe to
re-run. Consumer groups are `${KAFKA_GROUP_PREFIX}<name>`, default prefix `ein-`.

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
| `id` | `str` | `inc-<10 hex>`; also the `root_id` on incidents and events. |
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
| `root_id` | `str` | Incident id, `inc-<10 hex>`. |
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
| `root_id` | `str` | Incident id, `inc-<10 hex>`. |
| `cluster` | `str` | Cluster. |
| `priority` | `"critical" \| "normal"` | From the incident. |
| `title` | `str` | From the incident. |
| `status` | `"investigating" \| "waiting" \| "closed"` | Current lifecycle state. The PoC only writes `investigating`. |
| `opened_at` | `str` | `IncidentMessage.opened_at`. |
| `updated_at` | `str` | ISO 8601 UTC timestamp of this record. |

## Terraform

`deploy/terraform` creates no topics. It deploys `kafka-k8s` and a `data-integrator` whose user
gets `producer,consumer` on `${topic_prefix}*` (PREFIXED) and on consumer groups
`${consumer_group_prefix}*`. Variables and defaults are in `deploy/terraform/variables.tf`.
