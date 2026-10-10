# Roles and integrators reference

Roles are `Role` subclasses in `src/ein_mesh/roles/`; integrators are plain coroutines in
`src/ein_mesh/integrators/`. Both talk to Kafka only through `MessageBus` (`src/ein_mesh/bus/`). The CLI is `src/ein_mesh/__main__.py` (click);
`ein-mesh <cmd>` and `python -m ein_mesh <cmd>` are the same thing.

Every process `ein-mesh` can run: what it reads, what it writes, what it keeps, and how it is
configured. Record formats are in the [topics reference](topics.md). Topic names below are the
suffixes; the full name is `TOPIC_PREFIX` + suffix unless overridden.

## Role × topic matrix

| | `alerts` | `triage.state` | `incidents.critical` / `.normal` | `investigation.events` | `investigation.state` |
| --- | --- | --- | --- | --- | --- |
| `gateway` | **write** | | | | |
| `triage` | read, group `ein-triage` | read at start, **write** per batch | **write** on `open` | **write** per decision | |
| `orchestrator` | | | read, group `ein-orchestrator` | **write** `investigation.started` | read at start, **write** per transition |
| `tail` | read, no group | read, no group | read, no group | read, no group | read, no group |

## The `Role` base class

Every role is a subclass of `Role` in `src/ein_mesh/roles/base.py`; the roles themselves are modules in `src/ein_mesh/roles/`. The base class owns the consumer
loop; a subclass supplies only its decision logic.

| Member | Kind | Description |
| --- | --- | --- |
| `name` | class attribute | Role name. Consumer group is `${KAFKA_GROUP_PREFIX}<name>`; also the `role` field on every event it writes. |
| `inputs` | abstract property | Topics to consume. |
| `restore()` | coroutine, optional | Rebuild in-memory state from compacted topics via `bus.read_to_end`. Default: nothing. |
| `handle(records)` | abstract coroutine | Process one poll, possibly empty. Return `True` when every record received so far has had its outputs produced; the loop then commits. |
| `idle()` | method, optional | `True` when nothing is buffered inside the role. Batching roles override it. Used by `run_until_idle`. |
| `consumer_options()` | method, optional | Extra consumer settings, e.g. `max_poll_interval_ms`. |
| `poll_max_records()` | method, optional | Cap per poll. |
| `event(root_id, kind, body)` | helper | Write an `Event` with `role=name` to `investigation.events`. |
| `run()` | loop | `subscribe` → `start` → `restore` → poll / `handle` / `commit` forever. |
| `run_until_idle()` | test loop | Same, but returns once the bus has no unread records and `idle()` is true. |

Roles never import Kafka. They receive a `MessageBus` and call `produce`, `read_to_end`, `poll`,
`commit` on it.

## `MessageBus`

`src/ein_mesh/bus/`: `base.py` (`Record`, the `MessageBus` ABC), `kafka.py` (`KafkaBus`, the only module
that imports aiokafka), `memory.py` (`InMemoryBus`).

| Member | Kind | Description |
| --- | --- | --- |
| `start()`, `stop()` | abstract | Open and close the transport. |
| `_produce(topic, key, value: dict)` | abstract | Send one record. |
| `read_to_end(topic)` | abstract | Everything on a topic now; the restore step for compacted state. |
| `subscribe(group, *topics, **opts)` | abstract | Declare what `poll` reads. `group=None` means no consumer group. |
| `poll(timeout_ms, max_records)` | abstract | Next unread records. |
| `commit()` | abstract | Commit the subscription's offsets. |
| `produce(topic, key, value)` | concrete | Normalises a pydantic model to a dict, then `_produce`. |
| `now()` | concrete, overridable | Monotonic clock; the in-memory bus returns a fake one. |
| `pending()` | concrete, overridable | Unread records remain; only the in-memory bus can say yes. |

| Implementation | Use | Notes |
| --- | --- | --- |
| `KafkaBus(KafkaSettings)` | production | One idempotent producer; one consumer created at `start()` from the `subscribe()` call. |
| `InMemoryBus()` | tests, evals | Topics are lists. `poll` returns unread records of the subscribed topics; `commit` is a no-op; an empty poll advances the fake clock behind `now()` by the poll timeout so time-based batching can be tested; `pending()` reports unread records. Helper: `values(topic)`. |

`Record` is `(topic, key, value: dict, offset)`. `produce` accepts a pydantic model or a dict.

## Testing a role

```python
bus, topics = InMemoryBus(), Topics(prefix="t.")
await bus.produce(topics.incidents("critical"), "inc-1", incident)
await Orchestrator(bus, topics).run_until_idle()
assert bus.values(topics.investigation_state)[0]["status"] == "investigating"
```

`just unit-test` runs `tests/unit`: no broker, no LLM. Two levels of mocking: most tests replace
`Triage.classify` with a coroutine that returns a `TriageOutput` or raises; `test_storm.py` keeps
`classify` and mocks `litellm.acompletion`, so prompt building and JSON parsing are exercised too.

## Scenario evals

`just eval` runs `python -m tests.eval`: every directory under `tests/eval/scenarios` with these files.

| File | Content |
| --- | --- |
| `alerts.json` | Alertmanager webhook payload, the recorded input; or `{"waves": [payload, ...]}` when alerts keep arriving. Each wave is ingested and triaged by a fresh `Triage` (restoring from `triage.state`, like a restart), with the orchestrator run after every wave. |
| `expected.json` | `incidents`: list of `{priority, alerts: [matcher]}`; `ignored`: list of matchers; optional `attached`: matchers for alerts that must have joined an existing incident rather than opened one. A matcher is a dict of labels that must select exactly one alert; `"_wave": n` picks one wave's instance of a repeated alert. |
| `model.json` | The scripted model: triage decisions with matchers instead of ids, resolved per batch at run time. An `attach` names its target as `"incident": <matcher>`, the open incident that already holds that alert, looked up in `triage.state`. |

The harness runs three stages on one `InMemoryBus`, each reading only what the previous stage left:

| Stage | Does | Scored on |
| --- | --- | --- |
| `gateway` | `to_alert` on every payload alert, produced to `alerts` | one record per alert, unique ids, no `unknown` cluster |
| `triage` | `Triage.run_until_idle` with `litellm.acompletion` patched to the scripted model (`--llm` keeps the real one) | grouping, ignore, priority, triage invariants (table below) |
| `orchestrator` | `Orchestrator.run_until_idle` | every incident has an `investigating` state and one `investigation.started` event, state agrees with its incident, nothing extra |

Stage control, so one stage can be debugged alone:

| Flag | Effect |
| --- | --- |
| `--until STAGE` | stop after that stage; later stages show `skip`. |
| `--dump DIR` | write every bus topic as `DIR/<scenario>/<topic>.json` after the run. |
| `--start-at STAGE --from-dump DIR` | seed the bus from a dump and run from that stage on. `--start-at orchestrator` replays the orchestrator on recorded triage output with no LLM call. |
| `--show` | per stage: gateway records, triage expected vs produced with titles and reasons, orchestrator states and event counts. |

The scorer reports per scenario:

| Column | Meaning |
| --- | --- |
| `gw` | gateway stage: `yes`, `NO` or `skip`. |
| `llm` | LLM calls made. |
| `tok_in`, `tok_out` | prompt and completion tokens summed over those calls, from the provider's `usage`; 0 for the scripted model. |
| `cost` | litellm's price for those tokens, `-` when it cannot price the model. |
| `groups` | produced / expected incidents, read from the final `triage.state` snapshot. |
| `exact` | expected groups reproduced exactly. |
| `false_merge` | alert pairs grouped together that the expected grouping keeps apart. Any value above 0 fails. |
| `splits` | alert pairs kept apart that the expected grouping has together. Costs recall only. |
| `pair_r` | 1 - splits / expected pairs. Compared with `--threshold`. |
| `ign_p`, `ign_r` | precision and recall of `ignore`. |
| `prio` | exact groups with the expected priority. |
| `fallback` | batches that went through the rule-based fallback. |
| (detail) `attached` | alerts attached to an open incident vs the number `expected.json` says must attach; a shortfall is a violation. Shown in `--show` and failure output, not in the table. |
| `orch` | orchestrator stage: `yes`, `NO` or `skip`. |
| `ok` | every stage that ran is ok: no violation anywhere, and for triage no false merge, ignore precision and recall 1.0, priorities right on exact groups, `pair_r` at or above the threshold. |

A `TOTAL` row sums calls, tokens, cost, groups and failures across scenarios, and counts how many were `ok`.

`--show` prints, per scenario, the expected grouping and the produced one side by side, each alert as
`alertname{distinguishing labels}`, with the model's title and reason per incident and per ignore.
`just eval-llm` passes `--show` by default. `--dump DIR` writes every bus topic as
`DIR/<scenario>/<topic>.json` for inspection by hand.

Invariants are checked per stage regardless of the model. Triage: every alert placed exactly once
across incidents and ignored; every incident has an `incident.opened` event; `triage.state` agrees with
`incidents.*`. Orchestrator: every incident is `investigating` with one `investigation.started` event
and matching priority, title and cluster. A violation names its stage. `--threshold F` fails the run when any `pair_r` is below `F` (default 1.0); a
false merge fails at any threshold. The asymmetry is the under-merge policy, see
[triage](../explanation/triage.md#not-root-cause-attribution). `tests/eval/test_eval.py` keeps the
harness honest: the scripted model must be clean at every stage, a lumping model must fail triage and
only triage, a splitting model must pass at a low threshold and fail at 1.0, a dead model must show as
fallback, `--until` must stop where told, and the orchestrator must replay from a dump with no LLM call.

Scenarios, each probing one thing triage must get right:

| Scenario | Alerts | Expected | Probes |
| --- | --- | --- | --- |
| `ceph-osd-storm` | 10 | 3 incidents, 2 ignored | Cross-layer cause (Ceph → PVC → pods) is not expected of triage: the temporal symptoms stay their own incident. |
| `same-namespace-two-faults` | 8 | 3 incidents, 2 ignored | Same namespace is not evidence: Loki OOM, Prometheus TSDB volume, and a lone catalogue restart stay apart, grouped by application. |
| `noise-storm-one-real` | 11 | 2 incidents, 8 ignored | Ignore precision: heartbeats, HPA notices, CPU throttling and a slow job are dropped; node-7 NotReady/Unreachable is one critical incident; a clock skew on another node stays separate. |
| `rolling-ceph-storm` | 11 in 3 waves | 2 incidents, 2 ignored, 6 attaches | Alerts keep arriving: wave 2 and 3 must attach to incidents restored from `triage.state` by a restarted triage (new OSD, health warning, re-sent osd.3, second temporal pod, PVC) and open only what is new; one LLM call per wave; the orchestrator must not re-open on replay. |

`just eval-test` runs the harness self-tests in `tests/eval/test_eval.py` (pytest); `just unit-test` runs
`tests/unit` only. `just fire` curls its `alerts.json` at a live gateway.

## Common settings

Read by every command from the environment, see `src/ein_mesh/config.py`.

| Variable | Default | Description |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | Broker `host:port`. |
| `KAFKA_USERNAME` | empty | SASL/SCRAM-SHA-512 user. When empty, connects without SASL. |
| `KAFKA_PASSWORD` | empty | SASL password. |
| `KAFKA_GROUP_PREFIX` | `ein-` | Prefix of every consumer group id. Must match Terraform's `consumer_group_prefix`. |
| `TOPIC_PREFIX`, `TOPIC_*` | see [topics](topics.md#naming) | Topic names. |
| `GROUP_BY` | `cluster,namespace` | Labels the gateway joins into an alert's `group_key` when the webhook carries no `groupKey`. |

All producers run with `enable_idempotence=True` and gzip compression.

## `ein-mesh init`

Creates every topic in the [topics reference](topics.md) with `TOPIC_PARTITIONS` partitions
(default 12) and `TOPIC_REPLICATION` replicas (default 1), applying each topic's cleanup and
retention config. Existing topics are left unchanged, then every mesh topic's partition count is
compared with the setting; a mismatch exits non-zero and names the topic. Safe to re-run. Needs no
running role; it is the only deploy prerequisite. See [partitions](topics.md#partitions).

## `ein-mesh gateway`

Integrator. Accepts Alertmanager webhook POSTs and produces one `Alert` per new alert.

| | |
| --- | --- |
| Source | `src/ein_mesh/integrators/gateway.py` |
| Reads | HTTP only |
| Writes | `alerts`, key = cluster |
| State | In-memory set of seen `Alert.id`. Lost on restart. |
| LLM | None |

**Flags**

| Flag | Default | Description |
| --- | --- | --- |
| `--host` | `0.0.0.0` | Listen address. |
| `--port` | `8088` | Listen port. |

**HTTP endpoints**

| Method | Path | Body | Response |
| --- | --- | --- | --- |
| `POST` | `/alertmanager` | Alertmanager webhook JSON (`alerts: [...]`) | `{"received": <n>, "produced": <m>}` where `m` excludes alerts whose `id` was already seen |
| `GET` | `/healthz` | | `ok` |

No authentication and no TLS. Alertmanager side: a `webhook_configs` receiver with
`url: http://<host>:8088/alertmanager`.

**Field mapping** from an Alertmanager alert to a `Alert` is given under `alerts` in the
[topics reference](topics.md#alerts).

## `ein-mesh triage`

Role. Batches alerts per cluster, classifies each batch with one LLM call, opens incidents.

| | |
| --- | --- |
| Source | `src/ein_mesh/roles/triage.py` |
| Reads | `alerts`, consumer group `${KAFKA_GROUP_PREFIX}triage`; `triage.state` at startup without a group |
| Writes | `incidents.<priority>` (key = `root_id`), `investigation.events` (key = `root_id`, or `triage-<cluster>` for `alerts.ignored`), `triage.state` (key = cluster) |
| State | In-memory `cluster -> incident id -> Incident` (each with its `group_key`) and `cluster -> ignored fingerprints`. Restored from `triage.state` at startup, rewritten after every change. |
| LLM | One `litellm.acompletion` per batch of alerts the rules did not settle, `temperature=0`, `response_format=json_object`. |

**Settings**

| Variable | Default | Description |
| --- | --- | --- |
| `EIN_AGENT_MODEL` | `gemini/gemini-3.7-flash` | litellm model name. |
| `GEMINI_API_KEY` | | Or whatever key the chosen litellm provider needs. |
| `TRIAGE_BATCH_SIZE` | `50` | Flush when pending alerts across all clusters reach this count. Also the consumer's `max_records` per poll. |
| `TRIAGE_BATCH_SECONDS` | `30` | Flush when the oldest pending alert is this old. |
| `TRIAGE_LLM_TIMEOUT` | `120` | Seconds before a model call is abandoned and the fallback runs. The consumer's `max_poll_interval_ms` is set to this plus 60 s. |

**Loop**

1. Poll `alerts`. Records that fail `Alert` validation are logged and skipped. Records with
   `status != "firing"` are skipped.
1b. Rules, per alert: fingerprint in the cluster's ignored set → `alerts.ignored` event with
   `how=rule`; `group_key` with exactly one open incident in the cluster → attached to it,
   `alerts.attached` with `how=rule`, state snapshot rewritten. Everything else becomes pending.
2. Pending alerts are grouped by cluster. When the total reaches `TRIAGE_BATCH_SIZE` or the batch
   is `TRIAGE_BATCH_SECONDS` old, every cluster's pending list is handled as one batch each.
3. Per batch: `classify` (LLM) then `validate`; on `TimeoutError`, `ValidationError`, `ValueError`
   or a litellm `APIError`, `fallback` runs instead.
4. `apply` produces the incidents, events and state snapshot.
5. Offsets are committed. Pending is cleared.

**Decisions** the model may return, and what `apply` does with each:

| `action` | Fields | Effect |
| --- | --- | --- |
| `open` | `alerts`, `priority`, `title`, `reason` | New `Incident` with id `inc-<UTC time>-<10 hex>`, e.g. `inc-20261010T074252Z-3f9a1c2b7d`; `IncidentMessage` on `incidents.<priority>`; event `incident.opened`. |
| `attach` | `alerts`, `incident`, `reason` | Only toward a candidate (an open incident sharing a group with an alert in the batch). Alert ids and names appended; event `alerts.attached`. Idempotent on alert id. |
| `ignore` | `alerts`, `reason` | Event `alerts.ignored` keyed `triage-<cluster>`; fingerprints added to the cluster's ignored set. |

**Validation** rejects an output when any alert id in the batch is missing or appears twice, or when
`attach` names an incident that is not a candidate.

**Fallback** ignores alerts with severity `info` or `none`, then groups the rest by `group_key`
(namespace when empty). A group with candidates gets one `attach` to the newest; otherwise one `open` with priority `critical` if any alert has
`severity=critical`, else `normal`, titled `<cluster>/<namespace>: <alertnames>`.

Events record `how` as `rule`, `llm` or `fallback`.

## `ein-mesh orchestrator`

Role, PoC slice. Takes ownership of every incident and moves it to `investigating`. No contracts,
no questions, no closing, no LLM yet.

| | |
| --- | --- |
| Source | `src/ein_mesh/roles/orchestrator.py` |
| Reads | `incidents.critical`, `incidents.normal`, consumer group `${KAFKA_GROUP_PREFIX}orchestrator`; `investigation.state` at startup without a group |
| Writes | `investigation.state` (key = `root_id`), `investigation.events` (key = `root_id`, `role=orchestrator`) |
| State | In-memory `root_id -> InvestigationState`, including the bounded `context` summary. Restored from `investigation.state` at startup, rewritten on every transition; events are not replayed. |
| LLM | None in this slice. |

**Loop**

1. Poll both incident topics. Records that fail `IncidentMessage` validation are logged and skipped.
2. A `root_id` already in the table is skipped with a log line (replay after a crash).
3. Otherwise: write `InvestigationState` with `status=investigating`, then event
   `investigation.started` with `priority`, `title` and the alert ids.
4. Commit offsets.

**Co-partitioning.** `incidents.*`, `investigation.events` and `investigation.state` are all keyed
by `root_id` and created with the same partition count (`TOPIC_PARTITIONS`), so one instance holds
the state for every record it receives. `Role.run` verifies the layout through `MessageBus.verify`
before restoring state and refuses to start on a mismatch. Running more than one instance
additionally needs the range assignor and state load/unload on rebalance, neither of which this
slice implements.

## `ein-mesh tail [TOPIC]...`

Integrator, `src/ein_mesh/integrators/tail.py`. Prints every record on the given topics as
`[<topic>] key=<key> <json>`.

| Argument / flag | Default | Description |
| --- | --- | --- |
| `TOPIC` (repeatable) | every mesh topic | A short name (`alerts`, `triage.state`, `incidents.critical`, `incidents.normal`, `investigation.events`, `investigation.state`) resolved through `Topics.resolve`, or a full topic name passed through unchanged. |
| `--from-start` | off | Read from the earliest offset instead of only new records. |

No consumer group, so it never affects anyone's offsets. `just tail incidents.critical investigation.events`.

## `just fire`

Not a program. A `curl` of `tests/eval/scenarios/ceph-osd-storm/alerts.json` at the gateway, with every `startsAt` set
to now by `jq` so each replay is a new alert instance for the gateway's dedup. Arguments: `cluster`
(default `dev`, rewrites the `cluster` label) and the `gateway_url` variable (default
`http://127.0.0.1:8088`). Writing to `alerts` without the gateway happens only in unit tests, where
the same fixture goes through `to_alert` onto an `InMemoryBus`.
