# Settings

Every process reads these from the environment (`src/ein_mesh/config.py`). `just` recipes load
`.env` from the repo root first. Credentials and keys stay out of git.

## Kafka

| Variable | Default | Meaning |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP` | `127.0.0.1:9092` | broker `host:port` |
| `KAFKA_USERNAME` | empty | SASL/SCRAM-SHA-512 user; empty means no SASL |
| `KAFKA_PASSWORD` | empty | |
| `KAFKA_GROUP_PREFIX` | `ein-` | consumer group prefix; must match Terraform `consumer_group_prefix` |

Producers run idempotent with gzip compression.

## Topics

| Variable | Default | Meaning |
| --- | --- | --- |
| `TOPIC_PREFIX` | `ein.` | prefix of every topic; must match Terraform `topic_prefix` |
| `TOPIC_ALERTS` | `${TOPIC_PREFIX}alerts` | full name, overrides the prefix |
| `TOPIC_ALERTS_STATE` | `${TOPIC_PREFIX}alerts.state` | |
| `TOPIC_INCIDENTS_CRITICAL` | `${TOPIC_PREFIX}incidents.critical` | |
| `TOPIC_INCIDENTS_NORMAL` | `${TOPIC_PREFIX}incidents.normal` | |
| `TOPIC_INCIDENTS_STATE` | `${TOPIC_PREFIX}incidents.state` | |
| `TOPIC_INCIDENTS_TASKS` | `${TOPIC_PREFIX}incidents.tasks` | |
| `TOPIC_INCIDENTS_REPORTS` | `${TOPIC_PREFIX}incidents.reports` | |
| `TOPIC_PARTITIONS` | `12` | partitions of every topic; fixed once created |
| `TOPIC_REPLICATION` | `1` | at most the broker count |

A full name outside the prefix needs its own ACL.

## Gateway

| Variable | Default | Meaning |
| --- | --- | --- |
| `GROUP_BY` | `cluster,namespace` | labels joined into `group_key` when the webhook has no `groupKey` |

## Triage

| Variable | Default | Meaning |
| --- | --- | --- |
| `EIN_AGENT_MODEL` | `gemini/gemini-3.7-flash` | litellm model name |
| `GEMINI_API_KEY` | | or the key the chosen provider needs |
| `TRIAGE_PROMPT` | shipped `prompts/triage.md` | a file with the system prompt; `ein-mesh triage --prompt FILE` overrides both |
| `TRIAGE_BATCH_SIZE` | `50` | send the batch at this many pending alerts; also the poll cap |
| `TRIAGE_BATCH_SECONDS` | `30` | or when the oldest pending alert is this old |
| `TRIAGE_LLM_TIMEOUT` | `120` | seconds before the fallback runs; `max_poll_interval_ms` is this plus 60 s |
| `ALERTS_STATE_MAX_ATTACHMENTS` | `20` | attachments kept per incident |
| `IGNORED_TTL_DAYS` | `7` | forget an ignored fingerprint unseen for this long |

## Orchestrator

| Variable | Default | Meaning |
| --- | --- | --- |
| `TASK_DEADLINE_SECONDS` | `900` | a task not reported by then is `expired` |
| `TASK_MAX_ATTEMPTS` | `2` | re-issues after `failed` / `expired` stop here; the task becomes `exhausted` |

## Investigator

| Variable | Default | Meaning |
| --- | --- | --- |
| `EIN_AGENT_MODEL` | `gemini/gemini-3.7-flash` | shared with triage |
| `INVESTIGATOR_PROMPT` | shipped `prompts/investigator.md` | a file with the system prompt; `$tools` is replaced by the toolbox's guide and catalogs, `$submit` by the submit tool's name; `--prompt FILE` overrides both |
| `INVESTIGATOR_TIMEOUT` | `600` | seconds per task before it is reported `failed`; `max_poll_interval_ms` is this plus 60 s; `--timeout` overrides |
| `INVESTIGATOR_SUBMIT_RESERVE` | `120` | the last part of the timeout, kept for submitting: the model stops looking at `timeout - reserve`; `--submit-reserve` overrides |
| `INVESTIGATOR_MAX_DEPTH` | `3` | tasks deeper than this are refused |
| `INVESTIGATOR_TOKEN_BUDGET` | `200000` | tokens (prompt and completion, as the model reports them) one task may spend before the model is told to submit; `--token-budget` overrides |

## Tools

The investigator's read-only sources. A source whose URL is unset is not offered. Reference:
[tools](tools.md).

| Variable | Default | Meaning |
| --- | --- | --- |
| `UTCP_PROMETHEUS_URL` | unset | base URL of the Prometheus HTTP API, proxy prefix included (`http://<proxy>/cos-prometheus-0`) |
| `UTCP_LOKI_URL` | unset | base URL of the Loki HTTP API |
| `UTCP_ALERTMANAGER_URL` | unset | base URL of the Alertmanager v2 API |
| `UTCP_PROMETHEUS_TOKEN`, `UTCP_LOKI_TOKEN`, `UTCP_ALERTMANAGER_TOKEN` | unset | bearer token sent as `Authorization` |
| `UTCP_ALLOW_HTTP` | `false` | accept plain `http://` to a non-loopback host; `https://` and loopback never need it |
| `TOOL_CALL_TIMEOUT` | `60` | seconds per tool call; a slow call is an error the model reads, not a failed task |
| `DIGEST_MAX_BYTES` | `8192` | a result up to this size is returned whole; larger ones come back as a digest of this size |
| `TOOL_RESULT_MAX_BYTES` | `16384` | ceiling on one `read_result` / `grep_result` response |
| `RESULT_CACHE_MAX_BYTES` | `67108864` | snapshot cache per task; an evicted snapshot is re-fetched |
| `SSL_CERT_FILE` | system store | OpenSSL's variable: a private CA for an `https://` source |

## Terraform

Inputs of `deploy/terraform` (`variables.tf`).

| Variable | Default | Meaning |
| --- | --- | --- |
| `model_name` | `ein-mesh` | Juju model |
| `workload_storage` | `ceph-xfs` | storage class for every PVC |
| `kafka_channel` | `4/stable` | `kafka-k8s` channel |
| `kafka_units` | `1` | brokers; `TOPIC_REPLICATION` must not exceed it |
| `topic_prefix` | `ein.` | PREFIXED topic ACL; must equal `TOPIC_PREFIX` |
| `consumer_group_prefix` | `ein-` | PREFIXED group ACL; must equal `KAFKA_GROUP_PREFIX` |

The data-integrator user gets `producer,consumer` on the topic prefix, `READ` on the group prefix and
`DELETE` on the topic prefix (`entity-permissions`). The Kafka charm applies ACLs when the integration
is created; a later change needs the integration replaced and the credentials refreshed.
