# Tools

The investigator's read-only tool plane, from `packages/ein-utcp`. Settings:
[tools](settings.md#tools). Concept: [tools](../explanation/tools.md).

## Sources

| Source | Spec | Query check | Records |
| --- | --- | --- | --- |
| `prometheus` | HTTP API 3.5.0, generated upstream | `format-query` before any `query` is sent | `data.result` |
| `loki` | HTTP API 3, written by hand (Loki publishes none) | `format-query` | `data.result`, or `data` for labels and series |
| `alertmanager` | API v2 0.34.1 | none | the root array |

Only GET operations are loaded. `just specs-download` refreshes the specs that have an upstream; a new
version is a new file under `packages/ein-utcp/src/ein_utcp/specs/<source>/`, chosen per deployment.

## Tools the model calls

| Tool | Arguments | Returns |
| --- | --- | --- |
| `grep_operations` | `source`, `pattern`, `limit` | matching catalog lines |
| `call_operation` | `source`, `operation`, `arguments` as `[{name, value}]`, values strings | the result whole when it fits `DIGEST_MAX_BYTES`, else a digest with a handle |
| `read_result` | `handle`, `offset`, `limit`, `fields` | a window of records, projected to `fields` when given |
| `grep_result` | `handle`, `pattern`, `before`, `after`, `max_matches`, `fields` | matching records |
| `refresh_result` | `handle` | the call run again; a changed snapshot is flagged |
| `list_results` | | one line per handle |
| `submit_report` | `summary`, `notes`, `identifiers`, `resources_checked` | ends the task |

Every failure is `{"error": ...}` the model reads: an unknown operation, a misspelled or missing
parameter (with the schema attached), a query the server's parser rejects, an HTTP error, a timeout, a
repeat of a call already registered.

## Catalog line

`<operation> | <path> | (<parameters>, * = required) | <description>`, one per operation, all of them
in the system prompt. Example: `prometheus.query | /cos-prometheus-0/api/v1/query | (query*, time, timeout, limit) | Evaluate an instant query`.

## Digest

```
Result call-3: alertmanager.getAlerts - 312 items, 96.1 KB, hash 7f3a1c0e...
Computed over all 312 items:
  status.state: active 312
  labels.severity: warning 201, critical 111
  labels.namespace: 14 distinct values (top: ceph 120, cos 61, ...)
First 3 records (trimmed):
  {"annotations":{...},"labels":{"alertname":"CephOSDDown",...}...
[Full result reachable: read_result('call-3', fields=[...]) ... refresh_result('call-3') ...]
```

Fields with more than 20 distinct values are not aggregated; timestamps and long hex identifiers are
skipped; a paginated call is flagged. The footer is never cut.

## Sizes

| Limit | Setting | Default |
| --- | --- | --- |
| tokens per task, then the model is told to submit | `INVESTIGATOR_TOKEN_BUDGET` | 200000 |
| time for looking, then the model is told to submit | `INVESTIGATOR_TIMEOUT` less `INVESTIGATOR_SUBMIT_RESERVE` | 480 s |
| seconds per call | `TOOL_CALL_TIMEOUT` | 60 |
| result returned whole, and digest size | `DIGEST_MAX_BYTES` | 8 KiB |
| one read / grep response | `TOOL_RESULT_MAX_BYTES` | 16 KiB |
| snapshot cache per task | `RESULT_CACHE_MAX_BYTES` | 64 MiB |
| identical call accepted | | twice, never back to back |

## What the report carries

`resources_checked` starts with the calls the runtime made, in order, as `<source>.<operation>
<key>=<value> ...` (a refresh adds `(refreshed)`); the model's own entries follow. Notes and identifiers
are trimmed from the oldest until the report fits its limit ([topics › reports](topics.md)).
