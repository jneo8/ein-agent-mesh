# Gateway

- The integrator that writes the `detected` state: Alertmanager webhook in, one `Alert` per alert
  out on `alerts`.
- Flags and endpoints: [roles reference](../reference/roles.md#ein-mesh-gateway).

## Why it exists

- Alertmanager can only POST a webhook; it cannot write to Kafka.
- Everything after the gateway sees only the `Alert` shape and knows nothing about Alertmanager's
  payload.
- A second source (Grafana alerting, PagerDuty) is another route in the gateway, not a change
  anywhere else.

## What it does

- **Normalizes.**
  - Pulls cluster, namespace, alertname, severity and status out of the free-form labels.
  - Keeps the full labels and annotations alongside.
  - Cluster comes from `cluster`, then `juju_model`, then `juju_model_uuid` (where COS puts it).
- **Deduplicates.**
  - Alertmanager re-sends firing alerts every group interval.
  - Key = fingerprint + `startsAt`; repeats are dropped.
  - Done here without an LLM: it is cheap, and triage should spend its one call per batch on new
    alerts only.
- **Keys by cluster.**
  - One cluster's alerts share a partition, so triage can batch per cluster.

## What it does not do

- No LLM, no decisions.
- No state beyond the dedup set.
- No authentication, no TLS.
- A pure I/O boundary: can sit next to Alertmanager and restart independently of triage.

## Assigns the group key

- Every `Alert` carries `group_key` and `group_labels`, the handle triage uses to attach by rule.
- Alertmanager's webhook `groupKey` / `groupLabels` are used when present. They are optional
  information: Alertmanager has no persistent "open group", the key is just a stable string for a
  label combination.
- Without them the gateway derives the key from the `GROUP_BY` labels (default `cluster,namespace`),
  so the mesh does not depend on how Alertmanager is configured and recorded payloads work unchanged.

## Known limit

- The dedup set is in memory. A restart forgets it; Alertmanager's next repeat goes through.
- Accepted in v1: triage absorbs duplicates as an `attach` or an `ignore`.
- Fix if ever needed: a compacted seen-set topic, the same pattern as `triage.state`.

## Standing in for Alertmanager

- The recorded storm lives in `tests/eval/scenarios/ceph-osd-storm/alerts.json`, an Alertmanager
  webhook payload.
- `just fire` curls it at the gateway with `startsAt` set to now: the whole live path.
- In unit tests the same fixture goes through `to_alert` onto an `InMemoryBus`, no HTTP at all.
- There is no program that writes to `alerts` while bypassing the gateway.
