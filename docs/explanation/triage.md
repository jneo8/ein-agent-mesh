# Triage

- The role that owns `detected → open` and the changes within `open` (attach, merge): reads
  `alerts`, decides what each alert means, writes incidents, events and its own checkpoint.
- Settings, decisions and the loop: [roles reference](../reference/roles.md#ein-mesh-triage).

## The problem it is shaped by

- A Ceph OSD failure produces dozens of alerts across Ceph, PVCs and pods within a minute.
- One investigation per alert would flood the orchestrator and the SRE.
- Alertmanager's own grouping is not used: the mesh needs the individual alerts and its own
  grouping logic.

## Rules first, the model only for what is new

- Every alert carries a `group_key`: Alertmanager's `groupKey` when the webhook had one (optional
  information, Alertmanager keeps no "open group" entity), else derived by the gateway from the
  `GROUP_BY` labels, default `cluster,namespace`.
- Before anything reaches the model, triage applies two rules per alert:
  - fingerprint already judged noise for this cluster → ignored by rule. Without this, a re-sent
    Watchdog sharing the `cos` group with a TargetDown incident would attach to it; the storm replay
    test caught exactly that.
  - group has exactly one open incident → attached by rule. Re-sends and same-group new symptoms
    never cost a model call.
- Everything else is batched for the model: open or ignore, with priority and title. `attach` is
  allowed only toward candidate incidents, the open incidents that share a group with an alert in
  the batch, which only exist when the model earlier split one group into several incidents (node-7
  and node-3 in `kube-system`). The prompt lists only those candidates, not every open incident.
- No `merge` in triage. Linking two incidents is the orchestrator's, on evidence.
- What the model still earns its call for: noise, priority, title, and splitting a group the labels
  alone would lump together.
- The live picture ("what else is firing right now") belongs to the investigator, which will get a
  read-only Alertmanager tool; not implemented yet.

## Not root-cause attribution

- Triage sees labels and annotations, nothing else. It cannot know that a filling PVC in one
  namespace sits on the Ceph pool that just lost two OSDs.
- So it does not try. It groups what the labels tie together: same namespace, same host, same
  entity, same alert family. Cross-layer causes are left for the orchestrator to merge once an
  investigator has evidence.
- The error costs are not symmetric. A false merge hides a second root cause inside the first
  incident; a split only opens one more investigation that can be merged later. Triage therefore
  under-merges on purpose, and the eval scores it that way (`false_merge` is a hard failure, `splits`
  only cost recall).
- Where the LLM earns its call: dropping noise, choosing priority, writing a title. The grouping
  itself stays close to the label structure.

## One LLM call per cluster per batch

- Alerts the rules did not settle accumulate per cluster until 50 alerts or 30 seconds.
- The whole batch goes to the model in one call, with the candidate incidents (same group as an alert
  in the batch), if any.
- One decision per alert: `open` a new incident with title and priority, `ignore`, or `attach` to a
  listed candidate.

## Validate before trusting

- Checks on the model output:
  - every alert in the batch appears in exactly one decision;
  - an `attach` names a listed candidate.
- On timeout, malformed JSON or a failed check, a rule-based fallback runs:
  - ignore severity `info` and `none` outright;
  - attach to the newest candidate of the alert's group;
  - otherwise open one incident per group (per namespace when there is no group key).
- Ignored fingerprints are remembered in `triage.state`, so re-sends are ignored by rule next time.
- Every decision, LLM or fallback, is recorded on `investigation.events` with its reason and which
  path produced it, so model quality can be read off the log.

## Two outputs for two readers

- `open` → a record on `incidents.critical` or `incidents.normal`.
  - The work order: a new investigation exists.
  - Grouped alerts ride inline so the orchestrator need not look them up.
  - Consumed once and discarded.
- Every decision (open, attach, ignore), by rule or by model → a record on `investigation.events`
  with `how` = `rule`, `llm` or `fallback`.
  - The audit log: why.
  - Kept for weeks; read by the orchestrator, the frontend and retrospectives.
- Incidents are split by priority:
  - Kafka has no priority inside a topic;
  - two topics let the downstream dedicate a consumer to critical so a normal backlog never delays
    it;
  - with two levels the topic count stays small; more levels would be one topic plus a header.

## The open-incident table and its checkpoint

- To attach by rule and to offer candidates, triage must know what is already open and what it
  already judged noise.
- The table (incidents with their `group_key`, plus ignored fingerprints) lives in memory per cluster.
- After every applied batch, the whole table for that cluster is written to `triage.state`.
- `triage.state` is compacted: only the latest snapshot per cluster survives. Triage rebuilds from
  it on startup.
- This is the changelog half of the Kafka Streams pattern; the in-memory dict stands in for RocksDB.
- Why Kafka rather than local disk:
  - a restarted pod can land on any node, no PVC, no backup;
  - `alerts` and `triage.state` are both keyed by cluster, so a second replica that takes over a
    partition loads exactly the clusters it now owns;
  - a topic can sit inside a Kafka transaction with the offset commit; a local file cannot. v1 does
    not use transactions yet, so this is a door left open.
- Nothing else should read `triage.state`:
  - its fields are shaped for the prompt, not as a contract;
  - the outward-facing picture is `investigation.state`, owned by the orchestrator.

## Commit last

- Offsets on `alerts` are committed only after the batch's incidents, events and state snapshot
  are produced.
- A crash mid-batch replays the batch; duplicates are absorbed as attaches.
- The v1 stand-in for the transactional commit the design asks for.

## Known gaps

- **Nothing removes an incident from the table.**
  - It only grows, and so does the open-incident summary in the prompt.
  - Fix: subscribe to `investigation.state`, drop an incident when its status becomes `closed`,
    rewrite `triage.state`.
  - Prune only on `closed`. While the report awaits sign-off (`waiting`, `accept_report`) the
    incident stays in the table, so new alerts still attach and a send-back needs no rebuild.
  - The ignored-fingerprint set has no bound or TTL yet either.
- **`resolved` alerts are skipped.**
  - Alertmanager's resolved notifications already arrive on `alerts`.
  - Plan: emit `alerts.resolved` when every alert of an incident has resolved, as a hint to the
    orchestrator.
- **`ignore` has no `root_id`.**
  - Ignored alerts are keyed `triage-<cluster>`.
  - A `root_id` lookup will not show what a storm discarded; retrospectives must query by cluster
    too.
