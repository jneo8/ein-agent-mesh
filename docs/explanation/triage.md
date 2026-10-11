# Triage

Triage owns `detected → open`: it decides what each alert means. Rules handle what is already known;
the model is asked only about what is new.

## The problem

- One failure produces dozens of alerts across layers within a minute.
- One investigation per alert floods the orchestrator and the person. One investigation for
  everything hides a second failure inside the first.

## Rules first

Every alert carries a `group_key` (Alertmanager's `groupKey`, or the gateway's `cluster,namespace`).
Before the model sees anything:

- fingerprint already judged noise in this cluster → ignored. Re-sent noise never costs a call.
- group with exactly one open incident → attached. Re-sends and same-group symptoms never cost a call.

Everything else is batched per cluster, 50 alerts or 30 seconds, and sent to the model in one call.

## What the model decides

- `open`: a new incident with a title and a priority.
- `ignore`: noise, heartbeat, flapping.
- `attach`: only toward a listed candidate, which exists only when the model earlier split one
  group into several incidents.
- Never `merge`. Linking two incidents on evidence is the orchestrator's job.

## Under-merge on purpose

- Triage sees labels and annotations, nothing else. It cannot know that a filling volume sits on
  the pool that just lost two disks.
- So it groups what the labels tie together and never guesses at causes across layers.
- The costs are asymmetric: a false merge hides a root cause, a split only opens one more
  investigation that can be merged later. The evals score it that way: a false merge fails, a split
  only costs recall.

## Validate, then fall back

- Every alert in the batch must appear in exactly one decision; an `attach` must name a candidate.
- On timeout, bad JSON or a failed check, rules decide instead: informational severities are ignored,
  the rest open one incident per group. The decision is marked `fallback`.

## What triage remembers

- Per cluster: the open incidents with why they were opened and what was attached to them, and the
  fingerprints judged noise with why.
- Rewritten whole after every change; read back on restart. Nobody else reads it.
- Bounded: recent attachments only, noise forgotten after a TTL.
- Why Kafka and not a file: a restarted pod can land anywhere, and a replica that takes over a
  partition loads exactly the clusters it now owns.

## Known gaps

- Nothing removes a closed incident from the table yet; that arrives with `closed`.
- Resolved alerts are skipped; "all alerts resolved" as a hint is not emitted yet.
- Ignored alerts have no incident id; a storm's discards are found by cluster, not by `root_id`.

Settings and the loop: [commands › triage](../reference/cli.md#triage).
