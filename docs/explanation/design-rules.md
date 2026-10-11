# Design rules

The rules every topic and agent follows, each with its reason.

## Agents talk through topics, never to each other

- An agent reads one topic, decides, writes to the topics it owns.
- The topic is the permission boundary: the Kafka ACL says who may write where.
- Why: no agent needs to know another exists; a role can be replaced, moved or scaled alone.

## Two kinds of topic, nothing else

- **Flow**: one message type, from one agent to the next. Consumed, then gone.
- **State**: one agent's memory, keyed by its unit of work, compacted, read only by that agent on
  restart. Named after the topic it consumes plus `.state`.
- A topic exists only when something consumes it.
- Why no shared log: a stream everybody writes and nobody reads is never defined by its reader, so
  it grows without a shape. The reason behind a decision lives in the memory of the agent that made it.
- Why no separate lifecycle topic: the orchestrator owns the lifecycle, so its memory *is* the
  lifecycle status. A view for other readers is derived when such a reader exists.

## Roles decide, integrators translate

- A **role** runs a model and makes decisions: triage, orchestrator, investigator.
- An **integrator** moves data across the boundary and decides nothing: gateway (webhook in),
  frontend (chat in and out), tail (stdout).
- Why: a translator with judgement is a second decision-maker nobody designed.

## One owner per transition

- Every lifecycle move is written by exactly one role: triage into `open`, the orchestrator after.
- Workers report; they never change a state.
- Why: two roles reacting to the same state duplicate work or loop.

## One identity per incident

- Everything about an incident carries its `root_id` and uses it as the Kafka key.
- Every topic has the same partition count, so all records of one incident land on the same
  partition number and the instance holding its state receives them.
- The count is fixed at creation; changing it would move live incidents between instances.
- Alerts are keyed by cluster instead, because triage batches per cluster.

## Produce first, commit after

- Offsets are committed only once everything a poll produced has been written.
- A crash replays the poll. Replays are harmless: a known `root_id` is skipped, an attach is
  idempotent on alert id, a report is matched by task id.
- Why: no lost decisions, no transaction machinery in v1.

## Memory is small and bounded

- A state record is rewritten whole, so it must stay small: a summary, never a transcript.
- The orchestrator's `context` is capped at 16 KB. Triage keeps only recent attachments and forgets
  ignored noise after a TTL.
- Why: compacted topics stay at roughly active incidents times record size, and a restart is one read.

## Work is re-issued, never edited

- A task is immutable. A retry or a change of direction is a new task that points at the old one.
- A late result still counts; only a finished task is final.
- Why: the lineage of an investigation stays readable, and duplicates cost tokens, not correctness.

## The model earns every call

- Rules first: known noise is dropped and same-group alerts attach without the model.
- The model is asked only about what is new, and its answer is validated before it is applied. A
  rule-based fallback always exists.
- The orchestrator never sees raw tool output, only reports. That is the prompt-injection boundary.

## Code sees a bus, not Kafka

- Roles use a `MessageBus`; one module imports Kafka. An in-memory bus runs the same code in tests
  and evals.
- Nothing Kafka-specific leaks into a role: no manual partition assignment, no partition numbers.
- Why: per-role tests and scenario evals cost nothing and run anywhere.

## What stays fixed

- Flow topic names and the single record type on each.
- Task headers: `root_id`, `task_id`, `parent_id`, `role`, `deadline`.
- The public fields of `incidents.state`: `status`, `title`, `priority`, `updated_at`. The rest
  evolves behind `schema_version`.
