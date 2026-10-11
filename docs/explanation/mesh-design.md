# Mesh design

- The rules every role and topic follows.
- The states themselves: [the incident lifecycle](incident-lifecycle.md).

## Roles talk through topics, never to each other

- A role reads one topic, does its work, writes to the topics it is allowed to write.
- The topic boundary is the permission boundary:
  - the data-integrator ACL says which principal may write where;
  - a role's config says which topics it may target.
- Intent is a separate topic, not a field in a shared one:
  - ACLs stop at the topic;
  - a consumer group cannot subscribe to half a topic.
- Partitions are only for parallelism. The key keeps related records together:
  - `cluster` for everything triage touches;
  - `root_id` for everything in one investigation.
- Two kinds of participant:
  - **Roles** run an LLM and make decisions.
  - **Integrators** bridge an external system onto a topic and make none.
  - v1 has two roles (triage, orchestrator PoC slice) and two integrators (gateway, tail).
  - Packages mirror this: `ein_mesh.roles`, `ein_mesh.integrators`, and `ein_mesh.bus` beneath both.

## Two kinds of topic, nothing else

- **Flow**: one message type, from one role to the next. `alerts`, `incidents.critical` /
  `incidents.normal`, `incidents.tasks`, `incidents.reports`; later `incidents.questions` and
  `incidents.answers` for the frontend.
- **State**: one role's memory, keyed by that role's unit of work, compacted, read back only by that
  role on restart. Named after the topic the role consumes plus `.state`: `alerts.state` (triage),
  `incidents.state` (orchestrator). A role that consumes several topics names its state after the one
  that defines it.
- A topic exists only when something consumes it. There is no log that everybody writes and nobody
  reads; the reason behind a decision lives in the state of the role that made it.
- The orchestrator owns the lifecycle, so its state record is the incident's lifecycle status. A
  public view for other readers (`cluster.status`, the bot) is derived from it when such a reader
  exists, never written by anyone else.
- One noun per object: everything about an incident is `incidents.*`. Work handed to a role is a
  `Task`, its result a `Report`.
- Roles are added when someone has to own a transition or do a piece of work. One `incidents.tasks`
  topic serves every worker role (a `role` field selects the consumer) until a second role exists;
  then it splits into `incidents.tasks.<role>` under the same prefix.

## One owner per transition

- Design decision four: every investigation has a single owner.
- Applied to the lifecycle: **each state transition is written by exactly one role.**
  - Into and within `open` (open, attach, merge): triage.
  - Out of `open` and every state after it: orchestrator.
  - Investigators and consultants only write reports to `incidents.reports`.
  - The frontend only relays questions and answers. A question lists its allowed replies and a
    timeout default; the frontend never interprets free text.
- Pure choreography (every role reacts to any state it sees) is rejected: two LLM roles reacting to
  the same state duplicate work or loop.
- Same rule: a future reviewer role returns a `review` event instead of writing `closed` itself.

## Reports are a queue, not a log

- 2026-10-10 put reports on the event log, because the orchestrator read every event anyway.
- 2026-10-11 removed the event log: it had no consumer, and a work result is a message to be
  processed, not a record to be kept. Reports go on `incidents.reports`, keyed by `root_id`, consumed
  by the orchestrator. Lag on that topic is the number of results waiting.
- A task is immutable; re-issuing one is a new `task_id` with `parent_id` pointing at the old. A late
  report overrides an `expired` or `failed` task; only `done` is final.

## Knowing who is on the mesh

- **Consumer groups are the roster.**
  - Every role consumes with group `${KAFKA_GROUP_PREFIX}<role>`.
  - Describe the groups with that prefix to list live roles, subscriptions and lag.
  - Blind spots: readers without a group (`tail`, a role reading its state topic at startup); a
    stopped role keeps its group until offsets expire (7 days default), so check for members, not
    existence.
- **A startup announcement, later.**
  - Each role writes one `role.started` event: name, version, topics, `delegate_to`.
  - Answers "what can it do", which group membership cannot.
  - A declaration, written once.
- **No heartbeat.**
  - Kafka's group protocol already answers "is it alive" (its own heartbeat, `max.poll.interval.ms`).
  - "Is it absent" is answered lazily by a task deadline expiring; acceptable at minute-scale
    deadlines.
  - Reconsider only if waiting out a deadline becomes measurably painful.
  - Producer-only integrators (gateway, frontend) are monitored via `/healthz` and external checks.

## Roles are decision logic behind one bus interface

- Roles see `MessageBus`, never Kafka. `bus/kafka.py` is the only module that imports aiokafka.
- `Role` owns the loop, the restore step and the commit-after-produce rule. A subclass is its
  `inputs`, its `restore` and its `handle`.
- `InMemoryBus` replaces Kafka in tests and evals: feed records in, run the role until idle, read
  the output topics back. The same role code runs in both.
- Scenario evals (`tests/eval`) are the same harness one level up: a recorded payload, a scripted or
  live model, a score against the expected grouping. The scripted score is the baseline the live
  model is measured against.
- Why: the design note's rule that agent logic must not use anything Kafka-specific (no manual
  `assign()`, no partition numbers), so an in-memory bus can always stand in. This is what makes
  per-role evals cheap.

## What stays fixed

- Flow topic names and the one record on each.
- The headers a task carries: `root_id`, `task_id`, `parent_id`, `role`, `deadline`.
- In a state record, the fields a later reader may rely on (`status`, `title`, `priority`,
  `updated_at` on `incidents.state`); the rest evolves behind `schema_version`.
- Roles can move between processes and be rewritten behind these without the others noticing.
