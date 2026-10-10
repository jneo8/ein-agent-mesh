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

## Topics are states, roles are arrows

- Topics are designed from what an SRE needs to know about an incident, not from which agents
  exist.
- Each lifecycle state is a topic. Each role owns the transition out of one state.
- Roles are added when someone has to own a transition. The states do not change when they are.

## Two layers of topics

- **Lifecycle topics**: `alerts`, `incidents.*`, `investigation.events`, `investigation.state`,
  `user.questions`, `user.answers`.
  - Carry no role name.
  - Meant to stay fixed while roles come and go.
- **Work topics**: `contracts.<role>`, `dlq.<role>`.
  - Exist only because an ACL is per topic; a role needs a topic of its own to read.
  - Adding a role = adding this pair + listing it in the orchestrator's `delegate_to`.
  - Nothing in the lifecycle layer moves.
- `triage.state` is neither. It is triage's private checkpoint, see [triage](triage.md).

## One owner per transition

- Design decision four: every investigation has a single owner.
- Applied to the lifecycle: **each state transition is written by exactly one role.**
  - Into and within `open` (open, attach, merge): triage.
  - Out of `open` and every state after it: orchestrator.
  - Investigators and consultants only write evidence to `investigation.events`.
  - The frontend only relays questions and answers. A question lists its allowed replies and a
    timeout default; the frontend never interprets free text.
- Pure choreography (every role reacts to any state it sees) is rejected: two LLM roles reacting to
  the same state duplicate work or loop.
- Same rule: a future reviewer role returns a `review` event instead of writing `closed` itself.

## Reports go on the event log

- Original design: sub-agent reports on their own topic `reports`, the reply side of a contract.
- Deferred on 2026-10-10:
  - reports are written to `investigation.events` with `kind=report`;
  - a contract's `reply_to` header is kept but fixed to the events topic.
- Why:
  - the orchestrator already reads every event; a second topic added nothing to how it receives a
    report;
  - reports on the log let the frontend post an investigator's findings into the thread as they
    arrive;
  - "consume once and discard" is only a retention setting; keeping reports 30 days costs little.
- When it comes back: several orchestrator replicas need a reply routed to the one that issued the
  contract. `events` is a broadcast; a keyed `reports` topic can route. `reply_to` is a header, so
  that is a new value, not a change to any role.

## Knowing who is on the mesh

- **Consumer groups are the roster.**
  - Every role consumes with group `${KAFKA_GROUP_PREFIX}<role>`.
  - Describe the groups with that prefix to list live roles, subscriptions and lag.
  - Blind spots: readers without a group (`tail`, triage reading `triage.state` at startup); a
    stopped role keeps its group until offsets expire (7 days default), so check for members, not
    existence.
- **A startup announcement, later.**
  - Each role writes one `role.started` event: name, version, topics, `delegate_to`.
  - Answers "what can it do", which group membership cannot.
  - A declaration, written once.
- **No heartbeat.**
  - Kafka's group protocol already answers "is it alive" (its own heartbeat, `max.poll.interval.ms`).
  - "Is it absent" is answered lazily by a contract deadline expiring; acceptable at minute-scale
    deadlines.
  - Reconsider only if waiting out a deadline becomes measurably painful.
  - Producer-only integrators (gateway, frontend) are monitored via `/healthz` and external checks.

## Roles are decision logic behind one bus interface

- Roles see `MessageBus`, never Kafka. `bus/kafka.py` is the only module that imports aiokafka.
- `Role` owns the loop, the restore step, the commit-after-produce rule and the event envelope.
  A subclass is its `inputs`, its `restore` and its `handle`.
- `InMemoryBus` replaces Kafka in tests and evals: feed records in, run the role until idle, read
  the output topics back. The same role code runs in both.
- Scenario evals (`tests/eval`) are the same harness one level up: a recorded payload, a scripted or
  live model, a score against the expected grouping. The scripted score is the baseline the live
  model is measured against.
- Why: the design note's rule that agent logic must not use anything Kafka-specific (no manual
  `assign()`, no partition numbers), so an in-memory bus can always stand in. This is what makes
  per-role evals cheap.

## What stays fixed

- Lifecycle topic names.
- The record on each topic.
- The headers a contract carries: `root_id`, `reply_to`, `deadline`.
- Roles can move between processes and be rewritten behind these without the others noticing.
