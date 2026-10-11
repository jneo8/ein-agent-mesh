# ein-agent-mesh

Kafka as the control plane between SRE investigation agents. This repo holds the
v1 shape from the design note: Juju `kafka-k8s` deployed with Terraform, plus the
**triage** role and the **gateway** integrator.

```
        sources                    integrator (no LLM)              Kafka · control plane                            roles
        ───────                    ───────────────────              ─────────────────────                            ─────

Alertmanager ──webhook──▶ ┌───────────────────┐                ┌──────────────────────────┐  batch per cluster   ┌────────────────────────────┐
 (not wired in v1)        │ gateway           │  dedup alert   │ alerts                   │  30 s or 50 alerts   │ triage                     │
                          │ POST /alertmanager│──key=cluster──▶│ $TOPIC_ALERTS · delete   │─────────────────────▶│ 1 batch = 1 LLM call       │
just gateway-fire ───────▶│ dedup: fingerprint│                └──────────────────────────┘  group ein-triage    │ validate ─┬─▶ apply        │
 (curl storm.json)        │   + startsAt      │                                                                  │ fallback ─┘   commit       │
                          └───────────────────┘                ┌──────────────────────────┐                      │                            │
                                                               │ triage.state             │◀── open incidents ───│ memory: open incidents     │
unit tests ─── storm.json ──to_alert──▶ InMemoryBus            │ $TOPIC_TRIAGE_STATE      │── restore on start ─▶│   per cluster              │
 (no broker, LLM mocked)                                       │ compacted, key=cluster   │                      └─────────────┬──────────────┘
                                                               └──────────────────────────┘                                    │ decisions
                                                               ┌──────────────────────────┐                                    │
                                                               │ incidents.critical       │◀── open                            │
                                                               │ $TOPIC_INCIDENTS_CRITICAL│    key=root_id, alerts inline      │
                                                               │ incidents.normal         │                                    │
                                                               │ $TOPIC_INCIDENTS_NORMAL  │                                    │
                                                               └──────────────────────────┘                                    │
                                                               ┌──────────────────────────┐                                    │
                                                               │ investigation.events     │◀── incident.opened/attached ───────┘
                                                               │ $TOPIC_EVENTS · 30 d     │    alerts.ignored (rule/llm/fallback)
ein-mesh tail ◀──── any topics (args) ─────────────────────────│ key=root_id              │    each with the LLM's reason
                                                               └──────────────────────────┘◀── investigation.started ──────────┐
                                                                                                                               │
                                                               ┌──────────────────────────┐                      ┌─────────────┴──────────────┐
                                                               │ investigation.state      │◀── status ───────────│ orchestrator (PoC slice)   │
                                                               │ $TOPIC_INVESTIGATION_    │    =investigating    │ reads incidents.*          │
                                                               │   STATE · compacted,     │── restore on start ─▶│ group ein-orchestrator     │
                                                               │   key=root_id            │                      │ memory: state per root_id  │
                                                               └──────────────────────────┘                      │ no LLM, no contracts yet   │
                                                                                                                 └────────────────────────────┘
```

Topic names in the diagram omit the prefix: the real name is TOPIC_PREFIX + suffix, so
`ein.alerts` by default. Set TOPIC_PREFIX to move the whole family, or one TOPIC_<NAME>
variable to rename a single topic.

Who touches what:

| | `alerts` | `triage.state` | `incidents.*` | `investigation.events` | `investigation.state` |
| --- | --- | --- | --- | --- | --- |
| `gateway` | write | | | | |
| `triage` | read | read at start, write | write on `open` | write per decision | |
| `orchestrator` | | | read | write `investigation.started` | read at start, write |
| `tail` | | | read | read | read |

Docs:

- Explanation: [incident lifecycle](docs/explanation/incident-lifecycle.md) (states and who moves them),
  [mesh design](docs/explanation/mesh-design.md) (the rules topics and roles follow),
  [gateway](docs/explanation/gateway.md), [triage](docs/explanation/triage.md).
- Reference: [roles](docs/reference/roles.md) (per process: reads, writes, state, flags),
  [topics](docs/reference/topics.md) (per topic: key, retention, record fields).

## Deploy Kafka (Terraform + juju provider)

`deploy/terraform` creates model `ein-mesh` on the current Juju controller with
`kafka-k8s` 4/stable (single unit, `roles=broker,controller`, `profile=testing`,
`expose-external=nodeport`) and a `data-integrator` named `ein-mesh-client`
that turns the client listeners on and mints a SASL/SCRAM user with PREFIXED
ACLs on `${topic_prefix}*` (default `ein.*`) and consumer groups `${consumer_group_prefix}*` (default `ein-*`).

The provider reads the local juju client, so run it wherever `juju` is logged
in to a Kubernetes controller:

```bash
just deploy-init
just deploy-plan
just deploy-apply
just deploy-creds     # KAFKA_USERNAME / KAFKA_PASSWORD for .env
```

## Run it

Every process is a plain `uv run ein-mesh <cmd>` that reads its Kafka settings
from the environment; the `justfile` wraps each one and loads `.env` first:

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP` | the broker's advertised NodePort, `host:port` |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | the data-integrator user (`juju run ein-mesh-client/leader get-credentials`) |
| `KAFKA_GROUP_PREFIX` | `ein-`, must match the data-integrator's `consumer-group-prefix` |
| `TOPIC_PREFIX` | `ein.`, prepended to every default topic name; must match Terraform's `topic_prefix` so the PREFIXED ACL covers them |
| `TOPIC_ALERTS`, `TOPIC_TRIAGE_STATE`, `TOPIC_INCIDENTS_CRITICAL`, `TOPIC_INCIDENTS_NORMAL`, `TOPIC_EVENTS`, `TOPIC_INVESTIGATION_STATE` | optional, rename one topic outright (ignores the prefix); a name outside the prefix needs its own ACL |
| `TOPIC_PARTITIONS`, `TOPIC_REPLICATION` | `12` and `1`; one partition count for every topic keeps the `root_id`-keyed topics co-partitioned, fixed once created; replication must not exceed the broker count |
| `GROUP_BY` | `cluster,namespace`, labels the gateway joins into each alert's group key when the webhook has no `groupKey`; triage attaches by rule inside a group |
| `GEMINI_API_KEY`, `EIN_AGENT_MODEL` | LLM for triage; live in the repo's `.env` (git-ignored, `chmod 600`) |

The helper scripts that fill these in for one specific environment are kept
outside git (`scripts/` holds symlinks, excluded via `.git/info/exclude`).

1. Topics (the only deploy prerequisite): `just kafka-init`
2. Four terminals:

```bash
just tail             # every mesh topic; or e.g. just tail incidents.critical investigation.events
just triage           # detected → open: batch → one LLM call → validate → apply → commit
just orchestrator     # open → investigating (PoC slice: state + event, no contracts yet)
just gateway          # Alertmanager webhook → alerts, on :8088
just gateway-fire     # curl the recorded storm at the gateway with startsAt set to now
```

The storm is test data: `tests/eval/scenarios/ceph-osd-storm/alerts.json` is an Alertmanager
webhook payload with 10 alerts, and `just gateway-fire` is a curl of it. Roles are `Role` subclasses on a
`MessageBus`, so the same code runs on an `InMemoryBus` in tests, see
[roles](docs/reference/roles.md#the-role-base-class).

- `just unit-test`: per-role behaviour, LLM replaced at `Triage.classify` or `litellm.acompletion`.
- `just eval`: every scenario under `tests/eval/scenarios` through three scored stages, gateway
  mapping → triage → orchestrator, with the scripted model from its `model.json`; `--until` stops after
  a stage and `--start-at ... --from-dump` replays a later stage from a saved dump. A scenario may
  deliver its alerts in waves, each triaged by a restarted triage that restores from `triage.state`. Triage is scored
  against `expected.json`
  (under-merge policy: a false merge fails, a split only costs recall; ignore precision/recall and
  priority are hard) plus mesh invariants. Must be perfect.
- `just eval-llm`: the same scenarios against the real model; the scripted run is the baseline.
- `just gates` = lint + unit tests + scripted evals.

Tuning: `TRIAGE_BATCH_SECONDS` (default 30), `TRIAGE_BATCH_SIZE` (50),
`TRIAGE_LLM_TIMEOUT` (120), `EIN_AGENT_MODEL` (litellm name, default `gemini/gemini-3.7-flash`).

## What is deliberately not here (v1)

Registry/heartbeat, scheduler, topology lint, profiles, per-role Kafka principals,
transactional offset commit (roles commit after the producer acks instead), contracts,
`ask_user`, closing, and every role except triage and the orchestrator's first transition. Alertmanager is not wired to the gateway: `just gateway-fire` replays the recorded storm instead.
