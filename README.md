# ein-agent-mesh

Kafka as the control plane between SRE investigation agents. This repo holds the
v1 shape from the design note: Juju `kafka-k8s` deployed with Terraform, plus the
**triage** role and the **gateway** integrator.

```
Alertmanager ──webhook──▶ gateway ──Trigger──▶ ops.triggers ──▶ triage ──▶ ops.incidents.{critical,normal}
                                                                   │
                                                                   ├──▶ ops.investigation.events (why)
                                                                   └──▶ ops.triage.state (compacted, per cluster)
```

## Deploy Kafka (Terraform + juju provider)

`deploy/terraform` creates model `ein-mesh` on the current Juju controller with
`kafka-k8s` 4/stable (single unit, `roles=broker,controller`, `profile=testing`,
`expose-external=nodeport`) and a `data-integrator` named `ein-mesh-client`
that turns the client listeners on and mints a SASL/SCRAM user with PREFIXED
ACLs on `ops.*` and consumer groups `ein-*`.

The provider reads the local juju client, so run it wherever `juju` is logged
in to a Kubernetes controller:

```bash
cd deploy/terraform
terraform init
terraform plan -out=ein-mesh.tfplan
terraform apply ein-mesh.tfplan
```

## Run it

Every process is a plain `uv run ein-mesh <cmd>` that reads its Kafka settings
from the environment:

| Variable | Meaning |
| --- | --- |
| `KAFKA_BOOTSTRAP` | the broker's advertised NodePort, `host:port` |
| `KAFKA_USERNAME` / `KAFKA_PASSWORD` | the data-integrator user (`juju run ein-mesh-client/leader get-credentials`) |
| `KAFKA_GROUP_PREFIX` | `ein-`, must match the data-integrator's `consumer-group-prefix` |
| `TOPIC_PREFIX` | `ops.` |
| `GEMINI_API_KEY`, `EIN_AGENT_MODEL` | LLM for triage; live in the repo's `.env` (git-ignored, `chmod 600`) |

The helper scripts that fill these in for one specific environment are kept
outside git (`scripts/` holds symlinks, excluded via `.git/info/exclude`).

1. Topics (the only deploy prerequisite): `uv run ein-mesh init`
2. Three terminals:

```bash
uv run ein-mesh tail                 # watch ops.incidents.* and ops.investigation.events
uv run ein-mesh triage               # the role: batch → one LLM call → validate → apply → commit
uv run ein-mesh fire --cluster dev   # synthetic storm straight onto ops.triggers
# or through the integrator:
uv run ein-mesh gateway & uv run ein-mesh fire --gateway http://127.0.0.1:8088
```

Tuning: `TRIAGE_BATCH_SECONDS` (default 30), `TRIAGE_BATCH_SIZE` (50),
`TRIAGE_LLM_TIMEOUT` (60), `EIN_AGENT_MODEL` (litellm name, default `gemini/gemini-3.7-flash`).

## What is deliberately not here (v1)

Registry/heartbeat, scheduler, topology lint, profiles, per-role Kafka principals,
transactional offset commit (triage commits after the producer acks instead), and
every role except triage. Alertmanager is not wired to the gateway: `fire` plays
the storm instead.
