# Run the mesh

From an empty Kubernetes model to a replayed alert storm. Every step is a `just` recipe; the full
list is in the [commands reference](../reference/cli.md).

## 1. Deploy Kafka

Needs `juju` logged in to a Kubernetes controller and `terraform`.

```bash
just deploy-init
just deploy-plan
just deploy-apply
just deploy-creds        # prints KAFKA_USERNAME / KAFKA_PASSWORD
```

- Creates model `ein-mesh`: `kafka-k8s` (one unit, KRaft) and a `data-integrator` that mints the
  SASL user with ACLs on `ein.*` topics and `ein-*` groups.
- Terraform inputs: [settings](../reference/settings.md#terraform).
- Changing an ACL later needs the integration recreated:
  `terraform -chdir=deploy/terraform apply -replace=juju_integration.kafka_client`, then new credentials.

## 2. Configure

Put these in `.env` at the repo root (git-ignored); every recipe loads it.

```bash
KAFKA_BOOTSTRAP=<node ip>:<nodeport>
KAFKA_USERNAME=relation-N
KAFKA_PASSWORD=...
GEMINI_API_KEY=...
```

Defaults cover everything else; see [settings](../reference/settings.md).

## 3. Create the topics

```bash
just kafka-init
```

- Creates the seven topics with 12 partitions each. Safe to re-run.
- Fails when an existing topic has a different partition count: recreate rather than alter.

## 4. Start the roles

One terminal each:

```bash
just gateway        # :8088
just triage
just orchestrator
just investigator
just tail           # everything; or: just tail incidents.tasks incidents.reports incidents.state
```

Order does not matter; a role that starts late reads what it missed.

## 5. Replay a storm

```bash
just gateway-fire                 # the recorded Ceph storm, cluster=dev
just gateway-fire cluster=staging # another cluster: everything is new again
```

Expected, in about a minute: triage logs one model call and three new incidents; the orchestrator
issues one task each; the investigator reports three times; the orchestrator logs `report received`
and `incidents.state` shows each task `done` with a filled `context`. A second replay of the same
storm attaches by rule and makes no triage call.

## 6. Watch

```bash
just kafka-topics --watch 5    # records per topic
just kafka-roles               # consumer groups (ein-triage, ein-orchestrator, ein-investigator), members, lag
```

## 7. Start over

Stop the roles first.

```bash
just kafka-reset --yes           # delete and recreate the seven topics, empty
just kafka-reset --prune --yes   # also drop topics from an earlier topic model
```

Needs the `DELETE` ACL the Terraform module grants.
