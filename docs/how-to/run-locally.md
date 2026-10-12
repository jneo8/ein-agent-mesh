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
UTCP_PROMETHEUS_URL=http://<cos proxy>/cos-prometheus-0     # the investigator's sources; any may be left out
UTCP_LOKI_URL=http://<cos proxy>/cos-loki-0
UTCP_ALERTMANAGER_URL=http://<cos proxy>/cos-alertmanager
UTCP_ALLOW_HTTP=true                                       # only for plain http:// to a non-loopback host
```

Defaults cover everything else; see [settings](../reference/settings.md). Leave every `UTCP_*_URL`
out to run the investigator without tools: it then reasons over the quoted alerts alone.

To change what a role is told, copy its prompt out of `src/ein_mesh/prompts/`, edit the copy and point
`TRIAGE_PROMPT` or `INVESTIGATOR_PROMPT` at it (or pass `--prompt FILE` to the recipe's command). The
investigator's gates (`--timeout`, `--submit-reserve`, `--token-budget`) take the same route.

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
just gateway-fire                 # the recorded Ceph storm, cluster dev
just gateway-fire staging         # another cluster: everything is new again
```

Or send a real fault instead of a recording: break something in the cluster the sources watch, wait for
its alerts to fire, then let the recipe stand in for Alertmanager's webhook:

```bash
just gateway-alerts                                      # what is firing now; nothing sent
just gateway-alerts 'alertname=~"Ceph.*"'                # narrowed by an Alertmanager matcher
just gateway-alerts '' all                               # silenced and inhibited ones too
just gateway-fire-live                                   # send everything firing now, cluster dev
just gateway-fire-live dev 'alertname=~"Ceph.*"'         # only the alerts the fault raised
```

The quoted alerts then match what the investigator's tools find, so a report's evidence can be checked
against the live systems. `Watchdog` always fires; triage ignores it.

Expected, in about a minute: triage logs one model call and three new incidents; the orchestrator
issues one task each; the investigator logs its sources at start (`tools: alertmanager 6 operations,
...`), then per task `tool call 1 call_operation {"source": "alertmanager", ...}` lines, a token count per turn, and
`report for inc-...: ... | n tool calls (alertmanager.getAlerts, prometheus.query x2) in 42.1s`; the
orchestrator logs `report received` and `incidents.state` shows each task `done` with a filled
`context`. `just tail incidents.reports` shows `resources_checked` starting with the calls that were
made. A second replay of the same storm attaches by rule and makes no triage call.

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
