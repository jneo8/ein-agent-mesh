set ignore-comments
set dotenv-load

root := justfile_directory()
storm := root / 'tests/eval/scenarios/ceph-osd-storm/alerts.json'
gateway_url := 'http://127.0.0.1:8088'

[doc('Describe usage and list the available recipes.')]
help:
    @just --list --unsorted

[group('gates')]
[doc('Everything required before handing work back: lint, unit tests, harness tests, scripted evals.')]
gates: lint unit-test eval-test eval
    @echo 'All gates passed.'

[group('gates')]
[doc('ruff check on src and tests.')]
lint:
    uv run --directory {{ root }} ruff check src tests

[group('gates')]
[doc('ruff format on src and tests.')]
fmt:
    uv run --directory {{ root }} ruff format src tests

[group('gates')]
[doc('Unit tests: InMemoryBus, no broker, no LLM.')]
unit-test *args:
    uv run --directory {{ root }} --group test pytest tests/unit {{ args }}

[group('gates')]
[doc('Harness self-tests in tests/eval: scripted model perfect, lumping/splitting/dead models scored as intended.')]
eval-test *args:
    uv run --directory {{ root }} --group test pytest tests/eval {{ args }}

[group('gates')]
[doc('Scenario evals, staged gateway → triage → orchestrator, scripted model; must be clean. Args: scenario names, --show, --until STAGE, --start-at STAGE --from-dump DIR, --dump DIR, --threshold.')]
eval *args:
    uv run --directory {{ root }} --group test python -m tests.eval {{ args }}

[group('gates')]
[doc('Scenario evals against the real model (needs the LLM key in .env). Shows expected vs produced grouping; exits 1 below --threshold.')]
eval-llm *args:
    uv run --directory {{ root }} --group test python -m tests.eval --llm --show {{ args }}

# deploy-*: the Kafka deployment itself (Terraform + Juju). kafka-*: the topics and groups on a
# running broker. Recipe names carry their group as prefix so verbs like init or reset stay unambiguous.

[group('deploy')]
[doc('terraform init in deploy/terraform.')]
deploy-init:
    terraform -chdir={{ root }}/deploy/terraform init

[group('deploy')]
[doc('terraform plan, saved to ein-mesh.tfplan.')]
deploy-plan:
    terraform -chdir={{ root }}/deploy/terraform plan -out=ein-mesh.tfplan

[group('deploy')]
[doc('terraform apply of the saved plan.')]
deploy-apply:
    terraform -chdir={{ root }}/deploy/terraform apply ein-mesh.tfplan

[group('deploy')]
[doc('Print the data-integrator credentials (KAFKA_USERNAME / KAFKA_PASSWORD).')]
deploy-creds:
    juju run ein-mesh-client/leader get-credentials

[group('kafka')]
[doc('Create every mesh topic. The only deploy prerequisite; safe to re-run.')]
kafka-init:
    uv run --directory {{ root }} ein-mesh init

[group('kafka')]
[doc('Development only: delete and recreate every mesh topic, empty. Stop the roles first. `--yes` skips the prompt, `--prune` also drops topics the layout no longer names.')]
kafka-reset *args:
    uv run --directory {{ root }} ein-mesh reset {{ args }}

[group('kafka')]
[doc('Every mesh topic: partitions and record count. `just kafka-topics --watch 5` refreshes until Ctrl-C.')]
kafka-topics *args:
    uv run --directory {{ root }} ein-mesh topics {{ args }}

[group('kafka')]
[doc('Who is consuming: every ${KAFKA_GROUP_PREFIX}* consumer group with members, assignments and lag.')]
kafka-roles:
    uv run --directory {{ root }} ein-mesh roles

[group('roles')]
[doc('Triage role: detected → open.')]
triage:
    uv run --directory {{ root }} ein-mesh triage

[group('roles')]
[doc('Orchestrator role: open → investigating.')]
orchestrator:
    uv run --directory {{ root }} ein-mesh orchestrator

[group('integrators')]
[doc('Gateway: Alertmanager webhook → alerts, on :8088.')]
gateway *args:
    uv run --directory {{ root }} ein-mesh gateway {{ args }}

[group('integrators')]
[doc('Tail: print records from the given topics (short or full names); none means every mesh topic. e.g. just tail incidents.critical incidents.state')]
tail *topics:
    uv run --directory {{ root }} ein-mesh tail {{ topics }}

[group('integrators')]
[doc('Replay the recorded storm at a running gateway: curl the ceph-osd-storm scenario with startsAt set to now.')]
gateway-fire cluster='dev':
    jq --arg now "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg c '{{ cluster }}' \
        '.alerts[] |= (.startsAt = $now | .labels.cluster = $c)' {{ storm }} \
        | curl -sS -X POST -H 'Content-Type: application/json' --data-binary @- {{ gateway_url }}/alertmanager
    @echo
