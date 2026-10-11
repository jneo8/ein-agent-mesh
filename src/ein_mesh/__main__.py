"""ein-mesh CLI: init topics, inspect topics and the roles consuming them, run a role (triage,
orchestrator) or an integrator (gateway, tail)."""

from __future__ import annotations

import asyncio
import logging
import time

import click
from aiokafka.errors import TopicAuthorizationFailedError

from ein_mesh.bus import KafkaBus, consumer_groups, create_topics, reset_topics, topic_status
from ein_mesh.config import KafkaSettings, Topics, TriageSettings


@click.group()
def cli() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("aiohttp.access").setLevel(logging.WARNING)  # the gateway logs each webhook itself


def _quiet_litellm() -> None:
    """litellm installs its own stderr handler at import, so every warning would print twice (its format
    and ours). Keep ours; raise the level to ERROR, its warnings are about the SDK, not the mesh."""
    lg = logging.getLogger("LiteLLM")
    lg.handlers.clear()
    lg.setLevel(logging.ERROR)


def _env() -> tuple[KafkaBus, Topics]:
    return KafkaBus(KafkaSettings()), Topics()


@cli.command()
def init() -> None:
    """Create every mesh topic with TOPIC_PARTITIONS partitions and update the config of existing
    ones. The only deploy prerequisite; safe to re-run. Fails when an existing topic has a different
    partition count."""
    ks, topics = KafkaSettings(), Topics()
    try:
        created, updated = asyncio.run(create_topics(ks, topics))
    except RuntimeError as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"created {created or 'nothing new'} ({topics.partitions} partitions, replication "
               f"{topics.replication}); config updated {updated or 'nothing'}; all topics: {list(topics.all())}")


@cli.command()
@click.option("--yes", is_flag=True, help="Do not ask for confirmation.")
@click.option("--prune", is_flag=True, help="Also delete topics under TOPIC_PREFIX that the layout no longer names.")
def reset(yes: bool, prune: bool) -> None:
    """Development only: delete every mesh topic and create it again, empty. Every alert, incident
    and state snapshot is lost and every role's committed offsets go with the topics. Refuses while
    a role is connected; stop triage, orchestrator, gateway and tail first. --prune also removes
    topics left over from an earlier topic model."""
    ks, tp = KafkaSettings(), Topics()
    if not yes:
        what = f"Delete all records in {list(tp.all())}" + (" and every other topic under the prefix" if prune else "")
        click.confirm(f"{what} on {ks.bootstrap}?", abort=True)
    try:
        deleted, created = asyncio.run(reset_topics(ks, tp, prune=prune))
    except TopicAuthorizationFailedError as e:  # before RuntimeError: aiokafka errors subclass it
        raise click.ClickException(
            f"{e}. This user may not DELETE topics. The Terraform module grants it through "
            "entity-permissions, which the Kafka charm applies only when the integration is created: "
            "apply the module, then replace the integration "
            "(`terraform apply -replace=juju_integration.kafka_client`) and refresh "
            "KAFKA_USERNAME / KAFKA_PASSWORD.") from e
    except RuntimeError as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"deleted {deleted or 'nothing'}; created {created} ({tp.partitions} partitions, empty)")


@cli.command()
@click.option("--watch", type=float, default=0, metavar="SECONDS", help="Refresh every SECONDS until Ctrl-C.")
def topics(watch: float) -> None:
    """Every mesh topic with its partition count and record count.

    Records are end minus beginning offsets, so a compacted topic shows what compaction has kept.
    A partition count that differs from TOPIC_PARTITIONS is flagged; a missing topic says so.
    """
    ks, tp = KafkaSettings(), Topics()
    width = max(len(n) for n in tp.all())
    while True:
        rows = asyncio.run(topic_status(ks, tp))
        lines = [f"{'TOPIC':<{width}}  {'PARTITIONS':>10}  {'RECORDS':>8}"]
        for r in rows:
            if r["partitions"] is None:
                lines.append(f"{r['topic']:<{width}}  {'missing':>10}  {'-':>8}")
                continue
            parts = str(r["partitions"]) + ("" if r["partitions_ok"] else f"!={tp.partitions}")
            lines.append(f"{r['topic']:<{width}}  {parts:>10}  {r['records']:>8}")
        if watch:
            click.clear()
            lines.append(f"\nevery {watch:g}s, Ctrl-C to stop")
        click.echo("\n".join(lines))
        if not watch:
            return
        time.sleep(watch)


@cli.command()
def roles() -> None:
    """Who is consuming: every ${KAFKA_GROUP_PREFIX}* consumer group with its members and lag.

    A role shows up as one group named after it. No members means the role is stopped but its
    offsets have not expired; roles that consume without a group (tail, state restores) do not appear.
    """
    ks = KafkaSettings()
    groups = asyncio.run(consumer_groups(ks, ks.group_prefix))
    if not groups:
        click.echo(f"no consumer groups with prefix {ks.group_prefix!r}")
        return
    for g in groups:
        lag = ", ".join(f"{t}={n}" for t, n in sorted(g["lag"].items())) or "no committed offsets"
        click.echo(f"{g['group']}  state={g['state']}  members={len(g['members'])}  lag: {lag}")
        for m in g["members"]:
            parts = ", ".join(f"{t}[{','.join(map(str, p))}]" for t, p in sorted(m["assignment"].items()))
            click.echo(f"  {m['client_id']} @ {m['host']}  {parts}")


@cli.command()
def triage() -> None:
    """Triage role: detected -> open."""
    from ein_mesh.roles import Triage

    _quiet_litellm()
    bus, topics = _env()
    asyncio.run(Triage(bus, topics, TriageSettings()).run())


@cli.command()
def orchestrator() -> None:
    """Orchestrator role: open -> investigating (PoC slice)."""
    from ein_mesh.roles import Orchestrator

    bus, topics = _env()
    asyncio.run(Orchestrator(bus, topics).run())


@cli.command()
@click.option("--host", default="0.0.0.0", show_default=True)
@click.option("--port", default=8088, show_default=True, type=int)
def gateway(host: str, port: int) -> None:
    """Gateway integrator: Alertmanager webhook -> the alerts topic."""
    from ein_mesh.integrators import serve

    bus, topics = _env()
    asyncio.run(serve(bus, topics, host, port))


@cli.command()
@click.argument("topic", nargs=-1)
@click.option("--from-start", is_flag=True, help="Read from the earliest offset instead of only new records.")
def tail(topic: tuple[str, ...], from_start: bool) -> None:
    """Tail integrator: print records from TOPIC... as they arrive.

    TOPIC is a short name (alerts, alerts.state, incidents.critical, incidents.normal, incidents.state,
    incidents.tasks, incidents.reports) or a full topic name. No TOPIC means every mesh topic.
    """
    from ein_mesh.integrators import tail as run_tail

    bus, topics = _env()
    asyncio.run(run_tail(bus, topics, list(topic), from_start))


if __name__ == "__main__":
    cli()
