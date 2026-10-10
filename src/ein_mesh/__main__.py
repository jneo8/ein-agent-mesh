"""ein-mesh CLI: init topics, run a role (triage, orchestrator) or an integrator (gateway, tail)."""

from __future__ import annotations

import asyncio
import logging

import click

from ein_mesh.bus import KafkaBus, create_topics
from ein_mesh.config import KafkaSettings, Topics, TriageSettings


@click.group()
def cli() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)


def _env() -> tuple[KafkaBus, Topics]:
    return KafkaBus(KafkaSettings()), Topics()


@cli.command()
def init() -> None:
    """Create every mesh topic with TOPIC_PARTITIONS partitions. The only deploy prerequisite; safe
    to re-run. Fails when an existing topic has a different partition count."""
    ks, topics = KafkaSettings(), Topics()
    try:
        created = asyncio.run(create_topics(ks, topics))
    except RuntimeError as e:
        raise click.ClickException(str(e)) from e
    click.echo(f"created {created or 'nothing new'} ({topics.partitions} partitions, replication "
               f"{topics.replication}); all topics: {list(topics.all())}")


@cli.command()
def triage() -> None:
    """Triage role: detected -> open."""
    from ein_mesh.roles import Triage

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

    TOPIC is a short name (alerts, triage.state, incidents.critical, incidents.normal,
    investigation.events, investigation.state) or a full topic name. No TOPIC means every mesh topic.
    """
    from ein_mesh.integrators import tail as run_tail

    bus, topics = _env()
    asyncio.run(run_tail(bus, topics, list(topic), from_start))


if __name__ == "__main__":
    cli()
