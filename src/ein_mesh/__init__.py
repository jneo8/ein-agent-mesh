"""ein-mesh CLI: init topics, run the triage role, the gateway, a tail, or fire a storm."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys

from ein_mesh.config import KafkaSettings, Topics, TriageSettings


async def _tail(ks: KafkaSettings, topics: Topics, from_start: bool) -> None:
    from ein_mesh import kafka

    names = [topics.incidents("critical"), topics.incidents("normal"), topics.events]
    c = kafka.consumer(ks, None, *names, auto_offset_reset="earliest" if from_start else "latest")
    await c.start()
    print(f"tailing {names}", file=sys.stderr)
    try:
        async for r in c:
            print(f"[{r.topic}] key={r.key} {json.dumps(r.value, ensure_ascii=False)}", flush=True)
    finally:
        await c.stop()


def main(argv: list[str] | None = None) -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    logging.getLogger("aiokafka").setLevel(logging.WARNING)
    logging.getLogger("LiteLLM").setLevel(logging.WARNING)
    p = argparse.ArgumentParser(prog="ein-mesh")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init", help="create every mesh topic (the only deploy prerequisite)")
    sub.add_parser("triage", help="run the triage role")
    g = sub.add_parser("gateway", help="Alertmanager webhook -> ops.triggers")
    g.add_argument("--host", default="0.0.0.0")
    g.add_argument("--port", type=int, default=8088)
    t = sub.add_parser("tail", help="print incidents and events as they arrive")
    t.add_argument("--from-start", action="store_true")
    f = sub.add_parser("fire", help="send a synthetic alert storm")
    f.add_argument("--cluster", default="dev")
    f.add_argument("--gateway", help="post to this gateway URL instead of producing directly")
    args = p.parse_args(argv)

    ks, topics = KafkaSettings(), Topics()
    if args.cmd == "init":
        from ein_mesh.kafka import create_topics

        created = asyncio.run(create_topics(ks, topics))
        print(f"created {created or 'nothing new'}; all topics: {list(topics.all())}")
    elif args.cmd == "triage":
        from ein_mesh.triage import Triage

        asyncio.run(Triage(ks, topics, TriageSettings()).run())
    elif args.cmd == "gateway":
        from ein_mesh.gateway import serve

        asyncio.run(serve(ks, topics, args.host, args.port))
    elif args.cmd == "tail":
        asyncio.run(_tail(ks, topics, args.from_start))
    elif args.cmd == "fire":
        from ein_mesh.storm import fire

        n = asyncio.run(fire(args.cluster, args.gateway, ks, topics))
        print(f"fired {n} alerts for cluster {args.cluster}")
