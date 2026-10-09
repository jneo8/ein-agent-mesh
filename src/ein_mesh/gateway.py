"""Gateway integrator: Alertmanager webhook -> deduplicated Trigger on ops.triggers. No LLM."""

from __future__ import annotations

import logging

from aiohttp import web

from ein_mesh import kafka
from ein_mesh.config import KafkaSettings, Topics
from ein_mesh.schemas import Trigger

log = logging.getLogger(__name__)

CLUSTER_LABELS = ("cluster", "juju_model", "juju_model_uuid")


def to_trigger(alert: dict) -> Trigger:
    labels = alert.get("labels", {})
    cluster = next((labels[k] for k in CLUSTER_LABELS if labels.get(k)), "unknown")
    return Trigger(
        id=f"{alert.get('fingerprint', '')}:{alert.get('startsAt', '')}",
        cluster=cluster,
        namespace=labels.get("namespace", labels.get("juju_application", "")),
        alertname=labels.get("alertname", "unknown"),
        severity=labels.get("severity", "warning"),
        status=alert.get("status", "firing"),
        labels=labels,
        annotations=alert.get("annotations", {}),
        starts_at=alert.get("startsAt", ""),
    )


async def serve(ks: KafkaSettings, topics: Topics, host: str, port: int) -> None:
    producer = kafka.producer(ks)
    await producer.start()
    seen: set[str] = set()

    async def webhook(request: web.Request) -> web.Response:
        payload = await request.json()
        sent = 0
        for alert in payload.get("alerts", []):
            t = to_trigger(alert)
            if t.id in seen:
                continue
            seen.add(t.id)
            await producer.send_and_wait(topics.triggers, key=t.cluster, value=t)
            sent += 1
        log.info("webhook: %d alerts, %d new", len(payload.get("alerts", [])), sent)
        return web.json_response({"received": len(payload.get("alerts", [])), "produced": sent})

    app = web.Application()
    app.router.add_post("/alertmanager", webhook)
    app.router.add_get("/healthz", lambda _: web.Response(text="ok"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    log.info("gateway listening on %s:%d -> %s", host, port, topics.triggers)
    try:
        import asyncio

        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        await producer.stop()
