"""Gateway integrator: Alertmanager webhook -> deduplicated Alert on the alerts topic. No LLM."""

from __future__ import annotations

import asyncio
import logging

from aiohttp import web

from ein_mesh.bus import MessageBus
from ein_mesh.config import GroupingSettings, Topics
from ein_mesh.schemas import Alert

log = logging.getLogger(__name__)

CLUSTER_LABELS = ("cluster", "juju_model", "juju_model_uuid")


def derive_group(labels: dict[str, str], group_by: tuple[str, ...]) -> tuple[str, dict[str, str]]:
    """The group an alert belongs to when Alertmanager did not say: the GROUP_BY labels, in order."""
    group_labels = {k: labels.get(k, "") for k in group_by}
    return "|".join(f"{k}={v}" for k, v in group_labels.items()), group_labels


def to_alert(alert: dict, group_key: str | None = None, group_labels: dict[str, str] | None = None,
             grouping: GroupingSettings | None = None) -> Alert:
    """Map one Alertmanager alert. ``group_key``/``group_labels`` come from the webhook payload when it has
    them (optional information); otherwise the group is derived from the GROUP_BY labels."""
    labels = alert.get("labels", {})
    cluster = next((labels[k] for k in CLUSTER_LABELS if labels.get(k)), "unknown")
    if not group_key:
        group_key, group_labels = derive_group(labels, (grouping or GroupingSettings()).group_by)
    return Alert(
        id=f"{alert.get('fingerprint', '')}:{alert.get('startsAt', '')}",
        cluster=cluster,
        namespace=labels.get("namespace", labels.get("juju_application", "")),
        alertname=labels.get("alertname", "unknown"),
        severity=labels.get("severity", "warning"),
        status=alert.get("status", "firing"),
        labels=labels,
        annotations=alert.get("annotations", {}),
        starts_at=alert.get("startsAt", ""),
        group_key=group_key,
        group_labels=group_labels or {},
    )


async def serve(bus: MessageBus, topics: Topics, host: str, port: int) -> None:
    await bus.start()
    seen: set[str] = set()
    grouping = GroupingSettings()

    async def webhook(request: web.Request) -> web.Response:
        """One Alertmanager payload in; one Alert per new alert out, grouped per cluster for the log."""
        payload = await request.json()
        sent: dict[str, list[Alert]] = {}
        duplicates = 0
        for alert in payload.get("alerts", []):
            t = to_alert(alert, payload.get("groupKey"), payload.get("groupLabels"), grouping)
            if t.id in seen:
                duplicates += 1
                continue
            seen.add(t.id)
            await bus.produce(topics.alerts, t.cluster, t)
            sent.setdefault(t.cluster, []).append(t)
        produced = sum(len(v) for v in sent.values())
        for cluster, alerts in sent.items():
            counts: dict[str, int] = {}
            for a in alerts:
                counts[f"{a.alertname}/{a.status}"] = counts.get(f"{a.alertname}/{a.status}", 0) + 1
            log.info("webhook: %d alerts for cluster %s -> %s: %s", len(alerts), cluster, topics.alerts,
                     ", ".join(f"{n} x{c}" if c > 1 else n for n, c in sorted(counts.items())))
        if duplicates:
            log.info("webhook: %d alerts already seen (same fingerprint and startsAt), not produced", duplicates)
        return web.json_response({"received": len(payload.get("alerts", [])), "produced": produced})

    app = web.Application()
    app.router.add_post("/alertmanager", webhook)
    app.router.add_get("/healthz", lambda _: web.Response(text="ok"))
    runner = web.AppRunner(app)
    await runner.setup()
    await web.TCPSite(runner, host, port).start()
    log.info("gateway listening on %s:%d -> %s", host, port, topics.alerts)
    try:
        await asyncio.Event().wait()
    finally:
        await runner.cleanup()
        await bus.stop()
