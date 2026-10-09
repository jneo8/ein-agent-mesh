"""Synthetic alert storm: an Alertmanager-shaped payload, posted to the gateway or produced directly."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime

import aiohttp

from ein_mesh import kafka
from ein_mesh.config import KafkaSettings, Topics
from ein_mesh.gateway import to_trigger

# A believable storm: one ceph OSD failure cascading into PVC and pod alerts, plus noise.
STORM = [
    ("CephOSDDown", "critical", "ceph", {"osd": "osd.3", "host": "ceph-2"}, "OSD osd.3 on ceph-2 is down"),
    ("CephPGDegraded", "warning", "ceph", {"pool": "k8s-rbd"}, "PGs degraded in pool k8s-rbd"),
    ("CephClusterWarningState", "warning", "ceph", {}, "Ceph health is HEALTH_WARN"),
    ("KubePersistentVolumeFillingUp", "warning", "temporal", {"persistentvolumeclaim": "pgdata-postgresql-k8s-0"},
     "PVC pgdata-postgresql-k8s-0 at 91%"),
    ("KubePodCrashLooping", "critical", "temporal", {"pod": "temporal-k8s-0"}, "temporal-k8s-0 restarting"),
    ("KubePodCrashLooping", "critical", "temporal", {"pod": "postgresql-k8s-0"}, "postgresql-k8s-0 restarting"),
    ("TargetDown", "warning", "cos", {"job": "node-exporter"}, "25% of node-exporter targets down"),
    ("Watchdog", "none", "cos", {}, "Always firing: alerting pipeline is working"),
    ("KubeHpaMaxedOut", "info", "iam", {"hpa": "login-ui"}, "HPA at max replicas"),
    ("CephOSDDown", "critical", "ceph", {"osd": "osd.7", "host": "ceph-2"}, "OSD osd.7 on ceph-2 is down"),
]


def payload(cluster: str) -> dict:
    now = datetime.now(UTC).replace(microsecond=0).isoformat()
    alerts = []
    for name, sev, ns, extra, summary in STORM:
        labels = {"alertname": name, "severity": sev, "namespace": ns, "cluster": cluster, **extra}
        fp = hashlib.sha1(json.dumps(labels, sort_keys=True).encode()).hexdigest()[:16]
        alerts.append({"status": "firing", "labels": labels, "annotations": {"summary": summary},
                       "startsAt": now, "fingerprint": fp})
    return {"receiver": "ein-mesh", "status": "firing", "alerts": alerts}


async def fire(cluster: str, gateway_url: str | None, ks: KafkaSettings, topics: Topics) -> int:
    body = payload(cluster)
    if gateway_url:
        async with aiohttp.ClientSession() as s, s.post(f"{gateway_url}/alertmanager", json=body) as r:
            print(await r.json())
        return len(body["alerts"])
    p = kafka.producer(ks)
    await p.start()
    try:
        for a in body["alerts"]:
            t = to_trigger(a)
            await p.send_and_wait(topics.triggers, key=t.cluster, value=t)
    finally:
        await p.stop()
    return len(body["alerts"])
