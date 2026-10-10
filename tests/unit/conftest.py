import json
from pathlib import Path

import pytest

from ein_mesh.bus import InMemoryBus
from ein_mesh.config import GroupingSettings, Topics
from ein_mesh.integrators import derive_group
from ein_mesh.schemas import Alert


@pytest.fixture
def topics() -> Topics:
    return Topics(prefix="t.")


@pytest.fixture
def bus() -> InMemoryBus:
    return InMemoryBus()


STORM = Path(__file__).resolve().parent.parent / "eval" / "scenarios" / "ceph-osd-storm" / "alerts.json"


def storm_payload() -> dict:
    return json.loads(STORM.read_text())


def make_alert(alert_id: str, cluster: str = "dev", ns: str = "ceph", name: str = "CephOSDDown",
               severity: str = "critical", status: str = "firing") -> Alert:
    labels = {"alertname": name, "severity": severity, "namespace": ns, "cluster": cluster}
    group_key, group_labels = derive_group(labels, GroupingSettings().group_by)
    alert_id = alert_id if ":" in alert_id else f"{alert_id}:t0"  # fingerprint:startsAt like the gateway
    return Alert(id=alert_id, cluster=cluster, namespace=ns, alertname=name, severity=severity, status=status,
                 labels=labels, group_key=group_key, group_labels=group_labels)
