from ein_mesh.integrators import to_alert

from .conftest import storm_payload


def test_to_alert_maps_labels_and_cluster_fallbacks():
    alert = {"status": "firing", "fingerprint": "f1", "startsAt": "t0",
             "labels": {"alertname": "X", "severity": "warning", "juju_model": "cos", "juju_application": "loki"},
             "annotations": {"summary": "s"}}
    t = to_alert(alert)
    assert (t.id, t.cluster, t.namespace, t.alertname, t.severity) == ("f1:t0", "cos", "loki", "X", "warning")


def test_storm_fixture_has_distinct_fingerprints():
    a = storm_payload()
    assert len({x["fingerprint"] for x in a["alerts"]}) == len(a["alerts"]) == 10
