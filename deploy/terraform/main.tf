resource "juju_model" "ein_mesh" {
  name = var.model_name

  config = {
    workload-storage = var.workload_storage
  }
}

# Single-app KRaft: one unit holds both the controller quorum and the broker.
# profile=testing keeps JVM heap and resource checks small enough for a dev cluster.
# expose-external=nodeport gives every broker a NodePort service, which is how
# anything outside the cluster (the laptop over sshuttle) reaches it.
resource "juju_application" "kafka" {
  name       = "kafka"
  model_uuid = juju_model.ein_mesh.uuid
  trust      = true
  units      = var.kafka_units

  charm {
    name    = "kafka-k8s"
    channel = var.kafka_channel
    base    = "ubuntu@24.04"
  }

  config = {
    roles           = "broker,controller"
    profile         = "testing"
    expose-external = "nodeport"
  }
}

# Kafka keeps client listeners off until something integrates. data-integrator
# is that something: it also mints the SASL/SCRAM user the mesh processes use.
# topic-name ending in "*" becomes a PREFIXED ACL in the charm.
resource "juju_application" "data_integrator" {
  name       = "ein-mesh-client"
  model_uuid = juju_model.ein_mesh.uuid

  charm {
    name    = "data-integrator"
    channel = "latest/stable"
    base    = "ubuntu@24.04"
  }

  config = {
    topic-name            = "${var.topic_prefix}*"
    extra-user-roles      = "producer,consumer"
    consumer-group-prefix = var.consumer_group_prefix
  }
}

resource "juju_integration" "kafka_client" {
  model_uuid = juju_model.ein_mesh.uuid

  application {
    name     = juju_application.kafka.name
    endpoint = "kafka-client"
  }

  application {
    name     = juju_application.data_integrator.name
    endpoint = "kafka"
  }
}
