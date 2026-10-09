output "model" {
  value = juju_model.ein_mesh.name
}

output "kafka_app" {
  value = juju_application.kafka.name
}

output "client_app" {
  value = juju_application.data_integrator.name
}
