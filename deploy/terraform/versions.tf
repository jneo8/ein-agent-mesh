terraform {
  required_version = ">= 1.11"
  required_providers {
    juju = {
      source  = "juju/juju"
      version = "~> 2.4"
    }
  }
}

# No controller settings here: the provider reads the local juju client's
# current controller, so run this where `juju` is logged in to a k8s controller.
provider "juju" {}
