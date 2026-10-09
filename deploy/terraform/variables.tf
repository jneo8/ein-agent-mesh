variable "model_name" {
  type    = string
  default = "ein-mesh"
}

variable "workload_storage" {
  description = "Storage class for every PVC in the model (the cluster has two defaults, so pin one)."
  type        = string
  default     = "ceph-xfs"
}

variable "kafka_channel" {
  type    = string
  default = "4/stable"
}

variable "kafka_units" {
  type    = number
  default = 1
}

variable "topic_prefix" {
  description = "Every ein-mesh topic starts with this; the data-integrator user gets PREFIXED ACLs on it."
  type        = string
  default     = "ops."
}

variable "consumer_group_prefix" {
  type    = string
  default = "ein-"
}
