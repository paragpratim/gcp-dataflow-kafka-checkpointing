variable "environment_display_name" {
  description = "Display name for the Confluent environment"
  type        = string
  default     = "dataflow-env"
}

variable "cluster_display_name" {
  description = "Display name for the Kafka cluster"
  type        = string
  default     = "dataflow-kafka-cluster"
}

variable "cluster_availability" {
  description = "Availability zone configuration: SINGLE_ZONE or MULTI_ZONE"
  type        = string
  default     = "SINGLE_ZONE"

  validation {
    condition     = contains(["SINGLE_ZONE", "MULTI_ZONE"], var.cluster_availability)
    error_message = "cluster_availability must be SINGLE_ZONE or MULTI_ZONE."
  }
}

variable "cluster_cloud" {
  description = "Cloud provider for the Kafka cluster (GCP, AWS, AZURE)"
  type        = string
  default     = "GCP"
}

variable "cluster_region" {
  description = "Cloud region for the Kafka cluster"
  type        = string
  default     = "europe-west4"
}

variable "cluster_type" {
  description = "Kafka cluster type: basic, standard, or dedicated"
  type        = string
  default     = "basic"

  validation {
    condition     = contains(["basic", "standard", "dedicated"], var.cluster_type)
    error_message = "cluster_type must be basic, standard, or dedicated."
  }
}

variable "topics" {
  description = "Map of Kafka topics to create"
  type = map(object({
    partitions_count = number
    config           = optional(map(string), {})
  }))
  default = {}
}

variable "service_account_display_name" {
  description = "Display name for the Kafka service account used by Dataflow"
  type        = string
  default     = "dataflow-kafka-sa"
}

variable "deployment_sa_display_name" {
  description = "Display name of the existing service account behind CONFLUENT_CLOUD_API_KEY (used to create the cluster-scoped Kafka key for topic/ACL management)"
  type        = string
  default     = "svc-deployment"
}

variable "github_repository_name" {
  description = "GitHub repository name where Actions secrets will be managed"
  type        = string
}

variable "github_owner" {
  description = "GitHub owner (org or user) for the target repository"
  type        = string
}