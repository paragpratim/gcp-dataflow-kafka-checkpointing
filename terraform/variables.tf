variable "project_id" {
  description = "The GCP project ID"
  type        = string
  default     = ""
}

variable "region" {
  description = "The GCP region"
  type        = string
  default     = ""
}

variable "my_domain" {
  description = "The domain to grant IAP access"
  type        = string
  default     = ""
}

variable "bigquery_datasets" {
  description = "List of BigQuery dataset IDs to create"
  type        = list(string)
  default     = ["test_topic"]

  validation {
    condition = alltrue([
      for dataset in var.bigquery_datasets :
      can(regex("^[a-zA-Z][a-zA-Z0-9_]*$", dataset))
    ])
    error_message = "Dataset names must start with a letter and contain only letters, numbers, and underscores."
  }
}

variable "artifact_registry_repository_id" {
  description = "The name (ID) of the Artifact Registry repository to create."
  type        = string
  default     = "dataflow-artifacts"
}

variable "artifact_registry_format" {
  description = "The format of the Artifact Registry repository (e.g., DOCKER, MAVEN, NPM, etc.)"
  type        = string
  default     = "DOCKER"
}

variable "create_dataflow_workflow_orchestrator" {
  description = "Whether to create the Cloud Workflow that checks and starts Dataflow jobs"
  type        = bool
  default     = true
}

variable "workflow_name" {
  description = "Name of the Cloud Workflow resource"
  type        = string
  default     = "dataflow-job-orchestrator"
}

# ── Confluent Cloud ───────────────────────────────────────────────────────────

variable "enable_confluent_cloud" {
  description = "Whether to create Confluent Cloud resources"
  type        = bool
  default     = false
}

variable "confluent_environment_display_name" {
  description = "Display name for the Confluent environment"
  type        = string
  default     = "dataflow-env"
}

variable "confluent_cluster_display_name" {
  description = "Display name for the Kafka cluster"
  type        = string
  default     = "dataflow-kafka-cluster"
}

variable "confluent_cluster_availability" {
  description = "Availability zone configuration: SINGLE_ZONE or MULTI_ZONE"
  type        = string
  default     = "SINGLE_ZONE"
}

variable "confluent_cluster_region" {
  description = "Cloud region for the Kafka cluster"
  type        = string
  default     = "europe-west4"
}

variable "confluent_cluster_type" {
  description = "Kafka cluster type: basic, standard, or dedicated"
  type        = string
  default     = "basic"
}

variable "github_repository_name" {
  description = "GitHub repository name where Actions secrets will be managed"
  type        = string
}

variable "github_owner" {
  description = "GitHub owner (org or user) for the target repository"
  type        = string
}

variable "confluent_kafka_topics" {
  description = "Map of Kafka topics to create"
  type = map(object({
    partitions_count = number
    config           = optional(map(string), {})
  }))
  default = {
    test_df = {
      partitions_count = 3
      config = {
        "retention.ms"    = "21600000"
        "cleanup.policy"  = "delete"
        "retention.bytes" = "524288000"
      }
    }
  }
}

# variable for memorystore
variable "create_memorystore" {
  description = "Whether to create a Memorystore Redis instance for Dataflow checkpointing"
  type        = bool
  default     = false
}