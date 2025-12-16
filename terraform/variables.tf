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