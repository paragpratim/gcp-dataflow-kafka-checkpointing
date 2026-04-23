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
  default     = []

  validation {
    condition = alltrue([
      for dataset in var.bigquery_datasets :
      can(regex("^[a-zA-Z][a-zA-Z0-9_]*$", dataset))
    ])
    error_message = "Dataset names must start with a letter and contain only letters, numbers, and underscores."
  }
}

variable "vpc_name" {
  description = "Name of the VPC network to create (if empty a default name will be used)"
  type        = string
  default     = ""
}

variable "subnet_name" {
  description = "Name of the subnet to create in the VPC (if empty a default name will be used)"
  type        = string
  default     = ""
}

variable "subnet_cidr" {
  description = "CIDR range for the subnet (e.g. 10.10.0.0/20)"
  type        = string
  default     = "10.10.0.0/20"
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

variable "flex_template_image" {
  description = "Optional Docker image for Dataflow Flex Template spec. If empty, a default image path is derived from region/project/repository."
  type        = string
  default     = ""
}

variable "create_nat" {
  description = "Whether to create a Cloud NAT for the subnet to provide outbound internet access without external IPs"
  type        = bool
  default     = true
}

variable "nat_name" {
  description = "Optional name for the Cloud NAT. If empty a default will be used."
  type        = string
  default     = ""
}

# variable for memorystore
variable "create_memorystore" {
  description = "Whether to create a Memorystore Redis instance for Dataflow checkpointing"
  type        = bool
  default     = false
}