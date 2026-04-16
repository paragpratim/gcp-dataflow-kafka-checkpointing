variable "project_id" {
  description = "The GCP project ID"
  type        = string
}

variable "region" {
  description = "The GCP region for the workflow and Dataflow API calls"
  type        = string
}

variable "workflow_name" {
  description = "Name of the Cloud Workflow"
  type        = string
  default     = "dataflow-job-orchestrator"
}

variable "workflow_source_path" {
  description = "Absolute path to the workflow YAML source file to deploy"
  type        = string
}
