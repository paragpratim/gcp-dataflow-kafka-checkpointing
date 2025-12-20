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

variable "flex_template_image" {
  description = "The Docker image for the Dataflow Flex Template"
  type        = string
  default     = ""
}

variable "flex_template_bucket" {
  description = "The GCS bucket to store the Dataflow Flex Template specification"
  type        = string
  default     = ""
}

variable "pipeline_name" {
  description = "The name of the Dataflow pipeline"
  type        = string
  default     = ""
}

variable "dataflow_subnetwork" {
  description = "The subnetwork for Dataflow jobs"
  type        = string
  default     = ""
}

variable "dataflow_service_account" {
  description = "The service account for Dataflow jobs"
  type        = string
  default     = ""
}

variable "dataflow_temp_location" {
  description = "The GCS location for Dataflow temporary files"
  type        = string
  default     = ""
}

variable "dataflow_staging_location" {
  description = "The GCS location for Dataflow staging files"
  type        = string
  default     = ""
}

variable "update_mode" {
  description = "The update mode for the Dataflow job (e.g., 'drain', 'cancel')"
  type        = string
  default     = "drain"
}