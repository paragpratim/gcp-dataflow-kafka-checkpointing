output "project_id" {
  description = "The GCP project ID"
  value       = module.dataflow.project_id
}

output "region" {
  description = "The GCP region"
  value       = module.dataflow.region
}

output "service_account_email" {
  description = "The service account email"
  value       = module.dataflow.service_account_email
}

# Output from dataflow module
output "dataflow_staging_bucket" {
  description = "The GCS bucket for Dataflow staging."
  value       = module.dataflow.dataflow_staging_bucket
}

output "bigquery_datasets" {
  description = "Map of created BigQuery dataset IDs and their details."
  value       = module.dataflow.bigquery_datasets
}

output "dataset_ids" {
  description = "List of created BigQuery dataset IDs."
  value       = module.dataflow.dataset_ids
}

output "vpc_network_self_link" {
  description = "Self-link of the created VPC network."
  value       = module.dataflow.vpc_network_self_link
}

output "vpc_network_id" {
  description = "ID of the created VPC network."
  value       = module.dataflow.vpc_network_id
}

output "subnet_self_link" {
  description = "Self-link of the created subnetwork."
  value       = module.dataflow.subnet_self_link
}

output "subnet_id" {
  description = "ID of the created subnetwork."
  value       = module.dataflow.subnet_id
}

output "router_name" {
  description = "Name of the Cloud Router (if created)."
  value       = module.dataflow.router_name
}

output "nat_name" {
  description = "Name of the Cloud NAT (if created)."
  value       = module.dataflow.nat_name
}

output "artifact_registry_repository_url" {
  description = "The URL of the created Artifact Registry repository."
  value       = module.dataflow.artifact_registry_repository_url
}

# Output from dataflow_flex_streaming module
output "flex_template_spec_gcs_path" {
  description = "GCS path to the Flex Template spec JSON."
  value       = module.dataflow_flex_streaming[0].flex_template_spec_gcs_path
}

output "dataflow_job_name" {
  description = "Name of the Dataflow Flex Template job."
  value       = module.dataflow_flex_streaming[0].dataflow_job_name
}

output "dataflow_job_id" {
  description = "ID of the Dataflow Flex Template job."
  value       = module.dataflow_flex_streaming[0].dataflow_job_id
}

output "dataflow_job_state" {
  description = "Current state of the Dataflow Flex Template job."
  value       = module.dataflow_flex_streaming[0].dataflow_job_state
}