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

output "artifact_registry_repository_name" {
  description = "The name of the created Artifact Registry repository."
  value       = module.dataflow.artifact_registry_repository_name
}

output "flex_template_spec_gcs_path" {
  description = "GCS path to the generated Dataflow Flex Template spec JSON."
  value       = module.dataflow.flex_template_spec_gcs_path
}

output "flex_template_image" {
  description = "Resolved container image used for Dataflow Flex Template launches."
  value       = module.dataflow.flex_template_image
}

# Outputs from dataflow_workflow_orchestrator module
output "workflow_name" {
  description = "Created workflow name."
  value       = var.create_dataflow_workflow_orchestrator ? module.dataflow_workflow_orchestrator[0].workflow_name : null
}

output "workflow_id" {
  description = "Created workflow ID."
  value       = var.create_dataflow_workflow_orchestrator ? module.dataflow_workflow_orchestrator[0].workflow_id : null
}

output "workflow_service_account_email" {
  description = "Service account used by the workflow."
  value       = var.create_dataflow_workflow_orchestrator ? module.dataflow_workflow_orchestrator[0].workflow_service_account_email : null
}