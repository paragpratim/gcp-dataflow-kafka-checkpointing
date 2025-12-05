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
