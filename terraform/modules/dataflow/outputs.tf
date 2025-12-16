output "project_id" {
  description = "The GCP project ID"
  value       = var.project_id
}

output "region" {
  description = "The GCP region"
  value       = var.region
}

output "service_account_email" {
  description = "The service account email"
  value       = google_service_account.dataflow_service_account.email
}

output "dataflow_staging_bucket" {
  description = "The GCS bucket for Dataflow staging"
  value       = google_storage_bucket.dataflow_staging.name
}

output "bigquery_datasets" {
  description = "Map of created BigQuery dataset IDs and their details"
  value = {
    for dataset_id, dataset in google_bigquery_dataset.datasets :
    dataset_id => {
      id            = dataset.dataset_id
      project       = dataset.project
      location      = dataset.location
      self_link     = dataset.self_link
      friendly_name = dataset.friendly_name
    }
  }
}

output "dataset_ids" {
  description = "List of created BigQuery dataset IDs"
  value       = [for dataset in google_bigquery_dataset.datasets : dataset.dataset_id]
}

output "vpc_network_self_link" {
  description = "Self-link of the created VPC network"
  value       = google_compute_network.dataflow_vpc.self_link
}

output "vpc_network_id" {
  description = "ID of the created VPC network"
  value       = google_compute_network.dataflow_vpc.id
}

output "subnet_self_link" {
  description = "Self-link of the created subnetwork"
  value       = google_compute_subnetwork.dataflow_subnet.self_link
}

output "subnet_id" {
  description = "ID of the created subnetwork"
  value       = google_compute_subnetwork.dataflow_subnet.id
}

output "router_name" {
  description = "Name of the Cloud Router (if created)"
  value       = var.create_nat ? google_compute_router.dataflow_router[0].name : ""
}

output "nat_name" {
  description = "Name of the Cloud NAT (if created)"
  value       = var.create_nat ? google_compute_router_nat.dataflow_nat[0].name : ""
}

output "artifact_registry_repository_url" {
  description = "The URL of the created Artifact Registry repository."
  value       = google_artifact_registry_repository.dataflow_artifact_registry.registry_uri
}
