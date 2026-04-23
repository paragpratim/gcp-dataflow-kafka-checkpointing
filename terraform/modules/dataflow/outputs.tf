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

output "artifact_registry_repository_name" {
  description = "The name of the created Artifact Registry repository."
  value       = google_artifact_registry_repository.dataflow_artifact_registry.name
}

output "flex_template_image" {
  description = "Resolved container image used for Dataflow Flex Template launches."
  value       = local.flex_template_image
}

output "firestore_database_name" {
  description = "The name of the created Firestore database for Kafka checkpointing."
  value       = google_firestore_database.dataflow_kafka_checkpointing_firestore.name
}

output "memorystore_redis_host" {
  description = "Private IP address of the Memorystore Redis instance."
  value       = var.create_memorystore ? google_redis_instance.dataflow_memorystore[0].host : null
}

output "memorystore_redis_port" {
  description = "Port of the Memorystore Redis instance."
  value       = var.create_memorystore ? google_redis_instance.dataflow_memorystore[0].port : null
}

output "memorystore_redis_name" {
  description = "Name of the Memorystore Redis instance."
  value       = var.create_memorystore ? google_redis_instance.dataflow_memorystore[0].name : null
}