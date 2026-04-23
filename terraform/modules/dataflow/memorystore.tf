resource "google_redis_instance" "dataflow_memorystore" {
  count          = var.create_memorystore ? 1 : 0
  name           = "dataflow-kafka-checkpointing"
  display_name   = "Dataflow Memorystore Redis for Kafka Checkpointing"
  project        = var.project_id
  region         = var.region
  tier           = "BASIC"
  memory_size_gb = 1

  # Attach Redis to the same private VPC used by Dataflow workers.
  authorized_network = google_compute_network.dataflow_vpc.id
  connect_mode       = "DIRECT_PEERING"

  deletion_protection = false

  depends_on = [
    google_project_service.required_apis,
    google_compute_network.dataflow_vpc,
  ]
}
