# Create Firestore database for Kafka checkpointing
resource "google_firestore_database" "dataflow_kafka_checkpointing_firestore" {
  name             = "dataflow-kafka-checkpointing"
  project          = var.project_id
  location_id      = var.region
  type             = "FIRESTORE_NATIVE"
  database_edition = "STANDARD"

  depends_on = [google_project_service.required_apis]
}