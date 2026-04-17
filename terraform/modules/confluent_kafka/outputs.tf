output "environment_id" {
  description = "Confluent environment ID"
  value       = confluent_environment.this.id
}

output "cluster_id" {
  description = "Kafka cluster ID"
  value       = confluent_kafka_cluster.this.id
}

output "cluster_bootstrap_endpoint" {
  description = "Kafka cluster bootstrap endpoint"
  value       = confluent_kafka_cluster.this.bootstrap_endpoint
}

output "cluster_rest_endpoint" {
  description = "Kafka cluster REST endpoint"
  value       = confluent_kafka_cluster.this.rest_endpoint
}

output "dataflow_service_account_id" {
  description = "Service account ID used by Dataflow"
  value       = confluent_service_account.dataflow.id
}

output "dataflow_kafka_api_key" {
  description = "Kafka API key for Dataflow service account"
  value       = confluent_api_key.dataflow_kafka.id
}

output "dataflow_kafka_api_secret" {
  description = "Kafka API secret for Dataflow service account"
  value       = confluent_api_key.dataflow_kafka.secret
  sensitive   = true
}
