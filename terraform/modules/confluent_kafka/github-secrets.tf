locals {
  kafka_github_actions_secrets = {
    KAFKA_BROKER_HOST   = confluent_kafka_cluster.this.bootstrap_endpoint
    KAFKA_SASL_USERNAME = confluent_api_key.dataflow_kafka.id
    KAFKA_SASL_PASSWORD = confluent_api_key.dataflow_kafka.secret
  }
}

resource "github_actions_secret" "kafka" {
  for_each = local.kafka_github_actions_secrets

  repository      = var.github_repository_name
  secret_name     = each.key
  plaintext_value = each.value
}
