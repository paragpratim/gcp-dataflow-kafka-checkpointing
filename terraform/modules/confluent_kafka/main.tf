terraform {
  required_providers {
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

# ── Environment ──────────────────────────────────────────────────────────────

resource "confluent_environment" "this" {
  display_name = var.environment_display_name
}

# ── Kafka Cluster ─────────────────────────────────────────────────────────────

resource "confluent_kafka_cluster" "this" {
  display_name = var.cluster_display_name
  availability = var.cluster_availability
  cloud        = var.cluster_cloud
  region       = var.cluster_region

  dynamic "basic" {
    for_each = var.cluster_type == "basic" ? [1] : []
    content {}
  }

  dynamic "standard" {
    for_each = var.cluster_type == "standard" ? [1] : []
    content {}
  }

  dynamic "dedicated" {
    for_each = var.cluster_type == "dedicated" ? [1] : []
    content {
      cku = 1
    }
  }

  environment {
    id = confluent_environment.this.id
  }
}

# ── Service Account ───────────────────────────────────────────────────────────

resource "confluent_service_account" "dataflow" {
  display_name = var.service_account_display_name
  description  = "Service account used by Dataflow to produce/consume Kafka messages"
}

resource "confluent_api_key" "dataflow_kafka" {
  display_name = "${var.service_account_display_name}-kafka-api-key"
  description  = "Kafka API key for Dataflow service account"

  owner {
    id          = confluent_service_account.dataflow.id
    api_version = confluent_service_account.dataflow.api_version
    kind        = confluent_service_account.dataflow.kind
  }

  managed_resource {
    id          = confluent_kafka_cluster.this.id
    api_version = confluent_kafka_cluster.this.api_version
    kind        = confluent_kafka_cluster.this.kind

    environment {
      id = confluent_environment.this.id
    }
  }
}

# ── Role binding: DeveloperRead + DeveloperWrite on all topics ────────────────

resource "confluent_role_binding" "dataflow_developer_write" {
  for_each = var.cluster_type == "basic" ? {} : var.topics

  principal   = "User:${confluent_service_account.dataflow.id}"
  role_name   = "DeveloperWrite"
  crn_pattern = "${confluent_kafka_cluster.this.rbac_crn}/kafka=${confluent_kafka_cluster.this.id}/topic=${each.key}"
}

resource "confluent_role_binding" "dataflow_developer_read" {
  for_each = var.cluster_type == "basic" ? {} : var.topics

  principal   = "User:${confluent_service_account.dataflow.id}"
  role_name   = "DeveloperRead"
  crn_pattern = "${confluent_kafka_cluster.this.rbac_crn}/kafka=${confluent_kafka_cluster.this.id}/topic=${each.key}"
}

# ── Topics ────────────────────────────────────────────────────────────────────

resource "confluent_kafka_topic" "topics" {
  for_each         = var.topics
  topic_name       = each.key
  partitions_count = each.value.partitions_count
  config           = each.value.config

  kafka_cluster {
    id = confluent_kafka_cluster.this.id
  }

  rest_endpoint = confluent_kafka_cluster.this.rest_endpoint

  credentials {
    key    = confluent_api_key.dataflow_kafka.id
    secret = confluent_api_key.dataflow_kafka.secret
  }
}