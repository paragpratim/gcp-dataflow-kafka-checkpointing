terraform {
  required_version = ">= 1.5"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 7.0"
    }
    google-beta = {
      source  = "hashicorp/google-beta"
      version = "~> 7.0"
    }
    confluent = {
      source  = "confluentinc/confluent"
      version = "~> 2.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

provider "google-beta" {
  project = var.project_id
  region  = var.region
}

provider "confluent" {
  cloud_api_key    = var.enable_confluent_cloud ? var.confluent_cloud_api_key : null
  cloud_api_secret = var.enable_confluent_cloud ? var.confluent_cloud_api_secret : null
}