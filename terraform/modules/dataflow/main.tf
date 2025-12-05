# Create service account
resource "google_service_account" "dataflow_service_account" {
  account_id   = "dataflow"
  display_name = "Dataflow Service Account"
  description  = "Service account for dataflow application"
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "bigquery.googleapis.com",
    "logging.googleapis.com",
    "storage.googleapis.com",
    "dataflow.googleapis.com",
  ])

  service            = each.value
  disable_on_destroy = false
}

# Assign multiple roles to service account
resource "google_project_iam_member" "dataflow_service_account_roles" {
  for_each = toset([
    "roles/bigquery.admin",
    "roles/storage.admin",
    "roles/dataflow.worker",
  ])

  project    = var.project_id
  role       = each.value
  member     = "serviceAccount:${google_service_account.dataflow_service_account.email}"
  depends_on = [google_service_account.dataflow_service_account]
}

# Create GCS bucket for Dataflow staging
resource "google_storage_bucket" "dataflow_staging" {
  name     = "dataflow-staging-${var.project_id}"
  location = var.region

  versioning {
    enabled = true
  }

  # Set uniform bucket-level access
  uniform_bucket_level_access = true

  depends_on = [google_project_service.required_apis]
}

# VPC network for Dataflow and related resources
resource "google_compute_network" "dataflow_vpc" {
  name                    = var.vpc_name != "" ? var.vpc_name : "dataflow-vpc-${var.project_id}"
  auto_create_subnetworks = false
  description             = "VPC for Dataflow and related services"
}

# Subnetwork in the given region
resource "google_compute_subnetwork" "dataflow_subnet" {
  name          = var.subnet_name != "" ? var.subnet_name : "dataflow-subnet-${var.project_id}-${replace(var.region, "-", "")}"
  ip_cidr_range = var.subnet_cidr
  region        = var.region
  network       = google_compute_network.dataflow_vpc.id

  private_ip_google_access = true

  depends_on = [google_compute_network.dataflow_vpc]
}

# Create BigQuery datasets
resource "google_bigquery_dataset" "datasets" {
  for_each = toset(var.bigquery_datasets)

  dataset_id    = each.value
  friendly_name = title(replace(each.value, "_", " "))
  description   = "Dataset for ${replace(each.value, "_", " ")} data"
  location      = var.region
  project       = var.project_id

  # Access controls
  access {
    role          = "OWNER"
    user_by_email = google_service_account.dataflow_service_account.email
  }

  # Domain read-only access
  access {
    role   = "READER"
    domain = var.my_domain
  }

  # Optional: Set deletion policy
  delete_contents_on_destroy = true

  # Labels for organization
  labels = {
    project    = "dataflow-datasets"
    managed_by = "terraform"
  }

  depends_on = [
    google_project_service.required_apis,
    google_service_account.dataflow_service_account
  ]
}
