# Create service account
resource "google_service_account" "dataflow_service_account" {
  account_id   = "dataflow"
  display_name = "Dataflow Service Account"
  description  = "Service account for dataflow application"
}

# Enable required APIs
resource "google_project_service" "required_apis" {
  for_each = toset([
    "iam.googleapis.com",
    "bigquery.googleapis.com",
    "logging.googleapis.com",
    "storage.googleapis.com",
    "dataflow.googleapis.com",
    "artifactregistry.googleapis.com",
    "firestore.googleapis.com",
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
    "roles/artifactregistry.reader",
    "roles/firestore.user",
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

# Create Artifact Registry repository for Dataflow artifacts
resource "google_artifact_registry_repository" "dataflow_artifact_registry" {
  project       = var.project_id
  location      = var.region
  repository_id = var.artifact_registry_repository_id
  description   = "Artifact Registry for Dataflow artifacts"
  format        = var.artifact_registry_format
  labels = {
    managed_by = "terraform"
    project    = var.project_id
  }
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

# Create internal firewall rule for Dataflow
resource "google_compute_firewall" "dataflow_internal" {
  name    = "dataflow-allow-internal-${var.project_id}"
  network = google_compute_network.dataflow_vpc.name

  direction   = "INGRESS"
  priority    = 1000
  source_tags = ["dataflow"]
  target_tags = ["dataflow"]

  allow {
    protocol = "tcp"
    ports    = ["12345", "12346"]
  }

  description = "Allow Dataflow worker-to-worker traffic (streaming TCP 12345, batch TCP 12346); scoped to Dataflow VMs via tag"
}

# Create a Cloud Router and Cloud NAT so workers without external IPs can egress
resource "google_compute_router" "dataflow_router" {
  count   = var.create_nat ? 1 : 0
  name    = "dataflow-router-${var.project_id}"
  region  = var.region
  network = google_compute_network.dataflow_vpc.name
}

resource "google_compute_router_nat" "dataflow_nat" {
  count = var.create_nat ? 1 : 0

  name   = var.nat_name != "" ? var.nat_name : "dataflow-nat-${var.project_id}"
  router = google_compute_router.dataflow_router[0].name
  region = var.region

  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  # Configure NAT to provide egress for the specific subnet
  subnetwork {
    name                    = google_compute_subnetwork.dataflow_subnet.name
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }

  log_config {
    enable = false
    filter = "ERRORS_ONLY"
  }

  depends_on = [google_compute_subnetwork.dataflow_subnet]
}

# Create Firestore database for Kafka checkpointing
resource "google_firestore_database" "dataflow_kafka_checkpointing_firestore" {
  name        = "dataflow-kafka-checkpointing"
  project     = var.project_id
  location_id = var.region
  type        = "FIRESTORE_NATIVE"

  depends_on = [google_project_service.required_apis]
}
