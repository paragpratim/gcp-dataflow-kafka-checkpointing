resource "google_project_service" "workflows_api" {
  project            = var.project_id
  service            = "workflows.googleapis.com"
  disable_on_destroy = false
}

resource "google_service_account" "workflow_service_account" {
  project      = var.project_id
  account_id   = "workflow-orchestrator"
  display_name = "Workflow Orchestrator Service Account"
  description  = "Service account used by Cloud Workflow to check and launch Dataflow jobs"
}

resource "google_project_iam_member" "workflow_service_account_roles" {
  for_each = toset([
    "roles/dataflow.developer",
    "roles/iam.serviceAccountUser",
  ])

  project = var.project_id
  role    = each.value
  member  = "serviceAccount:${google_service_account.workflow_service_account.email}"
}

resource "google_workflows_workflow" "dataflow_orchestrator" {
  name            = var.workflow_name
  region          = var.region
  service_account = google_service_account.workflow_service_account.email
  description     = "Checks if a Dataflow job is active and launches it if missing"

  source_contents = file(var.workflow_source_path)

  depends_on = [
    google_project_service.workflows_api,
    google_project_iam_member.workflow_service_account_roles,
  ]
}
