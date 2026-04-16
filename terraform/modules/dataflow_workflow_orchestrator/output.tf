output "workflow_name" {
  description = "Created workflow name"
  value       = google_workflows_workflow.dataflow_orchestrator.name
}

output "workflow_id" {
  description = "Created workflow ID"
  value       = google_workflows_workflow.dataflow_orchestrator.id
}

output "workflow_service_account_email" {
  description = "Service account used by the workflow"
  value       = google_service_account.workflow_service_account.email
}
