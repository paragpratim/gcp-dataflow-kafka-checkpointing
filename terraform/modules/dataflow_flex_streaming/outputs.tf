output "flex_template_spec_gcs_path" {
  description = "GCS path to the Flex Template spec JSON."
  value       = google_dataflow_flex_template_job.dataflow_flex_job.container_spec_gcs_path
}

output "dataflow_job_name" {
  description = "Name of the Dataflow Flex Template job."
  value       = google_dataflow_flex_template_job.dataflow_flex_job.name
}

output "dataflow_job_id" {
  description = "ID of the Dataflow Flex Template job."
  value       = google_dataflow_flex_template_job.dataflow_flex_job.job_id
}

output "dataflow_job_state" {
  description = "Current state of the Dataflow Flex Template job."
  value       = google_dataflow_flex_template_job.dataflow_flex_job.state
}