# Dataflow Flex Template Specification File
resource "local_file" "flex_template_spec" {
  content = jsonencode({
    image    = var.flex_template_image
    sdk_info = { language = "JAVA" }
    # Add other options if needed
  })
  filename = "${path.module}/dataflow-flex-template-spec.json"
}

resource "google_storage_bucket_object" "flex_template_spec" {
  name         = "templates/dataflow-flex-template-spec.json"
  bucket       = var.flex_template_bucket
  content      = local_file.flex_template_spec.content
  content_type = "application/json"

  depends_on = [local_file.flex_template_spec]
}

# Dataflow Flex Template Job Launch
resource "google_dataflow_flex_template_job" "dataflow_flex_job" {
  provider = google-beta
  project  = var.project_id
  region   = var.region

  name                    = lower("${var.pipeline_name}-${formatdate("YYYYMMDD-hhmmss", timestamp())}")
  container_spec_gcs_path = "gs://${google_storage_bucket_object.flex_template_spec.bucket}/${google_storage_bucket_object.flex_template_spec.name}"

  subnetwork              = var.dataflow_subnetwork
  service_account_email   = var.dataflow_service_account
  staging_location        = var.dataflow_staging_location
  temp_location           = var.dataflow_temp_location
  num_workers             = 1
  max_workers             = 2
  ip_configuration        = "WORKER_IP_PRIVATE"
  enable_streaming_engine = true

  additional_pipeline_options = []

  parameters = {
    pipelineName = var.pipeline_name
  }

  on_delete                    = var.update_mode
  skip_wait_on_job_termination = false
}
