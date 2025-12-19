# Dataflow Flex Template Specification File
resource "local_file" "flex_template_spec" {
  content = jsonencode({
    image    = var.flex_template_image
    sdk_info = { language = "JAVA" }
    # Add other options if needed
  })
  filename = "${path.module}/kafka-to-bq-spec.json"
}

resource "google_storage_bucket_object" "flex_template_spec" {
  name         = "templates/kafka-to-bq-spec.json"
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

  name                    = "${var.pipeline_name}-flex-job"
  container_spec_gcs_path = google_storage_bucket_object.flex_template_spec.self_link

  subnetwork              = var.dataflow_subnetwork
  service_account_email   = var.dataflow_service_account
  staging_location        = var.dataflow_staging_location
  temp_location           = var.dataflow_temp_location
  num_workers             = 2
  max_workers             = 2
  ip_configuration        = "WORKER_IP_PRIVATE"
  enable_streaming_engine = true

  parameters = {
    pipelineName = var.pipeline_name
    jobName      = "${var.pipeline_name}-${replace(replace(formatdate("yyyyMMdd-HHmmss", timestamp()), ":", ""), "_", "")}"
    runner       = "DataflowRunner"
    streaming    = "true"
    # Add more parameters as needed
  }

  on_delete                    = var.update_mode
  skip_wait_on_job_termination = false
}
