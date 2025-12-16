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
  source       = local_file.flex_template_spec.filename
  content_type = "application/json"
}

# Dataflow Flex Template Job Launch
resource "google_dataflow_flex_template_job" "kafka_to_bq" {
  project = var.project_id
  region  = var.region

  name                    = "${var.pipeline_name}-flex-job"
  container_spec_gcs_path = google_storage_bucket_object.flex_template_spec.self_link

  parameters = {
    pipelineName   = var.pipeline_name
    subnetwork     = var.dataflow_subnetwork
    serviceAccount = var.dataflow_service_account
    jobName        = "${var.pipeline_name}-${formatdate("YYYYMMDD-hhmmss", timestamp())}"
    runner         = "DataflowRunner"
    streaming      = "true"
    # Add more parameters as needed
  }

  environment = {
    tempLocation            = var.dataflow_temp_location
    stagingLocation         = var.dataflow_staging_location
    maxWorkers              = 2
    numWorkers              = 2
    noPublicIps             = true
    enable-streaming-engine = true
    use-streaming-engine    = true
  }

  on_delete             = "cancel"
  replace_job_on_update = "true"
  update_mode           = var.update_mode
}
