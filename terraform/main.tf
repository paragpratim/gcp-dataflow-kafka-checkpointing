# Call the dataflow module
module "dataflow" {
  source = "./modules/dataflow"

  project_id = var.project_id
  region     = var.region
  my_domain  = var.my_domain

  bigquery_datasets = var.bigquery_datasets

  artifact_registry_repository_id = var.artifact_registry_repository_id
  artifact_registry_format        = var.artifact_registry_format
}

module "dataflow_job" {
  source = "./modules/dataflow_flex_streaming"

  count = var.submit_dataflow_flex_job ? 1 : 0

  project_id                = var.project_id
  region                    = var.region
  flex_template_image       = "${var.region}-docker.pkg.dev/${var.project_id}/${var.artifact_registry_repository_id}/kafka-dataflow:latest"
  flex_template_bucket      = module.dataflow.dataflow_staging_bucket
  pipeline_name             = "KafkaToBqPipeline"
  dataflow_temp_location    = "gs://${module.dataflow.dataflow_staging_bucket}/temp"
  dataflow_staging_location = "gs://${module.dataflow.dataflow_staging_bucket}/staging"
  dataflow_subnetwork       = module.dataflow.subnet_self_link
  dataflow_service_account  = module.dataflow.service_account_email
  update_mode               = "drain"
}