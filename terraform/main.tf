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