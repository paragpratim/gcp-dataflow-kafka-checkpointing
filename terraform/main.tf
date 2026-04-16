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

module "dataflow_workflow_orchestrator" {
  source = "./modules/dataflow_workflow_orchestrator"

  count = var.create_dataflow_workflow_orchestrator ? 1 : 0

  project_id = var.project_id
  region     = var.region

  workflow_name        = var.workflow_name
  workflow_source_path = "${path.root}/modules/dataflow_workflow_orchestrator/workflows/dataflow_orchestrator.yaml"
}