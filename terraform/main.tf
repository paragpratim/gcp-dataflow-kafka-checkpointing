# Call the dataflow module
module "dataflow" {
  source = "./modules/dataflow"

  project_id = var.project_id
  region     = var.region

  bigquery_datasets = var.bigquery_datasets
}