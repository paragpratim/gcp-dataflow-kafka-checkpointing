# Call the dataflow module
module "dataflow" {
  source = "./modules/dataflow"

  project_id = var.project_id
  region     = var.region
  my_domain  = var.my_domain

  bigquery_datasets = var.bigquery_datasets
}