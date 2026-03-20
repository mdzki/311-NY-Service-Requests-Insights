variable "credentials" { description = "Path to GCP key" }
variable "project_name" { description = "GCP Project ID" }
variable "location" { description = "Dataset location" }
variable "region" { description = "Provider region" }
variable "bq_dataset_name" { description = "BigQuery Dataset Name" }
variable "gcs_bucket_name" { description = "Unique GCS Bucket Name" }
variable "gcs_storage_class" { description = "Storage Class" }