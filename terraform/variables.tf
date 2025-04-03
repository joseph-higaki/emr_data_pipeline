variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
  default     = "emr-data-pipeline"
}

variable "region" {
  description = "The Google Cloud region for resources"
  type        = string
  default     = "europe-southwest1"
}

variable "location" {
  description = "The location for BigQuery datasets and GCS buckets (e.g., EU, US)"
  type        = string
  default     = "EU"
}

variable "credentials_file" {
  description = "Path to the Google Cloud service account credentials file"
  type        = string
  default     = "/workspaces/emr_data_pipeline/.gcp.auth/terraform-infra/emr-data-pipeline-terraform-infra-2a7e3664fe29.json"
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
  default     = "emr_analytics"
}

variable "analytics_storage_bucket_name_suffix" {
  description = "Name of the GCS bucket for the analytics pipeline"
  type        = string
  default     = "emr_analytics"
}

variable "generator_storage_bucket_name_suffix" {
  description = "Name of the GCS bucket for the analytics pipeline"
  type        = string
  default     = "emr_generator"
}

locals  {
  analytics_storage_bucket_name = "${var.project_id}-${var.analytics_storage_bucket_name_suffix}"
  generator_storage_bucket_name = "${var.project_id}-${var.generator_storage_bucket_name_suffix}"
}