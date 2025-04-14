variable "project_id" {
  description = "The Google Cloud project ID"
  type        = string
}

variable "region" {
  description = "The Google Cloud region for resources"
  type        = string
}

variable "location" {
  description = "The location for BigQuery datasets and GCS buckets (e.g., EU, US)"
  type        = string
}

variable "credentials_file" {
  description = "Path to the Google Cloud service account credentials file"
  type        = string
}

variable "dataset_id" {
  description = "BigQuery dataset ID"
  type        = string
}

variable "analytics_storage_bucket_name" {
  description = "Name of the GCS bucket for the analytics pipeline"
  type        = string
}