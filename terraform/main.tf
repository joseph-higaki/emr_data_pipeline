terraform {
  required_providers {
    google = {
      source = "hashicorp/google"
      version = "6.8.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region = var.region
  credentials = var.credentials_file
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = var.dataset_id
  friendly_name               = "EMR Analytics"
  description                 = "EMR Analytics"
  location                    = var.location
  delete_contents_on_destroy = "true"
}


resource "google_storage_bucket" "emr_analytics_bucket" {
  name          = local.analytics_storage_bucket_name
  location      = var.location
  force_destroy = true
  soft_delete_policy {    
    retention_duration_seconds = 0
  }
}