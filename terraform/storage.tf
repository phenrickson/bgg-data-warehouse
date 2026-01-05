# Cloud Storage bucket for any data exports/staging

resource "google_storage_bucket" "bgg_data" {
  name          = "${var.project_id}-bgg-data"
  location      = var.location
  project       = var.project_id
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

# Terraform state bucket (created separately, referenced in backend)
# This should be created manually before running terraform init:
# gcloud storage buckets create gs://bgg-projects-terraform-state --location=US
