# Service Account for BGG Pipeline

resource "google_service_account" "bgg_pipeline" {
  account_id   = "bgg-data-warehouse"
  display_name = "BGG Data Warehouse Pipeline"
  description  = "Service account for BGG data pipeline jobs"
  project      = var.project_id
}

# BigQuery permissions
resource "google_project_iam_member" "bgg_pipeline_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}

resource "google_project_iam_member" "bgg_pipeline_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}

# Cloud Run permissions
resource "google_project_iam_member" "bgg_pipeline_run_invoker" {
  project = var.project_id
  role    = "roles/run.invoker"
  member  = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}

# Storage permissions (for any GCS operations)
resource "google_project_iam_member" "bgg_pipeline_storage" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}

# Dataform permissions
resource "google_project_iam_member" "bgg_pipeline_dataform" {
  project = var.project_id
  role    = "roles/dataform.editor"
  member  = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}
