# Dataform Repository

resource "google_dataform_repository" "bgg_warehouse" {
  provider = google-beta
  name     = "bgg-data-warehouse"
  region   = var.region
  project  = var.project_id

  git_remote_settings {
    url            = "https://github.com/phenrickson/bgg-data-warehouse.git"
    default_branch = "main"

    # Authentication token - stored in Secret Manager
    authentication_token_secret_version = google_secret_manager_secret_version.github_token.id
  }

  workspace_compilation_overrides {
    default_database = var.project_id
  }
}

# Secret for GitHub access token
resource "google_secret_manager_secret" "github_token" {
  secret_id = "github-dataform-token"
  project   = var.project_id

  replication {
    auto {}
  }
}

# Placeholder version - you'll need to add the actual token value manually
resource "google_secret_manager_secret_version" "github_token" {
  secret      = google_secret_manager_secret.github_token.id
  secret_data = "REPLACE_WITH_GITHUB_TOKEN" # Set this via terraform.tfvars or manually

  lifecycle {
    ignore_changes = [secret_data]
  }
}

# Grant Dataform service agent access to secrets
resource "google_project_iam_member" "dataform_secret_accessor" {
  project = var.project_id
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"

  depends_on = [google_dataform_repository.bgg_warehouse]
}

# Grant Dataform service agent BigQuery access
resource "google_project_iam_member" "dataform_bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"

  depends_on = [google_dataform_repository.bgg_warehouse]
}

resource "google_project_iam_member" "dataform_bigquery_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"

  depends_on = [google_dataform_repository.bgg_warehouse]
}

# Get current project info
data "google_project" "current" {
  project_id = var.project_id
}

# Cross-project access: Grant Dataform service agent read access to bgg-predictive-models
resource "google_project_iam_member" "dataform_cross_project_bigquery" {
  project = "bgg-predictive-models"
  role    = "roles/bigquery.dataViewer"
  member  = "serviceAccount:service-${data.google_project.current.number}@gcp-sa-dataform.iam.gserviceaccount.com"

  depends_on = [google_dataform_repository.bgg_warehouse]
}
