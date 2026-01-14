# Artifact Registry for Docker images

resource "google_artifact_registry_repository" "bgg_images" {
  location      = var.region
  repository_id = "bgg-data-warehouse"
  description   = "Docker images for BGG Data Warehouse"
  format        = "DOCKER"
  project       = var.project_id
}

resource "google_artifact_registry_repository" "bgg_dash_viewer" {
  location      = var.region
  repository_id = "bgg-dash-viewer"
  description   = "Docker images for BGG Dash Viewer"
  format        = "DOCKER"
  project       = var.project_id
}

# Cloud Run Jobs are created by the deploy workflow (config/cloudbuild.yaml)
# after the Docker image is built and pushed.

# Secret Manager for API token

resource "google_secret_manager_secret" "bgg_api_token" {
  secret_id = "bgg-api-token"
  project   = var.project_id

  replication {
    auto {}
  }
}

# Grant service account access to the secret
resource "google_secret_manager_secret_iam_member" "bgg_pipeline_secret_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.bgg_api_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}
