# Artifact Registry for Docker images

resource "google_artifact_registry_repository" "bgg_images" {
  location      = var.region
  repository_id = "bgg-data-warehouse"
  description   = "Docker images for BGG Data Warehouse"
  format        = "DOCKER"
  project       = var.project_id
}

# Cloud Run Job - Fetch New Games

resource "google_cloud_run_v2_job" "fetch_new_games" {
  name     = "bgg-fetch-new-games-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.bgg_images.repository_id}/bgg-processor:${var.environment}"

        args = ["src.pipeline.fetch_new_games"]

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }

        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }

        env {
          name = "BGG_API_TOKEN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.bgg_api_token.secret_id
              version = "latest"
            }
          }
        }
      }

      service_account = google_service_account.bgg_pipeline.email
      timeout         = "10800s" # 3 hours
      max_retries     = 3
    }
  }

  depends_on = [
    google_artifact_registry_repository.bgg_images
  ]
}

# Cloud Run Job - Refresh Old Games

resource "google_cloud_run_v2_job" "refresh_old_games" {
  name     = "bgg-refresh-old-games-${var.environment}"
  location = var.region
  project  = var.project_id

  template {
    template {
      containers {
        image = "${var.region}-docker.pkg.dev/${var.project_id}/${google_artifact_registry_repository.bgg_images.repository_id}/bgg-processor:${var.environment}"

        args = ["src.pipeline.refresh_old_games"]

        resources {
          limits = {
            cpu    = "1"
            memory = "2Gi"
          }
        }

        env {
          name  = "ENVIRONMENT"
          value = var.environment
        }

        env {
          name = "BGG_API_TOKEN"
          value_source {
            secret_key_ref {
              secret  = google_secret_manager_secret.bgg_api_token.secret_id
              version = "latest"
            }
          }
        }
      }

      service_account = google_service_account.bgg_pipeline.email
      timeout         = "10800s" # 3 hours
      max_retries     = 3
    }
  }

  depends_on = [
    google_artifact_registry_repository.bgg_images
  ]
}

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
