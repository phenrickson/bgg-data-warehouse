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

# =============================================================================
# bgg-viewer catalog artifact
#
# The viewer's in-browser catalog (~5.25 MB gzipped Arrow) was built from BigQuery
# inside the Cloud Run process on a 6h TTL. Because that cache is in-process and the
# service scales to zero, every cold container rebuilt it on its first request — a
# measured 14.6s, in the user's critical path, on what is normally a cold visit.
#
# The artifact is now built once per pipeline run by bgg-viewer's catalog-artifact.yml
# and stored here under a content-hashed name, with `catalog-current.json` naming the
# current hash. The app reads that pointer and hands the browser a signed URL.
#
# PRIVATE. The artifact carries model predictions and is served only to authenticated
# users; the signed URL is what makes that work without a public object.
#
# See bgg-viewer/docs/superpowers/specs/2026-09-10-gcs-catalog-artifact-design.md
# =============================================================================

resource "google_storage_bucket" "bgg_viewer_artifacts" {
  name          = "${var.project_id}-bgg-viewer-artifacts"
  location      = var.location
  project       = var.project_id
  force_destroy = var.environment != "prod"

  uniform_bucket_level_access = true
  public_access_prevention    = "enforced"

  # Content-hashed names never collide, so old artifacts would accumulate forever.
  # 30 days is well beyond any signed URL's 24h life, so nothing in flight is cut off.
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type = "Delete"
    }
  }

  # The browser fetches the signed URL from storage.googleapis.com while the page is on
  # boardgame-viz.com — cross-origin, so GCS must say the origin is allowed or the browser
  # refuses to hand the bytes to JavaScript.
  #
  # NOTE: only browsers enforce this. curl against a signed URL succeeds either way, and
  # GCS returns 200 with the data even when it is blocked, so server logs look healthy
  # while the app is broken. Verify from a real page on the real origin.
  cors {
    origin          = ["https://boardgame-viz.com", "https://www.boardgame-viz.com", "http://localhost:5173"]
    method          = ["GET", "HEAD"]
    response_header = ["Content-Type", "Content-Encoding", "Range"]
    max_age_seconds = 3600
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "viewer-artifacts"
  }
}
