# =============================================================================
# bgg-viewer — SvelteKit front-end on Cloud Run (public URL, app-level login gate).
#
# Dedicated least-privilege SA rather than reusing bgg-data-warehouse@ (which
# bgg-dash-viewer and the warehouse API share): the viewer's warehouse-API invoker
# grant then names this app specifically, and its BigQuery access can be revoked
# without touching the pipeline. Same rationale as bgg-thing-ids-scraper in iam.tf.
#
# The Cloud Run *service* is deployed by the app repo's release-please workflow
# (bgg-viewer/.github/workflows/release-please.yml). Terraform owns identity, IAM,
# and the secret containers only — never the service, never the secret values.
#
# See bgg-viewer/docs/superpowers/specs/2026-08-03-deployment-design.md
# =============================================================================

resource "google_service_account" "bgg_viewer" {
  account_id   = "bgg-viewer"
  display_name = "BGG Viewer (SvelteKit front-end)"
  description  = "Runtime SA for the bgg-viewer Cloud Run service"
  project      = var.project_id
}

# --- BigQuery ---------------------------------------------------------------

# Run query jobs. jobUser is project-scoped; there is no narrower form.
resource "google_project_iam_member" "bgg_viewer_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# The catalog artifact joins analytics.games_features + analytics.best_player_counts.
resource "google_bigquery_dataset_iam_member" "bgg_viewer_analytics_viewer" {
  dataset_id = google_bigquery_dataset.bgg_analytics.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# ...and predictions.bgg_predictions. The `predictions` dataset is NOT managed by this
# Terraform config (only core/raw/analytics are), so the id is literal. This member
# resource is non-authoritative, so it adds a grant without disturbing existing ones.
resource "google_bigquery_dataset_iam_member" "bgg_viewer_predictions_viewer" {
  dataset_id = "predictions"
  project    = var.project_id
  role       = "roles/bigquery.dataViewer"
  member     = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# core.users is read on login and WRITTEN on registration, so dataEditor, not
# dataViewer. Dataset-scoped: the viewer must not reach `raw`.
resource "google_bigquery_dataset_iam_member" "bgg_viewer_core_editor" {
  dataset_id = google_bigquery_dataset.bgg_data.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# --- Warehouse read API -----------------------------------------------------
# The run.invoker grant lives in warehouse_api.tf, whose authoritative binding is the
# single source of truth for that service's allow-list. Adding it here would fight it.

# --- Secrets ----------------------------------------------------------------
# Containers only. Versions are created manually with `gcloud secrets versions add`
# so no secret value ever enters git or Terraform state.

resource "google_secret_manager_secret" "bgg_viewer_session_secret" {
  secret_id = "bgg-viewer-session-secret"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "auth"
  }
}

resource "google_secret_manager_secret" "bgg_viewer_registration_code" {
  secret_id = "bgg-viewer-registration-code"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "auth"
  }
}

resource "google_secret_manager_secret_iam_member" "bgg_viewer_session_secret_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.bgg_viewer_session_secret.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

resource "google_secret_manager_secret_iam_member" "bgg_viewer_registration_code_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.bgg_viewer_registration_code.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# --- Deploy-time permission -------------------------------------------------
# The CI identity (bgg-data-warehouse@, via GCP_SA_KEY_BGG_DW) already holds run.admin
# and artifactregistry.writer from iam.tf. It additionally needs serviceAccountUser on
# THIS SA to deploy a service that runs as it — without this, `gcloud run deploy`
# fails with "iam.serviceaccounts.actAs" denied.
resource "google_service_account_iam_member" "bgg_viewer_ci_act_as" {
  service_account_id = google_service_account.bgg_viewer.name
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}
