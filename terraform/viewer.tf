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
# Custom domain: boardgame-viz.com is mapped to the service via a one-time manual
# `gcloud run domain-mappings create` run under a personal account (domain mapping
# requires the identity that verified ownership in Search Console — the CI service
# account isn't a verified owner). Not Terraform-managed; not tracked elsewhere.
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

# The self-serve collection filter reads `collections.user_collections` — settings page load,
# the sync trigger's staleness check, and /api/collection all go through it
# (bgg-viewer/src/lib/server/collections/read.ts). Like `predictions` above, the `collections`
# dataset is NOT managed by this Terraform config — Dataform creates it — so the id is literal
# and this non-authoritative member adds a grant without disturbing Dataform's ownership.
#
# NOTE: the dataset's `user_collections` is a VIEW over bgg-predictive-models. This grant alone
# is not enough; that view must also be authorized on the source dataset, which is done in
# bgg-predictive-models/terraform/bigquery.tf. Without both, reads fail with "Access Denied".
resource "google_bigquery_dataset_iam_member" "bgg_viewer_collections_viewer" {
  dataset_id = "collections"
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

# --- Catalog artifact bucket ------------------------------------------------
# The artifact moved out of the Cloud Run process and into GCS (storage.tf,
# bgg_viewer_artifacts) so a cold container no longer rebuilds it from BigQuery while a
# user waits. Three grants make that work.

# Read the pointer object and the artifact itself. Object-level read only — the app never
# writes here; the Actions build does.
resource "google_storage_bucket_iam_member" "bgg_viewer_artifacts_reader" {
  bucket = google_storage_bucket.bgg_viewer_artifacts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# Sign URLs as ITSELF. Cloud Run injects no service-account key file, so the client library
# cannot sign locally and falls back to the IAM `signBlob` API — which requires the caller
# to hold tokenCreator on the SA whose identity it is signing with. Without this, signed URL
# generation fails at runtime with a permission error, not at deploy time.
resource "google_service_account_iam_member" "bgg_viewer_self_sign" {
  service_account_id = google_service_account.bgg_viewer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "serviceAccount:${google_service_account.bgg_viewer.email}"
}

# Local development as the same identity. `gcloud auth application-default login
# --impersonate-service-account=bgg-viewer@...` makes every ADC call from a dev machine
# (BigQuery *and* URL signing) run as the SA, so `just dev` takes the real GCS signed-URL
# path instead of logging a SigningError and rebuilding the catalog from BigQuery. That
# needs tokenCreator on the SA for the person impersonating — without it, impersonation
# fails closed and takes the BigQuery fallback down with it (403 on every ADC call).
# Same literal-user pattern as warehouse_api.tf's invoker list.
resource "google_service_account_iam_member" "bgg_viewer_local_impersonation" {
  service_account_id = google_service_account.bgg_viewer.name
  role               = "roles/iam.serviceAccountTokenCreator"
  member             = "user:phil.henrickson@gmail.com"
}

# The CI identity behind GCP_SA_KEY_BGG_DW runs bgg-viewer's catalog-artifact.yml, which
# uploads the artifact and rewrites the pointer. objectAdmin rather than objectCreator: the
# pointer is overwritten in place on every run, not created once.
resource "google_storage_bucket_iam_member" "bgg_viewer_artifacts_ci_writer" {
  bucket = google_storage_bucket.bgg_viewer_artifacts.name
  role   = "roles/storage.objectAdmin"
  member = "serviceAccount:${google_service_account.bgg_pipeline.email}"
}
