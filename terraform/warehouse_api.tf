# Gating for the warehouse read API (bgg-warehouse-api Cloud Run service).
#
# The service itself is deployed by the Cloud Build Actions workflow
# (config/cloudbuild.warehouse-api.yaml). Terraform owns ONLY its inbound invoker IAM,
# as an AUTHORITATIVE binding so `allUsers` can never be (re)added out of band — the
# whole point of the gating. Applied by .github/workflows/terraform.yml.
#
# ORDERING: this binding targets an existing service, so it must be applied AFTER the
# service is first deployed (merge the deploy PR before merging this one).
#
# See docs/superpowers/specs/2026-07-16-service-auth-pattern-design.md

variable "warehouse_api_invoker_members" {
  description = <<-EOT
    Principals granted roles/run.invoker on bgg-warehouse-api (AUTHORITATIVE — this is
    the complete allow-list; anything not here, including allUsers, cannot invoke).

    This list IS the grant surface: to give a consumer access, add its identity here and
    merge (terraform.yml applies it) — grants stay in code, reviewed, and git-audited.
      - a person:  "user:someone@example.com"
      - a service: "serviceAccount:new-frontend@bgg-data-warehouse.iam.gserviceaccount.com"
    (A "group:..." member is possible too, but a group's membership is managed outside
    Terraform/Actions — prefer listing identities directly here.)
  EOT
  type        = list(string)
  default = [
    "user:phil.henrickson@gmail.com",
    # bgg-viewer's Cloud Run runtime SA. In prod the viewer mints an ID token for
    # ITS OWN identity (src/lib/server/warehouse/token.ts -> mintIdToken), not a
    # user's, so without this every game detail page 403s. See terraform/viewer.tf.
    "serviceAccount:bgg-viewer@bgg-data-warehouse.iam.gserviceaccount.com",
  ]
}

resource "google_cloud_run_v2_service_iam_binding" "warehouse_api_invokers" {
  project  = var.project_id
  location = var.region
  name     = "bgg-warehouse-api"
  role     = "roles/run.invoker"

  members = var.warehouse_api_invoker_members
}

# GitHub token the read API uses to list Actions runs for GET /monitoring/pipeline.
# The three repos are public; the token only lifts the rate limit, so a fine-grained
# token with public read-only access and no permissions is enough. The version is
# added by hand (never in git or state):
#   printf %s "$TOKEN" | gcloud secrets versions add warehouse-api-github-token \
#     --data-file=- --project=bgg-data-warehouse
resource "google_secret_manager_secret" "warehouse_api_github_token" {
  secret_id = "warehouse-api-github-token"
  project   = var.project_id

  replication {
    auto {}
  }

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "monitoring"
  }
}

resource "google_secret_manager_secret_iam_member" "warehouse_api_github_token_access" {
  project   = var.project_id
  secret_id = google_secret_manager_secret.warehouse_api_github_token.secret_id
  role      = "roles/secretmanager.secretAccessor"
  member    = "serviceAccount:bgg-data-warehouse@${var.project_id}.iam.gserviceaccount.com"
}
