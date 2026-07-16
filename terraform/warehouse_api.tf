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
    Consumer-agnostic: prefer adding a new consumer's SA to the invoker Google Group
    rather than listing it here. Add the group once it exists in Workspace, e.g.
    "group:bgg-api-invokers@googlegroups.com".
  EOT
  type    = list(string)
  default = ["user:phil.henrickson@gmail.com"]
}

resource "google_cloud_run_v2_service_iam_binding" "warehouse_api_invokers" {
  project  = var.project_id
  location = var.region
  name     = "bgg-warehouse-api"
  role     = "roles/run.invoker"

  members = var.warehouse_api_invoker_members
}
