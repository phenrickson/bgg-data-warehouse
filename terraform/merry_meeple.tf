# The Merry Meeple (cpdelehanty/MM-website): read access to bgg-viewer's vectors and catalog
# artifacts, for game recommendations ranked in the café site's browser.
#
# A Cloudflare Pages Function on the café's site holds a key for this account (Pages secret
# GCS_SA_KEY) and signs short-lived URLs; browsers on themerrymeeple.com then download the
# artifacts straight from the bucket (CORS for those origins is in storage.tf).
# Design: docs/superpowers/specs/2026-11-10-merry-meeple-artifact-access-design.md
#
# THE KEY IS NOT IN TERRAFORM. It was made by hand in the Cloud Console so the private key
# never sits in state, which means nothing here will remind anyone to rotate it.
# Creating, handing over, ROTATING (every 12 months) and revoking it:
#   docs/runbooks/merry-meeple-artifact-key.md

resource "google_service_account" "merry_meeple_artifacts" {
  account_id   = "merry-meeple-artifacts"
  display_name = "Merry Meeple site: reads vectors + catalog artifacts"
  description  = "Key held by the café's Cloudflare Pages Function. Rotation: docs/runbooks/merry-meeple-artifact-key.md"
  project      = var.project_id
}

# Read only the vectors and catalog artifacts: their content-hashed data files
# (vectors-<hash>.arrow.gz, catalog-<hash>.arrow.gz) and pointers (vectors-current.json,
# catalog-current.json), as bgg-viewer's scripts/lib/publish-artifact.ts names them.
# Thumbnails and coordinates are out of reach, and listing the bucket is denied (the
# condition is false at bucket level); the function never lists. Conditions need uniform
# bucket-level access, which the bucket has.
resource "google_storage_bucket_iam_member" "merry_meeple_artifacts_reader" {
  bucket = google_storage_bucket.bgg_viewer_artifacts.name
  role   = "roles/storage.objectViewer"
  member = "serviceAccount:${google_service_account.merry_meeple_artifacts.email}"

  condition {
    title       = "vectors-and-catalog-only"
    description = "Only the vectors and catalog artifacts and their pointers"
    expression  = <<-EOT
      resource.name.startsWith("projects/_/buckets/${google_storage_bucket.bgg_viewer_artifacts.name}/objects/vectors-") ||
      resource.name.startsWith("projects/_/buckets/${google_storage_bucket.bgg_viewer_artifacts.name}/objects/catalog-")
    EOT
  }
}

output "merry_meeple_artifacts_service_account" {
  value = google_service_account.merry_meeple_artifacts.email
}
