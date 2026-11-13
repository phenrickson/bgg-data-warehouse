# TEMPORARY, for BGG.CON 2026: the convention library recorder (bgg-viewer spec §8).
#
# A Cloud Run job, `bgg-bggcon-recorder` (created by config/cloudbuild.yaml like the other
# jobs), runs once a minute during the con. Each run records checkout events and a heartbeat
# here, and writes the current counts to the viewer bucket's bggcon/ for the dashboard.
#
# It runs as the pipeline service account, which already has BigQuery data editor, project-wide
# object admin (so bggcon/ in the viewer bucket) and run.invoker, so no new grants.
#
# Remove this file, the two schemas and the job after the con; the tables can stay for the
# end-of-con report.

resource "google_project_service" "cloud_scheduler" {
  project            = var.project_id
  service            = "cloudscheduler.googleapis.com"
  disable_on_destroy = false
}

resource "google_bigquery_table" "bggcon_checkout_events" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "bggcon_checkout_events"
  project             = var.project_id
  description         = "BGG.CON 2026 library: one row per copy checked out or returned, seen once a minute"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "observed_at"
  }

  clustering = ["bgg_id"]

  schema = file("${path.module}/schemas/bggcon_checkout_events.json")
}

resource "google_bigquery_table" "bggcon_runs" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "bggcon_runs"
  project             = var.project_id
  description         = "BGG.CON 2026 library recorder: one heartbeat per run, so coverage gaps show"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "observed_at"
  }

  schema = file("${path.module}/schemas/bggcon_runs.json")
}

# Every minute, Tue 17 Nov (test day) through Sun 22 Nov, Chicago time. The job itself stops at
# Sunday 3:00 PM and does nothing outside its window, so a stray run costs no request.
resource "google_cloud_scheduler_job" "bggcon_recorder" {
  name        = "bgg-bggcon-recorder"
  project     = var.project_id
  region      = var.region
  description = "Runs the BGG.CON library recorder once a minute during the con"
  schedule    = "* * 17-22 11 *"
  time_zone   = "America/Chicago"

  # One attempt: a missed minute is just a gap, and the next run catches up.
  retry_config {
    retry_count = 0
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/bgg-bggcon-recorder:run"

    oauth_token {
      service_account_email = google_service_account.bgg_pipeline.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_project_service.cloud_scheduler]
}

# TEMPORARY trial: the same job every minute, Fri 13 Nov 2:00–6:00 PM Chicago, to check the real
# schedule end to end before the con. The job treats these as test runs. Remove after the trial.
resource "google_cloud_scheduler_job" "bggcon_recorder_trial" {
  name        = "bgg-bggcon-recorder-trial"
  project     = var.project_id
  region      = var.region
  description = "Trial of the BGG.CON recorder's schedule, Fri 13 Nov 2-6 PM Chicago"
  schedule    = "* 14-17 13 11 *"
  time_zone   = "America/Chicago"

  retry_config {
    retry_count = 0
  }

  http_target {
    http_method = "POST"
    uri         = "https://run.googleapis.com/v2/projects/${var.project_id}/locations/${var.region}/jobs/bgg-bggcon-recorder:run"

    oauth_token {
      service_account_email = google_service_account.bgg_pipeline.email
      scope                 = "https://www.googleapis.com/auth/cloud-platform"
    }
  }

  depends_on = [google_project_service.cloud_scheduler]
}
