output "project_id" {
  value = var.project_id
}

output "data_dataset_id" {
  value = google_bigquery_dataset.bgg_data.dataset_id
}

output "raw_dataset_id" {
  value = google_bigquery_dataset.bgg_raw.dataset_id
}

output "service_account_email" {
  value = google_service_account.bgg_pipeline.email
}

output "fetch_job_name" {
  value = google_cloud_run_v2_job.fetch_new_games.name
}
