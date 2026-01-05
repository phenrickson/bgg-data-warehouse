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

output "artifact_registry_repository" {
  value = google_artifact_registry_repository.bgg_images.name
}
