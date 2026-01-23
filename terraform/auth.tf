# Authentication tables for bgg-dash-viewer
# These tables support user registration and login for the dashboard application

resource "google_bigquery_table" "users" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "users"
  project             = var.project_id
  description         = "Application users for bgg-dash-viewer"
  deletion_protection = true

  schema = jsonencode([
    {
      name        = "user_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "UUID primary key"
    },
    {
      name        = "email"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "User email (unique)"
    },
    {
      name        = "password_hash"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "bcrypt password hash"
    },
    {
      name        = "display_name"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Optional display name"
    },
    {
      name        = "created_at"
      type        = "TIMESTAMP"
      mode        = "REQUIRED"
      description = "Account creation timestamp"
    },
    {
      name        = "last_login"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "Last successful login"
    },
    {
      name        = "is_active"
      type        = "BOOLEAN"
      mode        = "REQUIRED"
      description = "Account active status"
    }
  ])

  labels = {
    environment = var.environment
    managed_by  = "terraform"
    purpose     = "auth"
  }
}
