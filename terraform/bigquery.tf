# BigQuery Datasets

resource "google_bigquery_dataset" "bgg_data" {
  dataset_id  = "core"
  project     = var.project_id
  location    = var.location
  description = "BGG Data Warehouse - processed game data"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_bigquery_dataset" "bgg_raw" {
  dataset_id  = "raw"
  project     = var.project_id
  location    = var.location
  description = "BGG Data Warehouse - raw API responses"

  labels = {
    environment = var.environment
    managed_by  = "terraform"
  }
}

resource "google_bigquery_dataset" "bgg_analytics" {
  dataset_id  = "analytics"
  project     = var.project_id
  location    = var.location
  description = "BGG Data Warehouse - analytics views and tables (managed by Dataform)"

  labels = {
    environment = var.environment
    managed_by  = "dataform"
  }
}

# =============================================================================
# Main Dataset Tables
# =============================================================================

resource "google_bigquery_table" "games" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "games"
  project             = var.project_id
  description         = "Core game information and statistics"
  deletion_protection = false

  time_partitioning {
    type  = "DAY"
    field = "load_timestamp"
  }

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/games.json")
}

resource "google_bigquery_table" "rankings" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "rankings"
  project             = var.project_id
  description         = "Game rankings by category"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "load_timestamp"
  }

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/rankings.json")
}

resource "google_bigquery_table" "player_counts" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "player_counts"
  project             = var.project_id
  description         = "Player count voting results"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/player_counts.json")
}

resource "google_bigquery_table" "alternate_names" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "alternate_names"
  project             = var.project_id
  description         = "Alternative names and translations for games"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/alternate_names.json")
}

# Dimension tables

resource "google_bigquery_table" "categories" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "categories"
  project             = var.project_id
  description         = "Game categories"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/categories.json")
}

resource "google_bigquery_table" "mechanics" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "mechanics"
  project             = var.project_id
  description         = "Game mechanics"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/mechanics.json")
}

resource "google_bigquery_table" "publishers" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "publishers"
  project             = var.project_id
  description         = "Game publishers"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/publishers.json")
}

resource "google_bigquery_table" "designers" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "designers"
  project             = var.project_id
  description         = "Game designers"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/designers.json")
}

resource "google_bigquery_table" "artists" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "artists"
  project             = var.project_id
  description         = "Game artists"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/artists.json")
}

resource "google_bigquery_table" "families" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "families"
  project             = var.project_id
  description         = "Game families/series"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/families.json")
}

# Bridge tables

resource "google_bigquery_table" "game_categories" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_categories"
  project             = var.project_id
  description         = "Game to category relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_categories.json")
}

resource "google_bigquery_table" "game_mechanics" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_mechanics"
  project             = var.project_id
  description         = "Game to mechanic relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_mechanics.json")
}

resource "google_bigquery_table" "game_publishers" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_publishers"
  project             = var.project_id
  description         = "Game to publisher relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_publishers.json")
}

resource "google_bigquery_table" "game_designers" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_designers"
  project             = var.project_id
  description         = "Game to designer relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_designers.json")
}

resource "google_bigquery_table" "game_artists" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_artists"
  project             = var.project_id
  description         = "Game to artist relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_artists.json")
}

resource "google_bigquery_table" "game_families" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_families"
  project             = var.project_id
  description         = "Game to family relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_families.json")
}

resource "google_bigquery_table" "game_implementations" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_implementations"
  project             = var.project_id
  description         = "Game implementation relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_implementations.json")
}

resource "google_bigquery_table" "game_expansions" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "game_expansions"
  project             = var.project_id
  description         = "Game expansion relationships"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/game_expansions.json")
}

resource "google_bigquery_table" "language_dependence" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "language_dependence"
  project             = var.project_id
  description         = "Language dependence ratings"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/language_dependence.json")
}

resource "google_bigquery_table" "suggested_ages" {
  dataset_id          = google_bigquery_dataset.bgg_data.dataset_id
  table_id            = "suggested_ages"
  project             = var.project_id
  description         = "Age suggestion voting results"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/suggested_ages.json")
}

# =============================================================================
# Raw Dataset Tables
# =============================================================================

resource "google_bigquery_table" "raw_thing_ids" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "thing_ids"
  project             = var.project_id
  description         = "Game IDs from BGG with processing status"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/raw_thing_ids.json")
}

resource "google_bigquery_table" "raw_request_log" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "request_log"
  project             = var.project_id
  description         = "API request tracking log"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "request_timestamp"
  }

  schema = file("${path.module}/schemas/raw_request_log.json")
}

resource "google_bigquery_table" "raw_responses" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "raw_responses"
  project             = var.project_id
  description         = "Raw API responses before processing"
  deletion_protection = var.environment == "prod"

  time_partitioning {
    type  = "DAY"
    field = "fetch_timestamp"
  }

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/raw_responses.json")
}

resource "google_bigquery_table" "fetch_in_progress" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "fetch_in_progress"
  project             = var.project_id
  description         = "Tracks game IDs currently being fetched to prevent duplicates"
  deletion_protection = false

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/fetch_in_progress.json")
}

resource "google_bigquery_table" "fetched_responses" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "fetched_responses"
  project             = var.project_id
  description         = "Tracks fetch status for each raw response"
  deletion_protection = var.environment == "prod"

  clustering = ["game_id"]

  schema = file("${path.module}/schemas/fetched_responses.json")
}

resource "google_bigquery_table" "processed_responses" {
  dataset_id          = google_bigquery_dataset.bgg_raw.dataset_id
  table_id            = "processed_responses"
  project             = var.project_id
  description         = "Tracks which raw responses have been processed"
  deletion_protection = var.environment == "prod"

  schema = file("${path.module}/schemas/processed_responses.json")
}
