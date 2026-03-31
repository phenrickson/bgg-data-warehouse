# On-Demand Game Fetch Workflow

## Problem

When a new game is added to the data warehouse but has missing data (e.g., `year_published = NULL`), it can take up to 14 days before the scheduled refresh job re-fetches it. There's no way to manually trigger a fetch for specific games outside the scheduled pipeline.

## Solution

A new GitHub Actions `workflow_dispatch` workflow that accepts a list of game IDs, fetches them from the BGG API, processes the responses into BigQuery, and triggers the downstream Dataform + ML scoring pipeline.

## Components

### 1. Pipeline script: `src/pipeline/fetch_games.py`

A thin orchestrator that:

- Accepts game IDs via the `GAME_IDS` environment variable (comma-separated)
- Parses and deduplicates the IDs
- Uses the existing `ResponseFetcher` to call the BGG API (same chunk_size=20 pattern)
- Validates responses — any game IDs that BGG doesn't return data for are logged and skipped
- Uses the existing `ResponseProcessor` to normalize responses into BigQuery tables (games, categories, mechanics, etc.)
- Logs a summary: how many games were requested, fetched successfully, and failed

Follows the same pattern as `fetch_new_games.py` and `refresh_old_games.py` — delegates to existing modules, no new fetching or processing logic.

### 2. Cloud Run job: `bgg-fetch-games`

Added to `config/cloudbuild.yaml` alongside existing jobs:

- Image: `gcr.io/$PROJECT_ID/bgg-processor:latest` (same image as all other pipeline jobs)
- Entrypoint args: `src.pipeline.fetch_games`
- Resources: 2Gi memory, 1 CPU (same as fetch-new-games)
- Timeout: 30m (these are small targeted fetches)
- Max retries: 1
- Game IDs passed via `GAME_IDS` env var, overridden at execution time

### 3. GitHub Actions workflow: `.github/workflows/fetch_games.yml`

- **Trigger:** `workflow_dispatch` with a `game_ids` text input (comma-separated, e.g., `467694,12345`)
- **Steps:**
  1. Authenticate to Google Cloud
  2. Execute `bgg-fetch-games` Cloud Run job with `--update-env-vars=GAME_IDS=<input>`
  3. Write job summary (games requested, fetched, failed)
- **Name:** `Run Fetch Games` (referenced by Dataform workflow trigger)

### 4. Dataform integration

Update `.github/workflows/dataform.yml` to include the new workflow in its `workflow_run` trigger:

```yaml
workflow_run:
  workflows: ["Run Fetch New Games", "Run Refresh Old Games", "Run Fetch Games"]
  types: [completed]
```

This ensures the full downstream pipeline (Dataform → ML scoring) fires automatically after an on-demand fetch.

## Validation

The BGG API itself handles validation. If a game ID doesn't exist or returns no data, the `ResponseFetcher` logs it and skips it. The workflow summary reports which IDs succeeded and which failed.

## What this doesn't change

- No modifications to existing fetch/refresh pipelines or their schedules
- No new infrastructure beyond the Cloud Run job definition in `cloudbuild.yaml`
- No new Python dependencies
- Reuses existing `ResponseFetcher`, `ResponseProcessor`, and `BGGAPIClient` modules
