# BGG Data Pipeline Architecture

## Overview

The BGG data pipeline is split into two main components:

1. Response Fetcher
2. Response Processor

This separation allows for more efficient data collection and better error handling.

## Components

### Response Fetcher

The fetcher component (`src/pipeline/fetch_responses.py`) continuously fetches game data from the BGG API and stores raw responses in BigQuery:

- Runs as a local process
- Fetches games in chunks (default 20 games per API call)
- Stores raw XML responses in `raw_responses` table
- Can run independently of processing
- Handles API rate limiting and retries

### Response Processor

The processor component (`src/pipeline/process_responses.py`) runs as a Cloud Run Job:

- Processes raw responses into normalized tables
- Runs multiple parallel tasks (default 5)
- Handles processing errors without affecting data fetching
- Automatically retries failed processing
- Scheduled to run every 10 minutes

## Data Flow

1. Fetcher:
   ```
   BGG API -> raw_responses table
   ```

2. Processor:
   ```
   raw_responses table -> normalized BigQuery tables
   ```

## Tables

### Raw Tables

- `thing_ids`: Game IDs to process
- `raw_responses`: Raw API responses
  - game_id (INT)
  - response_data (STRING)
  - fetch_timestamp (TIMESTAMP)
  - processed (BOOLEAN)
  - process_timestamp (TIMESTAMP)
  - process_status (STRING)
  - process_attempt (INT)

### Processed Tables

- `games`: Core game data
- `alternate_names`: Game translations
- Various dimension and bridge tables
- See `config/bigquery.yaml` for full schema

## Deployment

### Prerequisites

1. Google Cloud project with required APIs enabled:
   - Cloud Run
   - Cloud Build
   - Cloud Scheduler
   - BigQuery
   - Container Registry

2. Service account with permissions:
   ```bash
   # Create service account
   gcloud iam service-accounts create bgg-processor \
     --display-name="BGG Processor Service Account"

   # Grant required permissions
   gcloud projects add-iam-policy-binding $PROJECT_ID \
     --member="serviceAccount:bgg-processor@$PROJECT_ID.iam.gserviceaccount.com" \
     --role="roles/bigquery.dataEditor"
   ```

### Deploy Processor

1. Build and deploy using Cloud Build:
   ```bash
   gcloud builds submit
   ```

This will:
- Build the processor container
- Push to Container Registry
- Create Cloud Run Job
- Set up Cloud Scheduler

### Run Fetcher

The fetcher runs as a local process:

```bash
# Run in development
uv run python -m src.pipeline.fetch_responses --environment=dev

# Run in production
uv run python -m src.pipeline.fetch_responses --environment=prod
```

## Monitoring

### BigQuery Views

Create monitoring views:

```sql
-- Processing status
CREATE VIEW `monitoring.processing_status` AS
SELECT
  COUNT(*) as total_responses,
  COUNTIF(processed) as processed_count,
  COUNTIF(NOT processed) as unprocessed_count,
  COUNTIF(process_status IS NOT NULL) as error_count
FROM `raw.raw_responses`
WHERE fetch_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR);

-- Processing errors
CREATE VIEW `monitoring.processing_errors` AS
SELECT
  game_id,
  process_attempt,
  process_status as error,
  fetch_timestamp,
  process_timestamp
FROM `raw.raw_responses`
WHERE NOT processed
AND process_status IS NOT NULL
ORDER BY process_timestamp DESC;
```

### Cloud Monitoring

Set up alerts for:
- High error rates
- Processing delays
- Failed Cloud Run Jobs

## Error Handling

1. API Errors:
   - Fetcher retries with backoff
   - Failed requests logged in `request_log` table

2. Processing Errors:
   - Each response can be retried up to 3 times
   - Errors stored in `process_status` field
   - Failed items don't block other processing

## Development

### Local Testing

1. Test fetcher:
   ```bash
   uv run python -m src.pipeline.fetch_responses --environment=dev
   ```

2. Test processor:
   ```bash
   uv run python -m src.pipeline.process_responses --environment=dev
   ```

### Adding New Features

1. Modify processor:
   - Update `process_responses.py`
   - Build and test locally
   - Deploy new version:
     ```bash
     gcloud builds submit
     ```

2. Modify fetcher:
   - Update `fetch_responses.py`
   - Deploy new version to fetcher instance
