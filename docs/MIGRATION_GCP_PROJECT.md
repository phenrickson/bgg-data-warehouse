# Migration: Old GCP Project to New Project

## Overview

This guide documents how to migrate from the old GCP project (`gcp-demos-411520`) to the new dedicated project (`bgg-data-warehouse`). The new project uses Terraform-managed infrastructure with simplified configuration.

**Old Project:** `gcp-demos-411520`
**New Project:** `bgg-data-warehouse`

---

## Key Changes

### 1. Project Structure

| Aspect | Old | New |
|--------|-----|-----|
| Project ID | `gcp-demos-411520` | `bgg-data-warehouse` |
| Configuration | Multi-environment in `bigquery.yaml` | Single environment, hardcoded table names |
| Infrastructure | Manual/mixed | Terraform-managed |
| Datasets | `bgg_raw_{env}`, `bgg_data_{env}` | `raw`, `core`, `analytics` |

### 2. Configuration Simplification

**Old approach:** Environment-based configuration
```yaml
environments:
  dev:
    project_id: gcp-demos-411520
    datasets:
      raw: bgg_raw_dev
      core: bgg_data_dev
  prod:
    project_id: gcp-demos-411520
    datasets:
      raw: bgg_raw_prod
      core: bgg_data_prod
```

**New approach:** Single project, hardcoded names
```yaml
project:
  id: bgg-data-warehouse
  location: US
datasets:
  raw: raw
  core: core
```

### 3. Python Code Changes

Table names are now hardcoded as constants in each module instead of being read from config:

```python
# Old approach
config = get_bigquery_config(environment)
table_id = f"{config['project_id']}.{config['datasets']['raw']}.{config['raw_tables']['responses']['name']}"

# New approach
RAW_DATASET = "raw"
RAW_RESPONSES_TABLE = "raw_responses"
config = get_bigquery_config()
table_id = f"{config['project']['id']}.{RAW_DATASET}.{RAW_RESPONSES_TABLE}"
```

---

## Prerequisites

### 1. GCP Project Setup

1. Create the new project in GCP Console
2. Enable required APIs:
   ```bash
   gcloud services enable bigquery.googleapis.com --project=bgg-data-warehouse
   gcloud services enable run.googleapis.com --project=bgg-data-warehouse
   gcloud services enable cloudbuild.googleapis.com --project=bgg-data-warehouse
   gcloud services enable cloudscheduler.googleapis.com --project=bgg-data-warehouse
   ```

### 2. Terraform State

If migrating Terraform state:
```bash
cd terraform
terraform init -migrate-state
```

---

## Migration Steps

### Step 1: Deploy Infrastructure with Terraform

```bash
cd terraform
terraform init
terraform plan -var="project_id=bgg-data-warehouse"
terraform apply -var="project_id=bgg-data-warehouse"
```

This creates:
- Datasets: `raw`, `core`, `analytics`
- All BigQuery tables with correct schemas
- Service account with required IAM permissions
- Cloud Run jobs and schedulers

### Step 2: Create Service Account Key

```bash
gcloud iam service-accounts keys create credentials/bgg-sa-key.json \
  --iam-account=bgg-data-warehouse@bgg-data-warehouse.iam.gserviceaccount.com \
  --project=bgg-data-warehouse
```

### Step 3: Update Local Environment

Update `.env`:
```bash
GOOGLE_APPLICATION_CREDENTIALS=credentials/bgg-sa-key.json
```

### Step 4: Migrate Data (Optional)

If you need to migrate existing data from the old project:

#### Export from old project
```bash
# Export thing_ids
bq extract --destination_format=NEWLINE_DELIMITED_JSON \
  'gcp-demos-411520:bgg_raw_prod.thing_ids' \
  gs://your-bucket/migration/thing_ids.json

# Export raw_responses (if needed)
bq extract --destination_format=NEWLINE_DELIMITED_JSON \
  'gcp-demos-411520:bgg_raw_prod.raw_responses' \
  gs://your-bucket/migration/raw_responses.json
```

#### Import to new project
```bash
# Import thing_ids
bq load --source_format=NEWLINE_DELIMITED_JSON \
  'bgg-data-warehouse:raw.thing_ids' \
  gs://your-bucket/migration/thing_ids.json

# Import raw_responses
bq load --source_format=NEWLINE_DELIMITED_JSON \
  'bgg-data-warehouse:raw.raw_responses' \
  gs://your-bucket/migration/raw_responses.json
```

#### Cross-project query (alternative)
```sql
-- Copy thing_ids directly between projects
INSERT INTO `bgg-data-warehouse.raw.thing_ids` (game_id, type)
SELECT game_id, type
FROM `gcp-demos-411520.bgg_raw_prod.thing_ids`
```

### Step 5: Verify Migration

Run tests to verify connectivity:
```bash
uv run pytest tests/test_bigquery_integration.py -v
```

Expected: All tests pass

### Step 6: Update GitHub Secrets

Update the following secrets in GitHub repository settings:
- `GCP_SA_KEY_BGG_DW`: Service account key JSON for the new project

---

## Tracking Tables

The schema uses append-only tracking tables instead of UPDATE operations:

| Table | Purpose |
|-------|---------|
| `raw_responses` | Raw API responses (append-only) |
| `fetched_responses` | Tracks fetch status per response |
| `processed_responses` | Tracks processing status per response |
| `fetch_in_progress` | Prevents duplicate concurrent fetches |

See [MIGRATION_TRACKING_TABLES.md](MIGRATION_TRACKING_TABLES.md) for details.

---

## IAM Permissions

The service account requires these roles (managed by Terraform):

| Role | Purpose |
|------|---------|
| `roles/bigquery.dataEditor` | Read/write BigQuery tables |
| `roles/bigquery.jobUser` | Run BigQuery jobs |
| `roles/bigquery.readSessionUser` | BigQuery Storage Read API (for pandas) |
| `roles/run.invoker` | Invoke Cloud Run jobs |
| `roles/storage.objectAdmin` | GCS operations |
| `roles/dataform.editor` | Dataform operations |

---

## Troubleshooting

### Error: `bigquery.readsessions.create` permission denied

The service account needs `roles/bigquery.readSessionUser`. Add via Terraform or manually:
```bash
gcloud projects add-iam-policy-binding bgg-data-warehouse \
  --member=serviceAccount:bgg-data-warehouse@bgg-data-warehouse.iam.gserviceaccount.com \
  --role=roles/bigquery.readSessionUser
```

### Error: `Missing required field: record_id`

The BigQuery table is missing the default value for `record_id`. The Terraform schema should include `defaultValueExpression`:

```json
{"name": "record_id", "type": "STRING", "mode": "REQUIRED", "defaultValueExpression": "GENERATE_UUID()"}
```

Run `terraform apply` to update the table, or manually:
```sql
ALTER TABLE `bgg-data-warehouse.raw.raw_responses`
ALTER COLUMN record_id SET DEFAULT GENERATE_UUID();
```

### Error: `Failed to log request to BigQuery: 'raw_tables'`

Old code is referencing the removed `raw_tables` config key. Ensure you're running the updated code with hardcoded table names.

### Error: Wrong project in credentials

Check which project your credentials are for:
```bash
cat credentials/bgg-sa-key.json | jq '.project_id'
```

Generate new key if needed (see Step 2 above).

---

## Rollback

To rollback to the old project:

1. Update `.env` to point to old credentials
2. Revert code changes that hardcode table names
3. Restore the multi-environment config in `bigquery.yaml`

---

## Files Changed in Migration

### Configuration
- `config/bigquery.yaml` - Simplified structure

### Python Modules (hardcoded table names)
- `src/api_client/client.py`
- `src/modules/response_fetcher.py`
- `src/modules/response_processor.py`
- `src/modules/id_fetcher.py`
- `src/modules/refresh_games.py`
- `src/config/__init__.py`

### Terraform
- `terraform/bigquery.tf` - All table definitions
- `terraform/iam.tf` - Service account and permissions
- `terraform/schemas/*.json` - Table schemas

### Tests
- Removed `environment` parameter from test fixtures
- Updated config access patterns
