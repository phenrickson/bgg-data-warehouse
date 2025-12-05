# Pub/Sub + Cloud Functions Deployment Guide

This guide covers deploying the decoupled fetch/process architecture using Pub/Sub and Cloud Functions.

## Overview

The architecture has been updated to separate fetching and processing:
- **Fetch scripts** write responses to `raw_responses` and publish a Pub/Sub message
- **Cloud Function** processes responses asynchronously when triggered by Pub/Sub

## Deployment via GitHub Actions (Recommended)

The deployment is **fully automated** through GitHub Actions and Cloud Build.

### Automatic Deployment

Simply push to these branches to trigger deployment:
- **`test` branch** → deploys to test environment
- **`main` branch** → deploys to prod environment
- **`develop` branch** → deploys to dev environment

The workflow automatically:
1. Builds Docker image with your code
2. Creates `process-responses` Pub/Sub topic (if it doesn't exist)
3. Deploys `process-responses-{environment}` Cloud Function
4. Updates Cloud Run Jobs for fetch scripts

### Manual Deployment via GitHub Actions

Trigger a deployment manually from GitHub:

1. Go to **Actions** → **"Deploy BGG Data Warehouse"**
2. Click **"Run workflow"**
3. Select environment (dev/test/prod)
4. Click **"Run workflow"**

### What Gets Deployed

When Cloud Build runs:
- ✅ Pub/Sub topic: `process-responses`
- ✅ Cloud Function: `process-responses-{environment}`
- ✅ Cloud Run Job: `bgg-fetch-new-games-{environment}`
- ✅ Cloud Run Job: `bgg-refresh-old-games-{environment}`

## Manual Deployment (Advanced)

If you need to deploy manually outside of GitHub Actions:

### Prerequisites

1. GCP CLI (`gcloud`) installed and authenticated
2. Project: `gcp-demos-411520`
3. Service account: `bgg-data-warehouse@gcp-demos-411520.iam.gserviceaccount.com`

### Manual Steps

1. **Create Pub/Sub Topic**
```bash
gcloud pubsub topics create process-responses --project=gcp-demos-411520
```

2. **Deploy Cloud Function**
```bash
gcloud functions deploy process-responses-test \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./src/cloud_functions/process_responses \
  --entry-point=process_responses \
  --trigger-topic=process-responses \
  --timeout=540s \
  --memory=512MB \
  --set-env-vars=ENVIRONMENT=test \
  --service-account=bgg-data-warehouse@gcp-demos-411520.iam.gserviceaccount.com \
  --project=gcp-demos-411520
```

## Testing

### Test the Cloud Function

1. Run a fetch script:
```bash
python -m src.pipeline.fetch_new_games
```

2. Check Cloud Function logs:
```bash
gcloud functions logs read process-responses \
  --region=us-central1 \
  --project=gcp-demos-411520 \
  --limit=50
```

3. Verify processing in BigQuery:
```sql
SELECT COUNT(*) FROM `gcp-demos-411520.bgg_raw_test.processed_responses`
WHERE process_status = 'success'
AND process_timestamp > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
```

### Manual Processing

If you need to process responses without fetching:

```bash
python -m src.pipeline.process_responses_manual
```

### Manual Pub/Sub Trigger

To manually trigger processing via Pub/Sub:

```bash
gcloud pubsub topics publish process-responses \
  --message="manual_trigger" \
  --project=gcp-demos-411520
```

## Monitoring

### View Cloud Function Logs
```bash
gcloud functions logs read process-responses \
  --region=us-central1 \
  --project=gcp-demos-411520
```

### Check Function Status
```bash
gcloud functions describe process-responses \
  --region=us-central1 \
  --project=gcp-demos-411520
```

### Monitor Pub/Sub Topic
```bash
gcloud pubsub topics describe process-responses \
  --project=gcp-demos-411520
```

## Rollback

If issues occur, revert to synchronous processing:

1. Modify fetch scripts to call `ResponseProcessor.run()` directly
2. Remove Pub/Sub trigger calls
3. Keep Cloud Function for manual processing option

## Cost Optimization

- Cloud Function uses pay-per-invocation model
- Timeout: 9 minutes (sufficient for processing batches)
- Memory: 512MB (adjust if needed)
- Pub/Sub: First 10GB/month free

## Files Changed

**New files:**
- `src/utils/pubsub_client.py` - Pub/Sub helper
- `src/cloud_functions/process_responses/main.py` - Cloud Function
- `src/cloud_functions/process_responses/requirements.txt` - Dependencies
- `src/cloud_functions/process_responses/.gcloudignore` - Deployment config
- `src/pipeline/process_responses_manual.py` - Manual processing

**Modified files:**
- `src/pipeline/fetch_new_games.py` - Triggers Pub/Sub instead of processing
- `src/pipeline/refresh_old_games.py` - Triggers Pub/Sub instead of processing

## Next Steps

1. Test in test environment
2. Validate end-to-end flow
3. Monitor for a few days
4. Deploy to prod when confident
