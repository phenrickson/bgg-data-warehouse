# Pub/Sub + Cloud Functions Deployment Guide

This guide covers deploying the decoupled fetch/process architecture using Pub/Sub and Cloud Functions.

## Overview

The architecture has been updated to separate fetching and processing:
- **Fetch scripts** write responses to `raw_responses` and publish a Pub/Sub message
- **Cloud Function** processes responses asynchronously when triggered by Pub/Sub

## Prerequisites

1. GCP CLI (`gcloud`) installed and authenticated
2. Project: `gcp-demos-411520`
3. Service account: `bgg-data-warehouse@gcp-demos-411520.iam.gserviceaccount.com`

## Deployment Steps

### 1. Create Pub/Sub Topic

```bash
gcloud pubsub topics create process-responses --project=gcp-demos-411520
```

### 2. Deploy Cloud Function (Test Environment)

```bash
gcloud functions deploy process-responses \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./src/cloud_functions/process_responses \
  --entry-point=process_responses \
  --trigger-topic=process-responses \
  --timeout=540s \
  --memory=512MB \
  --set-env-vars=ENVIRONMENT=test \
  --project=gcp-demos-411520
```

### 3. Grant Permissions (if needed)

```bash
gcloud functions add-invoker-policy-binding process-responses \
  --region=us-central1 \
  --member=serviceAccount:bgg-data-warehouse@gcp-demos-411520.iam.gserviceaccount.com \
  --project=gcp-demos-411520
```

### 4. Deploy for Production

To deploy for production environment:

```bash
gcloud functions deploy process-responses-prod \
  --gen2 \
  --runtime=python311 \
  --region=us-central1 \
  --source=./src/cloud_functions/process_responses \
  --entry-point=process_responses \
  --trigger-topic=process-responses \
  --timeout=540s \
  --memory=512MB \
  --set-env-vars=ENVIRONMENT=prod \
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
