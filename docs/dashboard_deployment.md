# BGG Dashboard Deployment Guide

This guide explains how to deploy the BGG Data Warehouse dashboard to Google Cloud Run, making it accessible via a public URL.

## Prerequisites

Before deploying the dashboard, ensure you have:

1. A Google Cloud Platform (GCP) account with billing enabled
2. The necessary permissions to deploy to Cloud Run and access BigQuery
3. A GitHub repository with the BGG Data Warehouse code

## Required GitHub Secrets

The deployment workflow requires the following GitHub secrets:

1. `SERVICE_ACCOUNT_KEY`: A JSON key for a GCP service account with the following permissions:
   - Cloud Run Admin
   - Storage Admin
   - BigQuery User
   - Service Account User

2. `GCP_PROJECT_ID`: Your Google Cloud Platform project ID

## Setting Up GitHub Secrets

1. Go to your GitHub repository
2. Navigate to Settings > Secrets and variables > Actions
3. Click "New repository secret"
4. Add the secrets mentioned above

## Environment Variables

You can also set the following GitHub environment variables:

- `ENVIRONMENT`: Set to `dev` or `prod` to determine which BigQuery dataset to use (defaults to `dev` if not specified)

## Deployment Process

The dashboard is deployed using a GitHub Actions workflow that:

1. Builds a Docker image using `Dockerfile.dashboard`
2. Pushes the image to Google Container Registry
3. Deploys the image to Cloud Run as a service

## Manual Deployment

To manually trigger the deployment:

1. Go to your GitHub repository
2. Navigate to Actions > Deploy BGG Dashboard
3. Click "Run workflow"
4. Select the branch to deploy from
5. Click "Run workflow"

## Accessing the Dashboard

After successful deployment, the workflow will output the URL where your dashboard is accessible. The URL will be in the format:

```
https://bgg-dashboard-[hash].run.app
```

## Troubleshooting

If the deployment fails, check:

1. GitHub Actions logs for specific error messages
2. Ensure all required secrets are correctly set
3. Verify the service account has the necessary permissions
4. Check if the BigQuery dataset exists and is accessible

## Security Considerations

The dashboard is deployed with public access (`--allow-unauthenticated`). If you need to restrict access:

1. Remove the `--allow-unauthenticated` flag from the deployment command
2. Set up Identity-Aware Proxy (IAP) for more granular access control

## Cost Considerations

Cloud Run charges based on usage. To minimize costs:

1. Consider setting CPU and memory limits appropriately
2. Set up auto-scaling to handle traffic efficiently
3. Monitor usage to avoid unexpected charges
