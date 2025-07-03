# PowerShell script to build and run the BGG dashboard locally for testing

# Set default environment variables
$env:ENVIRONMENT = if ($env:ENVIRONMENT) { $env:ENVIRONMENT } else { "dev" }
$env:PORT = if ($env:PORT) { $env:PORT } else { "8501" }

# Check if credentials exist
if (-not (Test-Path "credentials/service-account-key.json")) {
    Write-Error "Error: credentials/service-account-key.json not found."
    Write-Host "Please place your GCP service account key in this location."
    exit 1
}

# Set Google Application Credentials
$env:GOOGLE_APPLICATION_CREDENTIALS = (Resolve-Path "credentials/service-account-key.json").Path

# Check if .env file exists, create if not
if (-not (Test-Path ".env")) {
    Write-Host "Creating .env file..."
    $projectId = & gcloud config get-value project 2>$null
    @"
GCP_PROJECT_ID=$projectId
ENVIRONMENT=$env:ENVIRONMENT
GOOGLE_APPLICATION_CREDENTIALS=$env:GOOGLE_APPLICATION_CREDENTIALS
"@ | Out-File -FilePath ".env" -Encoding utf8
}

# Build the Docker image
Write-Host "Building Docker image..."
docker build -t bgg-dashboard:local -f Dockerfile.dashboard .

# Run the container
Write-Host "Starting dashboard container..."
docker run --rm -p ${env:PORT}:${env:PORT} `
    -e PORT=${env:PORT} `
    -e ENVIRONMENT=${env:ENVIRONMENT} `
    -e GOOGLE_APPLICATION_CREDENTIALS="/app/credentials/service-account-key.json" `
    -v "${PWD}/credentials:/app/credentials" `
    bgg-dashboard:local

# Note: The dashboard will be available at http://localhost:8501
