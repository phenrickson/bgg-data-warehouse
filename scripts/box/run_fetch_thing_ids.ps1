#Requires -Version 5.1
# Home-box wrapper for the residential-IP thing_ids scrape.
# Runs the native scrape, and on success fires a GitHub repository_dispatch.
# Secrets live in <repo>/credentials/ (gitignored); never logged.
# See docs/superpowers/specs/2026-06-18-home-box-scrape-design.md

$ErrorActionPreference = 'Stop'

$RepoRoot = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$CredDir  = Join-Path $RepoRoot 'credentials'
$SaKey    = Join-Path $CredDir 'sa-key.json'
$PatFile  = Join-Path $CredDir 'github-pat.txt'
$LogDir   = Join-Path $RepoRoot 'logs'
$LogFile  = Join-Path $LogDir ('fetch_thing_ids_{0:yyyyMMdd}.log' -f (Get-Date))
$Repo     = 'phenrickson/bgg-data-warehouse'

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Log($msg) {
  $line = "{0:u} {1}" -f (Get-Date), $msg
  $line | Tee-Object -FilePath $LogFile -Append
}

Set-Location $RepoRoot
Log "=== Home-box fetch_thing_ids run starting ==="

# Optional: stay current with main (comment out for manual-rebuild discipline).
git pull --ff-only 2>&1 | Tee-Object -FilePath $LogFile -Append

if (-not (Test-Path $SaKey))   { Log "FATAL: missing $SaKey";   exit 1 }
if (-not (Test-Path $PatFile)) { Log "FATAL: missing $PatFile"; exit 1 }

# Scoped SA, set for THIS process only (not a global/system env var).
$env:GOOGLE_APPLICATION_CREDENTIALS = $SaKey

Log "Running scrape..."
uv run python -m src.pipeline.fetch_thing_ids 2>&1 | Tee-Object -FilePath $LogFile -Append
$code = $LASTEXITCODE

if ($code -ne 0) {
  Log "Scrape FAILED (exit $code) - NOT dispatching."
  exit $code
}

Log "Scrape OK - firing repository_dispatch..."
$pat = (Get-Content $PatFile -Raw).Trim()
$headers = @{
  Authorization = "Bearer $pat"
  Accept        = 'application/vnd.github+json'
  'User-Agent'  = 'bgg-home-box'
}
$body = @{ event_type = 'thing_ids_fetched' } | ConvertTo-Json
Invoke-RestMethod -Method Post -Uri "https://api.github.com/repos/$Repo/dispatches" `
  -Headers $headers -Body $body
Log "Dispatch sent. Done."
