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
  Write-Host $line
  # Append as UTF-8 (no BOM) so PS log lines match the cmd/Python output below
  # (PS 5.1 Tee-Object/Out-File default to UTF-16, which mojibakes the file).
  [System.IO.File]::AppendAllText($LogFile, $line + [Environment]::NewLine, (New-Object System.Text.UTF8Encoding $false))
}

Set-Location $RepoRoot
Log "=== Home-box fetch_thing_ids run starting ==="

# Stay current with main. Run native git via cmd so its stderr (progress
# output) is redirected to the log without PowerShell's Stop preference
# treating it as a terminating error.
Log "Updating repo (git pull --ff-only)..."
cmd /c "chcp 65001 >nul & git pull --ff-only >> `"$LogFile`" 2>&1"

if (-not (Test-Path $SaKey))   { Log "FATAL: missing $SaKey";   exit 1 }
if (-not (Test-Path $PatFile)) { Log "FATAL: missing $PatFile"; exit 1 }

# Scoped SA, set for THIS process only (not a global/system env var).
$env:GOOGLE_APPLICATION_CREDENTIALS = $SaKey

# Force UTF-8 from Python so its log output matches the log file's encoding.
$env:PYTHONUTF8 = '1'

Log "Running scrape..."
# Run via cmd so the native command's combined output goes cleanly to the log
# and its exit code is read from $LASTEXITCODE. PowerShell 5.1 mangles native
# `2>&1` (wraps stderr as ErrorRecords and, under ErrorActionPreference Stop,
# aborts on the first stderr line - which Python logging emits immediately).
cmd /c "chcp 65001 >nul & uv run python -m src.pipeline.fetch_thing_ids >> `"$LogFile`" 2>&1"
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
