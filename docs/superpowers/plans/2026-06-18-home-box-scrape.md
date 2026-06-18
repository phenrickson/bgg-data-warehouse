# Home-Box Scrape Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Move the Cloudflare-blocked `fetch_thing_ids` scrape off datacenter GitHub runners onto the maintainer's always-on Windows 11 home box (residential IP), driven by Task Scheduler running native `uv run`. On success the box fires a GitHub `repository_dispatch` that resumes the existing downstream chain. No GitHub-controlled code runs on the box (public-repo fork-PR safety).

**Architecture:** Task Scheduler @ 06:00 UTC → PowerShell wrapper → `uv run python -m src.pipeline.fetch_thing_ids` (authenticated with a dedicated least-privilege SA key) → merges into `raw.thing_ids` → on exit 0, `curl POST /repos/.../dispatches {event_type: thing_ids_fetched}`. `fetch_new_games.yml` gains a `repository_dispatch` trigger; `fetch_thing_ids.yml` loses its `schedule:`. A scheduled heartbeat workflow warns if no run lands within the threshold.

**Tech Stack:** Python 3.12 + uv, Playwright/Chromium (stealth), BigQuery, GitHub Actions, Terraform (GCP IAM), Windows Task Scheduler + PowerShell

**Spec:** `docs/superpowers/specs/2026-06-18-home-box-scrape-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `terraform/iam.tf` | Add dedicated least-privilege scraper SA + scoped IAM |
| Modify | `.github/workflows/fetch_new_games.yml:3-18` | Add `repository_dispatch` trigger + extend `if:` guard |
| Modify | `.github/workflows/fetch_thing_ids.yml:3-6` | Remove `schedule:`, keep `workflow_dispatch` |
| Create | `.github/workflows/scrape_heartbeat.yml` | Scheduled warning if no recent box dispatch |
| Create | `scripts/box/run_fetch_thing_ids.ps1` | Box wrapper: scrape → dispatch → log |
| Create | `scripts/box/README.md` | Box setup runbook (Task Scheduler, secrets, ACLs) |
| Manual | GCP / GitHub consoles | Export SA key, create fine-grained PAT, place in `credentials/` |

---

### Task 1: Least-privilege service account (Terraform)

**Files:**
- Modify: `terraform/iam.tf`

- [ ] **Step 1: Add the scoped scraper SA and IAM**

Append to `terraform/iam.tf`:

```hcl
# Dedicated least-privilege SA for the home-box thing_ids scrape.
# Scoped to raw-dataset write only so a leaked box key cannot touch
# GCS / Cloud Run / other datasets, and rotates independently of CI.
resource "google_service_account" "thing_ids_scraper" {
  account_id   = "bgg-thing-ids-scraper"
  display_name = "BGG thing_ids home-box scraper"
  description  = "Least-privilege SA for the residential-IP fetch_thing_ids scrape"
  project      = var.project_id
}

# Run BQ query/load jobs (project-level is the minimum scope for jobUser).
resource "google_project_iam_member" "thing_ids_scraper_job_user" {
  project = var.project_id
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.thing_ids_scraper.email}"
}

# BigQuery Storage Read API — IDFetcher.get_existing_ids() uses .to_dataframe().
resource "google_project_iam_member" "thing_ids_scraper_read_session" {
  project = var.project_id
  role    = "roles/bigquery.readSessionUser"
  member  = "serviceAccount:${google_service_account.thing_ids_scraper.email}"
}

# Read/write/create tables in the `raw` dataset ONLY (temp table + MERGE).
resource "google_bigquery_dataset_iam_member" "thing_ids_scraper_raw_editor" {
  dataset_id = google_bigquery_dataset.bgg_raw.dataset_id
  project    = var.project_id
  role       = "roles/bigquery.dataEditor"
  member     = "serviceAccount:${google_service_account.thing_ids_scraper.email}"
}
```

- [ ] **Step 2: Validate and review the plan**

Run: `cd terraform && terraform init -backend=false && terraform validate`
Expected: `Success! The configuration is valid.`

Then review intent (requires GCP creds/backend): `terraform plan` — confirm it adds exactly one SA + three IAM bindings and changes nothing else.

- [ ] **Step 3: Apply (maintainer, with GCP credentials)**

Run: `cd terraform && terraform apply`
Expected: SA `bgg-thing-ids-scraper@bgg-data-warehouse.iam.gserviceaccount.com` created.

- [ ] **Step 4: Commit**

```bash
git add terraform/iam.tf
git commit -m "feat: add least-privilege SA for home-box thing_ids scrape"
```

---

### Task 2: GitHub workflow triggers

**Files:**
- Modify: `.github/workflows/fetch_new_games.yml`
- Modify: `.github/workflows/fetch_thing_ids.yml`

- [ ] **Step 1: Add `repository_dispatch` to `fetch_new_games.yml`**

Replace the `on:` block (lines 3-8) with:

```yaml
on:
  workflow_run:
    workflows: ["Fetch Thing IDs"]
    types:
      - completed
  repository_dispatch:
    types: [thing_ids_fetched]
  workflow_dispatch:
```

- [ ] **Step 2: Extend the job `if:` guard (line 18)**

Change:

```yaml
    if: ${{ github.event_name == 'workflow_dispatch' || github.event.workflow_run.conclusion == 'success' }}
```

to:

```yaml
    if: ${{ github.event_name == 'workflow_dispatch' || github.event_name == 'repository_dispatch' || github.event.workflow_run.conclusion == 'success' }}
```

- [ ] **Step 3: Remove the `schedule:` from `fetch_thing_ids.yml`**

Replace the `on:` block (lines 3-6) with:

```yaml
on:
  workflow_dispatch:
```

(Keeps the workflow as a manual fallback; the home box is now the scheduled scraper. No code revert needed — `main` already runs the bundled-Chromium + stealth version.)

- [ ] **Step 4: Validate both workflows**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/fetch_new_games.yml')); yaml.safe_load(open('.github/workflows/fetch_thing_ids.yml')); print('YAML valid')"`
Expected: `YAML valid`

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/fetch_new_games.yml .github/workflows/fetch_thing_ids.yml
git commit -m "feat: trigger fetch_new_games on repository_dispatch; drop fetch_thing_ids schedule"
```

---

### Task 3: Heartbeat workflow

> **Approach (decided):** This task uses a **GitHub-API freshness check** — it asks whether a `repository_dispatch`-triggered "Run Fetch New Games" run was *created* within the threshold. Chosen because it needs **no pipeline change** and keeps the scoped SA minimal. It is a clean "did the box dispatch?" signal: the run row is created the moment the dispatch arrives, independent of whether Fetch New Games later succeeds, so downstream failures do **not** false-alarm here (they surface on their own). It catches the failure set we care about — box offline, Task Scheduler didn't fire, scrape exited non-zero (wrapper skips dispatch), or dispatch call failed — all of which produce "no recent dispatch-triggered run". Alternative considered and rejected: a `raw.scrape_heartbeat` marker table (unambiguous, but adds pipeline code + a Terraform table for ~no extra coverage, since the wrapper only dispatches after the scrape's merge succeeds).

**Files:**
- Create: `.github/workflows/scrape_heartbeat.yml`

- [ ] **Step 1: Create the heartbeat workflow**

Create `.github/workflows/scrape_heartbeat.yml`:

```yaml
name: Scrape Heartbeat

# The home box is the only scheduled scraper; with fetch_thing_ids' schedule
# removed there is no daily red-X on failure. This warns if the box hasn't
# successfully dispatched within the threshold (box offline, scrape error, etc).
on:
  schedule:
    - cron: '0 12 * * *'  # 6h after the box's 06:00 UTC slot
  workflow_dispatch:

permissions:
  actions: read

jobs:
  check-heartbeat:
    name: Warn if no recent box dispatch
    runs-on: ubuntu-latest
    steps:
      - name: Check for a recent dispatch-triggered Fetch New Games run
        env:
          GH_TOKEN: ${{ github.token }}
          THRESHOLD_HOURS: '26'  # normal age ~6h; a single missed daily run = ~30h, trips cleanly
        run: |
          # Most recent "Run Fetch New Games" run triggered by repository_dispatch.
          LAST=$(gh api \
            "repos/${{ github.repository }}/actions/workflows/fetch_new_games.yml/runs?event=repository_dispatch&per_page=1" \
            --jq '.workflow_runs[0].created_at // empty')

          if [ -z "$LAST" ]; then
            echo "::error::No repository_dispatch-triggered Fetch New Games run found at all."
            exit 1
          fi

          LAST_EPOCH=$(date -d "$LAST" +%s)
          NOW_EPOCH=$(date +%s)
          AGE_HOURS=$(( (NOW_EPOCH - LAST_EPOCH) / 3600 ))
          echo "Last box dispatch: $LAST (${AGE_HOURS}h ago)"

          if [ "$AGE_HOURS" -gt "$THRESHOLD_HOURS" ]; then
            echo "::error::No box dispatch in ${AGE_HOURS}h (> ${THRESHOLD_HOURS}h). Check the home box."
            exit 1
          fi
          echo "Heartbeat OK."
```

- [ ] **Step 2: Validate YAML**

Run: `python -c "import yaml; yaml.safe_load(open('.github/workflows/scrape_heartbeat.yml')); print('YAML valid')"`
Expected: `YAML valid`

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/scrape_heartbeat.yml
git commit -m "feat: add scrape heartbeat workflow for home-box monitoring"
```

---

### Task 4: Box wrapper script

**Files:**
- Create: `scripts/box/run_fetch_thing_ids.ps1`

- [ ] **Step 1: Write the wrapper**

Create `scripts/box/run_fetch_thing_ids.ps1`:

```powershell
#Requires -Version 5.1
# Home-box wrapper for the residential-IP thing_ids scrape.
# Runs the native scrape, and on success fires a GitHub repository_dispatch.
# Secrets live in <repo>/credentials/ (gitignored); never logged.

$ErrorActionPreference = 'Stop'

$RepoRoot   = Split-Path -Parent (Split-Path -Parent $PSScriptRoot)
$CredDir    = Join-Path $RepoRoot 'credentials'
$SaKey      = Join-Path $CredDir 'sa-key.json'
$PatFile    = Join-Path $CredDir 'github-pat.txt'
$LogDir     = Join-Path $RepoRoot 'logs'
$LogFile    = Join-Path $LogDir ('fetch_thing_ids_{0:yyyyMMdd}.log' -f (Get-Date))
$Repo       = 'phenrickson/bgg-data-warehouse'

New-Item -ItemType Directory -Force -Path $LogDir | Out-Null

function Log($msg) {
  $line = "{0:u} {1}" -f (Get-Date), $msg
  $line | Tee-Object -FilePath $LogFile -Append
}

Set-Location $RepoRoot
Log "=== Home-box fetch_thing_ids run starting ==="

# Optional: stay current with main (comment out for manual-rebuild discipline).
git pull --ff-only 2>&1 | Tee-Object -FilePath $LogFile -Append

if (-not (Test-Path $SaKey))  { Log "FATAL: missing $SaKey";  exit 1 }
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
```

- [ ] **Step 2: Lint the script parses**

Run: `powershell -NoProfile -Command "[System.Management.Automation.PSParser]::Tokenize((Get-Content -Raw scripts/box/run_fetch_thing_ids.ps1), [ref]$null) > $null; 'parse OK'"`
Expected: `parse OK`

- [ ] **Step 3: Commit**

```bash
git add scripts/box/run_fetch_thing_ids.ps1
git commit -m "feat: add home-box wrapper script for fetch_thing_ids + dispatch"
```

---

### Task 5: Box setup runbook + manual provisioning

**Files:**
- Create: `scripts/box/README.md`

- [ ] **Step 1: Write the runbook**

Create `scripts/box/README.md` documenting the one-time manual setup:

1. **Toolchain:** confirm `uv --version` works; run `uv sync` and `uv run playwright install chromium` in the repo.
2. **Secrets** (`<repo>/credentials/`, gitignored by both `credentials/` and `*.json`):
   - `sa-key.json` — key for `bgg-thing-ids-scraper` SA. Export:
     `gcloud iam service-accounts keys create credentials/sa-key.json --iam-account=bgg-thing-ids-scraper@bgg-data-warehouse.iam.gserviceaccount.com`
   - `github-pat.txt` — fine-grained PAT (Task 6), single line.
   - Lock the dir to your account: `icacls credentials /inheritance:r /grant:r "$env:USERNAME:(OI)(CI)F"`
3. **Task Scheduler** task:
   - Trigger: daily at 06:00 **UTC** (Task Scheduler uses local time — convert; confirm the box is awake then, ~22:00 PT / 01:00 ET prior day).
   - Action: `powershell.exe -NoProfile -ExecutionPolicy Bypass -File <repo>\scripts\box\run_fetch_thing_ids.ps1`
   - Settings: "Run only when user is logged on", "Wake the computer to run this task"; disable sleep/fast-startup so the box is reachable at the trigger hour.
4. **Verify:** run the task manually once; confirm new IDs merge and "Run Fetch New Games" starts from the dispatch.

- [ ] **Step 2: Commit**

```bash
git add scripts/box/README.md
git commit -m "docs: add home-box setup runbook"
```

---

### Task 6: GitHub PAT (manual)

> No code — operator action, recorded here for completeness.

- [ ] **Step 1: Create a fine-grained PAT**
  - Resource owner: `phenrickson`; repository access: **only** `bgg-data-warehouse`.
  - Permissions: **Contents: Read and write** + **Metadata: Read** (required by the create-a-repository-dispatch-event endpoint; read-only is insufficient).
  - Set a sane expiry and a calendar reminder to rotate.

- [ ] **Step 2: Place it on the box**
  - Save the token as a single line in `<repo>/credentials/github-pat.txt`.

- [ ] **Step 3: Smoke-test the dispatch**
  - Run `scripts/box/run_fetch_thing_ids.ps1` manually; confirm a `repository_dispatch`-triggered "Run Fetch New Games" appears in the Actions tab.

---

## Sequencing & dependencies

1. **Task 1** (SA) before Task 5 (box needs the key).
2. **Task 2** (fetch_new_games trigger) before Task 6 smoke-test (dispatch is a no-op without the trigger).
3. **Task 6** (PAT) before Task 5 Step 4 verify.
4. **Task 3** (heartbeat) can land anytime, but only becomes meaningful once the box is live and `fetch_thing_ids` schedule is removed (Task 2 Step 3).
5. Final cutover: confirm one full end-to-end box run (scrape → merge → dispatch → Fetch New Games → Dataform) before relying on the heartbeat.

## Open item to confirm before/at implementation

- **Heartbeat signal** (Task 3 assumption): GitHub-API dispatch-freshness check vs. a `raw.scrape_heartbeat` marker table. Default is the API check (no pipeline change).
