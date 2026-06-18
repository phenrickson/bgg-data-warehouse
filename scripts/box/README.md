# Home-box scrape setup (Windows)

One-time setup to run the residential-IP `fetch_thing_ids` scrape on this box
via Task Scheduler. Background and rationale:
`docs/superpowers/specs/2026-06-18-home-box-scrape-design.md`.

The box is the only scheduled scraper. On success it fires a GitHub
`repository_dispatch` (`thing_ids_fetched`) that resumes the downstream
`Run Fetch New Games -> Run Dataform` chain. GitHub runs nothing *on* the box.

## 1. Toolchain

- Confirm `uv --version` works.
- In the repo: `uv sync` then `uv run playwright install chromium`.
- `config/bigquery.yaml` is already in the repo working tree — no extra config needed.

## 2. Secrets (`<repo>/credentials/`, gitignored)

The `credentials/` directory is ignored by both `credentials/` and `*.json` rules
in `.gitignore`, so keys placed here can never be committed.

- **SA key** — key for the scoped `bgg-thing-ids-scraper` SA (created by Terraform,
  applied via the Terraform CI workflow on merge). Export it:

  ```
  gcloud iam service-accounts keys create credentials/sa-key.json \
    --iam-account=bgg-thing-ids-scraper@bgg-data-warehouse.iam.gserviceaccount.com
  ```

- **GitHub PAT** — fine-grained token, single line, at `credentials/github-pat.txt`.
  Scope: repository access limited to `bgg-data-warehouse`; permissions
  **Contents: Read and write** + **Metadata: Read** (the create-a-repository-dispatch
  endpoint requires `contents: write`; read-only is insufficient). Set an expiry and a
  rotation reminder.

- **Lock down the directory** to your account only:

  ```
  icacls credentials /inheritance:r /grant:r "$env:USERNAME:(OI)(CI)F"
  ```

## 3. Task Scheduler

Create a daily task that runs the wrapper:

- **Trigger:** daily at **06:00 UTC**. Task Scheduler uses *local* time — convert
  accordingly (~22:00 PT / ~01:00 ET the prior day) and confirm the box is awake then.
- **Action:** start a program:

  ```
  powershell.exe -NoProfile -ExecutionPolicy Bypass -File <repo>\scripts\box\run_fetch_thing_ids.ps1
  ```

- **Settings:** "Run only when user is logged on" (native process, no Docker daemon
  needed); "Wake the computer to run this task"; and disable sleep / fast-startup so the
  box is reachable at the trigger hour.

## 4. Verify

- Run the task manually once (or run the wrapper directly).
- Confirm new IDs merge into `bgg-data-warehouse.raw.thing_ids`.
- Confirm a `repository_dispatch`-triggered **Run Fetch New Games** appears in the GitHub
  Actions tab, and the downstream chain proceeds.
- The **Scrape Heartbeat** workflow (`.github/workflows/scrape_heartbeat.yml`) will warn
  if no dispatch lands within ~26h.

## Notes

- Idempotent: the pipeline dedups via `MERGE`, so a re-run or a missed-then-catch-up run
  is safe.
- `fetch_thing_ids.yml` remains as a manual `workflow_dispatch` fallback (its schedule was
  removed).
