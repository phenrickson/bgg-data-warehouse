# Home-box scrape via Task Scheduler + repository_dispatch

**Status:** Design APPROVED. All sections reviewed; open questions resolved (see
"Resolved decisions"). Ready for an implementation plan.

**Branch:** `feature/home-box-scrape`

## Problem

`fetch_thing_ids` scrapes `boardgamegeek.com/sitemapindex` (Cloudflare-protected) and
merges new game IDs into `bgg-data-warehouse.raw.thing_ids`, which feeds the downstream
chain `Run Fetch New Games → Run Dataform`.

Around 2026-06-09 Cloudflare began serving an unsolvable `managed` challenge to
**datacenter egress IPs**. Confirmed root cause: challenge strictness is gated primarily
by IP reputation. The browser-fingerprint ladder was exhausted and did NOT fix it from
datacenter IPs:

- naive headless Chromium → blocked
- `playwright-stealth` + Chromium → cleared for ~2 days (06-11/06-12), then re-blocked 06-13+
- real Google Chrome headed under xvfb → still blocked (proves IP, not fingerprint, is the wall)

From a **residential IP** the scrape works trivially (even a no-browser `curl_cffi` GET
returns 200). So the fix is residential egress, not more browser cleverness. A home box
(the maintainer's always-on Windows 11 machine) is available to host it.

Constraint: **the repo is public**, so a registered GitHub self-hosted runner is unsafe
(fork PRs could execute arbitrary code on the home machine). The chosen architecture
avoids running any GitHub-controlled code on the box.

## Section 1 — Architecture & data flow (APPROVED)

```
Home box (Windows 11, Task Scheduler @ 06:00 UTC, residential IP)
  └─ wrapper script:
       • GOOGLE_APPLICATION_CREDENTIALS → credentials/sa-key.json (scoped SA)
       • runs: uv run python -m src.pipeline.fetch_thing_ids
       • stealth + bundled Chromium clears Cloudflare (residential IP)
       • merges new IDs into bgg-data-warehouse.raw.thing_ids
  └─ on exit 0: curl GitHub API → repository_dispatch {event_type: thing_ids_fetched}
        │
        ▼
GitHub Actions: Run Fetch New Games  (now also triggers on repository_dispatch)
        │ workflow_run (unchanged)
        ▼
                 Run Dataform
```

The box is the only scheduled scraper. GitHub executes nothing *on* the box — it only
*receives* an API call — so the public-repo fork-PR attack surface never touches the home
machine. The downstream Actions chain is unchanged except for one added trigger.

## Resolved decisions

| Decision | Choice | Rationale |
|---|---|---|
| Architecture | Task Scheduler + `repository_dispatch` | No GitHub code runs on the box → eliminates public-repo runner risk |
| Runtime on box | **Native `uv run`** (not Docker) | Box is a Windows machine the maintainer actively maintains; Docker Desktop needs an interactive session (perpetual login + daemon-must-be-up). Native is simpler, has no daemon dependency, and already proven by the interim manual runs. |
| BigQuery auth | **Dedicated least-privilege SA key** in `credentials/` | Bounds blast radius if the home-box key leaks (BQ-write to `raw` only — no GCS/Cloud Run/other datasets); rotates independently of CI's `GCP_SA_KEY_BGG_DW`. |
| Box | Maintainer's always-on Windows 11 desktop | Must be powered on, logged in, and awake at 06:00 UTC (~22:00 PT / 01:00 ET prior day). |
| Downstream trigger | Add `repository_dispatch` to `fetch_new_games.yml` alongside existing `workflow_run` | Minimal change; chain runs unchanged; only fires on box success |
| Old GitHub workflow | `fetch_thing_ids.yml`: remove `schedule:`, keep `workflow_dispatch` fallback | Stops daily red noise without deleting work. (The real-chrome/xvfb experiment lives only on the unmerged `fix/cloudflare-real-chrome` branch — `main` already has the simple stealth version, so no revert needed here.) |
| Failure visibility | GitHub **heartbeat workflow** | Scheduled workflow warns if `raw.thing_ids` hasn't been written recently — replaces the red-X signal lost when the scheduled scrape is disabled. |

## Section 2 — Dispatch handshake (APPROVED)

- Box, on successful merge, calls:
  `POST /repos/phenrickson/bgg-data-warehouse/dispatches`
  with `{"event_type": "thing_ids_fetched"}`.
- Auth: a **fine-grained GitHub PAT** scoped to this repo with **Contents: write** +
  **Metadata: read** (the create-a-repository-dispatch-event endpoint requires
  `contents: write` — read-only is NOT sufficient). Stored on the box at
  `credentials/github-pat.txt` (gitignored; see Section 3), read by the host wrapper —
  never injected anywhere GitHub controls.
- `fetch_new_games.yml` adds:
  ```yaml
  on:
    workflow_run: { workflows: ["Fetch Thing IDs"], types: [completed] }  # existing
    repository_dispatch: { types: [thing_ids_fetched] }                   # new
    workflow_dispatch:                                                     # existing
  ```
- The job's `if:` guard currently gates on
  `github.event_name == 'workflow_dispatch' || github.event.workflow_run.conclusion == 'success'`;
  add `|| github.event_name == 'repository_dispatch'`.

## Section 3 — Box setup (APPROVED, Windows native)

The box is a Windows 11 machine; runtime is native `uv run` (no Docker), driven by Task
Scheduler.

**Secrets** — a gitignored `credentials/` directory at the repo root (already covered by
`.gitignore`: both `credentials/` and `*.json`). Lock it down with NTFS ACLs to the
maintainer's account only.

- `credentials/sa-key.json` — the scoped SA key (Section "Least-privilege SA" below).
  Used by the scrape via `GOOGLE_APPLICATION_CREDENTIALS`.
- `credentials/github-pat.txt` — the fine-grained PAT (one line). Read by the wrapper for
  the dispatch call only.

**Toolchain** — confirm `uv` is installed and `uv run playwright install chromium` has
provisioned the bundled Chromium on the box. (`config/bigquery.yaml` is already in the
repo working tree, so no extra config placement is needed.)

**Wrapper script** (PowerShell), run by Task Scheduler ~06:00 UTC:

1. `cd` to the repo; optionally `git pull` to stay current with `main`.
2. Set `GOOGLE_APPLICATION_CREDENTIALS` to `credentials/sa-key.json` for this process only
   (not a global/system env var).
3. `uv run python -m src.pipeline.fetch_thing_ids`.
4. On exit 0, read the PAT and `curl` the `repository_dispatch`.
5. Append stdout/stderr to a log file on the box for debugging.

**Task Scheduler config** — trigger at 06:00 UTC; "Run only when user is logged on" (native
process, no Docker daemon needed); "Wake the computer to run this task"; and Windows
sleep/fast-startup settings adjusted so the box is reliably awake. The box must stay
powered on and logged in at that hour.

**Idempotency** — the pipeline dedups via `MERGE`, so a re-run (or a missed-then-catch-up
run) is safe.

## Least-privilege SA (APPROVED, Terraform)

New Terraform-managed service account, scoped to exactly what the scrape does (read
existing IDs, load a temp table, `MERGE` into `raw.thing_ids`, delete the temp table):

- `google_service_account` (e.g. `bgg-thing-ids-scraper`).
- `roles/bigquery.jobUser` at project level — run query/load jobs.
- `roles/bigquery.readSessionUser` at project level — the code's `.to_dataframe()` uses the
  BigQuery Storage Read API.
- `roles/bigquery.dataEditor` scoped to the **`raw` dataset only** via
  `google_bigquery_dataset_iam_member` referencing `google_bigquery_dataset.bgg_raw` — NOT
  a project-level grant.
- A key for the SA, exported and placed at `credentials/sa-key.json` on the box.

A leaked copy of this key can touch only the `raw` dataset and BQ job execution — no GCS,
no Cloud Run, no other datasets — and is rotatable without affecting CI.

## Section 4 — Old workflow changes (APPROVED)

- `fetch_thing_ids.yml`: delete the `schedule:` block; keep `workflow_dispatch` as a manual
  fallback.
- No code revert needed on `main`: the real-chrome/xvfb experiment was never merged (it
  lives only on `fix/cloudflare-real-chrome`); `main` already runs the bundled-Chromium +
  stealth version.

## Section 5 — Failure visibility (APPROVED, heartbeat)

With the scheduled scrape disabled, GitHub no longer shows a daily red X on failure. A
scheduled **heartbeat workflow** replaces that signal:

- Runs on a `schedule:` (e.g. a few hours after the box's 06:00 UTC slot).
- Queries the freshness of `raw.thing_ids` (e.g. `MAX(load_timestamp)`); if no write has
  landed within a staleness threshold (e.g. ~36h, allowing for "no new IDs" days — TBD in
  implementation), the workflow fails so it surfaces as a notification.
- Uses the existing `GCP_SA_KEY_BGG_DW` (read-only query in CI is fine; the scoped SA is
  for the box only).
- Threshold tuning note: "no new IDs found" is a *successful* run that may still write a
  `load_timestamp`-bearing row only when there ARE new IDs — confirm during implementation
  whether to heartbeat on table writes or on a dedicated run-marker, so a legitimately
  empty day doesn't false-alarm.

## Out of scope

- Fixing the datacenter Cloudflare block itself (abandoned — IP reputation, not fixable
  by browser changes).
- Migrating other workflows off datacenter egress (only `fetch_thing_ids` scrapes the
  Cloudflare-protected sitemap).

## Verification (manual stop-gap, already working)

Until the box automation is set up, the maintainer runs the scrape locally from a
residential IP: `use-personal` → `uv run python -m src.pipeline.fetch_thing_ids`, then
manually dispatches `Run Fetch New Games`. This is the interim process and validates the
residential-IP premise (local runs on 06-15 and 06-18 succeeded: 71 and 117 new IDs merged
respectively).
