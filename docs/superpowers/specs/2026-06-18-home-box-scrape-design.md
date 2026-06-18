# Home-box scrape via cron + repository_dispatch

**Status:** Design in progress. Section 1 (architecture) approved. Sections 2–4 are
drafted from decided answers but NOT yet reviewed/approved — see "Open questions".

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
is available to host it.

Constraint: **the repo is public**, so a registered GitHub self-hosted runner is unsafe
(fork PRs could execute arbitrary code on the home machine). The chosen architecture
avoids running any GitHub-controlled code on the box.

## Section 1 — Architecture & data flow (APPROVED)

```
Home box (cron @ 06:00 UTC, residential IP)
  └─ docker run pipeline-image:
       • GOOGLE_APPLICATION_CREDENTIALS → mounted SA key
       • runs: python -m src.pipeline.fetch_thing_ids
       • stealth + bundled Chromium clears Cloudflare (residential IP)
       • merges new IDs into bgg-data-warehouse.raw.thing_ids
  └─ on success: curl GitHub API → repository_dispatch {event_type: thing_ids_fetched}
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

## Decided parameters

| Decision | Choice | Rationale |
|---|---|---|
| Architecture | Cron + `repository_dispatch` | No GitHub code runs on the box → eliminates public-repo runner risk |
| Runtime on box | Docker (reuse `docker/Dockerfile.pipeline`) | Pins Python/Chromium/system-libs → survives host OS drift for years |
| BigQuery auth | Service-account key file on the box | Works unattended; same identity family as `GCP_SA_KEY_BGG_DW` |
| Downstream trigger | Add `repository_dispatch` to `fetch_new_games.yml` alongside existing `workflow_run` | Minimal change; chain runs unchanged; only fires on box success |
| Old GitHub workflow | `fetch_thing_ids.yml`: remove `schedule:`, keep `workflow_dispatch` fallback; revert real-chrome/xvfb experiment to simpler version | Stops daily red noise without deleting work |

## Section 2 — Dispatch handshake (DRAFT, not approved)

- Box, on successful merge, calls:
  `POST /repos/phenrickson/bgg-data-warehouse/dispatches`
  with `{"event_type": "thing_ids_fetched"}`.
- Auth: a GitHub PAT (fine-grained, scoped to this repo, "Contents: read" + the
  permission required to send dispatches) stored on the box (env/secrets file, not in repo).
- `fetch_new_games.yml` adds:
  ```yaml
  on:
    workflow_run: { workflows: ["Fetch Thing IDs"], types: [completed] }  # existing
    repository_dispatch: { types: [thing_ids_fetched] }                   # new
    workflow_dispatch:                                                     # existing
  ```
- The job's `if:` guard must accept the new event (currently gates on
  `workflow_run.conclusion == 'success'` / `workflow_dispatch`); add
  `github.event_name == 'repository_dispatch'`.

## Section 3 — Box setup (DRAFT, not approved)

- Install Docker on the home box (OS TBD — see open questions).
- Build or pull the `docker/Dockerfile.pipeline` image.
- Place the SA key JSON at a fixed path; mount read-only into the container.
- Place config: the container needs `config/bigquery.yaml` (baked into the image already
  via `COPY . .`) — confirm no host config needed.
- Cron entry (~06:00 UTC) runs a small wrapper script:
  1. `docker run` the fetch (creds mounted, network access)
  2. on exit 0, `curl` the `repository_dispatch`
  3. log output somewhere on the box for debugging
- Idempotency: the pipeline already dedups via MERGE, so a re-run is safe.

## Section 4 — Old workflow changes (DRAFT, not approved)

- `fetch_thing_ids.yml`: delete the `schedule:` block; keep `workflow_dispatch`.
- Revert the `BROWSER_CHANNEL=chrome` / xvfb / real-chrome experiment back to the
  bundled-Chromium + stealth version (the `feature/home-box-scrape` branch is off `main`,
  which already has stealth; the real-chrome experiment lives only on
  `fix/cloudflare-real-chrome` and was never merged — confirm no cleanup needed on main).

## Open questions (resolve when resuming)

1. **Box OS/hardware** — what is the home box (Pi / Linux desktop / macOS / always-on
   server)? Determines Docker install details and cron mechanics. Must be on & online at
   06:00 UTC.
2. **PAT scope & storage** — fine-grained vs classic; exact permission needed to send
   `repository_dispatch`; where on the box the token lives.
3. **SA identity** — reuse the existing `GCP_SA_KEY_BGG_DW` service account, or mint a new
   least-privilege SA with only BQ write to the warehouse? (Least-privilege preferred.)
4. **Failure visibility** — if the box's cron fails (box offline, scrape error), how do we
   find out? (No GitHub red X anymore since the scheduled workflow is disabled.) Options:
   box-side alert, a heartbeat, or a GitHub workflow that warns if no dispatch arrived.
5. **Confirm** Sections 2–4 with the user before writing the implementation plan.

## Out of scope

- Fixing the datacenter Cloudflare block itself (abandoned — IP reputation, not fixable
  by browser changes).
- Migrating other workflows off datacenter egress (only `fetch_thing_ids` scrapes the
  Cloudflare-protected sitemap).

## Verification (manual stop-gap, already working)

Until the box is set up, the maintainer runs the scrape locally from a residential IP:
`use-personal` → `uv run python -m src.pipeline.fetch_thing_ids`, then manually dispatches
`Run Fetch New Games`. This is the interim process and validates the residential-IP premise
(local runs on 06-15 and 06-18 succeeded: 71 and 117 new IDs merged respectively).
