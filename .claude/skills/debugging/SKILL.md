---
name: debugging
description: Systematically find the root cause of a failure before fixing it. Use when something is broken — a failing GitHub Actions workflow, a pipeline error, an unexpected/empty BigQuery result, a failing test, or the home-box scrape not running. Emphasizes evidence and reproduction over guessing.
---

# Debugging

Find the *root cause* with evidence, then fix it — don't pattern-match to a
plausible-sounding fix and move on. Distinguish the symptom from the cause.

## Process (scientific method)

1. **Reproduce and observe.** Get the exact error text, the failing command, and
   the conditions. Read the actual logs — never infer the error from the
   description alone.

2. **Establish a timeline.** When did it last work vs. first fail? Correlate with
   what changed (a merge, a dependency bump, an upstream/BGG change, a schedule).
   `git log`, the run history, and dated logs are your friends here.

3. **Form ranked hypotheses.** List the plausible causes most-likely first, and for
   each note *what evidence would confirm or refute it*. Prefer the hypothesis that
   explains **all** the observations (e.g. a multi-hour gap between start and error
   is not a 30s timeout — it's a freeze/suspend).

4. **Isolate.** Reduce to the smallest reproduction. Binary-search the surface:
   which file, which step, which record, which commit (`git bisect` when useful).

5. **Confirm the root cause** before changing anything. Can you explain the full
   causal chain? Can you reproduce and then *un*-reproduce it on demand?

6. **Fix, then verify.** Re-run and confirm the failure is actually gone — check
   real output, not just "no exception." State what you observed.

7. **Prevent regression.** Add a test or guard if the bug could recur, and check
   the fix didn't break an adjacent path.

## This repo — where to look

- **Pipeline logs:** `logs/*.log` (dated). The home-box scrape writes
  `logs/fetch_thing_ids_YYYYMMDD.log`. Note these stamps are **local time** despite a
  trailing `Z`.
- **CI:** `gh run list` and `gh run view <id> --log-failed` for GitHub Actions.
  The daily chain is `Fetch Thing IDs → Fetch New Games → Dataform`, wired via
  `repository_dispatch`; a missing dispatch shows up in the **Scrape Heartbeat** run.
- **Data:** `bq query --nouse_legacy_sql '<SQL>'` to inspect `raw.*` and warehouse
  tables. Check row counts / freshness before assuming code is wrong.
- **Run a pipeline locally:** `uv run python -m src.pipeline.<name>`.
- **Tests:** `uv run --extra test python -m pytest tests/ -q`.

## Repo-specific gotchas

- **Scrape failures are often environmental, not code:** Cloudflare challenges,
  the home box sleeping/offline (Modern Standby), or a residential-IP block — confirm
  the scrape works on demand before touching the scraper.
- **Partial sitemap fetches misclassify types** — the pipeline intentionally fails
  the whole run rather than upload partial results.
- On Windows PowerShell, native-command stderr is *not* an error; don't treat a
  non-empty stderr as failure — check the exit code.
