# ID Probe Lookback — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** The API probe finds IDs *below* the known frontier. Daily runs look back
500 IDs (new/upcoming games); Sunday runs look back 5,000 (catch-up). Known IDs are
never re-requested; a failed request never counts as a miss.

**Architecture:** `ProbeIDFetcher.probe()` gains a `frontier_id` and a `skip` set.
Below the frontier every unknown ID is probed and misses don't stop the walk; at
and above it the existing 500-consecutive-miss rule applies. `IDFetcher` computes
the window and the skip set from `raw.thing_ids`. The workflow owns the two
lookback values.

**Tech Stack:** Python 3.12, BigQuery, GitHub Actions.

**Spec:** `docs/superpowers/specs/2026-09-18-id-probe-lookback-design.md`

**Scope:** probe range/skip semantics · `IDFetcher` window · `--lookback` CLI ·
workflow crons + summary · tests · docs.

**Out of scope:** known-absent tracking / decay; heartbeat rewrite; removing the
sitemap path; any change to `raw.thing_ids` schema, Dataform, or the downstream
chain.

---

## Branching & delivery

**Never commit to `main`.** Feature branch, squash-merged via PR, conventional
commit with scope (`feat(probe): …`).

- Branch off latest `main` before Task 1: `git switch main && git pull && git switch -c feat/probe-lookback`.
- One PR for the whole increment. It's one behaviour change; `--lookback 0` is
  today's code path, so the PR is inert until the workflow passes a value.
- Merge with `[skip ci]` is **not** needed — `fetch_thing_ids.yml` has no push
  trigger.

---

## File Structure

| File | Change |
|---|---|
| `src/modules/id_probe_fetcher.py` | `probe()` takes `frontier_id`, `skip`; batch builder skips known IDs; miss counter gated on `>= frontier_id` |
| `src/modules/id_fetcher.py` | `_fetch_via_probe(lookback)`, `_known_ids_from(floor)`, `run(source, lookback=0)` |
| `src/pipeline/fetch_thing_ids.py` | `--lookback` int arg, default 0 |
| `.github/workflows/fetch_thing_ids.yml` | two crons, `lookback` input, Sunday selection, summary rows |
| `tests/test_id_probe_fetcher.py` | new cases (Task 1) |
| `tests/test_id_fetcher.py` | new cases (Task 2) |
| `README.md`, `docs/architecture.md` | one paragraph each |

---

### Task 1: Probe range + skip semantics

**Files:** `src/modules/id_probe_fetcher.py`, `tests/test_id_probe_fetcher.py`

- [ ] **Step 1: Write failing tests** in `tests/test_id_probe_fetcher.py` (reuse the
  existing fake-client pattern that records requested ID lists):
  - `test_probe_skips_known_ids` — `skip={101,102}`, start 100: first batch is
    `[100,103,104,…]` (20 IDs, none in skip).
  - `test_probe_misses_below_frontier_do_not_stop` — start 1000, frontier 2000, all
    empty responses, `max_ids=1500`: walk reaches ≥ 2000 before stopping (misses only
    count from the frontier).
  - `test_probe_stops_after_misses_above_frontier` — frontier == start; 500 empty
    IDs ends the walk (existing behaviour, now explicit).
  - `test_probe_lookback_zero_matches_current_behaviour` — `start=frontier`, empty
    skip: requested sequence equals what today's code requests (regression guard;
    capture from the current implementation before changing it).
- [ ] **Step 2: Run, watch fail** — `uv run --extra test python -m pytest tests/test_id_probe_fetcher.py -v`.
- [ ] **Step 3: Implement.** Signature:
  `probe(self, start_id: int, *, frontier_id: int | None = None, skip: frozenset[int] = frozenset(), max_ids: int | None = None)`.
  `frontier_id` defaults to `start_id`. Replace `batch = list(range(current, current+batch_size))`
  with a generator that yields the next `batch_size` IDs `>= current` not in `skip`,
  advancing `current` past the last one. Miss accounting:
  `if not items and batch[-1] >= frontier_id: consecutive_misses += len(batch)`.
  Keep the raise-on-`None` response. Log `examined`, `found`, `start_id`,
  `frontier_id`, `len(skip)` in the final info line.
- [ ] **Step 4: Run, watch pass** — whole file, including the eight existing tests.
- [ ] **Step 5: Commit** — `feat(probe): probe a range below the frontier, skipping known IDs`

---

### Task 2: IDFetcher window

**Files:** `src/modules/id_fetcher.py`, `tests/test_id_fetcher.py`

- [ ] **Step 1: Write failing tests** (mock the BigQuery client and `ProbeIDFetcher`):
  - `test_fetch_via_probe_window` — `max_id=479570`, `lookback=500`, known
    `{479100, 479570}`: `probe` is called with `start_id=479070`,
    `frontier_id=479570`, `skip` containing both known IDs.
  - `test_fetch_via_probe_lookback_zero` — called with `start_id=max_id+1`,
    `frontier_id=max_id+1`, empty skip (today's call, unchanged).
  - `test_run_threads_lookback` — `run(source=SOURCE_PROBE, lookback=500)` passes
    500 through.
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement.**
  - `_known_ids_from(self, floor: int) -> frozenset[int]`:
    `SELECT game_id FROM raw.thing_ids WHERE game_id >= @floor` (parameterised).
  - `_fetch_via_probe(self, lookback: int = 0)`:
    `max_id = self.get_max_game_id()`; if `lookback == 0` keep the exact current
    call; else `start = max_id - lookback`, `skip = self._known_ids_from(start)`,
    `ProbeIDFetcher().probe(start, frontier_id=max_id, skip=skip)`.
  - `run(self, source=SOURCE_PROBE, lookback: int = 0)`; forward to `_fetch_via_probe`.
- [ ] **Step 4: Run, watch pass** — both test files.
- [ ] **Step 5: Commit** — `feat(probe): compute lookback window and known-ID skip set`

---

### Task 3: CLI flag

**Files:** `src/pipeline/fetch_thing_ids.py`

- [ ] **Step 1: Add** `--lookback` (`type=int`, `default=0`, help: "IDs below the
  known max to re-probe; 0 = frontier only"). Pass to `id_fetcher.run(source=…, lookback=…)`.
  Log it in the "Starting fetch_thing_ids" line.
- [ ] **Step 2: Verify** — `uv run python -m src.pipeline.fetch_thing_ids --help` shows it.
- [ ] **Step 3: Smoke against prod** (needs creds; ~10 polite requests, idempotent MERGE):
  `uv run python -m src.pipeline.fetch_thing_ids --source bgg_api_probe --lookback 200`.
  Confirm the log shows `start_id = max-200`, a nonzero skip count, and the run
  completes. **Ask before running** — it writes to `raw.thing_ids`.
- [ ] **Step 4: Commit** — `feat(probe): --lookback flag on fetch_thing_ids`

---

### Task 4: Workflow

**Files:** `.github/workflows/fetch_thing_ids.yml`

- [ ] **Step 1: Triggers.** Replace the single cron with:
  ```yaml
  schedule:
    - cron: '0 6 * * 1-6'   # daily: frontier + 500 back
    - cron: '0 6 * * 0'     # Sunday: frontier + 5,000 back
  ```
  Add input `lookback` (`type: number`, `default: 500`, description
  "IDs below max to re-probe").
- [ ] **Step 2: Resolve the value** in a step before the fetch:
  ```bash
  if [ -n "${{ inputs.lookback }}" ]; then LB=${{ inputs.lookback }}
  elif [ "$(date -u +%u)" = "7" ]; then LB=5000
  else LB=500; fi
  echo "lookback=$LB" >> $GITHUB_OUTPUT
  ```
  Pass `--lookback "${{ steps.lookback.outputs.lookback }}"` to the fetch step.
- [ ] **Step 3: Summary.** Add `| Lookback | ${LB} |` above `New IDs added`.
  (`New IDs added` is already the "found" count.)
- [ ] **Step 4: Verify** — `actionlint` if available, else a careful read; the
  `workflow_run` chain into Fetch New Games is untouched.
- [ ] **Step 5: Commit** — `feat(ci): daily 500 / Sunday 5,000 lookback for Fetch Thing IDs`

---

### Task 5: Docs

**Files:** `README.md`, `docs/architecture.md`

- [ ] **Step 1:** In the `fetch_thing_ids` rows/section, state: daily = frontier +
  500 back, Sundays = 5,000 back, known IDs skipped; link the spec.
- [ ] **Step 2: Commit** — `docs(readme): describe probe lookback windows`

---

### Task 6: PR + first-run verification

- [ ] **Step 1:** `gh pr create` — title `feat(probe): look below the ID frontier (daily 500, Sunday 5,000)`; body links the spec and states rollback (`--lookback 0` ≡ today).
- [ ] **Step 2 (after merge, read-only):** next 06:00 UTC run — step summary shows
  `Lookback 500`, `New IDs added` > 0 on a typical day; log shows a nonzero skip
  count. First Sunday: completes without 429 failures.
- [ ] **Step 3 (a week later, read-only):**
  `SELECT DATE(load_timestamp), COUNT(*) FROM raw.thing_ids WHERE load_timestamp >= <merge date> GROUP BY 1`
  — daily counts back in the sitemap-era range (~20–70).

---

## Risks / rollback

- **429s on Sunday.** Calibrated against `refresh_old_games` (~50 requests/day, same
  cadence, no failures). The client raises after 3 retries, so a rate-limited run
  fails visibly. Tunable: drop to `2000` in the workflow.
- **Rollback:** set both workflow values to `0` — identical to today's behaviour.
- No schema, Dataform, or chain changes.
