# Warehouse API — Reader Latency Slice Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Cut the wall-clock latency of `GET /games/{id}` with **code-only** changes —
no Dataform, no infra, no migration, **no response-contract change**.

**Success criteria:** latency drops from the *sum* of six sequential queries to roughly
the *slowest* one (baseline: **5–10 s** observed live). Bytes scanned stay ~the same;
this is deliberately a **latency** slice, not a cost slice.

**Specs:** `docs/superpowers/specs/2026-07-16-game-detail-api-design.md`,
`docs/superpowers/specs/2026-07-16-warehouse-services-architecture-design.md`

## Why this shape (measured, not assumed)

Dry-run measurements against live `analytics.games_features`:

| Selection | Scanned |
|---|---|
| `SELECT *` (current) | 215.5 MB |
| all except `description` | 83.5 MB |
| `description` alone | 133.1 MB |
| scalars only | 50.8 MB |

Per request today: features 215.5 + player_counts 33.3 + similar 70.1 + provenance 26.7
+ embedding 9.4 + predictions 6.0 ≈ **361 MB**, run **sequentially** → 5–10 s live.

We considered making `description` opt-in (would cut 361 → 229 MB) and **rejected it**:
a ~37% saving is column-dimension only, it churns the API contract, and **clustering
would make you want to reverse it** (with `clusterBy game_id`, `description` becomes
nearly free). Column trimming here is therefore **hygiene only** — the real cost fix is
clustering (follow-up), and the real win available *today* is concurrency.

## Scope

**In:** explicit column lists (replacing `SELECT *`) for hygiene and a small saving;
splitting `get_features` so `/players` stops reading `games_features`; running
`get_game`'s block queries concurrently; tests.

**Out of scope:** `description` opt-in / any response-contract change (rejected above);
`clusterBy game_id` (separate spec + plan — needs a Dataform change and full refresh);
caching; games list/search; front-end work; PR #87 gating (independent).

## Branching & delivery

**Never commit to `main`.** Branch **`feature/warehouse-api-perf`** (off `main`, which
includes merged PR #86). All task commits land there; one PR to `main`, squash-merged.
Merging triggers `deploy-warehouse-api.yml` (paths include `src/warehouse/**`), which
redeploys the service — no other workflow fires.

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `src/warehouse/readers/games.py` | Explicit columns; split `get_features`; concurrent `get_game` |
| Modify | `tests/test_games_reader.py` | Column assertions + concurrency test |
| Modify | `services/warehouse_api/routers/games.py` | `/players` → `get_player_counts` |
| Modify | `tests/test_games_router.py` | Update mocks for new signatures |

---

### Task 1: Explicit columns (hygiene) + split `get_features`

**Files:** `src/warehouse/readers/games.py`, `tests/test_games_reader.py`

- [ ] **Step 1: Write failing tests.** Assert (a) no query contains `SELECT *`;
  (b) `get_feature_row` and `get_player_counts` exist and query the right tables;
  (c) `get_features` still returns the row with `player_counts` attached;
  (d) `description` is **still present** in the selected columns (no contract change).
- [ ] **Step 2: Run, watch fail** — `uv run --extra test python -m pytest tests/test_games_reader.py -v`.
- [ ] **Step 3: Implement** — module-level `FEATURE_COLUMNS` / `PLAYER_COUNT_COLUMNS`;
  replace `SELECT *` everywhere with explicit lists; split `get_features` into
  `get_feature_row` + `get_player_counts` (composing in `get_features`).
- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Sanity-check bytes** — dry-run the new features query; expect ~the same
  as before (~215 MB, since `description` stays). Record it; a big *increase* is a bug.
- [ ] **Step 6: Commit** — `refactor(api): explicit column lists; split feature reads`

---

### Task 2: Concurrent block queries in `get_game`

**Files:** `src/warehouse/readers/games.py`, `tests/test_games_reader.py`

- [ ] **Step 1: Write failing tests.** (a) `get_game` still composes all six blocks and
  still returns `None` when the features row is missing; (b) a **timing** test with a
  fake client sleeping ~0.1 s per query — sequential would be ≥0.6 s, so assert the call
  completes well under that (e.g. < 0.4 s), proving concurrency.
- [ ] **Step 2: Run, watch fail** (the timing test fails while sequential).
- [ ] **Step 3: Implement** — run the six leaf queries via `ThreadPoolExecutor` in
  `get_game`, assembling after. `bigquery.Client` is thread-safe for query submission.
- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Commit** — `perf(api): run game block queries concurrently`

---

### Task 3: Router + PR

**Files:** `services/warehouse_api/routers/games.py`, `tests/test_games_router.py`

- [ ] **Step 1: Update tests** — `/players` calls `get_player_counts` (not
  `get_features`), returning `[]` for an unknown game; other 200/404 behaviour unchanged.
- [ ] **Step 2: Run, watch fail.**
- [ ] **Step 3: Implement** the `/players` delegation.
- [ ] **Step 4: Run, watch pass**, then the whole suite `-m "not integration"`.
- [ ] **Step 5: Commit** — `perf(api): slim /players to player-count reads`
- [ ] **Step 6: Push + open PR** to `main`:
  `gh pr create --base main --title "perf(api): run game block queries concurrently"`.
- [ ] **Step 7: After merge**, the deploy workflow redeploys. Re-measure live:
  `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <url>/games/13`
  and compare wall-clock against the 5–10 s baseline. **Record the number** — it decides
  how urgent clustering is.

---

## Risks / unknowns / rollback

- **No early short-circuit:** concurrent `get_game` issues all six queries even for a
  nonexistent game, so a 404 now costs a full scan instead of one query. 404s should be
  rare; accepted deliberately for the latency win on the common path.
- **Concurrency load:** six simultaneous BigQuery queries per request instead of one at
  a time. Fine at current (zero) traffic; revisit under real load.
- **Timing-test flakiness:** keep the bound generous (sleep 0.1 s, assert < 0.4 s) so CI
  jitter doesn't fail it.
- **Rollback:** pure code, no schema/infra. Revert the PR and redeploy.
- **Honest ceiling:** a request still scans ~361 MB afterwards. This slice buys latency
  only. **Clustering is the actual fix** and remains the next follow-up.
