# Warehouse Read API — Skeleton + Games Router Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up `services/warehouse_api/` (modular-monolith FastAPI read API) with a
`/health` endpoint and a `games` router serving `GET /games/{game_id}` and its
per-block sub-resources, backed by a pure, tested reader layer in
`src/warehouse/readers/`. Deploy it authenticated to Cloud Run.

**Architecture:** A reader layer (`src/warehouse/readers/games.py`, pure functions over
BigQuery, dependency-injected client for testability) holds the query logic; a thin
FastAPI app (`services/warehouse_api/`) mounts one router per resource and shapes
responses. This PR delivers the skeleton + the `games` router only; other resource
routers and the front-end repoint are separate slices.

**Tech Stack:** Python 3.12, FastAPI + uvicorn, Google BigQuery, Cloud Run, Cloud
Build, GitHub Actions.

**Specs:**
- `docs/superpowers/specs/2026-07-16-warehouse-services-architecture-design.md`
- `docs/superpowers/specs/2026-07-16-game-detail-api-design.md`
- `docs/superpowers/specs/2026-07-16-service-auth-pattern-design.md` (gating)

**Scope (this plan):** config datasets · BigQuery client helper · `games` reader
(get-by-id + blocks) · FastAPI skeleton + `/health` + auth · `games` router · Docker +
local run · Cloud Build + deploy workflow.

**Out of scope (later slices):** `GET /games` list/search/new/summary; publishers /
designers / other resource routers; `clusterBy game_id` on serving tables; the
`bgg-dash-viewer` repoint (separate PR).

---

## Branching & delivery

**Never commit to `main`.** All work lands via a PR — repo convention is feature
branches squash-merged to `main` with the PR number in the subject (e.g. `… (#85)`),
conventional-commit style (`feat(api): …`).

- **Create the branch before Task 1** (off latest `main`):
  `git switch main && git pull && git switch -c feature/warehouse-api-games`.
- **Every task's commits land on this branch.** They accumulate as review history and
  give per-step rollback; they collapse on squash-merge.
- **One PR for this whole increment** (skeleton + games router + deploy). It's one
  coherent feature, and the deployed service is inert until the front-end repoint, so
  there's no risk in shipping it together. Split into a skeleton PR + a deploy PR only
  if review gets unwieldy.
- **Cross-repo:** the `bgg-dash-viewer` repoint (Follow-up 1) is a **separate branch and
  PR in that repo** — it must not ride on this branch.

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `config/bigquery.yaml` | Add `predictions`, `analytics` to `datasets:` |
| Modify | `src/config.py` | Surface new dataset keys (unchanged signature) |
| Create | `src/warehouse/__init__.py` | Package marker |
| Create | `src/warehouse/bq.py` | `get_client()` + `dataset(name)` resolver helper |
| Create | `src/warehouse/readers/__init__.py` | Package marker |
| Create | `src/warehouse/readers/games.py` | Per-block reader fns + `get_game()` aggregator |
| Create | `tests/test_config_datasets.py` | Assert new datasets present in config |
| Create | `tests/test_games_reader.py` | Reader unit tests (mocked `bigquery.Client`) |
| Create | `services/warehouse_api/__init__.py` | Package marker |
| Create | `services/warehouse_api/main.py` | FastAPI app, `/health`, mounts routers |
| Create | `services/warehouse_api/auth.py` | `GCPAuthenticator` (copied pattern) |
| Create | `services/warehouse_api/routers/__init__.py` | Package marker |
| Create | `services/warehouse_api/routers/games.py` | `games` `APIRouter` |
| Create | `tests/test_games_router.py` | Router tests via FastAPI `TestClient` (reader mocked) |
| Create | `services/warehouse_api/Dockerfile` | uv-based image (model: `docker/Dockerfile.pipeline`) |
| Create | `services/warehouse_api/README.md` | Service overview + run/deploy notes |
| Create | `.dockerignore` | Keep secrets/.venv/.git out of images |
| Modify | `pyproject.toml` | Add `api` extra (fastapi, uvicorn) + httpx to `test`; single dep source (no service-local pyproject) |
| Modify | `config/cloudbuild.yaml` | Add `bgg-warehouse-api` Cloud Run service build/deploy |
| Create | `.github/workflows/deploy-warehouse-api.yml` | Deploy on changes to the service |

---

### Task 1: Dataset config

**Files:** Modify `config/bigquery.yaml`, `src/config.py`; Create `tests/test_config_datasets.py`

- [ ] **Step 1: Write the failing test** — `tests/test_config_datasets.py`:

```python
from src.config import get_bigquery_config

def test_datasets_include_serving_layers():
    datasets = get_bigquery_config()["datasets"]
    for name in ("core", "raw", "predictions", "analytics"):
        assert name in datasets
```

- [ ] **Step 2: Run it, watch it fail** — `uv run --extra test python -m pytest tests/test_config_datasets.py -v` → FAIL (`predictions`/`analytics` missing).
- [ ] **Step 3: Add datasets** to `config/bigquery.yaml`:

```yaml
datasets:
  core: core
  raw: raw
  predictions: predictions
  analytics: analytics
```

(`src/config.py` needs no change — `get_bigquery_config` already returns `config["datasets"]`; confirm.)

- [ ] **Step 4: Run it, watch it pass.**
- [ ] **Step 5: Commit** — `feat(api): add predictions/analytics datasets to config`

---

### Task 2: BigQuery client helper

**Files:** Create `src/warehouse/__init__.py`, `src/warehouse/bq.py`

- [ ] **Step 1: Write the helper** — `src/warehouse/bq.py`:

```python
"""Shared BigQuery access for warehouse readers."""
from functools import lru_cache
from google.cloud import bigquery
from src.config import get_bigquery_config

@lru_cache(maxsize=1)
def _cfg():
    return get_bigquery_config()

def get_client() -> bigquery.Client:
    return bigquery.Client(project=_cfg()["project"]["id"])

def dataset(name: str) -> str:
    """Fully-qualified `project.dataset` for a configured dataset key."""
    c = _cfg()
    return f'{c["project"]["id"]}.{c["datasets"][name]}'
```

- [ ] **Step 2: Verify import** — `uv run python -c "from src.warehouse.bq import dataset; print('ok')"`.
- [ ] **Step 3: Commit** — `feat(api): add warehouse BigQuery client/dataset helper`

---

### Task 3: Games reader (get-by-id)

**Files:** Create `src/warehouse/readers/__init__.py`, `src/warehouse/readers/games.py`, `tests/test_games_reader.py`

Reader functions take an injected `client` (default `get_client()`) so tests mock the
`bigquery.Client`. Blocks: `get_features`, `get_predictions`, `get_embedding`,
`get_similar`, `get_provenance`, and `get_game()` aggregating them (returns `None` if
the game has no features row).

- [ ] **Step 1: Write failing tests** — `tests/test_games_reader.py`. Mock a
  `bigquery.Client` whose `.query(...).result()` returns canned rows; assert each block
  function issues a query referencing the expected table and returns the shaped dict,
  and that `get_game(13)` composes `{game_id, features, predictions, embedding, similar,
  provenance}`. Assert `get_game()` returns `None` when the features query is empty.

- [ ] **Step 2: Run, watch fail** — `uv run --extra test python -m pytest tests/test_games_reader.py -v` → FAIL (module missing).

- [ ] **Step 3: Implement** `src/warehouse/readers/games.py` — parameterized queries
  (`@game_id`) against `dataset("analytics")` / `dataset("predictions")` tables per the
  spec's data contract; `get_similar` uses `ML.DISTANCE` over
  `analytics.game_similarity_search`. No f-string interpolation of `game_id` into SQL —
  use `bigquery.ScalarQueryParameter`.

- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Smoke against real BigQuery** (needs creds) —
  `uv run python -c "from src.warehouse.readers.games import get_game; import json; print(json.dumps(get_game(13), default=str)[:400])"`
  → a populated document for Catan.
- [ ] **Step 6: Commit** — `feat(api): add games reader (get_game by id)`

---

### Task 4: FastAPI skeleton + health + auth

**Files:** Create `services/warehouse_api/{__init__.py, main.py, auth.py}`; Modify root `pyproject.toml`

- [ ] **Step 1: Add the `api` extra** to root `pyproject.toml`:

```toml
[project.optional-dependencies]
api = ["fastapi>=0.115", "uvicorn>=0.30", "httpx>=0.27"]
```

Then `uv lock`.

- [ ] **Step 2: Copy `auth.py`** — port `GCPAuthenticator` from
  `bgg-predictive-models/services/scoring/auth.py` (strip bucket-specific bits not
  needed for a read API).

- [ ] **Step 3: Write `services/warehouse_api/main.py`** — `FastAPI(title="BGG
  Warehouse API")`, `GET /health` returning `{"status": "ok"}`, and mount the games
  router (Task 5) under `/games`. `uvicorn.run(app, host="0.0.0.0", port=8080)` in
  `__main__`.

- [ ] **Step 4: Write failing health test** — `tests/test_games_router.py` (health part):

```python
from fastapi.testclient import TestClient
from services.warehouse_api.main import app

def test_health():
    assert TestClient(app).get("/health").json() == {"status": "ok"}
```

- [ ] **Step 5: Run, watch pass** — `uv run --extra test --extra api python -m pytest tests/test_games_router.py -v`.
- [ ] **Step 6: Commit** — `feat(api): FastAPI warehouse-api skeleton with /health`

---

### Task 5: Games router

**Files:** Create `services/warehouse_api/routers/{__init__.py, games.py}`; extend `tests/test_games_router.py`

- [ ] **Step 1: Write failing router tests** — mock
  `services.warehouse_api.routers.games.get_game` to return a canned doc; assert
  `GET /games/13` → 200 + doc, `GET /games/999999` (reader returns `None`) → 404, and one
  sub-resource (`GET /games/13/predictions`) → the predictions block.

- [ ] **Step 2: Run, watch fail.**

- [ ] **Step 3: Implement** `routers/games.py` — `APIRouter`; `GET /{game_id}` calls
  `get_game`, raises `HTTPException(404)` on `None`; sub-resource routes
  (`/{game_id}/predictions|features|similar|embedding|players|provenance`) call the
  matching block function. Mount it in `main.py`.

- [ ] **Step 4: Run, watch pass.**
- [ ] **Step 5: Local run** — `uv run --extra api uvicorn services.warehouse_api.main:app --port 8080`,
  then `curl localhost:8080/health` → ok and `curl localhost:8080/games/13` → populated (needs creds).
- [ ] **Step 6: Commit** — `feat(api): add games router (get-by-id + sub-resources)`

---

### Task 6: Container + service pyproject

**Files:** Create `services/warehouse_api/{Dockerfile, pyproject.toml, README.md}`

- [ ] **Step 1: Write `services/warehouse_api/pyproject.toml`** — minimal project with
  `fastapi`, `uvicorn`, `google-cloud-bigquery`, `pyyaml`, `google-auth`.
- [ ] **Step 2: Write `Dockerfile`** — model on `docker/Dockerfile.pipeline` (uv base,
  copy repo, `uv sync`), `CMD ["uvicorn", "services.warehouse_api.main:app", "--host", "0.0.0.0", "--port", "8080"]`.
- [ ] **Step 3: Build locally** — `docker build -f services/warehouse_api/Dockerfile -t warehouse-api .` → succeeds; `docker run -p 8080:8080` + `curl /health` → ok.
- [ ] **Step 4: Commit** — `feat(api): containerize warehouse-api`

---

### Task 7: Deploy (Cloud Build + workflow) — gated per the auth-pattern spec

**Files:** Modify `config/cloudbuild.yaml`; Create `.github/workflows/deploy-warehouse-api.yml`

Gating follows `docs/superpowers/specs/2026-07-16-service-auth-pattern-design.md`:
deploy authenticated, then grant `run.invoker` via the invoker group (or, day one,
direct member bindings).

- [ ] **Step 1: Add a `bgg-warehouse-api` Cloud Run *service*** block to
  `config/cloudbuild.yaml` — build/push the image, then `gcloud run deploy
  bgg-warehouse-api --region us-central1 --no-allow-unauthenticated
  --service-account=bgg-data-warehouse@$PROJECT_ID.iam.gserviceaccount.com`.
- [ ] **Step 2: Grant invoker access** (consumer-agnostic — see auth-pattern spec).
  Preferred (group): `gcloud run services add-iam-policy-binding bgg-warehouse-api
  --region us-central1 --member="group:bgg-api-invokers@googlegroups.com"
  --role=roles/run.invoker`. **Day one, grant your own identity** so the API is usable
  without tying it to any front-end:
  `--member="user:phil.henrickson@gmail.com"`. Add each consumer's SA (a new front-end,
  dash-viewer, …) to the group as it comes online. Confirm `allUsers` is **absent**.
- [ ] **Step 3: Validate YAML** — `python -c "import yaml; yaml.safe_load(open('config/cloudbuild.yaml'))" && echo valid`.
- [ ] **Step 4: Create `.github/workflows/deploy-warehouse-api.yml`** — mirror
  `deploy.yml` (auth with `GCP_SA_KEY_BGG_DW`, `gcloud builds submit`), triggered on
  `push` to `main` touching `services/warehouse_api/**`, `src/warehouse/**`, plus
  `workflow_dispatch`. Validate its YAML too.
- [ ] **Step 5: Commit** — `ci(api): deploy gated warehouse-api to Cloud Run`
- [ ] **Step 6: Post-deploy check** (after merge):
  - `curl <url>/health` with no token → **403** (gate works).
  - `curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" <url>/health` → **200**.
  - `gcloud run services proxy bgg-warehouse-api --region us-central1` → hit `/games/13`
    → real, populated document.
  - Re-confirm the IAM policy has **no `allUsers`**.

---

### Task 8: Open the PR

- [ ] **Step 1: Push the branch** — `git push -u origin feature/warehouse-api-games`.
- [ ] **Step 2: Open the PR** against `main` —
  `gh pr create --base main --title "feat(api): warehouse read API skeleton + games router" --body "<summary + links to both specs>"`.
- [ ] **Step 3: CI green** — the existing test workflow passes on the PR.
- [ ] **Step 4: After merge**, run the post-deploy checks (Task 7 Step 6) against the
  deployed service.

---

## Follow-up (separate PRs / plans)

1. **`bgg-dash-viewer` repoint** — `warehouse_api_client.py` (carrying the reusable
   `id_token_headers(WAREHOUSE_API_URL)` helper from the auth-pattern spec) +
   `WAREHOUSE_API_URL`; rewrite `game_details.py` to call the API; delete the game SQL
   from `bigquery_client.py`. Verify the game page renders identically, authenticated.
2. **Games list/search slice** — `GET /games`, `/games/search`, `/games/new`, `/games/summary`.
3. **`clusterBy game_id`** on `games_features` / `bgg_predictions` /
   `game_similarity_search` (needs full-refresh — see dataform-incremental-schema-drift).
4. **Remaining resource routers** — publishers/designers/…, predictions, collections,
   similarity, experiments, monitoring.
5. **SECURITY — gate the predictive-models services.** All five are currently
   `run.invoker: allUsers` (confirmed live). Apply the same auth pattern per the
   auth-pattern spec (identify callers → add to invoker group → redeploy authenticated).
   `bgg-streamlit-prod` is a browser UI → needs IAP/app-login, handled separately. Track
   as its own security spec + plan.

## Risks / rollback

- **Additive & reversible:** the API is unused until the dash-viewer repoint (follow-up
  1); nothing here touches existing pipelines, Dataform, or schemas. Revert = delete the
  service dir + workflow.
- **Cost:** unclustered serving tables mean each `/games/{id}` call full-scans
  `games_features` etc. Acceptable at low traffic; the `clusterBy` follow-up removes it.
  Watch bytes-scanned in the Task 3 smoke test.
- **Auth wiring** (resolved to IAM + ID token, see auth-pattern spec): the dash-viewer
  runtime SA already runs on Cloud Run via ADC, so it *can* mint an ID token — but the
  consumer code to attach it doesn't exist yet, so it's built in the repoint follow-up.
  Until then the deployed API is gated and reachable only by your own identity (via
  `gcloud`) — inert for the front-end, which is fine (nothing consumes it yet).
