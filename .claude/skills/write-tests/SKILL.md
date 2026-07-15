---
name: write-tests
description: Write or extend pytest tests for this repo. Use when adding tests for new or existing pipeline/module code, reproducing a bug as a failing test, or improving coverage. Favors fast, deterministic unit tests that mock external services (BigQuery, Playwright/BGG).
---

# Write tests

Add tests that are fast, deterministic, and actually pin down behavior. Tests live
in `tests/`, run with `uv run --extra test python -m pytest`, and config is in
`pyproject.toml` (`[tool.pytest.ini_options]`, `pythonpath = ["src"]`).

## Approach

1. **Pin the behavior, not the implementation.** Test observable inputs → outputs and
   edge cases (empty input, errors, duplicates, boundary values), not private details.

2. **Mock the outside world.** These tests must not hit BigQuery, BGG, or launch a real
   browser:
   - **BigQuery:** patch the client / its `query`, `load_table_from_dataframe`,
     `delete_table` — see `tests/test_id_fetcher.py` for the established pattern
     (`mock.patch.object(fetcher.client, ...)`).
   - **Playwright/BGG:** pass a fake `page` (a `mock.Mock` with scripted `goto`,
     `title`, `content`) — see `tests/test_id_fetcher_browser.py`.
   - **Backoff/sleep:** patch `time.sleep` (autouse fixture) so retry tests run instantly.

3. **Cover the failure paths.** This codebase deliberately fails loudly (partial sitemap
   fetches abort; the index fetch retries then raises). Assert those: that it retries N
   times, that it raises after `MAX_RETRIES`, that a block page is treated as retryable.

4. **Reproduce bugs first.** For a bug fix, write the failing test that captures it,
   watch it fail, then fix — so the test proves the fix and guards against regression.

## Conventions

- File `tests/test_<module>.py`, functions `test_<behavior>`; import with the `src.`
  prefix (e.g. `from src.modules.id_fetcher_browser import BrowserIDFetcher`).
- Use fixtures for shared setup; keep each test focused on one behavior.
- Mark anything that needs real credentials/network with `@pytest.mark.integration`
  (declared in `pyproject.toml`) so it can be deselected: `-m "not integration"`.

## Run

- Just your file: `uv run --extra test python -m pytest tests/test_x.py -q`
- Fast unit suite: `uv run --extra test python -m pytest -m "not integration" -q`
