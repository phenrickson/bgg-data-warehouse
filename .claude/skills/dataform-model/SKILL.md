---
name: dataform-model
description: Add or modify a Dataform model in definitions/ correctly. Use when creating a new transformation/table/view, exposing a new source table, or changing an existing .sqlx model. Covers source declaration, config, ref() wiring, incremental patterns, and validation before merge.
---

# Dataform model

Add or change a Dataform model without breaking compilation, dependency lineage,
or incremental state. Models live in `definitions/*.sqlx`; project config is in
`workflow_settings.yaml` (defaultProject `bgg-data-warehouse`, defaultDataset
`analytics`, location `US`, dataformCore 3.0.0). Execution happens via the
**Run Dataform** GitHub Actions workflow, chained after fetches by `repository_dispatch`.

## Steps

1. **Understand the data.** Know the grain (one row per what?), the source table(s),
   and the target schema/dataset. Read a couple of existing `.sqlx` files first to
   match style — e.g. `bgg_complexity_predictions.sqlx`.

2. **Declare any new source.** Anything you `ref()` must be declared in
   `definitions/sources.js`. Same-project: `declare({ schema, name })`. Cross-project
   (e.g. `bgg-predictive-models`): `declare({ database, schema, name })`.

3. **Write the config block.** Choose the type deliberately:
   - `view` — cheap, always fresh, no storage; for light reshaping.
   - `table` — full rebuild each run; for small/derived outputs.
   - `incremental` — appends/merges only new rows; for large or append-only sources.
     Set `uniqueKey: [...]` and guard the source scan with
     `${when(incremental(), \`AND score_ts > (SELECT MAX(score_ts) FROM ${self()})\`)}`.
   ```
   config {
     type: "incremental",
     schema: "predictions",
     name: "my_model",
     uniqueKey: ["game_id"]
   }
   ```

4. **Wire dependencies via `ref()`** — never hard-code fully-qualified table names.
   `${ref("core", "games")}` or cross-project `${ref("bgg-predictive-models", "raw", "complexity_predictions")}`. This is what builds the lineage graph and the correct run order.

5. **Dedup to the grain.** The house pattern is `ROW_NUMBER() OVER (PARTITION BY <key>
   ORDER BY score_ts DESC, job_id DESC)` then `WHERE rn = 1` — keep the latest row per key.

6. **Validate before merge.** Compile to catch ref/syntax errors
   (`npx @dataform/cli compile`, or rely on the Run Dataform workflow's compile step).
   Sanity-check the SQL against BigQuery with a dry-run (see the `bigquery-cost-check`
   skill) so you know what it scans.

## Watch out for

- **Incremental correctness:** a wrong or missing `uniqueKey`/incremental guard causes
  duplicates or silently drops rows. When in doubt, a full-refresh `table` is safer.
- **New source not declared** → compile fails. Add it to `sources.js` in the same PR.
- **Downstream lineage:** changing a model's columns can break models that `ref()` it —
  grep `definitions/` for dependents.
- Assertions (data tests) land in the `analytics_assertions` dataset.
