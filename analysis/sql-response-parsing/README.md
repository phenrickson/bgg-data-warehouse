# SQL response parsing proof

Throwaway analysis for `docs/superpowers/specs/2026-10-05-sql-response-parsing-design.md`.
Nothing here is imported by the pipeline.

**Safety:** all writes go to `bgg-data-warehouse.scratch_parsing`. Every `.sql` file
goes through `sqlguard.check_sql` (scratch-only CREATE, no DML). Every billed query is
dry-run first, run with a cap, and approved one at a time.

    uv run python analysis/sql-response-parsing/run_sql.py sql/<file>.sql --dry-run
    uv run python analysis/sql-response-parsing/run_sql.py sql/<file>.sql --max-gb <cap>

Order: UDFs → `convert.py` → `verify_conversion.py` → input view → `core_inputs` →
`parse_items` → `parse_*` → `compare_*` → `edge_tags` / `checks_*` → `asof_*` →
`check_asof.py`.

Cleanup: `bq rm -r -f bgg-data-warehouse:scratch_parsing` (tables also expire after
30 days).
