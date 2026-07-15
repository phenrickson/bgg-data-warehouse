---
name: planning
description: Turn a non-trivial task into a concrete, sequenced implementation plan before writing code. Use when the user asks to "plan" or "how should we implement X", or before any change that touches multiple files, the BigQuery schema, Dataform models, or the pipeline/CI chain. Produces an ordered, verifiable plan for approval.
---

# Planning

Produce a plan the user can approve before implementation starts. A good plan is
**ordered, verifiable, and honest about risk** — each step should be something you
can check, not a vague intention.

## Process

1. **Understand first.** Restate the goal and its success criteria. **Read the
   relevant code before planning** — don't plan against assumptions. Note what
   already exists that you can reuse.

2. **Scope.** State what's in and what's explicitly out. Identify every affected
   surface: `src/` pipeline code, `definitions/` Dataform models, BigQuery schemas,
   `.github/workflows/`, config, tests, docs.

3. **Sequence the work.** Break it into ordered steps, each independently
   verifiable, smallest-safe-change first. Call out dependencies and anything that
   must land in a specific order (e.g. create a table before the model that reads it).

4. **Surface risks and unknowns.** What could break or is irreversible —
   schema/partitioning changes, **backfills**, BigQuery cost, Cloudflare/rate limits,
   scheduling, cross-project Dataform sources? Note the rollback story and any open
   questions to resolve before starting.

5. **Define verification.** For each step, how do you prove it works — a unit test,
   a Dataform dry-run/`--dry-run`, a manual `uv run` of the pipeline, a `bq query`
   sanity check, or a workflow run.

6. **Present for approval.** Show the plan and stop for a go-ahead before writing
   code. For anything sizeable, prefer using plan mode (`EnterPlanMode` /
   `ExitPlanMode`) so the plan is reviewed explicitly.

## Output shape

- **Goal & success criteria** — 1–2 lines
- **Affected files/systems** — the concrete list
- **Steps** — numbered, each with its verification
- **Risks / unknowns / rollback** — what to watch, what's one-way
- **Out of scope** — what you're deliberately not doing

## This repo (shape plans to fit it)

- Changes ship via **PRs to `main`**; keep each PR focused on one concern.
- Pipelines run as `uv run python -m src.pipeline.<name>`; tests via
  `uv run --extra test python -m pytest`.
- The daily flow is `Fetch Thing IDs → Fetch New Games → Dataform`, chained by
  `repository_dispatch` — a change to one stage often has downstream effects.
- Prefer **incremental** Dataform models and **idempotent** pipeline steps
  (`MERGE`-based upserts) so a re-run or catch-up is always safe.
- New raw data usually means: land in `raw.*` → declare a source in
  `definitions/sources.js` → add/adjust a Dataform model → wire any trigger.
- Watch BigQuery cost (bytes scanned) and whether a change requires a **backfill**
  of historical data.
