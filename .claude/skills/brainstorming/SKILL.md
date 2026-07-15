---
name: brainstorming
description: Explore options and generate ideas before committing to an approach. Use when the user wants to weigh alternatives, asks "what are my options / how could we approach X", or is at the start of a fuzzy problem and needs divergent thinking before converging on a plan. Not for tasks where the approach is already clear.
---

# Brainstorming

Help the user think broadly before narrowing. The goal is to surface genuinely
different approaches and their trade-offs, then land on a recommendation — not to
start implementing.

## Process

1. **Frame the problem.** Restate the goal in one sentence and name the hard
   constraints (cost, time, data volume, Cloudflare/rate limits, what must not
   break). If the request is underspecified, ask 1–3 targeted questions before
   diverging — don't brainstorm against a guess.

2. **Diverge.** Generate 3–5 *distinct* approaches, not variations of one idea.
   Push past the obvious first answer. Include at least one "cheap/boring" option
   and one "what if we rethought this" option. For each:
   - How it works (2–4 sentences)
   - Pros / cons
   - Rough effort and risk
   - How well it fits this repo's grain (see below)

3. **Converge.** Compare the options against the constraints. Recommend one (or a
   hybrid), and say plainly *why* it wins and what you'd give up.

4. **Name the unknowns.** What would you need to validate before committing —
   a spike, a cost estimate, a sample query, a question for the user? Hand off to
   `planning` once an approach is chosen.

## Keep it honest

- Don't inflate the count with near-duplicates. Three real options beat five fake ones.
- Surface the option you'd *not* pursue and why — the discarded ideas are part of the value.
- Flag anything that's a one-way door (schema migrations, backfills, deleting data,
  changing table partitioning, public-facing outputs).

## This repo (context to weigh options against)

- **Python pipelines** run as `uv run python -m src.pipeline.<name>`; logic in `src/`.
- **Data** lives in BigQuery — `raw.*` tables land raw data; **Dataform** models in
  `definitions/` transform it. BigQuery cost (bytes scanned) is a real constraint.
- **Orchestration** is GitHub Actions in `.github/workflows/`; the pipeline chain is
  stitched together with `repository_dispatch` events.
- **Scraping** BGG runs from a residential-IP **home box** because Cloudflare blocks
  datacenter egress — approaches that need heavy scraping must account for that.
- Favor **idempotent** designs (the ID pipeline dedups via `MERGE`) and incremental
  Dataform models over full rebuilds.
