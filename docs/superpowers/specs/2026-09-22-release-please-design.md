# Release-Please for the Warehouse

> bgg-viewer already runs this (`release-please.yml`, `rollback.yml`, manifest at
> `0.0.22`). This spec ports that pattern, and records where the warehouse cannot
> follow it.

## Problem

Releases are hand-cut. You write the `CHANGELOG.md` entry, bump `version` in
`pyproject.toml`, and `tag-release.yml` notices the string changed on `main` and
pushes `vX.Y.Z`. No GitHub Release object is created, and the tag names nothing
you can deploy or roll back to.

Meanwhile every deploy is path-triggered on push to `main` and has no
relationship to the version at all. "What is running right now" is answerable
only by reading Cloud Run revision timestamps against `git log`.

Commits are already conventional and scoped (`feat(dataform):`, `fix(api):`,
`fix(ci):`), which is the precondition release-please needs. The work is wiring,
not re-education.

## Four goals, and where they fight

1. **A changelog you read** — what changed in the warehouse, and when.
2. **Less manual work** — stop hand-writing the entry and the bump.
3. **A rollback / deploy anchor** — a version maps to a Cloud Run revision.
4. **A contract for downstream repos** — bgg-viewer and bgg-predictive-models
   can tell when the data model moved under them.

(1) and (2) point at a **single package**: one changelog, one version, scopes
doing the disambiguating. (3) and (4) pull toward **components**, so the API and
the data model can move independently.

They conflict, and (4) is the one that loses. Reasons below.

## Three surfaces, not one

| Surface | Paths | How it reaches production | Rollback today |
|---|---|---|---|
| Data model | `definitions/`, `includes/`, `workflow_settings.yaml` | `dataform.yml` on push to main, plus the daily cascade | Revert + full refresh |
| Warehouse API | `services/warehouse_api/`, `src/warehouse/` | Cloud Build → Cloud Run revision | Revision rollback (unused) |
| Orchestration | `.github/workflows/`, `terraform/` | **Self-deploying on merge** | Revert only |

The third row is the constraint that shapes everything. Scheduled and
`workflow_run`-triggered workflows always execute from the **default branch's
HEAD** — GitHub does not run a tagged version. `terraform.yml` applies on push to
main for the same reason. So for orchestration, a release can only ever describe
what already shipped. It cannot gate it.

The data model is nearly as bad: `dataform.yml` fires off the daily fetch
cascade, not off a release, and BigQuery datasets are not versioned. Gating the
*push-to-main* trigger on a release would still leave the scheduled cascade
running whatever is on main.

**Only the warehouse API can actually be gated on a release.** One surface out of
three.

## Why components lose

release-please routes a commit to a component by the **file paths it touched**,
not by the commit scope. Recent history shows why that misroutes:

- `a5919e1 fix(ci): chain Refresh Old Games off Fetch New Games so Dataform
  cascades once a day` — touches `.github/workflows/`, but it is a *data model
  freshness* change that consumers feel.
- `edfae29 fix(ci): take max run time over a window in the scrape heartbeat` —
  touches `.github/workflows/`, and is internal noise.

Same path, opposite audience. A `dataform` component keyed on `definitions/**`
would miss the first one entirely. This is a recurring pattern in this repo, not
an edge case, so per-surface components would need hand-correction as routine
work — which defeats goal (2).

The scope prefix already carries the signal a component split would encode. Keep
it in one changelog and let the prefix do the work.

## Decision

**Single package at the repo root.**

```json
{
  "packages": {
    ".": {
      "release-type": "python",
      "changelog-path": "CHANGELOG.md",
      "extra-files": ["uv.lock"]
    }
  }
}
```

Manifest seeded `{".": "0.6.7"}` so numbering continues rather than restarting.

- **Delete `tag-release.yml`.** It would double-tag when release-please's own
  version-bump PR merges. This is the only removal required.
- **`uv.lock` must be bumped with `pyproject.toml`.** The 0.6.6 release bumped one
  and not the other, which left the home box on a locally-modified lockfile that
  aborted `git pull --ff-only` for two months. Do not repeat it; if `extra-files`
  cannot patch the lock cleanly, add a `uv lock` step to the release PR instead.
- **`changelog-sections`** maps scopes to headings. `docs(spec):` commits are
  design records, not release content — hide them. `ci` gets its own
  `### Operations` heading so heartbeat tuning does not drown consumer-facing
  entries.

### Deploy gating, per surface

- **Warehouse API** — port the bgg-viewer pattern exactly: `deploy` job with
  `if: needs.release-please.outputs.release_created == 'true'`, tag the outgoing
  revision `stable` before deploying, and a manual-dispatch `rollback.yml` that
  shifts traffic `--to-tags stable=100`. This delivers goal (3) in full, for the
  one surface where it is possible.
- **Data model** — not gated. Changelog only.
- **Orchestration** — not gated, cannot be. Changelog only.

Stamp `VERSION` into the Cloud Run env vars as bgg-viewer does, so the running
service reports what it is.

## What this gives up

Goal (4), mostly. A downstream repo still cannot pin a data model version,
because the thing it reads is a live BigQuery dataset, not an artifact. The
honest deliverable is a **legible changelog section** a human checks when
something changes shape underneath them — not a machine-checkable contract.

If that turns out to be insufficient, the upgrade path is a `definitions/**`
component emitting `dataform-vX.Y.Z` tags. release-please bootstraps from
existing tags, so the split stays cheap to do later. Deliberately not doing it
now.

## Open questions

- Does `extra-files` patch `uv.lock`'s version field correctly, or does the
  release PR need a `uv lock` step? **Validate before committing** — this is the
  failure that already cost two months of stale home-box code.
- Should the API deploy's path filter be dropped entirely once it is
  release-gated, or kept as a second condition?
- release-please generates terser entries than the current hand-written
  changelog (the 0.6.7 home-box `uv.lock` explanation is the standard to beat).
  Accept terser, or keep editing the bot's PR before merging?

## Delivery

Branch `docs/release-please-design` → PR for this spec. Implementation lands on
its own branch off `main` with its own PR; never on `main` directly.
