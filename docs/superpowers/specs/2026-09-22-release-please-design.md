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
  "$schema": "https://raw.githubusercontent.com/googleapis/release-please/main/schemas/config.json",
  "last-release-sha": "6079bd7e2cfe1ce1287da3412717b01f382c62c2",
  "packages": {
    ".": {
      "release-type": "python",
      "package-name": "bgg-data-warehouse",
      "changelog-path": "CHANGELOG.md",
      "include-component-in-tag": false,
      "extra-files": [
        {
          "type": "toml",
          "path": "uv.lock",
          "jsonpath": "$.package[?(@.name.value=='bgg-data-warehouse')].version"
        }
      ]
    }
  }
}
```

Manifest seeded `{".": "0.6.7"}` so numbering continues rather than restarting.

- **Delete `tag-release.yml`.** It would double-tag when release-please's own
  version-bump PR merges. This is the only removal required.
- **`uv.lock` must be bumped with `pyproject.toml`.** The 0.6.6 release bumped one
  and not the other, which left the home box on a locally-modified lockfile that
  aborted `git pull --ff-only` for two months. A bare `"uv.lock"` string in
  `extra-files` uses the generic updater, which only rewrites lines marked
  `x-release-please-version`, and a lockfile cannot carry that marker. The `toml`
  updater with a JSONPath to the package's own entry does. Check the first release
  PR's diff touches exactly that one line in `uv.lock`.
- **`last-release-sha`.** `tag-release.yml` pushed tags but never created GitHub
  Releases, and release-please finds the previous release through Releases. Without
  this, the first release PR's changelog would cover the whole history. Remove it
  once the first release exists.
- **`include-component-in-tag: false`** keeps tags as `vX.Y.Z`, continuing `v0.6.7`.
  bgg-viewer's tags carry the component (`bgg-viewer-v0.0.26`).
- **`changelog-sections`** maps scopes to headings. `docs(spec):` commits are
  design records, not release content — hide them. `ci` gets its own
  `### Operations` heading so heartbeat tuning does not drown consumer-facing
  entries.
- **Repo setting:** "Allow GitHub Actions to create and approve pull requests" is off
  in this repo (on in bgg-viewer). release-please cannot open its PR without it.
  Phil turns it on in Settings → Actions → General.
- **The release PR runs no checks.** Pushes made with `GITHUB_TOKEN` do not trigger
  workflows, so `dataform-compile.yml` does not run on it. Acceptable: it only
  touches the version, the lockfile and the changelog.
- **Update the `release` skill** (`.claude/skills/release/SKILL.md`, its line in
  `.claude/skills/README.md`) and the README's release notes to the new flow:
  merge the release PR, editing its changelog first if needed.

### Deploy gating, per surface

**First round: no deploy gating.** Changelog and versioning only. Every surface,
the warehouse API included, keeps deploying exactly as it does today.

Gating the API changes how work ships: an API fix would wait for a release PR
merge instead of going live on merge, and the API is an admin-facing read
service, so the rollback it buys is worth less than the friction for now. Later,
if wanted:

- **Warehouse API** — port the bgg-viewer pattern: `deploy` job with
  `if: needs.release-please.outputs.release_created == 'true'`, tag the outgoing
  revision `stable` before deploying, and a manual-dispatch `rollback.yml` that
  shifts traffic `--to-tags stable=100`. Stamp `VERSION` into the Cloud Run env
  vars so the running service reports what it is. The deploy runs through Cloud
  Build (`config/cloudbuild.warehouse-api.yaml`), so the version reaches it as a
  substitution, not bgg-viewer's direct `docker build`.
- **Data model** — not gated. Changelog only.
- **Orchestration** — not gated, cannot be. Changelog only.

One interaction to know about now: the release PR changes `pyproject.toml` and
`uv.lock`, both in `deploy-warehouse-api.yml`'s path filter, so merging a release
PR redeploys the API. Same as today's hand-cut bumps; harmless.

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

- Should the API deploy's path filter be dropped entirely once it is
  release-gated, or kept as a second condition? (Only matters when gating lands.)
- release-please generates terser entries than the current hand-written
  changelog (the 0.6.7 home-box `uv.lock` explanation is the standard to beat).
  Accept terser, or keep editing the bot's PR before merging?

## Delivery

Branch `docs/release-please-design` → PR for this spec. Implementation lands on
its own branch off `main` with its own PR; never on `main` directly.

Lands before the daily-pipeline PRs (warehouse #146, #147), so they make up the
first release-please release.
