# Release-Please (first round) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace hand-cut releases (`tag-release.yml` plus a hand-written CHANGELOG entry and version bump) with release-please, changelog and versioning only.

**Architecture:** One release-please package at the repo root, `release-type: python`. A `release-please.yml` workflow on push to `main` keeps a release PR open. Merging that PR bumps `pyproject.toml`, `uv.lock` and the manifest, prepends a CHANGELOG section, and creates the `vX.Y.Z` tag plus a GitHub Release. No deploy is gated on a release in this round.

**Tech Stack:** GitHub Actions, `googleapis/release-please-action@v4`, pytest (with `tomllib`, `json`, `yaml`).

**Spec:** `docs/superpowers/specs/2026-09-22-release-please-design.md` (PR #129, branch `docs/release-please-design`)

## Global Constraints

- Single package at `.`; `release-type: python`; `package-name: bgg-data-warehouse`.
- Manifest seeded `{".": "0.6.7"}`.
- `last-release-sha: 6079bd7e2cfe1ce1287da3412717b01f382c62c2` (the commit tagged `v0.6.7`).
- `include-component-in-tag: false`, so tags stay `vX.Y.Z`.
- `uv.lock` is bumped by the `toml` updater with JSONPath `$.package[?(@.name.value=='bgg-data-warehouse')].version`.
- `docs` commits are hidden from the changelog; `ci` commits go under `Operations`.
- Delete `tag-release.yml`.
- No deploy gating: `deploy.yml`, `deploy-warehouse-api.yml`, `dataform.yml`, `terraform.yml` are untouched.
- Delivery: branch `feat/release-please` off `origin/main` → PR → Phil merges. Never commit to `main`. Commit and push each task as it passes.
- Commit subjects are conventional and scoped, e.g. `feat(release): …`, `docs(release): …`.
- All builds and deploys go through GitHub Actions. Nothing is run against GCP.

## Review Focus

- **`uv.lock` not bumped, or the wrong line bumped.** Expected: the release PR changes exactly one line in `uv.lock`, the `version` under `name = "bgg-data-warehouse"`. Pinned by `test_versions_agree` (Task 1) on every future PR, and by the dry-run inspection in Task 3.
- **The first release PR's changelog covers the whole history.** Expected: only commits after `6079bd7` (the 23 since `v0.6.7`, plus this branch). Pinned by the dry-run inspection in Task 3.
- **The new CHANGELOG section lands in the wrong place**, e.g. above `# Changelog` or inside the Keep a Changelog intro. Expected: directly above `## [0.6.7] - 2026-09-14`. Pinned by the dry-run inspection in Task 3.
- **Tag or release named `bgg-data-warehouse-v0.7.0`** instead of `v0.7.0`. Pinned by `test_config` (Task 1) and the dry-run's compare link in Task 3.
- **The release PR never opens** because Actions may not create PRs in this repo. Expected: Phil enables the setting before merging; the first `release-please` run after merge opens the PR. Pinned by the pre-merge step in Task 3 and the post-merge check.

---

## File Structure

- `release-please-config.json` (new): the package config.
- `.release-please-manifest.json` (new): current version, `0.6.7`.
- `.github/workflows/release-please.yml` (new): runs the action on push to `main`.
- `.github/workflows/tag-release.yml` (delete).
- `tests/test_release_please.py` (new): config shape, workflow shape, `tag-release.yml` gone, and the three version strings agree.
- `.claude/skills/release/SKILL.md`, `.claude/skills/README.md`, `README.md` (modify): describe the new flow.

---

### Task 1: release-please config, workflow, and retire Tag Release

**Files:**
- Create: `release-please-config.json`
- Create: `.release-please-manifest.json`
- Create: `.github/workflows/release-please.yml`
- Delete: `.github/workflows/tag-release.yml`
- Test: `tests/test_release_please.py`

**Interfaces:**
- Produces: the manifest's `"."` key holds the released version; Task 2's docs describe this flow.

- [ ] **Step 1: Create the branch**

```bash
git fetch origin
git switch -c feat/release-please origin/main
```

- [ ] **Step 2: Write the failing tests**

`tests/test_release_please.py`:

```python
# tests/test_release_please.py
"""release-please owns versioning: config shape, workflow, and versions that agree."""

import json
import tomllib
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "release-please-config.json"
MANIFEST = ROOT / ".release-please-manifest.json"
WORKFLOW = ROOT / ".github/workflows/release-please.yml"


def _package() -> dict:
    return json.loads(CONFIG.read_text(encoding="utf-8"))["packages"]["."]


def test_config():
    config = json.loads(CONFIG.read_text(encoding="utf-8"))
    pkg = config["packages"]["."]
    assert pkg["release-type"] == "python"
    assert pkg["package-name"] == "bgg-data-warehouse"
    assert pkg["include-component-in-tag"] is False
    assert config["last-release-sha"] == "6079bd7e2cfe1ce1287da3412717b01f382c62c2"


def test_uv_lock_is_bumped_by_the_toml_updater():
    lock = [f for f in _package()["extra-files"] if f["path"] == "uv.lock"]
    assert lock == [{
        "type": "toml",
        "path": "uv.lock",
        "jsonpath": "$.package[?(@.name.value=='bgg-data-warehouse')].version",
    }]


def test_changelog_sections():
    sections = {s["type"]: s for s in _package()["changelog-sections"]}
    assert sections["ci"]["section"] == "Operations"
    assert sections["ci"].get("hidden", False) is False
    assert sections["docs"]["hidden"] is True
    for visible in ("feat", "fix", "perf", "revert"):
        assert sections[visible].get("hidden", False) is False


def test_versions_agree():
    """pyproject, uv.lock and the manifest name the same version (0.6.6 bumped only one)."""
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["project"]
    lock = tomllib.loads((ROOT / "uv.lock").read_text(encoding="utf-8"))
    locked = [p["version"] for p in lock["package"] if p["name"] == project["name"]]
    manifest = json.loads(MANIFEST.read_text(encoding="utf-8"))["."]
    assert locked == [project["version"]]
    assert manifest == project["version"]


def test_workflow():
    wf = yaml.safe_load(WORKFLOW.read_text(encoding="utf-8"))
    on = wf[True] if True in wf else wf["on"]  # PyYAML parses the key `on` as True
    assert on == {"push": {"branches": ["main"]}}
    assert wf["permissions"] == {"contents": "write", "pull-requests": "write"}
    step = wf["jobs"]["release-please"]["steps"][0]
    assert step["uses"] == "googleapis/release-please-action@v4"
    assert step["with"]["config-file"] == "release-please-config.json"
    assert step["with"]["manifest-file"] == ".release-please-manifest.json"


def test_tag_release_retired():
    assert not (ROOT / ".github/workflows/tag-release.yml").exists()
```

- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run --extra test python -m pytest tests/test_release_please.py -q`
Expected: 6 FAIL (`FileNotFoundError` for the config, manifest and workflow; `test_tag_release_retired` asserts). `test_versions_agree` fails on the missing manifest, not on the versions.

- [ ] **Step 4: Write the config**

`release-please-config.json`:

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
      "bump-minor-pre-major": true,
      "extra-files": [
        {
          "type": "toml",
          "path": "uv.lock",
          "jsonpath": "$.package[?(@.name.value=='bgg-data-warehouse')].version"
        }
      ],
      "changelog-sections": [
        { "type": "feat", "section": "Features" },
        { "type": "fix", "section": "Bug Fixes" },
        { "type": "perf", "section": "Performance" },
        { "type": "revert", "section": "Reverts" },
        { "type": "ci", "section": "Operations" },
        { "type": "docs", "section": "Documentation", "hidden": true },
        { "type": "chore", "section": "Miscellaneous", "hidden": true },
        { "type": "refactor", "section": "Refactoring", "hidden": true },
        { "type": "test", "section": "Tests", "hidden": true },
        { "type": "build", "section": "Build", "hidden": true },
        { "type": "style", "section": "Style", "hidden": true }
      ]
    }
  }
}
```

`bump-minor-pre-major: true` keeps a breaking change at `0.x` from jumping to `1.0.0`. A `feat` bumps the minor version (`0.6.7` → `0.7.0`), matching the SemVer rules in the `release` skill. `changelog-sections` replaces release-please's defaults, which is why every type is listed. `docs` is hidden by type: release-please sections key on the commit type, not the scope, so `docs(readme)` is hidden along with `docs(spec)`.

`.release-please-manifest.json`:

```json
{
  ".": "0.6.7"
}
```

- [ ] **Step 5: Write the workflow and delete Tag Release**

`.github/workflows/release-please.yml`:

```yaml
name: release-please

# Keeps a release PR open on main. Merging it bumps pyproject.toml, uv.lock and the
# manifest, prepends the CHANGELOG section, and creates the vX.Y.Z tag and GitHub
# Release. Nothing deploys from a release yet: every deploy keeps its own trigger.
on:
  push:
    branches: [main]

permissions:
  contents: write
  pull-requests: write

jobs:
  release-please:
    runs-on: ubuntu-latest
    steps:
      - uses: googleapis/release-please-action@v4
        with:
          token: ${{ secrets.GITHUB_TOKEN }}
          config-file: release-please-config.json
          manifest-file: .release-please-manifest.json
```

```bash
git rm .github/workflows/tag-release.yml
```

- [ ] **Step 6: Run the tests to verify they pass**

Run: `uv run --extra test python -m pytest tests/test_release_please.py -q`
Expected: `6 passed`

- [ ] **Step 7: Lint the workflow**

Run: `uvx --from actionlint-py actionlint .github/workflows/release-please.yml && echo "actionlint: 0 errors"`
Expected: `actionlint: 0 errors`

- [ ] **Step 8: Commit and push**

```bash
git add release-please-config.json .release-please-manifest.json .github/workflows/release-please.yml tests/test_release_please.py
git commit -m "feat(release): release-please owns versioning; retire Tag Release"
git push -u origin feat/release-please
```

---

### Task 2: Docs follow the new flow

**Files:**
- Modify: `.claude/skills/release/SKILL.md` (whole body)
- Modify: `.claude/skills/README.md:34`
- Modify: `README.md:74` (workflow table row) and `README.md:170-174` (Versioning)

**Interfaces:**
- Consumes: Task 1's flow (release PR on `main`, merge creates the tag and Release).

- [ ] **Step 1: Rewrite the `release` skill**

Replace everything after the frontmatter in `.claude/skills/release/SKILL.md`, and set the frontmatter `description` to:
`Cut a release the way this repo does it. Use when the user wants to publish a new version, review or edit the pending release PR, or asks what the next version will be. Covers release-please's release PR, editing its changelog, and what merging it does.`

Body:

```markdown
# Release

Releases are cut by **release-please** (`.github/workflows/release-please.yml`). Every
push to `main` updates one open release PR (`chore(main): release …`). Merging it:

- bumps `version` in `pyproject.toml`, the package entry in `uv.lock`, and
  `.release-please-manifest.json`;
- prepends a `## [X.Y.Z]` section to `CHANGELOG.md`, built from the conventional
  commit subjects since the last release;
- creates the `vX.Y.Z` tag and a GitHub Release.

No version bump or changelog entry is written by hand, and there is no manual `git tag`.

## How the version is chosen

From the commit types since the last release (pre-1.0, `bump-minor-pre-major`):

- `feat` → minor (0.6.7 → 0.7.0)
- `fix`, `perf` → patch (0.6.7 → 0.6.8)
- a breaking change (`feat!:` or a `BREAKING CHANGE:` footer) → minor while below 1.0

To force a version, add a `Release-As: X.Y.Z` footer to a commit on `main`.

## Steps

1. **Check the release PR.** Read its changelog section. `docs` and `chore` commits are
   hidden; `ci` commits appear under **Operations**.
2. **Edit the changelog if it needs it.** Entries are commit subjects, terser than the
   old hand-written ones. Push edits to the release PR's branch; release-please keeps
   them unless a new commit lands on `main` first, which regenerates the PR.
3. **Merge the release PR.** Confirm the `vX.Y.Z` tag and the GitHub Release appear
   (`gh release list -L 1`).

## Notes

- Merging a release PR redeploys the warehouse API: `pyproject.toml` and `uv.lock` are
  in `deploy-warehouse-api.yml`'s path filter. Harmless, as with the old hand bumps.
- The release PR runs no checks. It is pushed with `GITHUB_TOKEN`, which does not
  trigger workflows.
- Nothing deploys from a release. Every surface keeps its own trigger; see
  `docs/superpowers/specs/2026-09-22-release-please-design.md` for why only the API
  could be gated, and how.
- `test_versions_agree` fails any PR where `pyproject.toml`, `uv.lock` and the manifest
  disagree. The 0.6.6 release bumped `pyproject.toml` without `uv.lock`, which stalled
  the home box's `git pull` for two months.
```

- [ ] **Step 2: Update the skills index**

`.claude/skills/README.md:34` becomes:

```markdown
| [`release`](release/SKILL.md) | Cutting a release — review and merge release-please's release PR (version, changelog, tag) |
```

- [ ] **Step 3: Update the README**

`README.md:74`, the workflow table row, becomes:

```markdown
| `release-please.yml` | push to `main` (keeps the release PR open; merging it tags `vX.Y.Z`) |
```

`README.md:172-174`, under `## Versioning`, becomes:

```markdown
Semantic versioning, cut by release-please from conventional commit subjects. Every
push to `main` updates an open release PR; merging it bumps the version, writes the
[CHANGELOG.md](CHANGELOG.md) section, and creates the `vX.Y.Z` tag and GitHub Release.
```

- [ ] **Step 4: Check nothing still points at Tag Release**

Run: `git grep -n -i -E "tag-release|Tag Release" -- . ':!docs/superpowers' ':!CHANGELOG.md'`
Expected: no output. (Past CHANGELOG entries and design docs keep their history.)

- [ ] **Step 5: Commit and push**

```bash
git add .claude/skills/release/SKILL.md .claude/skills/README.md README.md
git commit -m "docs(release): release-please flow in the release skill and README"
git push
```

---

### Task 3: Validate the release PR before merge, then open the PR

**Files:** none changed.

- [ ] **Step 1: Dry-run release-please against the branch (ask Phil first)**

Read-only against the GitHub API with Phil's `gh` token; creates nothing. Ask Phil before running it.

```bash
npx --yes release-please@16 release-pr \
  --token="$(gh auth token)" \
  --repo-url=phenrickson/bgg-data-warehouse \
  --target-branch=feat/release-please \
  --config-file=release-please-config.json \
  --manifest-file=.release-please-manifest.json \
  --dry-run 2>&1 | tee "$SCRATCH/release-please-dry-run.txt"
```

(`$SCRATCH` is the session scratchpad.) Check the output against the Review Focus:

- the proposed version is `0.7.0`, and the changelog heading's compare link reads `v0.6.7...v0.7.0`, not `bgg-data-warehouse-v…` (the PR title may still name the component, as bgg-viewer's do; only the tag matters);
- the `uv.lock` update targets the `bgg-data-warehouse` package's `version` and no other line;
- the changelog lists only commits after `6079bd7`: for example `perf(processor): select the batch without response_data` (#140) is present, and nothing from 0.6.7 or earlier;
- the new section sits directly above `## [0.6.7] - 2026-09-14`.

If any check fails, stop and report to Phil with the output. Do not adjust the config by guesswork.

- [ ] **Step 2: Phil enables Actions PR creation**

Phil turns on Settings → Actions → General → "Allow GitHub Actions to create and approve pull requests" in bgg-data-warehouse. Confirm with:

Run: `gh api repos/phenrickson/bgg-data-warehouse/actions/permissions/workflow --jq .can_approve_pull_request_reviews`
Expected: `true`

- [ ] **Step 3: Open the PR**

Check `gh pr list --head feat/release-please --state all` first; open only if none exists.

```bash
gh pr create --base main --head feat/release-please \
  --title "feat(release): release-please for warehouse releases" \
  --body-file "$SCRATCH/pr-body.md"
```

The body says: implements the spec in #129 (first round, no deploy gating); the repo setting is enabled; the dry-run results; and that on merge release-please opens a `release 0.7.0` PR, which should stay open until the daily-pipeline PRs (#146, #147) have merged, so the cutover is the first release. Phil merges.

---

## After merge (Phil's merge; checks only)

- The `release-please` run on `main` succeeds and opens a release PR for `0.7.0`.
- Its diff touches `pyproject.toml`, `uv.lock` (one line), `.release-please-manifest.json` and `CHANGELOG.md` only.
- When that release PR merges: tag `v0.7.0` and a GitHub Release exist, and no `tag-release` run fires.
- Follow-up PR: remove `last-release-sha` from `release-please-config.json` (and its assertion in `test_config`) once `v0.7.0` exists as a GitHub Release.
