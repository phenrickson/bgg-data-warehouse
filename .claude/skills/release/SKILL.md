---
name: release
description: Cut a release the way this repo does it. Use when the user wants to publish a new version, review or edit the pending release PR, or asks what the next version will be. Covers release-please's release PR, editing its changelog, and what merging it does.
---

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
