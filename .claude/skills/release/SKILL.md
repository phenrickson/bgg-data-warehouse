---
name: release
description: Cut a release the way this repo does it. Use when the user wants to publish a new version, bump the version, or update the changelog for a release. Covers the CHANGELOG entry, the SemVer bump, and the automatic tagging flow.
---

# Release

This repo releases by **bumping the version in `pyproject.toml`**. On push to `main`
touching that file, the **Tag Release** workflow (`.github/workflows/tag-release.yml`)
auto-creates and pushes a `vX.Y.Z` git tag. There is no manual `git tag` step.

## Steps

1. **Decide the bump (SemVer).** Look at what's landed since the last release:
   - `patch` (0.6.4 → 0.6.5): bug fixes, ops hardening, no behavior change for consumers.
   - `minor` (0.6.4 → 0.7.0): new tables/models/pipelines/workflows, backward-compatible.
   - `major` (0.6.4 → 1.0.0): breaking changes to schemas or published outputs.

2. **Update `CHANGELOG.md`.** Follow Keep a Changelog. Add a new
   `## [X.Y.Z] - YYYY-MM-DD` section above the previous one, grouped under
   `### Added` / `### Fixed` / `### Changed` etc. Write entries for *consumers* of the
   warehouse, not commit-by-commit. Review `git log <last-tag>..HEAD` to gather them.

3. **Bump `version` in `pyproject.toml`** to match the CHANGELOG heading exactly.

4. **PR → merge to `main`.** Keep the bump + changelog in one PR. On merge, Tag Release
   fires and creates `vX.Y.Z`. Confirm the tag appears (`git tag -l` / the Actions run).

## Checklist

- [ ] CHANGELOG version and date match the `pyproject.toml` version
- [ ] Entries are consumer-facing and grouped by change type
- [ ] Bump type matches the actual scope of changes (don't ship a feature as a patch)
- [ ] No unrelated changes in the release PR

## Notes

- The tag is created **only if** the version string actually changed vs. the previous
  commit — re-running with the same version is a no-op.
- Recent small ops fixes (e.g. the home-box PRs) shipped without a version bump; reserve
  a release for a meaningful, consumer-relevant set of changes.
