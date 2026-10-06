# bgg-data-warehouse

## Releases gate production

Releases are cut by release-please (`.github/workflows/release-please.yml`; the
`release` skill covers the mechanics). **Its purpose is to gate production.** Merging
a release PR should be the step that puts changes into production. Merging a feature PR
to `main` should not.

That is not yet true. Today a release only bumps the version, writes the changelog and
tags. Every surface still deploys from `main`:

| Surface | Deploys today on | Gated by a release? |
|---|---|---|
| Warehouse API | push to `main` (`deploy-warehouse-api.yml` path filter) | no |
| Dataform models | push to `main` under `definitions/**`, plus the daily cascade, which runs `main` | no |
| Pipeline jobs | push to `main` (`deploy.yml`) | no |
| Workflows, Terraform | self-deploying on merge (`terraform.yml` applies on push) | no |

Rules:

- **New or changed deploy triggers move toward the gate.** Deploy on
  `release_created` (see bgg-viewer's `release-please.yml`), or run against the latest
  release tag, not `main`. Never add a new push-to-`main` production deploy.
- **When a change touches a deploy or pipeline trigger, say how it relates to the
  gate.** Does it move a surface behind the gate, keep it as-is, or work against it?
- **Known hard parts:**
  - Scheduled and `workflow_run` workflows always run `main`'s HEAD.
  - The Dataform cascade compiles `gitCommitish: "main"`.
  - Gating either needs them to target the release tag.
  - The warehouse API is the easy first step: a release-gated deploy, a `stable`
    revision tag, and `rollback.yml`. See
    `docs/superpowers/specs/2026-09-22-release-please-design.md`.
