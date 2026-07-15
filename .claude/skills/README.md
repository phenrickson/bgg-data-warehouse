# Claude skills

Project-scoped [Claude Code](https://claude.com/claude-code) skills for this repo.
Each skill is a structured workflow Claude can follow, lightly tailored to this
project's stack (uv pipelines, BigQuery `raw`/`core`/`predictions`/`analytics`,
Dataform models in `definitions/`, the GitHub Actions `repository_dispatch` chain,
and the residential-IP home-box scrape).

## Using them

- Invoke explicitly with a slash command: `/debugging`, `/dataform-model`, …
- Or just describe your task — Claude auto-selects a skill when its `description` matches.
- New skills are picked up when Claude Code loads `.claude/skills/`; reload the window
  if a freshly added one doesn't appear.

## Available skills

### Generic workflow helpers

| Skill | Use it when |
|-------|-------------|
| [`brainstorming`](brainstorming/SKILL.md) | Weighing options — diverge into distinct approaches, then converge on a recommendation |
| [`planning`](planning/SKILL.md) | A non-trivial change — produce an ordered, verifiable plan with risks/rollback before coding |
| [`debugging`](debugging/SKILL.md) | Something's broken — evidence-first root-cause method (reproduce, timeline, isolate, verify) |
| [`write-tests`](write-tests/SKILL.md) | Adding/extending pytest with BigQuery + Playwright mocked; reproducing a bug as a failing test |
| [`explain-codebase`](explain-codebase/SKILL.md) | Onboarding or "where does X happen" — grounded, file-cited walkthroughs |
| [`data-exploration`](data-exploration/SKILL.md) | Ad-hoc BigQuery — dry-run-first, cost-aware, read-only by default |

### Repo-specific workflows

| Skill | Use it when |
|-------|-------------|
| [`dataform-model`](dataform-model/SKILL.md) | Adding/modifying a Dataform model — source decl, config/incremental, `ref()` wiring, validation |
| [`release`](release/SKILL.md) | Cutting a release — SemVer bump + Keep-a-Changelog entry → auto-tag via the Tag Release workflow |

> Built-in Claude Code skills (`/code-review`, `/simplify`, `/verify`, `/run`, …) are
> intentionally not duplicated here.

## Adding a skill

Create `.claude/skills/<name>/SKILL.md` with YAML frontmatter:

```markdown
---
name: <kebab-case-name>            # must match the directory
description: <what it does and, crucially, WHEN to use it — this drives auto-selection>
---

# <Title>

<the workflow: steps, repo-specific gotchas, commands to run>
```

Keep it focused, ground repo-specific claims in real files/paths, and add a row to the
table above.
