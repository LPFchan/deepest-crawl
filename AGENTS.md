<!-- template-managed:begin -->
# Agent Instructions

`AGENTS.md` is the canonical editable agent-instructions file. It enforces repo behavior while deferring canonical policy to `records/REPO.md`.

## Read First

- `records/REPO.md`
- `records/SPEC.md`
- `records/STATUS.md`
- `records/PLANS.md`
- `records/INBOX.md`
- `skills/README.md`

Before writing into an artifact directory, read its `README.md` and follow its prescriptive shape when it defines one.

## Skills

Load the skill before the trigger condition fires. Each skill defines the procedure; follow it.

| Trigger | Skill |
| --- | --- |
| Before creating a normal commit | `skills/commit-generator/SKILL.md` |
| Before any destructive file edit (replace, delete, rewrite) | `skills/clean-correction-gate/SKILL.md` |
| When routing work or creating repo artifacts | `skills/repo-orchestrator/SKILL.md` |
| When reviewing inbox pressure | `skills/daily-inbox-pressure-review/SKILL.md` |
| When reviewing upstream changes | `skills/upstream-intake/SKILL.md` |
| When sharpening or iteratively refining an artifact | `skills/sharpen-the-tip/SKILL.md` |
| When prototyping, greenfield building, or working pre-MVP | `skills/prototype-mode/SKILL.md` |

## Rules

- Keep durable truth in repo files, not only in external tools.
- Route work using the routing ladder in `records/REPO.md`.
- Preserve the boundary between `records/SPEC.md`, `records/STATUS.md`, `records/PLANS.md`, `records/INBOX.md`, `records/research/`, `records/decisions/`, commit-backed `LOG-*`, and `records/upstream-intake/`.
- Worker agents produce evidence, proposals, and compliant `LOG-*` commits. The orchestrator or operator owns truth-doc updates unless the operator explicitly allows otherwise.
- Treat `records/INBOX.md` as pressure, not a backlog. Cluster capture; promote only survived triage.
- Promote sparsely. Do not mirror one thought into research, decisions, plans, spec, status, upstream, and execution records.
- Every normal commit must be created from a skeleton registered by `scripts/new-commit-message.sh` and must pass local and remote provenance checks.
- Follow the stable-ID and provenance rules in `records/REPO.md`.
- Do not put `LOG-*` ids inside `artifacts:`.
- Do not invent a document shape when the repo already provides a canonical surface, directory `README.md`, or template.
- Do not promote exploratory debate into truth docs or decisions until there is a concise accepted outcome.
- Do not turn an inbox review into a digest of every low-confidence idea. Report counts or clusters.
- Do not write chatty transcripts where the repo expects normalized records.
- Do not bypass commit provenance checks unless the commit is an explicit bootstrap or migration exception.
<!-- template-managed:end -->

## Repo-Specific Rules

The managed section above is updated by `scripts/sync-from-template.sh`.
Keep repo-specific instructions below the end marker; sync preserves this tail.

Local operators may keep private machine-specific instructions outside this
repository. Do not commit local shell wrappers, credentials, tokens, or personal
paths into this file.

Project-specific note: deepest-crawl also stores validated generated crawl
extractors under `skills/<host>/extract.py`. Those host directories are runtime
extractor cache entries, not repo-template workflow skills unless they contain a
`SKILL.md`.
