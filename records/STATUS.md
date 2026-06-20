# deepest-crawl Status

## Snapshot

- Last updated: 2026-06-21 05-39-42 KST
- Overall posture: `active`
- Current focus: Stabilize the dashboard crawler and prepare the uncommitted
  WebUI/agent changes for GitHub publication.
- Highest-priority blocker: The current implementation work is still local and
  uncommitted; commit provenance and push/PR work remain.
- Next operator decision needed: Confirm whether to publish the current local
  changes as one PR or split them into smaller commits/PRs.
- Related decisions: none yet

## Current State Summary

The dashboard now runs as the primary operator surface on `0.0.0.0:8766`. The
service layer can launch or reuse the local MLX brain and the Open Browser Use
Chrome transport, and the current operator session has both Chrome and the
Holo/MLX brain reporting ready.

The crawler has moved beyond dashboard scaffolding. It supports a filtered URL
queue, status filters, multi-select `Crawl Selected`, cancelation, crawl
timeouts, bulk delay/jitter, live browser screenshots, merged agent/tool trace,
domain memory/playbooks, article extraction, Wayback fallback for true
content-down pages, same-brain summary verification, security-verification
handling, and post-job tab cleanup.

The working tree is dirty with broad dashboard, service, Chrome, perception,
runner, dependency, and script changes. These changes have not been committed or
pushed.

## Active Phases Or Tracks

### Dashboard-First Crawl Operation

- Goal: Make the dashboard the reliable operator workflow for selected and bulk
  deep crawls.
- Status: `in progress`
- Why this matters now: The operator is actively using dashboard-driven crawls
  against Eastself-derived URLs.
- Current work: Service startup, queue filtering, multi-select bulk crawl,
  summary verification, security-verification handling, sidebar refresh
  behavior, and tab cleanup have been implemented locally.
- Exit criteria: A representative selected batch can run without UI regressions,
  stuck verification loops, stale sidebar state, or orphaned crawler tabs.
- Dependencies: Real Chrome with Open Browser Use extension; local Holo/MLX
  brain; operator approval for real-profile crawl exposure.
- Risks: Cloudflare/Turnstile-like pages can still require human interaction;
  visual checkbox detection is heuristic and target-site behavior may change.
- Related ids: none

### Local-Service Readiness

- Goal: Dashboard can bring up Chrome transport and the MLX brain without manual
  shell choreography.
- Status: `mostly working`
- Why this matters now: Agentic crawls depend on both services being ready before
  a job starts.
- Current work: `/services/start`, model selection, Holo model support, service
  status chips, and startup polling are in place.
- Exit criteria: A cold dashboard start reliably reaches `brain=True` and
  `chrome=True`, and failures surface actionable log tails.
- Dependencies: Installed MLX model paths, Open Browser Use extension, local
  Chrome profile.
- Risks: Project-wide `uv sync` is currently blocked by a `browser-harness`
  version mismatch, so new extraction dependencies were installed into `.venv`
  with targeted `uv pip install`.
- Related ids: none

### Repo Publication

- Goal: Commit and publish the current local implementation to GitHub using the
  repo-template provenance rules.
- Status: `not started`
- Why this matters now: The operator noticed the changes have not been uploaded
  yet.
- Current work: None; no commit has been created.
- Exit criteria: Local diff is reviewed, scoped into one or more compliant
  commit-message skeletons, committed, pushed, and opened as a PR or published
  to the intended branch.
- Dependencies: Operator choice on commit/PR granularity.
- Risks: The worktree includes many modified and untracked files; unrelated or
  partially experimental changes must not be accidentally bundled without
  review.
- Related ids: none

## Active Blockers And Risks

- Blocker or risk: Current implementation is not committed or pushed.
  - Effect: Work can be lost locally and cannot be reviewed through GitHub.
  - Owner: operator/orchestrator
  - Mitigation: Inspect the diff, choose scope, generate compliant commit
    skeletons, commit, push, and open a PR.
  - Related ids: none
- Blocker or risk: Real-profile crawling touches target sites with the
  operator's Chrome profile, cookies, extensions, and network identity.
  - Effect: Bulk crawl may expose identity or trigger target-site anti-bot
    systems.
  - Owner: operator
  - Mitigation: Use filtered/selected batches, delay/jitter, cancellation, and
    per-job timeouts.
  - Related ids: none
- Blocker or risk: Security-verification solving is heuristic.
  - Effect: Some verification widgets may fail to click or require human input.
  - Owner: orchestrator/operator
  - Mitigation: Prefer wait/visible interaction, use screenshot vision and
    visual checkbox fallback, and mark persistent verification as failed instead
    of looping forever.
  - Related ids: none
- Blocker or risk: Project dependency sync is inconsistent.
  - Effect: `uv sync` may fail until the `browser-harness` requirement is
    reconciled with available package versions.
  - Owner: orchestrator
  - Mitigation: Resolve the dependency constraint before relying on clean
    environment rebuilds.
  - Related ids: none

## Immediate Next Steps

- Next: Review and scope the current local diff.
  - Owner: orchestrator
  - Trigger: Before any commit or PR.
  - Related ids: none
- Next: Decide commit/PR granularity for publishing to GitHub.
  - Owner: operator
  - Trigger: After reviewing the diff scope.
  - Related ids: none
- Next: Run a representative dashboard-selected batch after the latest
  verification-click and tab-cleanup changes.
  - Owner: operator/orchestrator
  - Trigger: Before calling the dashboard path stable.
  - Related ids: none
