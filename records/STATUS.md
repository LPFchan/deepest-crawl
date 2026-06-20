# deepest-crawl Status

## Snapshot

- Last updated: 2026-06-21 05-55-00 KST
- Overall posture: `active`
- Current focus: Stabilize the dashboard crawler and keep public-facing setup
  documentation portable.
- Highest-priority blocker: Public repository setup still needs a GitHub remote
  and push.
- Next operator decision needed: None; the operator requested public GitHub
  publication.
- Related decisions: none yet

## Current State Summary

The dashboard runs as the primary operator surface. The service layer can launch
or reuse a local MLX brain and the Open Browser Use Chrome transport.

The crawler has moved beyond dashboard scaffolding. It supports a filtered URL
queue, status filters, multi-select `Crawl Selected`, cancelation, crawl
timeouts, bulk delay/jitter, live browser screenshots, merged agent/tool trace,
domain memory/playbooks, article extraction, Wayback fallback for true
content-down pages, same-brain summary verification, security-verification
handling, and post-job tab cleanup.

The dashboard implementation is committed locally. The remaining publication
work is to sanitize public-facing text, commit that cleanup, create or attach a
GitHub remote, and push the repository publicly.

## Active Phases Or Tracks

### Dashboard-First Crawl Operation

- Goal: Make the dashboard the reliable operator workflow for selected and bulk
  deep crawls.
- Status: `in progress`
- Why this matters now: The operator is actively using dashboard-driven crawls
  against large selected URL batches.
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
- Risks: Clean-environment rebuild still needs a fresh smoke run after the
  public dependency cleanup.
- Related ids: none

### Repo Publication

- Goal: Commit and publish the current local implementation to GitHub using the
  repo-template provenance rules.
- Status: `in progress`
- Why this matters now: The operator noticed the changes have not been uploaded
  yet.
- Current work: Implementation commits exist; public-facing docs and records are
  being generalized before the first GitHub push.
- Exit criteria: Sanitization commit lands, an `origin` remote exists, the repo
  is pushed, and GitHub visibility is public.
- Dependencies: Authenticated GitHub CLI or equivalent GitHub publishing path.
- Risks: Public docs must not expose private machine paths, credentials,
  personal operator identity, or local-only setup claims.
- Related ids: none

## Active Blockers And Risks

- Blocker or risk: Repository is not pushed to GitHub yet.
  - Effect: Work cannot be shared with external collaborators through GitHub.
  - Owner: operator/orchestrator
  - Mitigation: Commit sanitization, create or attach a public GitHub remote,
    and push `main`.
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
- Blocker or risk: Project dependency sync needs revalidation.
  - Effect: A fresh checkout may still expose packaging or platform assumptions
    even after the public dependency cleanup.
  - Owner: orchestrator
  - Mitigation: Run a clean install/smoke path after the first public push.
  - Related ids: none

## Immediate Next Steps

- Next: Finish public-facing sanitization and commit it.
  - Owner: operator
  - Trigger: Before first public push.
  - Related ids: none
- Next: Create or attach the GitHub remote and push `main` publicly.
  - Owner: orchestrator/operator
  - Trigger: After sanitization commit.
  - Related ids: none
- Next: Run a representative dashboard-selected batch after the latest
  verification-click and tab-cleanup changes.
  - Owner: operator/orchestrator
  - Trigger: Before calling the dashboard path stable.
  - Related ids: none
