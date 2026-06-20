# deepest-crawl Plans

This document contains accepted future direction only.

## Approved Directions

### Publish Current Dashboard Crawler Work

- Outcome: The current local WebUI, service manager, agentic crawl, extraction,
  verifier, verification-handling, and bulk-selection work is available in a
  public GitHub repository with sanitized public-facing documentation.
- Why this is accepted: The operator explicitly noticed that the work has not
  been uploaded yet, and the current implementation is too substantial to remain
  local-only.
- Expected value: Preserves the work, enables review, and creates a stable base
  for continued dashboard/crawler hardening.
- Preconditions: Sanitization commit, authenticated GitHub publishing path.
- Earliest likely start: in progress.
- Related ids: none

### Dashboard-Driven Crawl Hardening

- Outcome: Dashboard-selected crawl batches can run with stable UI state,
  controlled pacing, visible browser state, non-looping verification behavior,
  clean tab lifecycle, and accurate sidebar status updates.
- Why this is accepted: The operator is actively using selected/bulk dashboard
  crawls and has found practical workflow bugs during real runs.
- Expected value: Makes the tool usable for long selected crawl batches without
  constant manual recovery.
- Preconditions: Live Chrome transport and MLX brain.
- Earliest likely start: now, continuing from current implementation.
- Related ids: none

### Dependency And Rebuild Hygiene

- Outcome: A clean environment rebuild can install all declared dependencies and
  run the dashboard without targeted manual package installs.
- Why this is accepted: Public collaborators need a reproducible setup path, and
  dependency metadata has changed during publication cleanup.
- Expected value: Reduces setup drift and makes GitHub/CI/local rebuilds
  reproducible.
- Preconditions: Sanitization commit and first public push.
- Earliest likely start: before or during publication cleanup.
- Related ids: none

### Reliable Smoke Path

- Outcome: A one-command smoke path verifies imports, local services, Chrome
  transport, dashboard service readiness, and one representative crawl.
- Why this is accepted: Dashboard functionality is now broad enough that
  targeted smoke coverage is needed before large batches or PR review.
- Expected value: Faster confidence before running real-profile crawl batches.
- Preconditions: Dependency sync issue resolved or documented; representative
  test URL selected.
- Earliest likely start: after initial publication scope is chosen.
- Related ids: none

### Security-Verification Handling

- Outcome: Verification pages are handled by wait/visible interaction/screenshot
  vision where possible, never by reload loops, and persistent verification
  produces a clear per-URL failure instead of blocking a batch.
- Why this is accepted: Real target sites frequently present Cloudflare or
  similar checks, and the operator wants the agent to solve visible controls
  when possible.
- Expected value: Fewer stuck crawls and less accidental Wayback use for pages
  that are temporarily challenge-gated rather than content-down.
- Preconditions: Real Chrome profile and screenshot capture available.
- Earliest likely start: now, with continued tuning from observed failures.
- Related ids: none

## Sequencing

### Near Term

- Initiative: Sanitize public-facing docs and records.
  - Why now: The repo is about to become public and should not expose
    private-machine assumptions as project requirements.
  - Dependencies: none
  - Related ids: none
- Initiative: Create or attach GitHub remote and push publicly.
  - Why now: The implementation is ready to share with collaborators.
  - Dependencies: Sanitization commit and authenticated GitHub CLI.
  - Related ids: none
- Initiative: Run a representative selected dashboard batch.
  - Why now: The latest changes affect captcha clicks, sidebar refresh, and tab
    cleanup.
  - Dependencies: Chrome and Holo/MLX brain ready.
  - Related ids: none

### Mid Term

- Initiative: Resolve dependency sync mismatch.
  - Why later: It is a publication/rebuild hygiene issue, not a blocker for the
    currently running local dashboard.
  - Dependencies: Package availability and dependency audit.
  - Related ids: none
- Initiative: Add focused smoke checks for dashboard services, queue operations,
  and representative agentic crawls.
  - Why later: Tests should follow after publication scope is stabilized.
  - Dependencies: Reliable representative URLs.
  - Related ids: none

### Deferred But Accepted

- Initiative: Broaden per-site extractor coverage.
  - Why deferred: Generated extractors should be driven by observed crawl
    failures, not preemptive scaffolding.
  - Revisit trigger: Repeated thin-DOM or blocked pages for the same host.
  - Related ids: none
