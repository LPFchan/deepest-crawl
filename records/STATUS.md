# deepest-crawl Status

## Snapshot

- Last updated: 2026-06-02 03-12-45 KST
- Overall posture: `active`
- Current focus: Make the repo operable and smoke-testable.
- Highest-priority blocker: The local MLX-VLM brain and OBU Chrome transport
  must both be running before end-to-end smoke tests can crawl.
- Next operator decision needed: Confirm whether real-profile bulk crawling is
  acceptable for the current `inputs/links.json` set.
- Related decisions: none yet

## Current State Summary

The Python modules compile and the core imports pass. The crawler has a real
runner, Chrome engine, DOM/screenshot perception policy, brain client,
self-amending skill store, and dashboard scaffolding. The current session cannot
complete an end-to-end crawl because `127.0.0.1:8765` is not serving the brain
and `/tmp/open-browser-use/active.json` is absent, meaning the OBU extension
transport is not active.

## Active Phases Or Tracks

### Smoke-Test Readiness

- Goal: Run `_smoke.py --crawl N` successfully against live local services.
- Status: `blocked`
- Why this matters now: It proves the real-browser, perception, and summarizer
  path before large crawls.
- Current work: One stale smoke argument was removed; live services remain the
  blocker.
- Exit criteria: `_smoke.py` passes and `_smoke.py --crawl 1` produces an `ok`
  summary record.
- Dependencies: `./serve-brain.sh`; real Chrome with Open Browser Use extension.
- Risks: The first link in `inputs/links.json` may be a poor smoke target.
- Related ids: none

### Repo-Template Adoption

- Goal: Make the project a Git repo using LPFchan/repo-template conventions.
- Status: `in progress`
- Why this matters now: Commit provenance and repo-local truth surfaces are
  needed before further agent work.
- Current work: Template scaffold, hooks, scripts, and project records are being
  installed.
- Exit criteria: Initial commit lands with repo-template commit metadata and
  local hooks configured.
- Dependencies: none
- Risks: The root `skills/` directory now has two roles: repo-template workflow
  skills and generated per-site crawl extractors.
- Related ids: none

## Active Blockers And Risks

- Blocker or risk: Brain server is not running.
  - Effect: `deepest.runner` exits before connecting to Chrome.
  - Owner: operator
  - Mitigation: Start `./serve-brain.sh` and verify `/v1/models`.
  - Related ids: none
- Blocker or risk: OBU Chrome transport is not active.
  - Effect: `_smoke.py` cannot connect to real Chrome.
  - Owner: operator
  - Mitigation: Open real Chrome with the Open Browser Use extension enabled and
    verify `open-browser-use info`.
  - Related ids: none
- Blocker or risk: Real-profile crawling touches all target links with the
  operator's browser context.
  - Effect: Bulk crawl may expose cookies, identity, or extension behavior to
    arbitrary sites.
  - Owner: operator
  - Mitigation: Review or filter the link set before large runs.
  - Related ids: none

## Immediate Next Steps

- Next: Start and verify the brain server.
  - Owner: operator
  - Trigger: Before any crawl smoke test.
  - Related ids: none
- Next: Start and verify the OBU Chrome transport.
  - Owner: operator
  - Trigger: Before any crawl smoke test.
  - Related ids: none
- Next: Choose a representative smoke URL instead of relying on the first input
  link.
  - Owner: orchestrator
  - Trigger: After live services are available.
  - Related ids: none
