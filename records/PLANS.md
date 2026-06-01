# deepest-crawl Plans

This document contains accepted future direction only.

## Approved Directions

### Reliable Smoke Path

- Outcome: A one-command smoke path verifies imports, local services, Chrome
  transport, and one representative crawl.
- Why this is accepted: Current smoke readiness is blocked by live services, and
  the first input URL is not guaranteed to be a useful representative page.
- Expected value: Faster confidence before running large crawl batches.
- Preconditions: Brain server and OBU transport are available.
- Earliest likely start: immediately after repo-template adoption.
- Related ids: none

### Dashboard Integration

- Outcome: The normal runner can optionally publish live state to the dashboard
  without a separate disconnected process.
- Why this is accepted: Dashboard scaffolding exists, but the standard runner
  does not call the instrumentation patch.
- Expected value: Makes crawl behavior inspectable during long runs.
- Preconditions: Smoke path works.
- Earliest likely start: after the reliable smoke path.
- Related ids: none

### Link-Set Safety Review

- Outcome: The large Eastself-derived link set is filtered or sampled before
  real-profile crawling.
- Why this is accepted: The current input contains thousands of arbitrary links,
  and this crawler intentionally uses the operator's real Chrome profile.
- Expected value: Reduces exposure and improves crawl signal.
- Preconditions: Operator confirms acceptable crawl policy.
- Earliest likely start: before any large batch crawl.
- Related ids: none

## Sequencing

### Near Term

- Initiative: Finish repo-template adoption and initial commit.
  - Why now: Future work needs local provenance and commit enforcement.
  - Dependencies: none
  - Related ids: none
- Initiative: Run live smoke tests.
  - Why now: End-to-end functionality is currently unproven.
  - Dependencies: Brain server and OBU Chrome transport.
  - Related ids: none

### Mid Term

- Initiative: Wire dashboard instrumentation into the runner CLI.
  - Why later: It is useful only after the base crawl path is verified.
  - Dependencies: Reliable smoke path.
  - Related ids: none

### Deferred But Accepted

- Initiative: Broaden per-site extractor coverage.
  - Why deferred: Generated extractors should be driven by observed crawl
    failures, not preemptive scaffolding.
  - Revisit trigger: Repeated thin-DOM or blocked pages for the same host.
  - Related ids: none
