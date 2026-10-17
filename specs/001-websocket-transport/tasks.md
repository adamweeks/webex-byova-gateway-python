# Tasks: BYOVA WebSocket Transport

## Implemented baseline

- [x] T001 [US1] Add independent transport/datasource selection and eligible-agent routing.
- [x] T002 [US2] Add strict JSON models, shared-engine adapter, and full-duplex listener.
- [x] T003 [US2] Preserve DTMF collection, terminal ordering, and peer-owned closure.
- [x] T004 [US4] Add the native CES WebSocket connector and local fake-provider tests.

## Local readiness

- [x] T005 [US3] Reproduce cancellation, saturated cleanup, and short-tail framing defects.
- [x] T006 [US3] Fix actual-job capacity accounting and skip cancelled queued side effects.
- [x] T007 [US3] Reserve bounded cleanup capacity and preserve queued finalizers on shutdown.
  Wait for actual per-conversation work before teardown, including late startup;
  treat executor-cancelled jobs as finished without skipping existing provider cleanup.
- [x] T008 [US2] Rebalance audio tails and remove redundant base64 transformations.
- [x] T009 [US1] Correct transport docs and prepare the live matrix and Spec Kit artifacts.
- [x] T010 [US2] Run full local suites, lint/format, and real-listener smoke checks (SC-001–003).
- [x] T011 [US3] Complete self-review and verify the final committed runtime archive (FR-006).

## Live acceptance — operator follow-up

- [ ] T012 [US1] Verify the intended private target, deployed commit, JWT, and discovery.
- [ ] T013 [US2] Run Local Audio DTMF transfer/session-end tests with gateway evidence.
- [ ] T014 [US4] Run applicable GECX voice, no-input, barge-in, and handoff scenarios.
- [ ] T015 [US3] Verify disconnect, shutdown, cleanup, and gRPC regression on that target.
- [ ] T016 [US2] Record pass/fail/pending and evidence for every live matrix row (SC-004).

T012–T016 remain pending under local-ready delivery. T013–T016 depend on T012;
live acceptance depends on completion of the local readiness tasks.
