# Bug: read opens retired notifications and cancellation lost cleanup failures

- Fix / issue: PR #1353, review repair at `b8b8ccc535fe864a16dea0800ef841c07b12748e`;
  TXN-503/504, TXN-802 and JAPI-103.
- Symptom: a successful cold read open could stop future hooks and notifications; cancellation
  or reset could silently lose a worker's first-close-only reader failure.
- Trigger: hold a listener on resource A while opening JSON resource B with valid-time paths but
  no derived declaration. Separately, cancel a producer whose reader closes and then throws only
  on its first close. Its worker records the failure; a second close succeeds.
- Root cause: optional derived initialization acquired notification retirement before deciding
  whether any initialization was needed. Cancellation fenced the helper but ignored its recorded
  failure, unlike exhaustion. A static JVM teardown hook also introduced global ownership that
  the explicit lifecycle contract did not require.
- Why tests missed it: open tests covered warm sessions and declared derived resources, while
  cleanup fixtures threw on every close and therefore hid lost first-attempt errors.
- Regression tests: `sessionOpenWithoutDerivedDeclarationsPreservesAdmission` checks cold/warm
  direct/handle opens, a subsequent synchronous hook and asynchronous delivery;
  `txn503CancellationPreservesFirstCloseFailure` checks JSON/XML close, reset and interrupted
  consumption with a real owned reader; `setExhaustionFencesUnusedProducerAndReleasesOwnedReader`
  includes first-close-only failures for both intersection directions and except;
  `txn503CancellationAccumulatesLaterCleanupFailure` checks primary/suppressed error identity.
- Correction: preflight derived initialization before retirement and revalidate afterward;
  transfer each helper failure once after termination at close/reset/exhaustion, accumulate later
  cleanup failures and preserve interruption precedence. Keep traversal failure separate from
  independent close failure so requested nested cancellation discards only the traversal
  interruption, retaining its suppressed failures and any cleanup interruption. Remove automatic database shutdown,
  retaining explicit close/shutdown APIs. No per-record or per-page paths change.
- Verification: the final focused verification passed all 472 selected cases with no failures,
  errors or skips: 361 notification, 47 concurrent-axis, six derived-initialization and 58 database
  ownership cases. Command: `:sirix-core:test --tests io.sirix.access.DatabaseNotificationLifecycleTest
  --tests io.sirix.axis.concurrent.ConcurrentAxisLifecycleTest
  --tests io.sirix.index.aggregate.DerivedIndexMaintainerTest
  --tests io.sirix.access.DatabaseOwnershipTest`, through the admitted `heavy.sh` wrapper with
  Temurin 25 and the existing Docker-free task-graph guard. Automatic startup dependencies also
  completed; no full repository suite or lint suite ran. Log and copied XML are under
  `build/review-103/final.log` and `build/review-103/final-results/`. No before-fix run
  was performed because this phase requires all fixes before verification; failure reproduction
  is established by static traces, not claimed executable evidence. Full PIT, budgets and
  compiled-success-path acceptance remain pending outer pipeline validation.
- First focused run: 465 cases, 452 passed and 13 failed. All 361 notification, 58 ownership and
  six derived-initialization cases passed. The axis failures exposed expected nested cancellation
  being propagated as failure, including six unchanged successful set-exhaustion controls. These
  failures justify the helper's separate traversal/cleanup outcomes; the original success
  assertions remain unchanged. The final focused round also checks interrupted cleanup identity
  and unrequested traversal interruption.
- Lesson: [check optional work before retirement and collect worker outcomes](../LESSONS.md#check-optional-work-before-retirement-and-collect-worker-outcomes).

Caller trace after correction: direct and handle opens both finish through the shared preflight;
lazy explicit initialization still initializes when no declaration exists, and declared eager
initialization rechecks under the database monitor after notification quiescence. No monitor is
held while waiting for notifications. Normal exhaustion reports the helper outcome once; early
close attempts producer cleanup even after worker failure; reset reports failure before replacing
the helper or resetting the producer. Interrupted consumption retains its threaded exception and
suppresses cleanup failure after fencing. Set exhaustion, union close and query stream forwarding
use these same axis boundaries. Ordinary successful reset and explicit database close/shutdown
retain their previous ownership paths. No callback/shutdown replacement machinery is introduced.
