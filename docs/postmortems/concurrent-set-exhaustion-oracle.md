# Bug: concurrent set exhaustion lacked an ownership oracle

- Fix / issue: PR #1353, CI findings ci-2 and ci-3.
- Symptom: PIT could remove `ConcurrentExceptAxis.done()`'s close call without
  running an exhaustion regression; a result could finish while an unused operand
  still owned a running producer and its resources.
- Trigger: exhaust an empty first operand while the second operand has published
  a result and is waiting to produce more. Intersection has the same obligation
  with either operand exhausted; union needs both operands exhausted.
- Root cause: explicit-close tests asserted cleanup calls, but did not establish
  that natural exhaustion itself reaches the producer termination boundary.
- Why tests missed it: the existing lifecycle suite exercised cancellation,
  reset, error suppression and single-axis exhaustion, not set-axis exhaustion
  with live unused work.
- Regression specification: extend `ConcurrentAxisLifecycleTest` with real
  concurrent producers, separate consumer and producer transactions, and a
  producer-owned reader. Hold publication with a latch, observe the live reader,
  then exhaust the result. Before any test cleanup, assert executor termination,
  closure of the owned reader, and continued usability of borrowed readers.
  Cover except and both intersection operand directions for JSON and XML. The oracle is the
  `Axis.close()` ownership contract and the set operations' empty-set identities.
  Removing the exhaustion close must fail an immediate assertion, with cleanup
  in `finally`, rather than strand the test until PIT's timeout.
- Error cases: retain the existing independent-close/suppressed-error tests;
  do not close borrowed transactions or change the steady-state row loop.
- Validation: guarded CI-configured Error Prone/NullAway compilation and Spotless
  passed. All seven affected suites passed (617 cases, including 27 in
  `ConcurrentAxisLifecycleTest`). Removing only the exhaustion close, while
  preserving line numbering, failed exactly the JSON and XML except cases at
  `exhaustion must terminate the unused producer` (0.789 s and 0.052 s).
  Both were `AssertionFailedError`, not timeouts; all four intersection cases
  passed. Production source was restored byte-for-byte before PIT.
  The exact original PIT part 3 then completed (13 targets, nine test patterns,
  four mutants/class): 6 KILLED, 13 SURVIVED, 16 NO_COVERAGE, 0 TIMED_OUT.
  Both changed-line mutants were KILLED; line 159 was killed by the new XML
  exhaustion case after one test. All 29 survivors/uncovered mutants are on
  unchanged lines. This supplemental partition does not replace the driver's
  required complete four-partition campaign; original parts 1/2's empty XML
  and 270-second limits are not acceptance evidence.
- Lesson: assert ownership release at the public terminal iterator operation,
  before a test's explicit close can hide a missing exhaustion fence; see
  [Lessons](../LESSONS.md).

## Remaining-core timing investigation

Main run 44126759879 uploaded complete XML (15,783 standard-task cases,
1,166.691 seconds summed class execution). Run 44177385323 exceeded 35 minutes
and skipped XML upload; `gh-axi`'s log routes fail (escape refusal through API,
UNKNOWN through run view). No PR per-test duration is inferred from that timeout.

The guarded local baseline records per-case durations through Gradle's test
listener. Its first 18 `JsonIdentityGraphGeneratedTest` cases take approximately
1.6–2.6 times main's corresponding cases, whereas the new ownership suites take
seconds, not minutes. The new concrete writer spy and final-class inline mocks
retransform shared classes used by later real operations. The investigation is
an A/B test of removing that instrumentation outside its owning suites.

The complete pre-repair remaining lane passed in 43m48s: 16,401 cases, 99
existing skips, zero failures, and 2,513.414 seconds summed class execution.
The 144-case identity-history matrix took 247.632 seconds (main: 123.281);
`ConcurrentAxisTest` took 300.634 seconds (main: 108.219). By comparison,
hash equivalence took 37.103 seconds (main: 35.149). The new notification and
shared-callback suites took seconds, so repeated lifecycle deadlines do not
account for this broad slowdown. Post-repair full-lane comparison is pending.

Invariant: fault-injection instrumentation must be released after the owning
test suite, before unrelated real storage/cache work executes. Relevant new
sites are the writer spy, valid-time cache and pool mocks in
`AbstractResourceSessionWriteFailureCleanupTest`, the guard mock in
`NodeTransactionAbortLifecycleTest`, the valid-time cache mock in
`SharedCallbackLifetimeTest`, the manager/cache mocks in
`BufferManagerTeardownTest`, and concrete/static injection in
`DatabaseNotificationLifecycleTest` and `AsyncFlushFailurePathTest`.
Replace the concrete writer spy with an interface delegate; after each owning
suite, release remaining required Mockito instrumentation with its supported
cache cleanup under JUnit isolation, so cleanup cannot affect concurrently executing mocks.
Preserve every existing assertion, fixture join and ownership
fence. Compare the same full remaining lane before and after; elapsed times
are diagnostic evidence, never test assertions or expanded CI budgets.

The cache-clearing experiment did not recover identity-history throughput
(247.5 seconds before, 244.5 after). It is not accepted as the root-cause fix.
The next experiment removes those cleanup callbacks and uses JUnit's built-in
order annotation consumer to execute the global fault-injection suites
after normal workloads, under isolation. Their class names, test patterns,
cases and assertions remain intact. This tests the invariant that runtime
class redefinition must not contaminate unrelated hot-workload execution.
The boundary includes all eleven changed suites introducing mocks: database
notifications, database ownership, resource-session write-failure cleanup,
node abort, shared callback lifetime, async-flush failure, concurrent-axis
lifecycle, buffer-manager teardown, parallel projection walks, RAM truncation,
and worker termination. No RAM marker or before-submit probe is changed.
The cache-clearing full run passed all 16,404 cases with the same 99 skips but
took 38 minutes, so that experiment is not accepted as a duration fix.
