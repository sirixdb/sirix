# Bug: a packed join side that published between a reader's two lookups was packed again

- Fix / issue: this change; branch `fm/sirix-macos-join-budget-flake`
- Symptom: `IntervalRangeJoinWorkBudgetTest.bucketedSidesAreScopedToTheValidTimeSnapshot[1] FULL`
  failed on the `macos-latest / query` lane of PR 1472 (merge head `bd8dd9942`) with
  `joinGroup.bucketedSidesBuilt = 1, budget is exactly 0` at the step that asserts "the first window
  must reuse its completed packing", and passed on a re-run of the same merge commit. The Linux and
  Windows lanes were green on that head. Impact beyond the test: a repeated query at an immutable
  revision could re-bucket a join side that was already resident, paying its count-and-pack pass
  again and leaving a duplicate packed side in the `packed` share of the derived allowance until it
  was evicted. The answer stayed exact.
- Trigger: two executions of the dense-extrema join over the same immutable revision, where the
  background packing of the probe side published and left the in-flight map in the window between
  the second execution's resident lookup and its in-flight lookup. No input or revision shape is
  needed beyond the one the budget test already uses; what varies is the completion order of a
  background task, which is why the failure was confined to one loaded runner.
- Root cause: `SirixJoinedGroupAggregateExpr.BucketedSideCache.get` decided "no side available"
  from two lookups made in sequence — first the published sides, then the in-flight builds — while
  `packAsync`'s task moves the side from the second set to the first in exactly that order: it
  publishes into the partition and only then removes its future from `IN_FLIGHT`
  (`finishPacking`). A reader whose resident lookup ran before the publication and whose in-flight
  lookup ran after the removal therefore saw the side in neither set, although it had been
  continuously available: the publication is what removed it from the place the reader looked
  second. The reader then packed a second copy. The broken invariant is that the published set is
  authoritative only once no build is in flight; reading it *before* observing that no build is in
  flight proves nothing.
- Why tests missed it: every existing test pinned one of the two orders and so could not reach the
  window. `IntervalRangeJoinWorkBudgetTest` lets the real executor run the packing and only ever
  observed "finished long before" or "still in flight", the two outcomes the sequence handles;
  `concurrentReaderReusesReservedPackingWithoutDuplicatingItsConstruction` holds the task and runs
  it while a second reader is parked on the future, which exercises the in-flight branch; and
  `PackingOwnershipFailureTest` drives the producer's failure paths. None of them could place a
  completed publication between a reader's two lookups, so the only oracle that saw it was a work
  budget on a loaded macOS runner — which is the one oracle that cannot say which path grew, only
  that one did.
- Regression test: `BucketedSideReuseInterleavingTest`
  `aSidePublishedBetweenTheTwoLookupsIsReusedRatherThanPackedAgain` (`./gradlew :sirix-query:test
  --tests 'io.sirix.query.compiler.translator.BucketedSideReuseInterleavingTest'`). It holds the
  first execution's packing task through the packing-executor seam and runs it from the
  `resident-miss` handoff, a point `get` reaches only after its resident lookup missed, so the
  publication and the in-flight removal land inside the window by construction on every platform.
  Before the fix the repeat reports `joinGroup.bucketedSidesBuilt = 1` against a budget of exactly
  0 and submits a second packing of the same side; after it, 0 and none. The interleaving is driven,
  not awaited: there is no sleep, no timeout and no wall-clock assertion.
- Lesson: a cache whose lookup spans two sets must read them in the order the producer fills them,
  or re-read the first set once the second says nothing is pending; otherwise the work it saves
  depends on a completion order, and a work budget over it reads differently on a loaded machine.
  See [LESSONS.md](../LESSONS.md) (fragment
  `docs/lessons.d/bucketed-side-reuse-interleaving.md`, per the
  [fragment convention](../../changelog.d/README.md)).
