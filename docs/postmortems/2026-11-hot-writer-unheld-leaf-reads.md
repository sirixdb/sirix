# Bug: the HOT writer read sibling leaves it did not hold

- Fix / issue: [#1459](https://github.com/sirixdb/sirix/issues/1459). The defect is in `main` and older than the storage-codec work whose CI
  failure led to it ([#1454](https://github.com/sirixdb/sirix/pull/1454), which fixes the same
  mistake in the structural validator).
- Symptom: none was ever reported, and three instrumented runs of the eviction test did not catch
  the writer reading an evicted leaf. What the regression tests show the unfixed writer does once
  the page cache evicts a sibling leaf in the middle of a put:
  - **A malformed index, without an error.** A put whose key would land below the last key of the
    neighbouring leaf has to be declined and placed by the frontier. With that neighbour evicted
    during the comparison, the writer read a zeroed frame, took the neighbour's last key for a
    shorter one, merged the key in place, and returned normally. The index then failed the
    structural invariant: two sibling ranges interleave.
  - **Wrong entries in the index.** Leaf consolidation copied the bytes of whichever page had
    taken an evicted sibling's frame into its merged leaf, and the writer publishes that leaf:
    `HOTIncrementalInsertLeafRetentionTest` read back a value byte of a foreign leaf (`112` where
    the sibling stored `0`). Nothing fails at the time; the entries are simply not the ones that
    were stored.
  - **A put that ends its transaction on a sound index.** With the source leaf of a two-leaf strand
    migration evicted, the put threw `MutationTraversalRefusal: … cannot complete mandatory bounded
    mutation traversal 'existing-key routing-to-new-slot' (status=UNRESOLVABLE_REFERENCE …)`. With a
    sibling evicted that an earlier put had left resolved, the periodic consolidation threw
    `IllegalStateException: HOT structural height requires a resolved child page`. An ordered merge
    beside an evicted neighbour was sent to the frontier, which threw `HOT incremental frontier
    inputs overlap at ordered child 2`. Under the generated streams the first failure of every case
    was `HOT structural validation cannot safely inspect its scope: status=UNRESOLVABLE_REFERENCE`.
    Each of them marks the transaction rollback-only.
  - **Maintenance that silently does not happen.** With a zeroed frame the consolidation of
    mergeable leaves was skipped or cut short at 11 of the 42 eviction points of one put.
- Trigger: a write transaction on a HOT index (CAS, PATH, NAME, valid-time, projection, temporal
  aggregate or change log) whose put reads a leaf that this transaction has not changed, while the
  page cache evicts that leaf. Two things evict it: another thread that shares the global cache (a
  resource and its `$derived` resource always do), and the writing thread itself, whenever it loads
  another page or allocates a frame under pressure. A released frame keeps its bytes until it is
  reused, so the read is usually still right; it goes wrong when the frame is reused first.
- Root cause: `AbstractHOTIndexWriter.resolveHOTPageForTraversal(ref)` handed out a leaf of the page
  cache without a guard. A leaf the transaction log owns lives as long as the transaction; any other
  leaf is released by `HOTLeafPage.close()` as soon as its guard count is zero. Only two groups of
  callers held what they read: the copy-on-write of the key's own leaf, and the structural handlers
  that pin the *direct* children of the nodes they rewrite (`ChildPageGuards`). Every other read of a
  sibling went through the unguarded resolver: the first and last key of a subtree (about 35
  callers), the spine-order check of every put that extends its leaf's range, the strand guards that
  scan every key of a bounded set of leaves planned earlier, the bounded size preflight, and the two
  paths that copy entries, `tryTwoLeafMigration` and `HOTIncrementalInsert.consolidateNodeLeaves`.
  The broken invariant: a reader of leaf slot memory holds the leaf, or validates a stamp and
  discards what it read (`docs/HOT_INDEX_SPECIFICATION.md` §4.9.2 to §4.9.4). Several sites checked
  `isClosed()` first, which says nothing about the bytes read next.
- Why tests missed it: every structural test resolves leaves nothing evicts.
  `HOTStructuralChildEvictionTest` evicts on the writing thread at one hook, between child
  resolution and integration, where the direct children are pinned. The eviction tests with the
  right interleaving cannot make a stale read visible, because the frame still holds the old bytes.
  And no test evicted at the points where the writer itself gives the cache a chance: its calls into
  the storage engine.
- Regression test: `HOTIncrementalInsertLeafRetentionTest` (the consolidation primitive, with all
  four input leaves evicted from the page-key allocator before copying and recompression;
  released frames zero-filled, and in a second mode taken at once by foreign leaves),
  `HOTIndexWriterLeafRetentionTest` (the writer on production storage: six puts, each repeated with
  every unguarded leaf evicted at one engine call after the other and at all of them, plus
  reclaimability after normal and exceptional mutation exits) and
  `HOTStructuralPropertyTest.mutationsSurviveLeafEviction`
  (generated CAS, PATH and NAME streams under all four versioning types, with every unguarded leaf
  evicted at every engine call of every mutation). In the recorded baseline run with the two
  production files as on `main`, ten tests failed: the primitive skips the merge on zeroed frames,
  copies a foreign byte on recycled ones
  and merges a pair it could not hold; the writer holds none of three committed leaves in the middle
  of a put; the crossing merge is accepted at 2 of 318 eviction points and the ordered merge
  mishandled at 2 of 24; 11 of 42, 4 of 108 and 5 of 31 eviction points of the other three puts
  fail as described above; and all 12 generated cases fail. Those baseline observations include
  guard-count tests since removed in favor of logical-result and reclaimability checks; they are
  historical evidence, not the current test inventory.
  `HotWriterLeafRetentionAllocationBudgetTest` guards the cost instead and passes on both. Run them
  with `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOT*LeafRetention*' --tests
  'io.sirix.index.hot.HOTStructuralPropertyTest.mutationsSurviveLeafEviction'` and `./gradlew
  :sirix-core:dictionaryReadViewAllocationTest`.
- Lesson: an eviction test has to evict where the code under test yields, and has to take the luck
  out of a stale read; and a guard has a price that belongs on the scale before it goes into a path
  every put takes. See [Lessons](../LESSONS.md).

## Review follow-up: retry-deadline test isolation

- Fix / issue: review finding R1 on the #1459 fix.
- Symptom: a later copy-on-write test can report leaf retirement while its replacement is still
  becoming available, although the production retry budget accommodates that retirement.
- Trigger: run `HOTIndexWriterLeafRetentionTest.aRequiredReadThatKeepsLosingItsLeafFailsByName`
  before `HOTLeafWriterGuardTest.abstractIndexWriterYieldsTheCoreToTheThreadRetiringTheLeaf`
  in the same JVM.
- Root cause: the starvation test lowers a JVM-wide retry deadline to 20 ms, but its shared
  teardown restores only the poison flag and consolidation cadence. The later test's deliberate
  300 ms retirement then exceeds the leaked deadline rather than the shipped five-second bound.
- Why tests missed it: each class passes alone; the failure requires their ordered execution
  in one process.
- Regression test: execute the two tests above with
  `JAVA_TOOL_OPTIONS='-Djunit.jupiter.testclass.order.default=org.junit.jupiter.api.ClassOrderer$ClassName'`
  and `./gradlew :sirix-core:test`, selecting both methods with `--tests`. JUnit's class-name
  order puts the deadline override before the retirement. The existing oracle requires the
  copy-on-write to return the replacement after retirement; it rejects a premature timeout
  without adding a wall-clock assertion. The common `@AfterEach` must restore the retry deadline
  after both successful and failed tests.
- Lesson: isolation from concurrent tests does not restore static settings between sequential
  tests; reset every override in common teardown. See [Lessons](../LESSONS.md).

## Review follow-up: eligibility reads and benchmark schedules

- Fix / issue: review findings R1 and R2 on the #1459 fix.
- Symptom: standalone consolidation can throw `FrameReusedException` instead of skipping an
  evicted pair; the steady benchmark's inherited 180-shot schedule exhausts its 127-shot fixture.
- Trigger: either swizzled sibling is fully retired before eligibility inspection, or JMH runs
  the steady benchmark with its annotation defaults. Gradle's default 2/3 schedule instead
  finishes before the documented compilation plateau.
- Root cause: the pair's entry-count and side-reference checks preceded guard acquisition.
  The benchmark used one class-level schedule for fixtures with different reset lifetimes.
- Why tests missed it: the unguardable-pair test kept the retired right sibling alive with a
  test-owned guard, so its side references remained readable. The first review regression then
  closed the leaf before consolidation, so `PageReference.getPage()` cleared its swizzle and
  returned `null`; it skipped the pair without exercising the eligibility reads. Latency runs
  supplied explicit 60/60 overrides for the steady benchmark, hiding its inherited 120/60 annotations.
- Regression test: `HOTIncrementalInsertLeafRetentionTest.consolidationDeclinesWhenAnyChildCannotBeHeld`
  retires either member of the pair after the real `PageReference.getPage()` captures its live leaf
  but before returning that leaf to consolidation, with full and deferred retirement cases,
  and also covers children closed before resolution.
  The oracle is the primitive's pair-decline behavior:
  the same parent retains all four children, no replacement is allocated, and every acquired
  guard is released. The retention windows are documented on
  [`consolidateNodeLeaves`](../../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTIncrementalInsert.java).
  Restoring the pre-guard eligibility ordering made both fully retired pair cases throw
  `FrameReusedException` at `segmentRefCount()` instead of declining.
  `consolidationReleasesGuardsForIneligiblePairs` checks both side-reference exclusions and the
  count bound, including guard cleanup and unchanged inputs. The existing copy tests exercise
  successful and repeated merges under poisoned and recycled frames.
  For R2, run the steady benchmark directly through JMH without iteration overrides: its
  method-level annotations must complete 60 warm-up and 60 measurement shots within 127 slots.
  The class Javadoc gives separate Gradle commands for the 120/60 and 60/60 schedules.
- Lesson: guard eligibility reads as well as payload copies, exercise both sides after actual
  retirement, and verify benchmark schedules through the harness that consumes them. See
  [Lessons](../LESSONS.md).

## Review follow-up: surviving children during recompression

- Fix / issue: review finding R3 on the #1459 fix.
- Symptom: standalone consolidation throws from `heightOf(null)` after eviction of a surviving
  child, although its merged pair is guarded.
- Trigger: in the four-leaf `[0, 1, 2, 4]` fixture, allocation of the first merged leaf closes the
  third or fourth input leaf before `compressHalf` reads its height. A previously skipped,
  unavailable child can also remain in the node when a later pair is recompressed.
- Root cause: the retention scope followed the payload-copy pair, while recompression consumes
  every child reference. The production writer's outer guards hid the standalone ownership gap.
- Why tests missed it: allocation-time eviction targeted only the copied pair. The skip tests
  covered that pair, so neither surviving children nor a missing swizzle blocked a later merge.
- Resolution: operation-wide retention in the primitive was later narrowed for the paired latency
  gate. The final division of responsibility is owned by the
  [`consolidateNodeLeaves` contract](../../bundles/sirix-core/src/main/java/io/sirix/index/hot/HOTIncrementalInsert.java).
- Regression test: `consolidationCopiesOnlyFromLeavesItHolds` closes all four original leaves
  at allocation while its caller holds the surviving children. It requires one merge with all ten
  entries of the copied pair intact; the next attempt declines when it cannot guard a child whose
  retirement was requested. `consolidationAssertsItsCallerHoldsTheSurvivingChildren` and
  `consolidationAcceptsACallerThatHoldsTheSurvivingChildren` exercise the method precondition.
  The pair-decline cases are described in the eligibility follow-up above.
  `consolidationReleasesEveryChildOnFailure` fails each of the four allocations across the two
  merges, both with and without a throwing pair-frame releaser, and requires the original
  exception, no retirement requests and no leaked pair guards. The caller's survivor guards remain
  held until its own cleanup; after that, no input or speculative frames remain live. The earlier
  operation-wide implementation caught the `heightOf(null)` failure; the current tests enforce
  the narrowed caller contract. Run these cases through
  `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTIncrementalInsertLeafRetentionTest'`.
- Lesson: derive retention scope from every downstream consumer, including recompression metadata,
  rather than just the entries being copied. See [Lessons](../LESSONS.md).

## Review follow-up: private replacements exceeded the retention bound

- Fix / issue: review finding R1 on the #1459 fix.
- Symptom: repeated consolidation at full fanout throws `ArrayIndexOutOfBoundsException`, leaks
  an acquired replacement guard and frame, and makes a production writer's transaction rollback-only.
- Trigger: a parent with 32 leaf children, partials 0 through 31 and one entry per leaf. The first
  merge retains all 32 original leaves; later pair acquisition or survivor traversal encounters
  privately owned replacements absent from that set.
- Root cause: the retention bound counts original children, but retention also admitted new leaves
  that consolidation owns. Guard acquisition preceded the array-slot check, so the failing write
  left a guard unrecorded. The current acquisition boundary, `holdOriginalLeafOnce`, excludes owned
  replacements and checks slot capacity before acquiring a guard. Survivor ownership follows the
  method contract linked above.
- Why tests missed it: the four-child fixture had spare retention slots even after repeated merges.
- Regression test: `HOTIncrementalInsertLeafRetentionTest.fullFanoutConsolidationDoesNotRetainPrivateReplacements`
  exercises both public overloads. The specification's greedy merge and no-collapse rules require
  two 16-entry leaves containing exactly the 32 known input key/value pairs. It checks unchanged
  inputs, retirement of every original reference, zero original and final replacement guards,
  cleared scratch storage, and live-frame counts proving consumed replacements were freed rather
  than left guarded. With the old retention logic the second merge attempts slot 32 and throws.
  Run with `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTIncrementalInsertLeafRetentionTest'`.
- Lesson: size a bounded lifetime set by ownership, and exclude private replacements consistently
  at every acquisition site. See [Lessons](../LESSONS.md).

## Review follow-up: replay lost the eviction trigger

- Fix / issue: review finding R1 on recorded property failures.
- Symptom: a saved eviction-only regression could replay successfully without exercising eviction
  or poisoning the frames whose stale reads caused the original failure.
- Trigger: record a failure from `mutationsSurviveLeafEviction`, then replay its saved stream.
- Root cause: eviction lived on the test instance, poisoning was enabled outside case execution,
  and neither setting belonged to the serialized case. Shrinking inherited the instance's mode,
  while replay explicitly constructed a non-evicting runner.
- Required invariant: generation, shrinking and replay consume the same case configuration,
  including explicit `leafEviction` and `framePoisoning` header fields. Runner execution enables
  the recorded poisoning setting and clears it on normal and exceptional exits. Headers predating
  these fields retain their ordinary, non-evicting and non-poisoning semantics.
- Why tests missed it: live adversarial runs asserted eviction, but saved-case replay did not.
- Regression tests: the `recordedFailure*` tests in `HOTStructuralPropertyTest` check the emitted
  header against a literal replay-format contract, parse both new and legacy headers, replay a
  committed multi-leaf stream with and without eviction, and observe released frame bytes during
  case execution and after normal or exceptional exits. A replay that disables eviction rejects
  the required positive eviction count; missing poisoning reads the original sentinel byte instead
  of zero. Run with `./gradlew :sirix-core:test --tests
  'io.sirix.index.hot.HOTStructuralPropertyTest.recordedFailure*'`.
- Lesson: serialize adversarial conditions with the failing input, and assert their observable
  effects during replay. See [Lessons](../LESSONS.md).

## CI follow-up: retirement diagnostic coverage

- Fix / issue: [#1462 PIT run 44793643611](https://github.com/sirixdb/sirix/actions/runs/44793643611/job/136489524910).
- Symptom: the changed-source lane completed but reported two surviving mutants and one uncovered
  mutant on changed lines. No contract test mapped directly to the exception.
- Trigger: mutate `SirixLeafRetiredException`'s negative-duration boundary from `< 0` to `<= 0`,
  or replace `getPageKey()` with zero. The writer starvation test only supplies a positive wait
  and checks the failure type, message and elapsed wait, so neither change fails it.
- Root cause: the new public diagnostic contract had only integration coverage for its ordinary
  failure path. The invariant is that zero and positive waits are accepted, negative waits are
  rejected, and the failure preserves its supplied page key and elapsed nanoseconds. It applies
  to the constructor and both accessors; both read acquisition and copy-on-write retry expiry
  construct this same exception. No runtime change is needed.
- Regression test: `SirixLeafRetiredExceptionTest` executes that public contract using literal
  page key `73`, waits `0`, `1`, `2_000_000`, `Long.MAX_VALUE`, `-1` and `Long.MIN_VALUE`, and a
  worked millisecond diagnostic (`2_500_000` ns reports `2` ms). The PR resolver selects its
  declared class name automatically as a related test for the exception. Run with
  `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.SirixLeafRetiredExceptionTest'`.
- Lesson: test the diagnostic boundary as well as the failure path, using the automatic CI test
  selection. See [Lessons](../LESSONS.md).

Verification: all seven contract cases and `:sirix-core:spotlessJavaCheck` passed. A fresh
`pr_mutation.py select` against base `ec746e7861589e19733c9ae17e16cc0e5415dc67` and head
`13f696f6afe07f8f24774c952f7000123f3fc0c4`, followed by `run --seconds 1320 --blocking`,
completed with the same 24 target classes and 12-mutant class cap as CI. The exception now maps
to a test, and all four of its mutants are killed by the new class, including the two reported
gaps. Changed-line detection improved from 5/8 to 7/8, with zero uncovered changed-line mutants.
The blocking reporter exits 1 for the survivor below; PIT itself completed successfully.
Local logs, XML and summaries are under `build/ci-diagnostics/`.

At that snapshot, the remaining reported survivor was
`AbstractHOTIndexWriter$ChildPageGuards.retain`, line 8839,
`CONDITIONALS_BOUNDARY`: the reverse scan changes `i >= 0` to `i > 0`. It skips duplicate
detection for the oldest leaf and may acquire and register another guard. Successful acquisitions
are still released on normal and exceptional exits; failed acquisitions still request resolution.
This is mechanism/efficiency coverage debt, not a claim of universal equivalence: an additional
entry can also cause scratch-array growth at capacity. The captain deliberately removed tests
that assert guard counts and dispositioned mechanism-only mutants. No such test or mutation
allowlist is reintroduced here. The blocking reporter still rejected this survivor; documenting
it did not make that check green. The next follow-up resolves that gate failure.

CodeFactor's status for commit `13f696f6afe07f8f24774c952f7000123f3fc0c4` says "3 issues found."
Its linked PR page returns HTTP 403 through both the browser and direct HTTP access. GitHub
provides no file-level CodeFactor annotations, so those three diagnostics remain unverified;
this follow-up does not guess at their cause or waive the remote check.

## CI follow-up: reverse-scan boundary and unused imports

- Fix / issue: [#1462 PIT run 44803037523](https://github.com/sirixdb/sirix/actions/runs/44803037523/job/136523044507).
- Symptom: before this follow-up, the blocking lane rejected the duplicate-detection boundary
  mutant described above. The runtime behavior is correct; the sampled mutation can merely acquire
  an extra, balanced guard, which the intentionally behavioral tests need not observe.
- Required invariant: inspect every occupied slot, newest first, before acquiring another guard
  or treating a speculative replacement as an original leaf. This applies to
  `ChildPageGuards.retain` (both mutation resolution and structural child loading) and both scans
  in `holdOriginalLeafOnce` (private replacements and retained original children).
- Small correction: express each scan as a positive remaining-slot count, decrementing before
  indexing. This preserves the same sequence of array accesses, including slot zero, and the
  empty-set path without adding allocation, state, or an extra branch. The boundary mutation now
  enters an empty scan and indexes outside the occupied range instead of only losing deduplication.
- Derived verification: the existing writer eviction sweeps and consolidation regressions use
  the FMT-304 data-preservation and reclaimability contracts as independent oracles. They cover
  empty and populated guard sets, repeated merges, full fanout, successful puts and exceptional
  cleanup. Run these before the bounded changed-source PIT lane; do not add guard-count tests or
  weaken the mutation criterion.
- Import invariant: every import in a changed Java file must be used, including test imports.
  Checkstyle 10.21.4 with the committed `checkstyle.xml` found exactly three violations across all
  11 changed Java files: unused imports of `fail`, `assertSame` and `Arrays`, all in
  `HOTIndexWriterLeafRetentionTest`. They remained after the earlier behavioral-test rewrite.
  Removing only those imports makes the same audit pass. Hosted CodeFactor diagnostics remain
  inaccessible (HTTP 403), so this is local analyzer evidence, not a confirmed hosted result.
- Lesson: when an inclusive-index boundary produces a mechanism-only mutation, a remaining-count
  loop can express the same traversal without a special zero-index boundary. Preserve the
  behavioral oracle and verify the new executable mutants rather than exempting them.

Verification: the 52 focused writer, consolidation and retirement-diagnostic cases passed,
as did Spotless and Checkstyle over all 11 changed Java files. The unchanged bounded CI command
(`pr_mutation.py select` against base `ec746e7861589e19733c9ae17e16cc0e5415dc67` and head
`c6a2170c59be9072ed10816ad8472f4c129e2763`, then `run --seconds 1320 --blocking`) exited zero:
**8/8 sampled changed-line mutants killed, zero survivors or uncovered changed-line mutants**.
The same 24 target classes, three test patterns and 12-mutant class cap were used. Unchanged-line
debt remains advisory: 11 survivors and 27 uncovered mutants; the full sampled report has 66
mutants. No threshold, test-selection rule or allowlist changed.

An additional uncapped PIT pass targeted only `ChildPageGuards.retain` and
`holdOriginalLeafOnce`: **20/25 mutants killed**, including all three changed boundary mutants
at lines 8839, 1467 and 1472. `whenAMutationEndsTheLeavesItHeldBecomeReclaimable` kills the writer
boundary; `consolidationReleasesGuardsForIneligiblePairs` kills both consolidation boundaries.
The five residuals below are individually dispositioned, not waived from the blocking lane:

| Class / method | Line / mutation | Disposition |
| --- | --- | --- |
| `AbstractHOTIndexWriter$ChildPageGuards.retain` | 8834, false return, survived | Rejecting a non-leaf requests another resolution; no leaf guard was acquired. Extra resolution is mechanism/work debt, not universal equivalence under I/O failure. |
| `AbstractHOTIndexWriter$ChildPageGuards.retain` | 8841, false return, survived | An already held leaf is resolved again. The original hold remains valid and all additional holds are released. Extra resolution/growth is the previously dispositioned mechanism debt. |
| `AbstractHOTIndexWriter$ChildPageGuards.retain` | 8848, false return, survived | Acquisition was already registered; false requests re-resolution and another balanced hold. The data/reclaimability oracle does not observe that extra work. |
| `AbstractHOTIndexWriter$ChildPageGuards.retain` | 8845, true return, uncovered | Real pre-existing concurrency coverage gap: retirement after swizzle capture but before acquisition must request re-resolution. The new countdown does not change this branch; the planned concurrent-evictor conformance case remains necessary. |
| `HOTIncrementalInsert.holdOriginalLeafOnce` | 1474, zero return, uncovered | No valid fixture revisits a retained original: successful pairs remove their originals from the current node, skipped pairs release their new holds, and private replacements use the ownership scan. The unmatched scan and both of its boundary mutations are covered; this duplicate-return arm remains unexercised. |

The complete *Work budgets* block in `docs/VERIFICATION.md` passed: main reports contain 326
core, 327 query (five skips) and three MCP cases with no failures/errors. The auxiliary isolated
tasks also passed, including `HotWriterLeafRetentionAllocationBudgetTest`: both sets of 32 puts
allocated 21,248 bytes, preserving the zero incremental allocation bound for a committed
neighbour. Logs, XML, selection and the local Checkstyle invocation are under
`build/ci-diagnostics/round3/`. A hosted CodeFactor rerun remains necessary; HTTP 403 prevents
claiming a new provider result locally.

## What the fix does

[FMT-304](../../spec/FMT-disk-format.md#fmt-304--hot-mutation-leaf-lifetime) owns the leaf-lifetime
contract. The [HOT specification §4.9.2](../HOT_INDEX_SPECIFICATION.md#492-writers-copy-on-write-through-the-intent-log)
describes the resolver retention scope, the optimistic neighbour comparison and standalone
consolidation. The regressions above cover their failure modes.

## What it costs

- Allocation: `HotWriterLeafRetentionAllocationBudgetTest` holds a put beside a committed neighbour
  to exactly the bytes of a put beside a logged one. The guard set's allocation lifetime is described
  in [§4.9.2](../HOT_INDEX_SPECIFICATION.md#492-writers-copy-on-write-through-the-intent-log).
- Earlier compiled-shape snapshot: 222 of the 240 methods of `AbstractHOTIndexWriter` were
  byte-identical to `main`, among them `doMutation`, `dispatchInsert` and `mergeIntoLeaf`. Nine
  changed and nine were new, and the new ones were small enough to inline: `resolveUnheld` 7 bytes,
  `enterMutation` 20,
  `isUnpublished` 22, `exitMutation` 31, `compareSubtreeExtreme` 81, `unheldExtremeLeafOfSubtree`
  122. `doReplacePostingChunk` went from 295 bytes to 41 by moving its body to
  `replacePostingChunk`, keeping the bracketing entry point inlinable. `prepareLeafOfTree`, the
  descent every put makes, was the same size as before.
- Earlier instructions-per-put snapshot, measured on the steady workload under deterministic
  compilation (two forks per arm, reproducible to 0.1 %): the two rewritten methods were cheaper
  than the ones they replaced -- `mergeKeepsSpineOrder` 381 against 451, `resolveOneHopForTraversal`
  433 against 523. The whole put read 6,414 against 6,136, with the difference in a different
  inlining tree for the same work (`analyzeDescentInto` 0 -> 1,352 and
  `serializeValueInto` 926 -> 0 represented the same work in another frame), and cost 1.5 % more cycles
  with fewer L1, LLC and branch misses.
- Earlier latency measurements for `HotWriterSiblingReadBenchmark`: 24 fresh-fork pairs
  per benchmark in alternating order, 60 measured shots after 120 warm-up ones (the steady benchmark
  60 after 60), median of shots per fork, paired Student interval on the log ratio, every fork under
  a quiet-machine criterion, contaminated pairs discarded before their shots were read and run again:

  | Benchmark | `main` | Fix | Change | 95 % interval |
  |---|---:|---:|---|---|
  | `insertsInsideLoggedLeaves` (no sibling is read) | 17.19 ms | 17.13 ms | +0.5 % | -1.0 to +1.9 |
  | `appendsBesideCommittedLeaves` | 16.97 ms | 17.23 ms | -0.3 % | -2.9 to +2.4 |
  | `appendsBesideCommittedLeavesThenCommit` | 19.42 ms | 19.61 ms | +0.2 % | -1.9 to +2.3 |
  | `steadyAppendsBesideCommittedLeaves` | 12.84 ms | 12.74 ms | +0.6 % | -2.4 to +3.8 |

  Allocation per put was unchanged in all four, and 0 B per put on the steady benchmark, whose shots
  contain nothing but puts.

  These recorded snapshots do not establish that the final change meets the preregistered
  +1 percent upper bound. The paired insert/commit latency gate must be rerun against
  then-current `main` (gate 11); meeting that bar is required before merge.

The first version of the fix guarded the neighbouring leaf in every put that extends its leaf's
range, three interlocked instructions on a page other threads read, and measured about six points
more than the validated read; that is why the comparison validates instead. The second version
reached the resolver through a field holding the guard set, which stopped the JIT folding the
holding code out of the descent and cost a call per hop plus a slower compile plan; that is why the
put descent and the lookup resolve through `resolveUnheld`, whose guard set is the constant `null`,
and why the merge guard's spine walk is a second copy of the walk rather than one walk with the
guard set as a parameter.

Measuring it took six gates, and the reason belongs here. The put path reaches its compiled plateau
only after about 2.5 million puts, and which compiled shape a build lands on varies between forks:
in a 10-fork census `main` itself takes a slow plan in 3 of them. The first gates warmed for 30
shots and compared two builds in the middle of compilation, which is why warm-up is 120 shots now.
The steady benchmark then stayed unresolved one gate longer, because its leaves held room for 15
shots and it measured 12 of them after 3: in 14 of 24 pairs a fork changed level inside that window
-- one ran four shots at about 15 ms and then seven at about 25 ms while its paired fork went from
25 ms down to 14.4 ms -- which put its interval at +-4.9 %, five times the bound it was tested
against, while its two fork-median distributions were indistinguishable. Two keys per group per shot
in a four times larger index leave room for 127 shots, so that arm now measures 60 after 60;
its dispersion fell from 11.6 % to 7.2 % and its answer from +2.7 % to +0.6 %. The lesson
for the next benchmark is in `docs/LESSONS.md`: a benchmark whose fork cannot hold enough shots to
reach past the runtime's level changes cannot test a one-percent bound, and the fix is the shot
schedule, not the bound.

## Not changed

- Reads outside a mutation. `acquireLeafForRead` and the chunk-view readers take their own guard or
  validate a stamp; the audit of the read side found no unprotected site.
- The package-private guard methods (`existingKeyRoutesToSlot`, `subtreeHasKeyWithBit` and their
  variants) hold nothing when a test calls them outside a mutation. Production reaches them only
  inside one.
- A concurrent evictor is not part of the regression tests. They evict on the writing thread, at
  every call into the storage engine. That is every point at which this thread can cause an
  eviction, and it stands in for another thread only at those points.

## Follow-up: the structural validator half (#1458)

- Fix / issue: [#1458](https://github.com/sirixdb/sirix/issues/1458), the validator half of the same
  mistake. No production change: the writer-wide retention of this fix already covers it. Verified on
  `main` at `31c9c16f1`.
- Symptom: a commit under page-cache pressure failed with `IllegalStateException: HOT published
  structural splice is malformed (first: I5-leaf-constancy — child[…] stored partial … is not a
  subset of dense PEXT … of subtree key …)` on a trie whose bytes were sound; on the derived-index
  maintainer thread the same error marked that resource's maintenance poisoned. Seen on
  `macos-latest / query` and `windows-latest / query` while
  [#1454](https://github.com/sirixdb/sirix/pull/1454) was in CI, and reproducible on Linux with
  released frames zero-filled.
- Trigger: `BitemporalStorageEpochEvictionTest.unboundedLoadMatchesColdPublicationsUnderEviction`
  (its cache budget holds one HOT leaf) with
  `FrameSlotAllocator.setPoisonOnReleaseForTesting(true)`.
- Root cause: the same unguarded resolver. `HOTMalformedSubtreeDetector` checks `isClosed()` once
  when it resolves a leaf and then reads `getFirstKey`, `getKey` and the entry count from that
  leaf's slot memory, so a frame released and reused between the resolution and the read was
  judged as the leaf's keys. It is the caller that must hold the leaf: the detector resolves
  through a caller-supplied `PageResolver`, and the writer's is `traversalPageResolver`.
- Why it no longer reproduces: `enterMutation` holds every cache-backed leaf this writer resolves
  through `resolveHOTPageForTraversal(ref)` until the mutation exits, and the validator and its
  bounded size preflight resolve through exactly that resolver, inside the mutation that published
  the splice (`dispatchInsert` under `doIndex`, `doRemovePostingBit` or `doReplacePostingChunk`).
  Measured on the trigger above with released frames zero-filled: 45,606 leaf resolves through the
  validator's resolver, of which 11,260 carried no guard and **none** was a leaf the page cache
  owned — the unguarded ones are transaction-log pages, which eviction cannot take. With
  `mutationGuards()` forced to `null` the same run logs the symptom above twice and poisons derived
  maintenance, while still exiting 0, because the test asserts nothing about the maintainer thread.
- Why tests missed it: the page cache's own pressure eviction cannot be interleaved between a
  validator's resolution and its read from a single-threaded test, because the validation walk makes
  no call into the storage engine once the splice's leaves are resident.
- Regression test: `HOTStructuralValidatorLeafRetentionTest` — three published splices (a branch, a
  consolidation and one whose scope holds subtree children), each with every leaf nobody holds
  retired at the publication boundary, the last point before validation, and evicted again at every
  engine call, with released frames zero-filled. All three pass on `main` and all three fail with
  `mutationGuards()` forced to `null`: `MutationTraversalRefusal … status=UNRESOLVABLE_REFERENCE`
  with a suppressed `whole-node endpoint proof status=0 beta=-1 first=0 last=0`,
  `IllegalStateException: HOT published structural path is malformed at page 8`, and a consolidation
  that never publishes.
  Run with `./gradlew :sirix-core:test --tests
  'io.sirix.index.hot.HOTStructuralValidatorLeafRetentionTest'`.
- Also re-verified for [#1459](https://github.com/sirixdb/sirix/issues/1459), the writer half, which
  this fix landed without a closing reference: its own guards still bind. All 45 tests of
  `HOTIncrementalInsertLeafRetentionTest` and `HOTIndexWriterLeafRetentionTest` pass on `main`, and
  8 of them fail with `mutationGuards()` forced to `null` — the crossing merge accepted, the ordered
  merge mishandled, the two-leaf migration and all three consolidation cases, and the reclaimability
  check. Nothing needed to be added for it.
- Lesson: an issue can name its fix in a pull request that never merged while a different merged one
  already removed the cause; re-prove the defect on the current default branch before changing code,
  and score such a run by its log, not by its exit code. See [Lessons](../LESSONS.md).
