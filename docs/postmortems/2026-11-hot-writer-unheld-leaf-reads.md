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
  every unguarded leaf evicted at one engine call after the other and at all of them, and the guard
  counts in the middle of a put) and `HOTStructuralPropertyTest.mutationsSurviveLeafEviction`
  (generated CAS, PATH and NAME streams under all four versioning types, with every unguarded leaf
  evicted at every engine call of every mutation). In the recorded baseline run with the two
  production files as on `main`, ten tests failed: the primitive skips the merge on zeroed frames,
  copies a foreign byte on recycled ones
  and merges a pair it could not hold; the writer holds none of three committed leaves in the middle
  of a put; the crossing merge is accepted at 2 of 318 eviction points and the ordered merge
  mishandled at 2 of 24; 11 of 42, 4 of 108 and 5 of 31 eviction points of the other three puts
  fail as described above; and all 12 generated cases fail. With the fix all pass.
  `HotWriterLeafRetentionAllocationBudgetTest` guards the cost instead and passes on both. Run them
  with `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOT*LeafRetention*' --tests
  'io.sirix.index.hot.HOTStructuralPropertyTest.mutationsSurviveLeafEviction'` and `./gradlew
  :sirix-core:dictionaryReadViewAllocationTest`.
- Lesson: an eviction test has to evict where the code under test yields, and has to take the luck
  out of a stale read; and a guard has a price that belongs on the scale before it goes into a path
  every put takes. See [Lessons](../LESSONS.md).

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
  retires each direct child after the real `PageReference.getPage()` captures its live leaf
  but before returning that leaf to consolidation, with full and deferred retirement cases,
  and also covers children closed before resolution.
  The oracle is the standalone primitive's documented skip-without-allocation contract:
  the same parent retains all four children, no replacement is allocated, and every acquired
  guard is released. Eligibility is decided while every input leaf is guarded; an unguardable
  child leaves consolidation for a later attempt. Before operation-wide retention, restoring
  the pre-guard eligibility ordering made both fully retired pair cases throw
  `FrameReusedException` at `segmentRefCount()` instead.
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
- Regression test: `consolidationCopiesOnlyFromLeavesItHolds` now closes all four original leaves
  at allocation and still requires both merges, all twelve copied entries and released guards.
  `consolidationDeclinesWhenAnyChildCannotBeHeld` requires an unchanged node, unchanged child
  references, no replacement allocation and no leaked guards for every child slot, with retirement
  before resolution, after capture, and deferred by an existing guard. Allocation-time eviction
  occurs after a merge has started, so that case must finish safely rather than claim no allocation.
  `consolidationReleasesEveryChildOnFailure` fails each of the four allocations across the two
  merges, both with and without a throwing input-frame releaser, and requires the original
  exception, no retirement requests, no leaked guards and no live input or speculative frames.
  Removing operation-wide retention restores the `heightOf(null)` failure in the allocation-time
  eviction cases. Run the three cases through
  `./gradlew :sirix-core:test --tests 'io.sirix.index.hot.HOTIncrementalInsertLeafRetentionTest'`.
- Lesson: derive retention scope from every downstream consumer, including recompression metadata,
  rather than just the entries being copied. See [Lessons](../LESSONS.md).

## What the fix does

[FMT-304](../../spec/FMT-disk-format.md#fmt-304--hot-mutation-leaf-lifetime) owns the leaf-lifetime
contract. The [HOT specification §4.9.2](../HOT_INDEX_SPECIFICATION.md#492-writers-copy-on-write-through-the-intent-log)
describes the resolver retention scope, the optimistic neighbour comparison and standalone
consolidation. The regressions above cover their failure modes.

## What it costs

- Allocation: `HotWriterLeafRetentionAllocationBudgetTest` holds a put beside a committed neighbour
  to exactly the bytes of a put beside a logged one. The guard set's allocation lifetime is described
  in [§4.9.2](../HOT_INDEX_SPECIFICATION.md#492-writers-copy-on-write-through-the-intent-log).
- Compiled shape: 222 of the 240 methods of `AbstractHOTIndexWriter` are byte-identical to `main`,
  among them `doMutation`, `dispatchInsert` and `mergeIntoLeaf`. Nine changed and nine are new, and
  the new ones are small enough to inline: `resolveUnheld` 7 bytes, `enterMutation` 20,
  `isUnpublished` 22, `exitMutation` 31, `compareSubtreeExtreme` 81, `unheldExtremeLeafOfSubtree`
  122. `doReplacePostingChunk` went from 295 bytes to 41 by moving its body to
  `replacePostingChunk`, so the bracketing entry point stays inlinable. `prepareLeafOfTree`, the
  descent every put makes, is the same size as before.
- Instructions per put, measured on the steady workload under deterministic compilation (two forks
  per arm, reproducible to 0.1 %): the two methods this change rewrote are cheaper than the ones
  they replace -- `mergeKeepsSpineOrder` 381 against 451, `resolveOneHopForTraversal` 433 against
  523. The whole put reads 6,414 against 6,136, and that difference is not in the changed code: it
  is a different inlining tree for the same work (`analyzeDescentInto` 0 -> 1,352 and
  `serializeValueInto` 926 -> 0 are the same work in another frame), and it costs 1.5 % more cycles
  with fewer L1, LLC and branch misses.
- Latency: at parity on all four benchmarks of `HotWriterSiblingReadBenchmark`. 24 fresh-fork pairs
  per benchmark in alternating order, 60 measured shots after 120 warm-up ones (the steady benchmark
  60 after 60), median of shots per fork, paired Student interval on the log ratio, every fork under
  a quiet-machine criterion, contaminated pairs discarded before their shots were read and run again:

  | Benchmark | `main` | Fix | Change | 95 % interval |
  |---|---:|---:|---|---|
  | `insertsInsideLoggedLeaves` (no sibling is read) | 17.19 ms | 17.13 ms | +0.5 % | -1.0 to +1.9 |
  | `appendsBesideCommittedLeaves` | 16.97 ms | 17.23 ms | -0.3 % | -2.9 to +2.4 |
  | `appendsBesideCommittedLeavesThenCommit` | 19.42 ms | 19.61 ms | +0.2 % | -1.9 to +2.3 |
  | `steadyAppendsBesideCommittedLeaves` | 12.84 ms | 12.74 ms | +0.6 % | -2.4 to +3.8 |

  Allocation per put is unchanged in all four, and 0 B per put on the steady benchmark, whose shots
  contain nothing but puts.

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
in a four times larger index leave room for 127 shots, so that arm now measures 60 after 60 like the
others; its dispersion fell from 11.6 % to 7.2 % and its answer from +2.7 % to +0.6 %. The lesson
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
