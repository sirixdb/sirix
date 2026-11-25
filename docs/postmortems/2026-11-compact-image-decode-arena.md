# Bug: an automatic arena per compact HOT-leaf decode

- Fix / issue: this pull request; the defect was found while fixing the HOT leaf-codec gate's
  allocation fixture (PR #1454's evidence round).
- Symptom: every compact HOT-leaf fragment decode allocated Java heap for the *ownership* of its
  native image. `GcOwnedNativeMemory.allocate` is `Arena.ofAuto().allocate(used, 8)`, so each decode
  created an arena: a 24-byte `jdk.internal.foreign.ArenaImpl` whenever escape analysis did not
  remove it, plus a session and a `Cleaner` registration. Measured on the `compactMaterialize`
  benchmark: 11 of 12 JMH forks at 994.5 B/op and one at 1018.4 — the arena object survives or not
  per compilation, so two legs of *identical* code allocate differently — and about 209 B/op for the
  arena itself, with ±1 B/op of jitter that follows GC timing, because the `Cleaner` runs when the
  collector does. A fragment decode is per element on a read path (`readHOTLeafFragments` is the
  default for every HOT fragment chain whose byte handler supports memory segments), where
  `AGENTS.md` forbids per-element allocation.
- Trigger: any read of a versioned HOT projection slot that walks a fragment chain. No input is
  special; the cost is on every decode.
- Root cause: the image needs native storage with a lifetime independent of the borrowed read
  buffer, and the original change (#1289) gave it one *automatic arena per image* rather than a
  pooled block, so the garbage collector became the owner of memory whose owner was already known —
  the decoded `HOTLeafPage`, which releases its frame exactly once from `releaseMemory()`. Paying a
  Java allocation and GC-timed bookkeeping per element for an ownership decision the page already
  makes is the defect.
- Why tests missed it: no test asserted anything about the storage acquisition. The suite checked
  the decode's results and, in two places, that the image's segment was exactly its packed size —
  which the arena satisfied — but nothing bounded the allocations of the path. The work-budget
  suites count engine work (reads, slots, records), not heap bytes, and the only allocation budgets
  were on other paths. A JMH allocation profile found it, and only because an A/A comparison of two
  legs of identical code disagreed.
- Regression test:
  `:sirix-core:dictionaryReadViewAllocationTest --tests 'io.sirix.budget.CompactImageStorageAllocationBudgetTest'`
  bounds a warmed lease/release cycle at 0 bytes (100,000 rounds, smallest of three legs) and keeps
  the automatic arena as the positive control, which allocates; a third case ties the bound to the
  decode by asserting that a compact fragment decode leases exactly one block and returns it on
  close. `io.sirix.page.CompactImageOwnershipTest` covers the lifetime: one block per image, a
  guarded image keeps its block until the last guard goes, a closed image whose block has been
  re-leased can no longer certify an optimistic reader's stamp (with the pool's poison-on-release
  switch on, so a stale read is certain rather than lucky), and the pool's refusal still answers the
  read. `io.sirix.cache.CompactImageStoreTest` covers the pool: the class table holds every legal
  image size with bounded slack under `CompactImageStore.blockBytesFor`, a released block is leased
  again rather than carved, a release of an unleased block is refused, and eight threads never receive
  the same block twice.
  On the parent commit the new tests fail (no lease exists to count); the two existing exact-size
  assertions in `HOTCompactFragmentReadTest` and `HOTCompactFragmentBatchReadTest` are now bounds on
  the slack instead, because a pooled block is its image rounded up to a class.
- Lesson: when a decoder knows exactly when its memory dies, it must own that memory explicitly; an
  automatic arena per element turns a known lifetime into GC work and makes the path's allocation
  depend on whether escape analysis fires. See
  [the lesson](../LESSONS.md) collected from `docs/lessons.d/compact-image-decode-arena.md`
  (see the [fragment convention](../../changelog.d/README.md)).

## Was the decrypted-page reclaim the same cause? No.

The flake investigation of `DerivedIndexMaintainerTest`
(`firstmate/data/sirix-derived-index-maintainer-flake/diagnosis.md`) listed, as one of three
hypotheses, that a GC-reclaimed automatically managed copy of a decrypted page could be freed while
the decoder still read it. That hypothesis is not this defect, and its own investigation retracted
it:

- Different site. The decrypted-page copy is `AbstractReader.nativeCopyOf`, one arena per *page* on
  the byte-array branch; the defect here is one arena per *decoded fragment image*. Nothing is shared
  but the `Arena.ofAuto()` idiom.
- The shape is not mechanically possible. An automatic arena frees its region when the segment and
  every segment derived from it become unreachable. The decoder holds the segment across the whole
  decode (it is the `BytesIn` source), so there is no point at which a read happens through a
  segment that is unreachable.
- The evidence pointed elsewhere. The investigation's write-side and read-side dumps showed
  `frameEqualsDisk=true`, a writer hash covering exactly those bytes, and the *same* 719-byte
  ciphertext decrypting to entropy once and to a valid `HOT_LEAF_PAGE` twice, in one thread,
  microseconds apart. That places the fault in the decrypt computation, not in the ownership of any
  buffer. It remains open there and is not affected by this change.

What this change does touch is the general rule the flake round was probing: an image whose storage
can be recycled needs a reader protocol that detects the recycle. The compact image has one — the
page's optimistic stamp — and `releaseMemory()` publishes `closed` and an odd stamp binding before
it calls the releaser, so the block cannot reach a second owner while a reader can still certify
reads of the first. That ordering is now an asserted invariant rather than an incidental one.

## Review: slab admission, ownership and cold population

- Symptom: a warm acquisition could race shutdown's zero-lease check and receive an unmapped
  segment; retained pool slabs could spend oversized headroom; a heap failure before publication
  could strand a mapped slab; repeated directory copies made cache population quadratic.
- Trigger: park an acquisition after its free-list pop, or park shutdown after its empty check;
  reach the permanent commitment ceiling; fail after constructing an unpublished slab's blocks;
  retain 32,768 images of the 4 KiB class.
- Root cause: the pool counted handed-out leases but not admissions, used a reclaimable allocation
  boundary for permanent storage, left newly mapped storage ownerless during heap allocation, and
  conflated directory capacity with its logical block count.
- Why tests missed it: concurrent exclusivity alone does not exercise shutdown, an unlimited arena
  stub cannot expose allocator headroom, ordinary allocation does not inject publication failure,
  and repeated reuse of one block never exercises directory growth.
- Regression tests: `CompactImageStoreTest` drives both shutdown orders with latches, checks cleanup
  and primary/suppressed failures with empty, expanding and spare-capacity directories, and bounds
  directory allocations while checking carved counts. `FrameAllocationAdmissionTest` fills the
  permanent allowance, closes every image and still allocates an oversized buffer; reclaimable
  slabs retain their separate allowance. The existing permanent-commit witness also exercises the
  new slab entry point. These tests are derived from the compact image storage contract in
  `HOT_INDEX_SPECIFICATION.md` §3.2. The focused command and outcome belong to this review phase's
  verification result; no failing-before run was made because this phase permits one final
  verification after all fixes.
- Lesson: lifetime determines commitment admission; shutdown admission must count work before
  ownership is published; newly mapped resources need rollback until publication; cold population
  needs a counted growth budget alongside the warmed allocation budget.

### Vision check for the review fixes

| Rule | Verdict | Evidence |
| --- | --- | --- |
| The advantages to protect | aligns | Pool fixes leave revision and fragment reconstruction unchanged. |
| History is the data model | aligns | No persisted bytes or revision operations change. |
| Speed is part of correctness | aligns | Geometric directory growth; unchanged zero-byte lease budget; no bound widened. |
| A fast answer is the same answer | aligns | Shutdown refuses admission safely; existing decode fallback remains. |
| The specification decides behaviour | aligns | Storage invariants and regression witnesses written before implementation. |
| One format, one route | aligns | Separate permanent/reclaimable admission feeds the same slab mapping and release. |
| A library first | aligns | No service or deployment dependency added. |
| Scope | aligns | Changes stay within native compact-image ownership and its allocator. |

Contract class: restore. These fixes restore safe ownership and the existing commitment limits;
performance measurement and mutation gates remain owned by the outer pipeline.

## Review: packed extents, native refusal and rejected shutdown

- Symptom: a malformed compact image could address a prior image's bytes in a rounded block;
  native slab reservation or commitment exhaustion could fail a read instead of falling back;
  rejected allocator shutdown could leave the compact pool permanently stopped.
- Trigger: decode a 38-byte image with a six-byte entry at offset 32, close it, then decode an
  eight-byte image declaring the same offset; refuse native reservation, first commitment or page
  population; reject shutdown after returning idle pool slabs because another slab or a
  foreign-owned oversized allocation remains live.
- Root cause: physical capacity had served as an implicit validity bound, a throwing native
  allocation interface conflated kernel refusal with heap failure, and the allocator's rollback
  restored only its own admission flag rather than the compact store's admission too.
- Fix: compact construction checks each offset, suffix and value once against the declared packed
  extent before publication. The immutable image then gives every search/key/value/slot consumer
  the same validated directory without per-access checks or a per-image segment slice. The
  ordinary recycled-frame exposure is pre-existing and remains outside this fix: writable leaves
  retain their capacity and promotion behavior. Permanent slab allocation uses native refusal
  results, refunds the charge and releases a reservation before returning null, while heap and
  publication failures propagate. Population distinguishes unsupported advice from native refusal.
  Permanent-slab admission also reports shutdown as null, replacing the store's broad
  `IllegalStateException` catch so an allocator publication failure cannot be mistaken for refusal.
  The allocator's shared rejected-shutdown rollback reopens compact admission once cleanup has
  emptied its directory; a cleanup failure cannot reopen a directory that may contain unmapped blocks.
- Why tests missed it: valid round-trip images never addressed recycled slack, budget refusals
  never exercised native refusals, and store-only cleanup tests never rejected allocator shutdown
  after idle slabs had already been returned.
- Regression tests: `CompactImageOwnershipTest.packedExtentRejectsSlackFromAnEarlierImage`
  checks the reported reuse sequence plus offset, suffix and value corruption on both segment and
  byte-array inputs. `CompactImageAllocatorLifecycleTest` drives native refusals and heap failures,
  checks budget refunds and reservation releases, reads through the GC-owned fallback at the real
  commitment ceiling, and reads through pooled storage after both rejected-shutdown cases.
  It also checks that shutdown refuses pool growth while an unrelated allocator publication
  `IllegalStateException` propagates unchanged through the pool.
  The lifecycle suite runs through `:sirix-core:compactImageAllocatorLifecycleTest` in a clean JVM:
  database suites can leave unrelated global owners, masking the intended shutdown rejection.
  `CompactImageStorageAllocationBudgetTest` retains its zero-byte lease bound unchanged. No
  failing-before run was made: this review phase permits a single final focused verification after
  all fixes. Mutation and performance gates remain with the outer executor.
- Lesson: validate immutable logical extents at their construction boundary, distinguish native
  refusal from heap failure at the native interface, and roll back admission for every participant
  in a rejected shutdown. These restore the existing safety and cost contracts without changing
  persisted bytes, revision behavior, public results or writable-leaf routing; the vision verdicts
  above remain applicable.

## CI: allocation measurement, mutation selection and publication diagnostics

- Symptom: the ARM non-index lane measured 896 bytes in 16,384 timestamp conversions; PIT
  reported 72 undetected changed-line mutants; Sonar's reliability gate rejected five volatile
  references.
- Root cause: the timestamp budget remained in the ordinary, asynchronously compiled test fork;
  the compact ownership, lifecycle, admission and native-page-preparation suites had no explicit
  mutation associations, and the lifecycle suite also inherited its ordinary-test exclusion in
  PIT. The volatile diagnostics did not account for the reference-publication protocol: callbacks
  synchronize their own latch state, the stateless native backend is replaced only by isolated
  tests, and directory entries are published through the atomic free stack before any reader
  receives them.
- Correction: run the unchanged timestamp zero bound and allocating reverse control in the existing
  clean `-Xbatch` allocation fork. Associate the existing contract tests with their production
  consumers, and let PIT execute the non-allocation lifecycle suite. Exercise Windows' native
  return-code translation through injected syscall handles on every host, and POSIX reservation
  and population through actual kernel calls. Assert legal empty slots and exact packed boundaries,
  as well as corrupt extents. Keep directory-growth accounting's positive witness alongside its
  upper bound. Explain the five publication protocols at narrowly suppressed Sonar diagnostics.
- Simplification: after the second admission check, the in-flight acquisition count already protects
  slabs until handout, so there is no need for a third stop check after the pop. Allocator rollback
  always reopens compact admission; it need not inspect the directory a second time.
  Slab admission now counts the operation before initialization checks terminal state. Free blocks
  are rechecked under the growth monitor and returned through one path. Only the carved directory
  prefix is published; retry overwrites any unpublished views after native ownership is rolled back.
- Mutation review: five surviving size-class arithmetic mutants have individual equivalence proofs
  in [the dispositions file](../../.github/mutation-dispositions.toml). The proofs cover every legal
  image size and identify the single-boundary cases allowed by CONV-201. No uncovered mutant is waived.
- Regression evidence: the failing CI artifact is run `45501472399`; local verification commands
  and their outcomes are recorded by this CI phase. No allocation bound was widened. The native
  tests use the documented syscall result contract as their independent oracle; they reject
  treating NULL as success, losing a mapped address, and swallowing heap failures.
- Vision check: these corrections preserve revision bytes and answers, retain the zero-allocation
  compact lease requirement, and add no storage format or production policy.
- Lesson: map cross-cutting ownership tests explicitly into mutation analysis, including the
  runner's exclusions, and complete warmup compilation before enforcing exact allocation budgets.

### Local CI verification

- Core regression/work-budget selection: 459 tests, no failures or skips. Clean allocation fork:
  19 tests passed; allocator lifecycle fork: 19 cases passed, including first frame, reclaimable-slab
  and permanent-slab allocation before explicit initialization. `:sirix-core:spotlessCheck` passed.
- Blocking changed-line PIT: 202 mutants, 194 killed, three detected by timeout, five individually
  dispositioned equivalents, zero uncovered or failing mutants; all disposition/association checks
  passed. Report: `build/ci-evidence/pit-verified/summary.md` (generated local evidence).
- The normal Gradle tasks and repository mutation runner were used with smaller local heaps and
  one mutation worker on the shared host. No production setting, assertion or budget was widened.
  Compiling `:sirix-kotlin-cli:classes` supplied the unchanged CLI bytecode the disposition checker
  also validates. Remote ARM and Sonar analysis still require the outer executor's updated CI run.

### CI: the quality gate's one bug, and the counter that became atomic

- Symptom: both Sonar checks failed in [run 45515806526](https://github.com/sirixdb/sirix/actions/runs/45515806526/job/138889681273).
  The analysis uploaded successfully; the quality gate rejected the new reliability rating (C).
- Which issue failed it: the rating is driven by bugs, and the pull request had exactly one —
  `java:S3078` on `directoryGrowths++`, a compound write to a volatile int. The five `java:S3077`
  entries on the same change are MINOR code smells with a reliability *impact*; they count toward
  maintainability, which passed at rating A, and they did not fail anything. Coverage was 94.6% and
  every other condition passed. Reading "six reliability issues" off an impact-filtered issue query
  and treating them all as gate blockers cost this round two wrong instructions to the pipeline.
- Root cause of the warning: the analyzer sees the compound volatile write without the caller's
  monitor. Every write to a size class's directory-growth counter holds that size class's monitor,
  and diagnostic reads need cross-thread visibility. `take()` is the only caller of private
  `grow()` and calls it inside `synchronized (this)`; `grow()` holds the only counter write; slab
  release takes the same monitor and leaves the historical count intact. Refusal and an
  unpublished-slab failure leave the counter untouched, and growth within existing capacity does
  not increment it.
- Correction: the counters (`carvedBlocks`, `directoryGrowths`) are `AtomicInteger`, and the block
  directory, the three test hooks and the allocator's slab-memory backend are final
  `AtomicReference` fields. That removes the construct each rule objects to instead of arguing with
  it, and leaves no suppression to rot. Growth is the cold path, so the counters cost nothing there;
  `blocks.get()[top]` on the lease path is the same single load the volatile array field was. The
  locking argument above still holds — it is why the change is behaviour-preserving, not why the
  warning was wrong.
- Regression evidence: the remote quality gate is the failing consumer rather than a race a unit
  test can reproduce. `coldPopulationHasLinearDirectoryCopyCost` and `concurrentLeasesAreExclusive`
  exercise growth accounting and concurrent lease ownership; the zero-byte lease allocation budget
  proves the holder added no allocation (124 focused cases, the budget twice, and the 26-case
  allocator suite passed locally on the edited tree).
- Also in that job's log, and not ours: the SonarSource Java analyzer crashed with a
  `NullPointerException` in `KnownCapacityHashBasedCollectionCheck` (S6485) on
  `bundles/sirix-core/src/main/java/io/sirix/cache/LRUCache.java`, a file this change does not
  touch. It is an analyzer defect on pre-existing code and it did not fail the analysis.
- Vision check: the lease path keeps its zero-allocation property and no engine behaviour, default
  or persisted byte changes.
- Lesson: read *which* issues drive the failing quality-gate condition before fixing anything — a
  bug and a smell with the same impact are not interchangeable — and on a cold path prefer removing
  the flagged construct to maintaining a suppression.
