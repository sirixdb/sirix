# Bug: an automatic arena per compact HOT-leaf decode

> **Status (2026-11-26): the defect is OPEN and the fix is withdrawn.** This change carries the
> measurement, this write-up and the lessons; it carries none of the pooled-storage implementation
> the sections below describe. Read the review and CI sections before "Outcome" as the history of a
> withdrawn attempt: the pool, its admission and accounting, the reclaimer and the pool regression suites
> are not in the tree, and every sentence written in the present tense there described that attempt,
> not the engine. What *is* in this change, besides the documents: the removal of
> `MemorySegmentAllocator.resetSegment` and the virtual-memory layer's `discardToZeros`, a contract
> the POSIX implementation could not honour on macOS and nothing in the engine called. The timestamp
> allocation-fork correction also remains; [Verification](../VERIFICATION.md#running-the-layers)
> owns its current execution guidance. The per-decode arena will be fixed by
> the unified memory-pool work (step 3), specification first; the accounting design and the five
> review rounds behind it are carried by
> [issue #1503](https://github.com/sirixdb/sirix/issues/1503).

- Fix: **none yet.** The pooled-storage fix this document describes was withdrawn before merge — see
  [the outcome section](#outcome-the-pool-is-withdrawn-and-the-defect-stays-open) — and
  [issue #1503](https://github.com/sirixdb/sirix/issues/1503) carries the accounting design the next
  attempt needs. The defect was found while
  fixing the HOT leaf-codec gate's allocation fixture (PR #1454's evidence round).
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
- Regression test (**withdrawn with the fix**; recorded because the next attempt needs the same
  witnesses):
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

The withdrawn pool also needed a reader protocol that detects recycled storage; its proposed
regression tests are recorded below as history. The current compact-image ownership and read
contract remains in [Projection read performance](../PROJECTION_READ_PERFORMANCE.md#versioned-hot-projection-slot-reads).

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
  The allocator's shared rejected-shutdown rollback restores compact admission. Cleanup safety
  on a failed release is covered by the
  [partial-release follow-up](#review-follow-up-rollback-after-partial-slab-release); the
  [compact image storage contract](../HOT_INDEX_SPECIFICATION.md#323-serialized-layout-pagekindhot_leaf_page)
  owns the current invariants.
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


## Review follow-up: rollback after partial slab release

- Symptom: a rejected shutdown could reopen lease admission with blocks pointing into unmapped
  memory, risking corruption or a JVM crash on the next compact decode.
- Trigger: release an idle pool containing multiple slabs; a release throws after earlier slabs
  have been unmapped, or allocation of the replacement empty directory fails after unmapping.
  Allocator shutdown catches the failure and restores pool admission.
- Root cause: the free stack, directory and carved count stayed published until all slab releases
  and an allocating reset completed. Cleanup confused ownership withdrawal with successful native
  release, so failure left reusable views alive longer than their mappings.
- Correction: each class detaches its free stack, directory and count before any slab release,
  using one shared empty directory. Cleanup attempts all slabs and classes and preserves the first
  failure with subsequent failures suppressed through the existing cleanup helpers. Rollback can
  safely reopen admission: detached classes grow fresh slabs rather than returning freed blocks.
- Why tests missed it: existing lifecycle tests covered rejected shutdown after successful pool
  cleanup, but never a release that unmapped storage and then threw partway through cleanup.
- Regression: `CompactImageAllocatorLifecycleTest.partialSlabReleaseFailureCannotReopenReleasedBlocks`
  injects runtime and heap failures at the allocator boundary after closing actual native arenas.
  It checks all releases, primary/suppressed errors, empty pool accounting, stopped admission and
  writable fresh leases in both affected classes after rollback. The independent oracle is the
  native arena lifetime plus the compact storage contract in HOT_INDEX_SPECIFICATION §3.2;
  reusing a closed segment or abandoning sibling cleanup fails these assertions. Existing live-slab
  and foreign-owner rejection cases continue to check real pooled decoding after rollback.
- Verification command: `./gradlew :sirix-core:compactImageAllocatorLifecycleTest`.
  The review phase permits one final focused verification, so no pre-fix run was performed.
- Vision check: protects readable, identical compact images without changing persisted bytes,
  defaults, lease operations or their allocation and work budgets.
- Lesson: withdraw reusable ownership before native teardown, and make teardown resets independent
  of heap availability; see the compact-image fragment collected into docs/LESSONS.md.

### CI/review: the pool's own budget, and why idle slabs are reclaimed

- Symptom: review of the atomic-holder commit reported that permanent compact slabs can starve
  ordinary page frames. With a supported 64 MiB native budget, retained compact images can fill the
  permanent allowance (about 52 MiB) before any 64 KiB frame exists; the first scan to want one
  fails, and closing every compact image cannot recover, because a released block returns to the
  pool's free list and not its bytes to the budget.
- Root cause, and that it is the fix's own: the first version took slabs from `allocateSlab`, which
  admits against the *reclaimable* ceiling and so would have eaten the oversized headroom that keeps
  large-value buffers allocatable — review round 1 (R2) rejected that correctly. Moving them to
  `reserveSlabCommittedBytes` put them in the allowance page frames commit from, which is permanent
  by design. Both admission choices are wrong in different directions: the pool needed a way to give
  bytes back.
- Correction **as it stood after round 2, superseded by round 4 below**: idle slabs are reclaimed. The allocator's pressure path first lets its listener evict
  the page caches — which closes cached compact images and returns their blocks — then asks the pool
  to give idle slabs back up to the refused caller's shortfall; the caller's existing retry loop
  observes the relief. A slab is idle only when every block carved from it is free, derived during
  the pass by draining the free stack and tallying per slab, so the lease path keeps no counter for
  it. A pass is skipped when free blocks are fewer than the smallest slab's block count, or when a
  hand-out is in flight. The strategy, its alternatives and the agreed fallback are in
  `firstmate/data/sirix-compact-decode-arena/reclaim-strategy.md`.
- Also superseded by round 4: an idle slab is **discarded in place**, not unmapped (Umbra/vmcache's decommit-keep-mapping): its
  pages are dropped and its commitment refunded while the mapping, its segment and every block view
  over it stay valid, and the next growth charges it again and leases from it instead of mapping a
  new one. That is what removes the unmap window entirely — see the next section for the review that
  rejected unmapping — and it is why the refund is immediate rather than parked among the
  allocator's recycled slabs.

#### Allocation audit of the per-read sites this change touches

| Per-read site | Allocation |
|---|---|
| `CompactImageStore.acquire` → class `take` → free-stack `pop` | none: one compare-and-set pop, one atomic increment, no object |
| `Block.run` → `give` → `push` | none: one compare-and-set push, one atomic decrement |
| `Block.segment()` handed to the decoder | none: the block's own segment object, created once when its slab was carved |
| `PageKind` compact branch | the page's own objects only; the storage acquisition adds nothing (it used to add one `Arena`, its session and a `Cleaner` registration) |
| reclaim pass (`reclaimIdleSlabs`) | removed in review round 4 together with the reclaimer; while it existed a pass allocated two arrays, and one of them was sized to the whole pool rather than to the pass's target |

Bounded by `CompactImageStorageAllocationBudgetTest`: a warmed lease/release cycle allocates 0 bytes
over 100,000 rounds, and a cycle measured after a reclamation pass still allocates 0, which is what
would catch per-lease bookkeeping added for a reclaim pass's benefit. The automatic arena is kept as
the positive control.

- Reviewed disposition, recorded rather than tested artificially: the charge check in `give()` that
  refuses a block of a discarded slab is unreachable by construction — a leased block keeps its slab
  charged, because a pass only selects a slab whose every block is on the free stack — and it is
  kept as a loud guard against a double release. A mutant removing it is expected to survive.

### Review round 2: unmapping was the wrong half of the design

- Symptom: review of the first reclamation commit returned five findings on one theme. The
  production sequence was still broken (R1): the pressure listener evicted only the native half of
  the fragment cache, so cached compact images kept their blocks and nothing became idle — the
  regression test had closed every image by hand and so could not see it. Around that: a stranding
  window if a release threw between detaching a slab's blocks and unmapping it (R2), a pass that
  drained the whole free stack per firing while the pressure loop fires ~2,000 times in ten seconds
  (R3), the reuse policy for a reclaimed slab's directory indices (R4, a policy question), and a
  reclaim target that asked for the whole refused request instead of its shortfall (R5).
- Root cause of the cluster: unmapping. Every one of R2, R3 and R4 exists only because a reclaimed
  slab's address was taken away, which forces detach-before-unmap ordering, dead directory indices
  and a directory that grows with history. Discarding in place removes all three at once: nothing is
  detached, no index dies, and the only state a slab carries is whether it is charged.
- Corrections: `HOTFragmentCache.evictUnderPressure` evicts both halves (R1), and the regression
  test now runs the production sequence with nothing closed by the test; the slab is discarded in
  place and revived on growth (R2, R4); a pass inspects only as many blocks as its target could need
  (R3); and each of the five pressure sites passes its own measured shortfall against the ceiling
  that refused it rather than the caller's whole request (R5).
- Found while implementing the above, and load-bearing for it: `PosixVirtualMemory.discardToZeros`
  issued `MADV_DONTNEED` on every POSIX platform. On Darwin that advice is advisory — it neither
  zeroes nor releases anonymous pages — so the documented contract of `resetSegment` (next read
  observes zeros, resident size drops) silently did not hold there, and a revived slab would have
  handed a decoder the previous image's bytes. Darwin now uses `MADV_FREE_REUSABLE`. The revival
  case's zeros assertion is what proves this on the macOS lane; it is a pre-existing defect of the
  virtual-memory layer, not of this change.
- Also here: the release path's span lookup is read off the class monitor, so the slab-span list is
  copy-on-write — two volatile array reads per block return, no allocation — while the only writer
  adds one span per mapped slab under the monitor.

### Review round 4: the pool gets its own budget, and the reclaimer goes

- Symptom: review of the discard-and-revive commit returned six findings, all on reclamation, after
  three earlier rounds on the same theme. Five were real: a bounded pass that examined the same
  free-stack prefix on every firing and so could never reach a wholly idle slab deeper down; a
  Windows discard that decommits and immediately recommits, refunding the allocator while the OS
  commitment stays charged; a discard failure that stranded both the drained blocks and the refund,
  because the charge flag was cleared before the discard could throw; a copy-on-write span list and
  a revival scan that made cold population quadratic in slab count; and a tally array sized to the
  whole historical pool on every pass. The sixth asked whether the reclaim target should be capped
  by the request.
- Root cause of four rounds of findings: **the pool had joined a budget it could not leave.** Every
  mechanism — the admission ceiling, the pass, the ordering, the discard, the revival, the shortfall
  — existed only to let cached compact images give bytes back to the allowance page frames commit
  from. A reclaimer is a small garbage collector, and each round found the part of a collector the
  previous round had not specified.
- Correction at round 4 (withdrawn at round 5): the pool is bounded by **its owner's share** —
  `HeapCacheBudget.Pool.HOT_IMAGES`, the ceiling that already bounds what the fragment cache's
  compact half retains — and charges none of the allocator's frame commitment. A full pool refuses a
  lease and the decoder keeps its own GC-owned image. Storage comes back the way the owning cache's
  storage always comes back: the cache evicts an image, the image closes, its block returns to the
  free stack, and the next decode of that class reuses it. An idle slab stays mapped as retained
  capacity. There is no pass, no discard, no revival, no refund and no pressure hook, so all five
  real findings cease to exist rather than being fixed, and the sixth is moot.
- Established shapes this follows, which is why it is the end of the sequence: memory charged in
  bytes to the cache that owns it and bounded by that cache's capacity (RocksDB block cache); a full
  owner budget refusing the insert so the caller keeps its own copy (RocksDB
  `strict_capacity_limit` — byte for byte our GC-owned fallback); memory returning through that
  cache's own eviction; no background reclaimer and no decay timer for a *budget* problem; and idle
  slabs retained and reused before mapping new ones (jemalloc's dirty extents, mimalloc's lazy page
  retire). Frames and the pool are separate budgets with separate owners, as in PostgreSQL's shared
  buffers versus memory contexts and RocksDB's block cache versus write-buffer manager. Comparison:
  `firstmate/data/sirix-dbcompare-slab-reclaim/report.md`; mapping and the one part with no
  counterpart: `firstmate/data/sirix-compact-decode-arena/reclaim-strategy.md`.
- Reverted with the reclaimer: the round-2 fix that evicted the fragment cache's compact half under
  allocator pressure. Once the pool holds none of the frame allowance, evicting a cache that owns no
  frames cannot relieve a frame shortage; pressure on one budget never evicts another budget's
  entries in RocksDB or PostgreSQL either. The compact half keeps its own clock sweeper.
- Kept, because it is independent: the Darwin `madvise` repair. `MADV_DONTNEED` is advisory on
  Darwin — it neither zeroes nor releases anonymous pages — so `resetSegment`'s documented contract
  (the next read observes zeros, resident size drops) silently did not hold there. macOS now uses
  `MADV_FREE_REUSABLE`, and `FrameSlotAllocatorTest.resetSegmentDiscardsTheContentsItDocuments`
  asserts the observable contract, which is what makes the macOS lane the witness.
- Regression witness: `CompactImagePoolBudgetTest` runs the production sequence — a cache full of
  compact images, nothing closed by the test, then the 64 KiB frame a scan needs, which succeeds —
  against a negative arm that charges the pool's slabs to the frame allowance through a forwarding
  allocator and starves on exactly the same sequence. Around it: the refusal at the ceiling, a zero
  ceiling, reuse instead of growth, the owning cache's eviction returning blocks, and the cleanup
  refusal naming what still holds the pool.
- Allocation audit: the per-read sites are unchanged from the table above, minus the reclaim-pass
  row, which no longer exists. `CompactImageStorageAllocationBudgetTest` still bounds a warmed
  lease and release at 0 bytes over 100,000 rounds with the automatic arena as the positive control.

### Outcome: the pool is withdrawn and the defect stays open

- Decision (maintainer, 2026-11-26): take the compact-image pool out of this change. Keep the
  measurement, this write-up, the spec text that stays true and anything independent of the pool
  that fixes a real defect; remove the pool, its admission and accounting, the reclaimer leftovers,
  and `resetSegment`/`discardToZeros`.
- Why, in one line: five consecutive review rounds were spent on **where the pooled bytes are
  charged**, never on the defect or on the pooled-block mechanism, and the accounting was still open
  at the end of the fifth.
  1. Round 1 — slabs taken from the reclaimable ceiling would eat the oversized headroom that keeps
     large-value buffers allocatable. Moved to the permanent frame allowance.
  2. Round 2 — that allowance is the one page frames commit from, so retained compact images could
     starve a frame with no way to recover. A reclaimer was added: a pass over idle slabs hooked
     into the allocator's pressure path.
  3. Round 3 — the unmap ordering, the pass's cost, the dead directory indices and the reclaim
     target. Replaced by discard-and-revive (decommit keeping the mapping, Umbra/vmcache style).
  4. Round 4 — the bounded pass could never reach an idle slab deeper in the free stack; the Windows
     discard refunded the allocator while the OS commitment stayed charged; a failed discard
     stranded both the blocks and the refund; the new span metadata was quadratic in slab count.
     The reclaimer was removed and the pool given its owner's share instead.
  5. Round 5 — that share is then granted twice, because the pool's slabs and the fragment cache's
     own per-image charge are two accounts over one number; admission also undercounted the
     page-rounded mapping, and an exception from the slab allocation consumed admission for good.
- What the next attempt needs, and what [issue #1503](https://github.com/sirixdb/sirix/issues/1503)
  carries: **one account**. The compact
  half's effective retention ceiling minus the pool's mapped slab bytes, with a pooled image charged
  only its heap metadata there and a GC-owned fallback image charged its storage, so pool, fallback
  and metadata can never exceed the share together. That is RocksDB's shape — one capacity, charged
  per entry whatever the memory's provenance, refusing the insert when full so the caller keeps its
  own copy. The comparison that settled the shape is in
  `firstmate/data/sirix-dbcompare-slab-reclaim/report.md`; the mapping and the parts with no
  counterpart are in `firstmate/data/sirix-compact-decode-arena/reclaim-strategy.md`.
- Kept in this change, as an independent defect: `resetSegment` and `discardToZeros` are **removed**.
  Their documented contract is that the next read observes zeros while the address stays valid.
  POSIX used `MADV_DONTNEED`, which is advisory on Darwin — it neither zeroes nor releases anonymous
  pages — and the releasing variant `MADV_FREE_REUSABLE` only marks pages reclaimable, so the old
  bytes stay readable until the kernel reclaims them; Apple documents neither. Nothing in the engine
  called either method (recycled frame slots are deliberately left dirty), so the promise is gone
  rather than fixed on one platform and broken on another.
- Measurement that remains the reason to fix this: about 209 B/op of heap for the automatic arena
  per compact decode, plus the 24-byte arena object whenever escape analysis does not remove it
  (11 of 12 JMH forks at 994.5 B/op, one at 1018.4 — identical code, different compilation), and
  ±1 B/op of jitter that follows GC timing through the `Cleaner`. Evidence:
  `firstmate/data/sirix-fp-s1-leaf-codec/packed-049/gate-4pct/compactmaterialize-alloc.md`.
