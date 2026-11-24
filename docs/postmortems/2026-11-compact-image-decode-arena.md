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
