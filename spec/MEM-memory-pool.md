# MEM — One evictable memory pool

Spec target: **1.0.0**. Chapter status: **Draft; captain decisions of 2026-11-26 folded in**.
Code baseline: `bf3e95c7fbb61c2a054d0af90da3c3f20f6bd64c`
(25 November 2026). [Conventions](CONV-conventions.md) define requirement status, evidence and
conformance. Every requirement below remains `proposed`. The named acceptance tests now have
sources in [the cache test package](../bundles/sirix-core/src/test/java/io/sirix/cache), but planned
scenarios still await implementation and passing evidence.
[MemSteps](../bundles/sirix-core/src/test/java/io/sirix/cache/MemSteps.java) owns their step
enablement and forced-execution option; test-source presence alone establishes no conformance.

## Scope

How memory freed in one place becomes usable in another without a restart: page frames give their
budget back on demand across size classes (step 1), the buffer-manager caches share one frame
budget with priority eviction (step 2), and the compact-image pool is charged as a reservation of
that budget (step 3). The design follows Umbra (one pool, `MADV_DONTNEED` on eviction), vmcache
(batched release, optimistic readers detect release by version) and CedarDB (one budget, per-part
caps only as optional overrides), adapted so that same-class reuse keeps its current zero-syscall
path. Decisions Q1–Q5 of 2026-11-25 are binding here.

**Terms.** *B*: the effective allocator budget (`sirix.allocator.maxSize` / persisted
`maxSegmentAllocationSize`, clamped by `MemorySegmentAllocator.clampToPhysicalHeadroom`).
*L_f*: the frame commit limit, `B − B/8 − oversizedHeadroom` (today's `frameCommitLimit`). A frame
slot of class *c* is in exactly one state: **fresh** (never minted), **owned** (handed out),
**recycled** (released by its owner, resident, charged), **in transfer** (claimed by a release
batch) or **returned** (its memory given back to the OS, uncharged). *Granule*: the larger of the
slot size and the OS page size *P* read from the platform at initialization (never assumed 4 KiB:
macOS arm64 uses 16 KiB, some Linux arm64 kernels 64 KiB). *Shortfall*: a **byte shortfall** of
`charged − (L − s)` when the budget refuses a request of *s* bytes at limit *L*, or a **slot
shortfall** of one slot of class *c* when the budget admits the bytes but *c* has no free slot.
*Batch*: 2 MiB, or one granule when a granule is larger.

## Requirements — step 1: frames return budget across classes

### MEM-101 — Accounting identity

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** [FrameSlotAllocator.java](../bundles/sirix-core/src/main/java/io/sirix/cache/FrameSlotAllocator.java) `commitFreshSlot`, `releaseSlot`, `shutdown`
- **Verified by:** **Planned:** `FrameBudgetAccountingTest` (deterministic, recording `VirtualMemory` fake): after every operation of a seeded sequence of allocate/release/transfer/re-charge/shutdown over all seven classes, assert both identities and the limit; jqwik property `FrameBudgetAccountingProperty` with the same oracle.
- **Evidence class:** doc-only
- **Gap/note:** Today a slot's charge is permanent until shutdown (`FrameSlotAllocator.java:66-70`). The recording fake needs an injectable `VirtualMemory`; the allocator holds it in a static field today.

For every class *c*: `minted(c) = owned(c) + recycled(c) + inTransfer(c) + returned(c)` and
`chargedFrames(c) = slotSize(c) · (owned(c) + recycled(c) + inTransfer(c))`. Process-wide,
`committedBytes = Σ chargedFrames(c) + non-frame charges` (scratch, slabs, oversized, aggregation).
At every linearization point the non-execution charge MUST NOT exceed the limit that admitted it,
and frame charges MUST NOT exceed *L_f*. Bytes MUST be refunded only after the OS release of those
bytes returned success (MEM-104), and MUST be re-charged before a returned slot is handed out
(MEM-105). After `shutdown()` `committedBytes` MUST be 0.

### MEM-102 — Same-class reuse and the read path are unchanged

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `acquireSlot`, `popRecycledSlot`, `releaseSlot`, `acquireVersion`, `validateVersion`
- **Verified by:** **Planned:** `FrameReuseSyscallFreeTest`: a warmed allocate/release cycle within one class, and a cycle that refills from the class's own recycled set, issue zero `VirtualMemory` calls (recording fake). **Planned:** `FrameReuseAllocationBudgetTest` (clean allocation JVM, CONV-301): zero bytes per allocate/release and per `readStamp`/`validateStamp` after warmup. The CONV-302 comparison of step 1.
- **Evidence class:** doc-only
- **Gap/note:** —

Allocating from a class's recycled set and releasing into it MUST NOT issue an OS call, take a lock,
allocate, or add a shared-memory write beyond today's. An optimistic read (stamp acquire and
validate) MUST execute the same operations as today. Transfers run only after a reservation was
refused; the re-charge check of MEM-105 runs only where a fresh slot is minted today.

### MEM-103 — Cross-class transfer on demand

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `reserveCommittedBytesUpTo`, `acquireSlotOrWait`, `reserveScratchCommittedBytes`
- **Verified by:** **Planned:** `CrossClassTransferTest` (independent allocators, *B* = 256 MiB, default reserves, 4 KiB OS-page fake): (a) fill the 64 KiB class to *L_f* = 208 MiB, release every slot, allocate one 4 KiB frame — throws `OutOfMemoryError` on the baseline and succeeds on the change without firing the pressure listener (counting listener) and without parking; (b) scratch: hold 208 MiB of frames plus 16 MiB of live scratch, exhausting its 224 MiB ceiling; slabs: independently hold 208 MiB of frames plus 12 MiB of live slabs, exhausting their 220 MiB ceiling. In each case the next 4 KiB reservation is refused while the frames are owned; release the frames, retain the live non-frame charges, then retry: mandatory scratch throws and slab allocation is refused on the baseline, while both succeed on the change without pressure or parking. (c) Batch bounds: with byte shortfalls of 1 byte, one granule, and one batch + 1, the refunded bytes are ≥ the shortfall and ≤ shortfall + one batch; (d) no recycled slot of the requesting frame's own class is returned while that class's recycled set is non-empty; (e) with recycled bytes in three other classes, slots are taken from the class holding the most recycled bytes first, re-ranked after each batch. **Planned:** `ClassSlotReliefTest`: exhaust class *c* with budget left through a bounded-capacity fake, leave unguarded class-*c* frames cached alongside other classes, and assert that a request for *c* succeeds by recycling one class-*c* slot without evicting other classes, transferring bytes or parking.
- **Evidence class:** doc-only
- **Gap/note:** Batch and source order decided 2026-11-26 (2 MiB, most recycled bytes first).

When a reservation has a byte shortfall, the allocator MUST, before firing the pressure listener,
claim recycled slots up to the shortfall and give them back (MEM-104). A frame request claims slots
of other classes; a non-frame request may claim any class. Classes MUST be drained in descending
order of recycled bytes, re-ranked after each batch. The refund MUST be at least the
shortfall when that many granule-releasable recycled bytes exist, and MUST NOT exceed the shortfall
by more than one batch. Claiming MUST use the same compare-and-set on the recycled bit set and the
same `recycledCount` credit as allocators, so a slot is never both claimed and allocated. After the
pressure listener runs, and on every round of the existing wait loop, the transfer MUST be retried
before parking. Transfers MAY be serialized among themselves (for example by a try-acquire flag);
a request that finds a transfer in progress MUST fall through to the existing wait loop and MUST
NOT block an allocate or release fast path.

When pressure is needed, `acquireSlotOrWait` MUST pass the requesting class, shortfall kind and
amount to the pressure listener. For a slot shortfall, the listener MUST evict unguarded entries
holding frames of that class until one slot is recycled, preserving the existing tier order
restricted to that class. It MUST NOT evict frames of other classes or transfer their budget for
slot relief. This applies in step 1 as well as under MEM-202 in step 2; each retry MUST distinguish
byte pressure from slot pressure anew.

### MEM-104 — Release protocol and reader safety

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** new transfer path in `FrameSlotAllocator`; [VirtualMemory.java](../bundles/sirix-core/src/main/java/io/sirix/cache/VirtualMemory.java)
- **Verified by:** **Planned:** `FrameReleaseProtocolTest` (fake): call order per batch is claim → all versions odd → one OS call per contiguous granule-aligned run → versions even → refund; each version is strictly greater after the transfer; a run never covers an unclaimed slot. **Planned:** `GranuleAlignmentTest`: with *P* = 16 KiB and 64 KiB (fake), a 4 KiB slot is returned only when all slots of its page are claimed, otherwise it goes back to the recycled set charged. *Existing, rerun:* `HOTLeafUseAfterCloseTest`, the poison-on-release suites and `HOTStructuralPropertyTest` (eviction + poisoning) with a test-only switch that returns every slot to the OS on every release (the worst case); **Planned:** `OptimisticReaderReleaseStressTest`: readers spin `readStamp`/read/`validateStamp` on slots being released, re-charged and reissued on real memory; no fault, no validated read of bytes other than the owner's.
- **Evidence class:** doc-only
- **Gap/note:** vmcache §3 is the precedent: a reader of a released page reads zeros and its version check fails.

Only recycled slots (no owner, no guard) MAY be returned. For each claimed slot the version MUST go
to odd before the OS call and to the next even value after it, and MUST never decrease. An optimistic
read overlapping any part of that window MUST fail validation. An OS call MUST cover only whole OS
pages that lie entirely inside claimed slots; slots that cannot form such a page MUST go back to the
recycled set, still charged. The slot's address MUST stay readable and writable for the allocator's
lifetime: slot memory MUST NOT be unmapped, decommitted (`MEM_DECOMMIT`) or offered
(`OfferVirtualMemory`) while the allocator lives. Readers MUST NOT rely on released contents being
zero or being stale; both occur.

### MEM-105 — Re-charge before minting

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `commitFreshSlot`, `popFreshSlot`
- **Verified by:** **Planned:** `FrameRechargeTest` (fake): with returned slots in class *c* and none recycled, the next allocation of *c* reserves the bytes, takes a returned slot and leaves `nextFreshIndex` unchanged; with the reservation refused it claims nothing and the returned count is unchanged; a re-charged slot is handed out with the normal odd/even version transition.
- **Evidence class:** doc-only
- **Gap/note:** —

A request that finds its class's recycled set empty MUST take a returned slot of that class before
minting a fresh index. It MUST reserve the bytes first (same limit as a fresh slot) and claim the
slot only after the reservation succeeded; a refused reservation MUST leave the returned set
untouched. A re-charged slot's contents are unspecified; the dirty-reuse contract applies.

### MEM-106 — Platform release primitives

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** [PosixVirtualMemory.java](../bundles/sirix-core/src/main/java/io/sirix/cache/PosixVirtualMemory.java), [WindowsVirtualMemory.java](../bundles/sirix-core/src/main/java/io/sirix/cache/WindowsVirtualMemory.java)
- **Verified by:** **Planned:** `PlatformReleaseSemanticsTest`, run on every CI lane (Linux x64, Linux arm64, macOS arm64, Windows) on a real reserved region: write a pattern to every page of a granule-aligned run and record residency, release the run, then (1) immediately measure residency before any read or write of that run: on Linux and macOS it drops by at least the run size (Linux `mincore` or `/proc/self/smaps_rollup`; macOS `proc_pid_rusage` `ri_phys_footprint`); on Windows the working set (`GetProcessMemoryInfo`) is recorded in the test output, not asserted; (2) assert a read does not fault; (3) Linux reads zeros; macOS and Windows read zeros or the old pattern (either accepted); (4) a write after release reads back, also after a second release of a neighbouring run; (5) on a separate released, untouched macOS run, re-charge issues `MADV_FREE_REUSE` and the footprint rises again on first touch.
- **Evidence class:** doc-only
- **Gap/note:** Sources read 2026-11-25. Linux: man-pages `madvise(2)`. macOS: XNU `osfmk/vm/vm_map.c` maps `MADV_DONTNEED` to `vm_map_msync(VM_SYNC_DEACTIVATE)` (deactivate only: pages stay resident, contents kept) and `MADV_FREE_REUSABLE` to `vm_map_reusable_pages`, which returns success without effect when the map page size is below the hardware page size. Windows: Microsoft Learn `VirtualAlloc` (`MEM_RESET`: "does not guarantee … zeros"; "should not be decommitted"; it does not state that pages stay accessible or when RAM is freed), `VirtualFree` (a decommitted page faults), `OfferVirtualMemory` (offered pages are inaccessible). The unused `resetSegment`/`discardToZeros` path, whose macOS `MADV_DONTNEED` did not zero, was removed by #1497; nothing in the allocator relies on `MADV_DONTNEED` zeroing on macOS.

| Platform | Release | Re-charge | Read after release |
|---|---|---|---|
| Linux | `madvise(MADV_DONTNEED)` | none | zero-fill on demand, RAM freed at the call |
| macOS | `madvise(MADV_FREE_REUSABLE)` (MUST NOT use `MADV_DONTNEED`) | `madvise(MADV_FREE_REUSE)` | zeros or old contents |
| Windows | `VirtualAlloc(MEM_RESET)` (decision Q2) | none (pages stay committed) | zeros or old contents; OS commit charge stays at its peak |

The budget refund MUST follow these calls on every platform. On Windows the RAM goes back when the
memory manager needs it, not necessarily at the call; this is accepted and documented, and the
budget identity holds either way. On macOS with a map page smaller than the hardware page
(Rosetta), `MADV_FREE_REUSABLE` succeeds without effect; this is accepted and documented, since
Sirix ships arm64 natively. Native-image builds MUST register any new downcall in both
reachability-metadata files.

### MEM-107 — Error cases

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** transfer path, `acquireSlotOrWait`, `shutdown`
- **Verified by:** **Planned:** `FrameReleaseFailureTest` (fake injecting failures): (a) a failed or throwing release call returns that run's slots to the recycled set, refunds nothing, increments the failure counter, and the transfer continues with other runs; a request whose shortfall is then not covered proceeds to pressure, wait and the existing `OutOfMemoryError`, whose message includes returned and failed counts; (b) a refunded byte taken by another thread before the requester retries: the requester retries in the wait loop and the accounting identity holds; (c) a failing `MADV_FREE_REUSE` does not fail the allocation and is counted; (d) shutdown racing a transfer ends with `committedBytes == 0` and no OS call after the regions were released.
- **Evidence class:** doc-only
- **Gap/note:** —

A release-call failure MUST NOT throw out of the transfer, MUST NOT refund, and MUST leave the slots
recycled and usable. A reservation refused after a successful refund is not an error: the refund
stands and the requester retries. Re-charge advice that fails MUST NOT fail the allocation.
Shutdown MUST close admission to transfers before releasing the regions.

### MEM-108 — No page-frame starvation

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** as MEM-103
- **Verified by:** MEM-103 (a) and `ClassSlotReliefTest`. **Planned:** `FrameStarvationTest`: through a real `BufferManagerImpl` with explicit cache-policy weights allowing *L_f*, fill one class through the record-page cache to *L_f*, then read pages needing every other class; every read succeeds within the existing 10 s wait bound and no `OutOfMemoryError` is thrown while the caches hold unguarded frames covering the byte shortfall. Repeat slot pressure using the bounded-capacity seam of `ClassSlotReliefTest`, with budget left and unguarded frames of the requesting class.
- **Evidence class:** doc-only
- **Gap/note:** Writer intent-log frames stay pinned consumers bounded by the intent-log limit; they are not covered.

A frame request MUST NOT fail while its own class has a recycled slot, or recycled slots of other
classes and unguarded cached frames cover its byte shortfall in releasable granules. For a slot
shortfall, it MUST NOT fail while an unguarded cached frame of its own class can be recycled.

### MEM-109 — Class capacity covers the frame budget

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `FrameSlotAllocator.reserveSizeClasses`, per-class version and scope metadata
- **Verified by:** **Planned:** `ClassCapacityTest` (recording `VirtualMemory` fake): for *B* = 256 MiB, 16 GiB and 64 GiB, every class reserves at least *L_f*, rounded up to whole slots; never-minted capacity has no materialized per-slot version or scope metadata. A refusing fake fails initialization with the class and requested bytes in the error and releases every earlier region. Existing allocator startup tests MUST pass on all four CI lanes with these reservations; filling real 64 KiB frames past 32 GiB is not required, since the fake verifies address-space arithmetic.
- **Evidence class:** doc-only
- **Gap/note:** Today `reserveSizeClasses` uses `VIRTUAL_PER_CLASS / slotSize`, capped at `1 << 20` slots: 4 KiB holds only 4 GiB and 64 KiB only 32 GiB.

On supported 64-bit platforms, each class *c* MUST reserve at least `ceil(L_f / slotSize(c))`
slots, replacing the current capacity cap. The seven regions reserve a total of 7 × *L_f*, rounded
up per class; this is virtual address space only, not physical commitment. Per-slot version and
scope metadata MUST be materialized only as slots are minted: never-minted capacity MUST cost no
heap beyond fixed per-class tables and the recycled bit set (one bit per slot).

Linux `mmap(MAP_NORESERVE)` is not charged under overcommit modes 0/1; mode
`vm.overcommit_memory=2` ignores that flag and may refuse the reservation. macOS anonymous `mmap`
is lazy. Windows `VirtualAlloc(MEM_RESERVE)` charges no commit, only user address space (128 TiB
on x64). A refused reservation MUST fail allocator initialization with an error naming the class
and requested bytes and MUST release all regions already reserved; silently reducing capacity is
forbidden.

## Requirements — step 2: one frame budget in the buffer manager

### MEM-201 — One frame budget, optional caps

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** [BufferManagerImpl.java](../bundles/sirix-core/src/main/java/io/sirix/cache/BufferManagerImpl.java) constructor; [Databases.java](../bundles/sirix-core/src/main/java/io/sirix/access/Databases.java) `initializeGlobalBufferManager`
- **Verified by:** **Planned:** `UnifiedFrameBudgetTest` (via the `HeapCacheBudget` and explicit-policy constructor seams): with no cap properties set, the record-page cache alone can hold frames up to *L_f*, then the HOT leaf cache alone; MEM-109 verifies every class's address-space capacity for that promise. With `sirix.cache.recordPage` set, the record-page cache never holds more than that cap. Existing cache-budget tests that encode the 1/2, 3/16 and 1/8 shares are updated to the cap semantics, not deleted.
- **Evidence class:** doc-only
- **Gap/note:** Today: record pages 1/2, record fragments 3/16, HOT 1/8 of *B*, summing to 13/16 (`Databases.java:863-865`, `BufferManagerImpl.java:302-331`).

The frame-holding caches (record pages, record fragments, HOT leaves, the native half of the HOT
fragment cache) MUST have no default weight cap; their joint frame use MUST be bounded only by *L_f*,
and every size class MUST have address-space capacity for *L_f* as required by MEM-109.
`sirix.cache.recordPage` and `sirix.cache.recordPageFragment`, when set, MUST act as maximum caps for
their cache. The HOT leaf and native HOT fragment caps MUST be derived from `sirix.cache.recordPage`
as today when, and only when, it is set. Heap-only caches (page, revision root, HOT mini pages, projection blobs, compact HOT
images) keep their current bounds.

### MEM-202 — Priority eviction up to the shortfall

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `FrameSlotAllocator.PressureListener`, [ShardedPageCache.java](../bundles/sirix-core/src/main/java/io/sirix/cache/ShardedPageCache.java) `evictUnderPressure`, [HOTFragmentCache.java](../bundles/sirix-core/src/main/java/io/sirix/index/hot/cache/HOTFragmentCache.java)
- **Verified by:** **Planned:** `PriorityEvictionTest`: with all three tiers populated and a byte shortfall of one frame, only combined record pages are evicted; with a byte shortfall larger than the unguarded record pages, HOT leaves are evicted next, then fragments, the two fragment caches each shedding in proportion to its weight (within one entry); freed frame bytes are ≥ the byte shortfall and ≤ byte shortfall + one batch. With class *c* exhausted and budget left (bounded-capacity fake), only class-*c* frames are eligible in that tier order; eviction stops once one slot of *c* is recycled and other classes retain their frames. A guarded entry is never evicted; a cache under its cap is not shed by 25%. *Existing:* all work-budget tests (`docs/VERIFICATION.md` *Work budgets* block).
- **Evidence class:** doc-only
- **Gap/note:** Replaces today's blanket shed of a quarter of every cache (`ShardedPageCache.java:1359-1367`).

The pressure listener MUST receive the shortfall kind, byte amount or slot count, and requesting
class for frame requests, and MUST evict unguarded entries in this order:
combined record pages, then HOT leaves, then fragments. In the fragments tier, the record-fragment
and native HOT fragment caches MUST be evicted in proportion to their current weights. For a byte
shortfall, all classes are eligible and eviction MUST stop once freed bytes cover the shortfall
rounded up to one batch. For a slot shortfall, only frames of the requesting class are eligible
and eviction MUST stop once one slot of that class is recycled; other classes MUST retain their
frames. It MUST NOT evict from a later tier while an earlier tier holds an eligible unguarded
entry. Within a tier, the cache's existing clock order applies. Freed
frames enter their class's recycled set; MEM-103 moves their budget across classes. FMT-304 holds
unchanged.

### MEM-203 — Step-2 acceptance or fallback

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** —
- **Verified by:** CONV-302 comparison (JVM and native image) on SH1, JSONBench and ClickBench subsets.
- **Evidence class:** doc-only
- **Gap/note:** Decision Q3.

Step 2 MUST land only with no `regression` verdict under CONV-302 (4% floor) on those suites. If it
fails, 1.0 ships step 1 with today's fixed shares; no runtime toggle between the two is required.

## Requirements — step 3: compact-image pool as a reservation

### MEM-301 — One account for the compact images

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** compact-image pool (withdrawn from PR 1497, re-introduced here); `HOTFragmentCache` compact half
- **Verified by:** **Planned:** `CompactImageAccountTest`: after every lease, release, slab map, fallback decode, eviction and shutdown of a seeded sequence, `pool slab bytes + fallback image bytes + pool metadata bytes ≤ share`, every byte of that account is included in `committedBytes`, and shutdown returns it to 0; at a full account a decode first evicts compact images up to the shortfall and, if the account still refuses, decodes uncached into a scratch lease that the cache does not retain. **Planned:** `CompactImageAllocationBudgetTest` (clean JVM): zero bytes per decode with a warm pool. CONV-302 comparison on HOT read suites.
- **Evidence class:** doc-only
- **Gap/note:** RocksDB `CacheReservationManager` precedent: reservations grow in fixed units as the account grows.

The compact pool, its fallback images and its native metadata MUST be charged to one account that
never exceeds the owner's share (today's heap-derived `HOT_IMAGES` ceiling, now also reserved from
*B*), and the account MUST be reserved from *B* (in slab-sized units, as
it grows) before the bytes are mapped or allocated. A refused reservation MUST make the pool refuse
the lease. A decode the account cannot admit MUST first evict compact images up to the shortfall;
if the account still refuses, it MUST decode uncached into a scratch lease, not into GC-owned
memory outside the account. Lease and release on a warm pool MUST NOT allocate or reserve.

### MEM-302 — Pool slabs are not elastic in 1.0

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** as MEM-301
- **Verified by:** MEM-301 test: idle slabs stay mapped and charged until shutdown.
- **Evidence class:** doc-only
- **Gap/note:** Elastic slabs bring back the occupancy reclaimer that failed four review rounds.

Mapped pool slabs MUST stay reserved until shutdown; MEM-103 MUST NOT take budget from them.

## Configuration and observability

### MEM-401 — Configuration surface

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** [DatabaseConfiguration.java](../bundles/sirix-core/src/main/java/io/sirix/access/DatabaseConfiguration.java) `SEGMENT_ALLOCATION_SIZE_PROPERTY`; `Databases`
- **Verified by:** **Planned:** `MemoryConfigurationTest`: no new public property exists; the caps of MEM-201 apply only when set; the reserves keep their values for budgets of 256 MiB, 16 GiB and 64 GiB.
- **Evidence class:** doc-only
- **Gap/note:** Decision Q4.

The one budget setting stays `sirix.allocator.maxSize` / `maxSegmentAllocationSize` (CONF-103; the
first initialization wins). The execution floor (*B*/8), oversized headroom (*B*/16, at most 1 GiB),
scratch reserve and slab-recycle bound MUST keep their current values and properties. The only
optional caps are those of MEM-201. There MUST be no property that restores permanent frames; a
faulty step is reverted instead. The worst-case switch of MEM-104 MUST be a test seam, not a
configuration property.

### MEM-402 — Observability

- **Level:** MUST · **Status:** proposed · **Since:** 1.0
- **Code:** `FrameSlotAllocator` stats and `dumpStateForOOM`; `BufferManagerImpl` pressure log
- **Verified by:** **Planned:** assertions inside the MEM-103, MEM-107 and MEM-202 tests on the counters below.
- **Evidence class:** doc-only
- **Gap/note:** —

The allocator MUST expose, per class, the current returned-slot count and monotonic counts of
transfers, slots returned, bytes refunded, bytes re-charged and failed release calls (with the
last error code), and include them in the out-of-memory dump. The pressure log MUST report the
shortfall kind, amount and requesting frame class, and the bytes freed per tier. Counters MUST be
updated only on the refusal path.

## Steps, each landable alone

| Step | Lands | Must prove |
|---|---|---|
| 1 | MEM-101–109, 401, 402 (`FrameSlotAllocator`, `VirtualMemory` backends, class-specific pressure listener) | the planned tests above, including independently exhausted frame/scratch/slab cases in MEM-103, `ClassSlotReliefTest` and `ClassCapacityTest` (capacity arithmetic, lazy metadata, refusal rollback); changed-line mutation testing (CONV-202); the work-budget block; CONV-302 on read suites plus a cross-class workload (JSONBench load then query), JVM and native image, expected parity; `PlatformReleaseSemanticsTest` with residency measured before access and existing allocator startup tests green on all four CI lanes; the native-library smoke runs the MEM-103 (a) sequence in the native image |
| 2 | MEM-201–203 | as step 1, plus MEM-203's suites; by about 2026-12-05 or 1.0 ships without it |
| 3 | MEM-301, 302 | lifecycle and account tests, clean-JVM allocation test, CONV-302 on HOT reads |

Order: 1, then 2, then 3. Step 3 does not depend on step 2.

## Out of scope for 1.0

Dynamic sharing between execution memory and caches (CedarDB's up to 3/4; measured after 1.0, Q4);
elastic pool slabs; returning memory on every eviction (Umbra/vmcache literal); background decay;
epoch-deferred decommit; shrinking the oversized headroom.

## Decisions folded in (2026-11-26)

The captain accepted all eight recommendations of the review draft: 2 MiB batch; most recycled
bytes first (MEM-103); Windows RAM returned lazily, accepted (MEM-106); Rosetta no-op accepted
(MEM-106); proportional fragment-tier eviction and HOT caps derived only from a set
`sirix.cache.recordPage` (MEM-201, 202); full compact account evicts then decodes into a scratch
lease (MEM-301); compact share stays the heap-derived `HOT_IMAGES` ceiling, reserved from *B*
(MEM-301); no kill switch (MEM-401). The subsequent capacity review adopted full-*L_f* virtual
capacity per class and class-specific slot relief (MEM-103, 108, 109, 201, 202). No open questions
remain.
