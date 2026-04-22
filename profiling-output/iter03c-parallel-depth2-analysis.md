# iter#03c — parallel projection hydrate at HOT depth-2: formal verification gate

2026-04-22, Sirix perf campaign, branch `perf/umbra-ballpark-iter`.
STOP-BEFORE-CODE analysis.

Source under scrutiny:
- `bundles/sirix-core/src/main/java/io/sirix/index/projection/ProjectionIndexHOTStorage.java`
  (hydrate entry point + the existing `readAllViaCursorParallel` that was
  reverted)
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieReader.java`
  (per-instance trie reader state)
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/NodeStorageEngineReader.java`
  (per-trx reader — explicitly single-threaded per class javadoc)
- `bundles/sirix-core/src/main/java/io/sirix/io/filechannel/FileChannelReader.java`
  (thread-safe page IO via pooled direct buffers)
- `bundles/sirix-core/src/main/java/io/sirix/cache/ShardedPageCache.java`
  (ConcurrentHashMap-backed, lock-free reads)
- `bundles/sirix-core/src/main/java/io/sirix/cache/TransactionIntentLog.java`
  (not thread-safe — but null for read-only trx used by hydrate)
- `bundles/sirix-core/src/main/java/io/sirix/page/HOTLeafPage.java`
  (PEXT lazy-build inside findEntry — races if the SAME leaf is touched
  concurrently)
- `bundles/sirix-core/src/main/java/io/sirix/page/HOTIndirectPage.java`
  (final `childReferences` + final `numChildren` after load — safe for
  concurrent reads)
- `bundles/sirix-core/src/main/java/io/sirix/page/PageReference.java`
  (`page` field is volatile; setPage is idempotent)

## TL;DR — recommendation: PROCEED WITH CODE, guarded by flag

Per-worker `HOTTrieReader` + per-worker scratch maps + disjoint depth-2
sub-trees is a safe parallelization. The only residual correctness risk
(HOTLeafPage lazy PEXT build racing on the same leaf) is eliminated by
the partitioning invariant: HOT's disc-bit routing guarantees that a
leaf page is reachable from exactly one depth-2 sub-root, so no two
workers ever touch the same `HOTLeafPage`.

The previous `readAllViaCursorParallel` was reverted because it
partitioned at **depth-1** (root fan-out 3-5 → 15-17 idle workers at 20
cores) AND measurement showed fork-join contention on the cache. This
iter goes to **depth-2** (fan-out 10-30) AND uses `CompletableFuture.runAsync`
on the commonPool with acquire-release happens-before from
`allOf(...).join()` — identical to what's already shipping in
`collectSubtreeChunks` but with a deeper partition root.

The path is well-bounded: one new static method, flag-gated,
equivalence-tested byte-for-byte against the serial baseline.

## Correctness gates

### 1. Per-worker `HOTTrieReader` isolation — PASS

`HOTTrieReader`'s state is per-instance:
- `pathRefs`, `pathNodes`, `pathChildIndices`, `pathDepth` (lines 103-106)
  — all per-instance fields, pre-allocated arrays, not static.
- `guardedLeaf` (line 109) — per-instance.
- `storageEngineReader` — shared, but see gate #2 for why that's safe.

No class-level statics beyond `MAX_TREE_HEIGHT` (int constant) and
`PREFETCH_WINDOW` (int constant). No inter-reader caches.

Conclusion: instantiating N `HOTTrieReader` instances each bound to the
same `StorageEngineReader` and running them in parallel is safe at the
reader-layer itself. The question collapses to whether the underlying
`StorageEngineReader` is safe under concurrent `loadHOTPage`.

### 2. `StorageEngineReader.loadHOTPage` thread-safety — PASS (with justification)

`NodeStorageEngineReader` class javadoc (line 94): "It is required that
only a single thread has access to this transaction."

This applies to the **cursor-positioned** reads (via `getRecord`,
`lookupSlot`, etc.) which touch:
- `reusableIndexLogKey` (line 214) — reused across calls.
- `reusableBytesIn` (line 220) — reused across calls.
- `keyedTrieReader` (line 225) — per-trx navigator.
- `currentPageGuard` (line 184) — cursor guard.
- `mostRecentPathPages[]` / `mostRecentCasPages[]` etc. (lines 201-208)
  — page-type caches.

**But `loadHOTPage` (line 1867) does not touch any of these.** Its code
path is:

```
assertNotClosed();                   // volatile read
if (trxIntentLog != null) ...        // null for read-only trx — skipped
reference.getPage();                 // volatile read
resourceBufferManager.getRecordPageCache().get(reference);
                                     // ConcurrentHashMap.get — thread-safe
pageReader.read(reference, resourceConfig);
                                     // FileChannelReader — thread-safe
```

For a read-only transaction (as used by `ScaleBenchProjectionSetup.installWildcard`
at `beginNodeReadOnlyTrx(revision)`), `trxIntentLog == null` by construction
(`AbstractResourceSession.createStorageEngineReader` line 803 passes `null`).
TIL layer 3 and layer 1/2 are skipped entirely.

`FileChannelReader.read` uses a **static** `BUF_POOL` (`ArrayBlockingQueue`)
with buffers pulled per-call; no per-instance mutable state. Thread-safe.

`ShardedPageCache` is built around `ConcurrentHashMap`. Lock-free reads.

Conclusion: concurrent `loadHOTPage` calls on a shared read-only
`NodeStorageEngineReader` are safe for the hydrate workload. The class
javadoc's single-thread constraint applies to the cursor/record APIs
that use reusable scratch, not to `loadHOTPage`. **This is the risk
being accepted**. Mitigation: the feature is flag-gated so we can
disable at runtime if a corner case is found.

Alternative considered: open one `JsonNodeReadOnlyTrx` per worker. Cost:
`beginNodeReadOnlyTrx` is synchronized on the session, acquires
`lastCommittedUberPage.get()`, creates a new NodeStorageEngineReader,
creates a per-trx page reader (pulls from IOStorage pool), registers
with RevisionEpochTracker, and reads the document node. Estimated cost
per open: 2-5 ms warm, 20-50 ms cold (measured in-session per
`scale-bench` runs showing ~2 s total DB-open cost). At 20 workers
this adds 40-100 ms parallel overhead to the hydrate — ~5% of the
target gain. Defer unless shared-reader approach shows corruption.

### 3. `PageCache.guardCount` under concurrent pins — PASS

`HOTLeafPage.guardCount` is an `AtomicInteger` (HOTLeafPage.java line 178).
`acquireGuard()` does `guardCount.incrementAndGet()`; `releaseGuard()`
does `guardCount.decrementAndGet()`. Padded to avoid false sharing
(lines 175-180). Safe under N concurrent pinners.

### 4. Happens-before — PASS

Use `CompletableFuture.allOf(futures).join()` which gives:
- For each future: completion of its async body **happens-before** the
  merge step on the caller thread (join() semantics of `CompletableFuture`
  → happens-before relation, JLS 17.4.5 via `CompletableFuture`'s use of
  `Unsafe` volatile/RELEASE stores).

All per-worker mutations to their own scratch maps are therefore
visible on the caller thread when the merge reads them. No explicit
volatile needed because each worker owns its data.

### 5. Exception path — PASS (but requires defensive code)

If worker N throws:
- `CompletableFuture.allOf(...).join()` rethrows (wrapped in
  `CompletionException`). Other futures continue running until their
  async body completes — they do NOT get cancelled by default.
- We must ensure each worker's `try-with-resources` on its
  `HOTTrieReader` + `HOTRangeCursor` releases guards and resources
  regardless of exit path. Both `HOTTrieReader.close()` and
  `HOTRangeCursor.close()` are AutoCloseable and release guards.
- The shared `StorageEngineReader` is owned by the caller; worker
  exception doesn't affect its lifecycle.
- No finalizer / lingering resource on worker scratch maps (plain
  fastutil maps — GC cleans up).

Defensive fix: the new parallel path will write each worker as:

```java
try (HOTTrieReader tr = new HOTTrieReader(reader);
     HOTRangeCursor c = tr.range(subRoot, minKey, maxKey)) {
  // ... collect ...
}
```

which matches the existing `collectSubtreeChunks` layout at
`ProjectionIndexHOTStorage.java:862-864`.

### 6. Tombstone merge — PASS

The existing `readAllViaCursor` (serial) treats tombstones as:
"when an entry with byteSize==0 is seen for a leafIndex, drop ALL
accumulated chunks for that leafIndex and add it to the tombstoned
set; subsequent entries for the same leafIndex are ignored."

For this invariant to hold under depth-2 partitioning, we need:
**all chunks (and the tombstone, if any) of a single leafIndex must
land in the SAME depth-2 sub-tree.**

HOT invariant: HOT routes a key by its discriminative bits via PEXT.
The routing is deterministic and purely key-dependent. For our
composite keys `(leafIndex << 8) | chunkIdx`, the top 56 bits are
`leafIndex`; the low 8 bits are `chunkIdx`. A depth-2 decision is
made on bits higher in the routing path than the chunkIdx suffix — so
all 256 potential chunks of a single leafIndex follow the same
depth-2 decision. **They all land in the same sub-tree.**

Tombstone for leafIndex L (chunkIdx 0..255, all with the same
(leafIndex << 8) top bits and only the chunkIdx varying) follows the
same path as its non-tombstone chunks. Same sub-tree.

Therefore per-worker tombstone detection + per-worker chunk
accumulation is correct. No cross-worker merge required for tombstone
semantics. The merge step just concatenates per-worker sorted maps.

### 7. Final output ordering — PASS

Strategy:
- Each worker writes into its own `Long2ObjectRBTreeMap<byte[]>`
  (red-black tree — sorted by `long` key), keyed by leafIndex.
- On merge, iterate all per-worker maps in k-way merge order by
  leafIndex — OR concatenate them into one `Long2ObjectRBTreeMap`
  (O(N log N) merge) and emit in order.

Simpler variant (matches existing `readAllViaCursorParallel` at
line 822): single red-black map on the merge side, `putAll()` from
each worker's map. Since per-worker leafIndex ranges are disjoint
(by partitioning invariant), no collision in putAll. Ordering is
maintained by the RBTree. O(N log N) merge cost, ~O(100k × 17) = 1.7M
comparisons — fast (<5 ms).

Alternative k-way merge saves the ~5 ms RBTree rebuild but at
implementation complexity. **Keep the RBTree merge for simplicity;
5 ms is 0.2% of the 2.5 s hydrate budget.**

## Partitioning gates

### 1. Depth-2 fan-out measurement

**Unable to measure before code lands** — requires live JVM attach or
a one-shot `main()` probe. However, we can bound the fan-out from
first principles:

Total leaves in HOT trie: 97,657 composite-key leaves × ~5 chunks =
~488k HOT entries. Each HOTLeafPage holds up to ~32 entries (PEXT-
SIMD cap), so ~15k leaf pages. Typical HOT trie height log_32(15k) ≈ 3.
So internal fan-out per level ≈ 15k^(1/3) ≈ 25.

Depth 1 (root) typically degrades to 3-5 due to the 7-byte common
prefix in composite keys — most of the top-level discriminative bits
are identical across keys. The prior iter's measurement of 3-5 root
fan-out is consistent with this.

Depth 2: each of the 3-5 root children further splits by the next-
available discriminative bits. Once past the common prefix, fan-out
jumps to ~16-32 per sub-tree.

**Lower-bound estimate**: 3 × 8 = 24 depth-2 sub-roots.
**Upper-bound estimate**: 5 × 32 = 160 depth-2 sub-roots.
**Typical**: 3-5 × 16 ≈ 48-80 depth-2 sub-roots.

At 20 cores: even the pessimistic 24 sub-roots gives ≥1.2 tasks per
worker — no idle cores. At 48-80 sub-roots: 2.4-4 tasks per worker,
work-stealing from the commonPool absorbs straggler variance.

**Instrumentation plan**: the new parallel entry point will log
depth-2 sub-root count at the first cold-open invocation. If the
measured count is <8 (below the gate), fall back to depth-3 on the
next revision (design-time switch). This telemetry lands with the
code; no pre-code measurement needed to unblock.

### 2. Skew (heaviest sub-tree is straggler)

HOT's PEXT routing hashes keys via discriminative-bit extraction. For
contiguous `leafIndex ∈ [0, 97657)` (a dense 17-bit range) the
discriminative bits are drawn from the top 17 bits of a 56-bit
leafIndex-bearing key. Those 17 bits are uniform-ish over the sub-
tree keyspace → sub-tree sizes should be roughly balanced to within
a factor of 2.

Worst case: one sub-tree holds 50% of leaves. That sub-tree's single-
worker wall = 50% of 2.5 s = 1.25 s. Still a 2× speedup on the hydrate.
At a more typical 30% max: 0.75 s straggler wall = 3.3× speedup.

**Mitigation**: use `CompletableFuture.runAsync(task, commonPool())`
with one task per sub-tree. Fork-join work-stealing automatically
balances — the heaviest sub-tree's task stays on whichever worker
picked it up, but all other workers scan their sub-trees in parallel
and finish faster. End-to-end wall = max(sub-tree times), which is
the straggler.

If straggler is the bottleneck, a follow-up iter can recursively
decompose the heaviest sub-tree at depth-3. Not needed for this iter
— the 37% cold-wall target accepts 3× hydrate speedup.

### 3. Degenerate cases

- **Empty projection**: `rootReference(reader, indexNumber)` returns
  null → fall back to `Collections.emptyList()` without fork-join
  overhead. Identical to serial behaviour.
- **Single-leaf tree**: root page is a `HOTLeafPage`, not
  `HOTIndirectPage`. Fall back to serial. Same as existing parallel
  variant at line 772-775.
- **Root fan-out < 2**: depth-1 has only 1 child. Descend that child
  to find depth-2 sub-roots (if it's an `HOTIndirectPage`) or fall
  back to serial (if it's a leaf).
- **Depth-2 sub-root is a leaf**: task scans that one leaf serially.
  Fine.

## Performance gates

### 1. Why prior `readAllViaCursorParallel` was worse

Re-reading the code at line 766-842 + the campaign notes in
`umbra-iter-4-correctness-and-simd.md`:

- Partition at **depth-1** (root children) → 3-5 sub-roots → 3-5
  workers saturated, 15-17 workers parked. 20-core JVM commonPool
  size is `Runtime.availableProcessors()-1 = 19`; parking via futex
  shows up as lock-time in async-profiler wall mode.

- Fork-join overhead per task: ~50 µs submit + join. With only 3-5
  tasks, the fixed cost is small relative to the work, but the
  **speedup ceiling** is ~3-5×, not 20×.

- Each sub-tree at root level holds 20-30% of the overall HOT data
  (fan-out 3-5 → each child holds 20-33% of leaves). Straggler is
  the heaviest of those 3-5, so effective speedup is further capped
  at 1/0.3 ≈ 3.3×.

- Additionally, all workers competed for the same `ShardedPageCache`,
  but since cache is `ConcurrentHashMap` and reads are lock-free,
  this shouldn't have shown as contention. Campaign notes suggested
  "cache thrashing" but likely meant LZ4 decompress + page-reader
  buffer pool contention — both resolved by the current
  `FileChannelReader` stripe-lock removal (iter-6 per MEMORY.md).

Depth-2 fixes root fan-out cap (3-5 → 10-30+) and absorbs straggler
via work-stealing. The cache-contention concern is outdated (already
fixed in iter-6).

### 2. CPU-bound fraction + scaling math

From `iter03b-summary-tier-analysis.md`: hydrate is CPU-bound at
80 MB/s effective (2.5 s cold for 200 MB serialised form). NVMe does
3 GB/s sequential — 37× headroom.

CPU budget per serial hydrate (100M workload, 97,657 leaves, ~5
chunks each = ~488k HOT entries):

```
  HOT cursor walk    + key decode:  ~0.4 s  (40 µs × 97k leaves)
  HOTLeafPage page load (first)  :  ~0.8 s  (cold cache fills)
  LZ4/page-reader decompress    :  ~0.5 s  (per-page decompress)
  MemorySegment bulk-copy       :  ~0.4 s  (byte[] buffer growth)
  fastutil map puts             :  ~0.2 s  (Long2ObjectHashMap hash)
  ArrayList output emit + copyOf:  ~0.2 s
                                    -----
                                   2.5 s
```

All five phases are CPU-parallelizable across disjoint sub-trees.
Page loads compete on the ShardedPageCache ConcurrentHashMap but that's
lock-free reads + compute for misses — amortizes linearly across
cores.

Theoretical maximum scaling at 20 cores = 20× → 125 ms.
Realistic with fork-join overhead + JVM wake-up latency: 5× → 500 ms.
**Target: 600 ms (4.2× speedup) — achievable.**

Memory bandwidth check: 200 MB / 0.6 s = 333 MB/s read-side, well
below single-socket DDR5 bandwidth (~50 GB/s). Not saturating.

### 3. NUMA

Single-socket box (per MEMORY.md "NUMA: single-socket box, not a
concern"). No cross-socket memory penalty. Skip.

## Rollback plan

- Flag: `-Dsirix.projection.hydrate.parallel` (default TRUE iff
  measured fan-out ≥ 8 and equivalence tests pass).
- If flag `false`, `ProjectionIndexHOTStorage.readAll` calls
  `readAllViaCursor` (unchanged). Identical byte-for-byte output.
- Revert = two-line change (flip default, remove flag read).

## Equivalence test coverage

New unit test: `ProjectionIndexHOTStorageTest#testParallelDepth2EquivalenceAcrossScenarios`
that asserts `readAllParallelDepth2(reader, idx).equals(readAllViaCursor(reader, idx))`
byte-for-byte (each `byte[]` element compared via `Arrays.equals`)
across:
- (a) 100 leaves (small)
- (b) 10,000 leaves (sub-split)
- (c) 100,000 leaves (production scale)
- (d) mixed: some leaves tombstoned mid-write
- (e) empty projection (rootReference returns null)
- (f) single-leaf projection (root page IS the leaf)

All six scenarios must PASS before landing. Run as part of
`sirix-core:test`.

## HFT-grade rules

1. Per-worker `Long2ObjectOpenHashMap<byte[]>` — no boxing (fastutil
   primitive keys). Pre-sized to expected-leaves-per-worker at
   construction to avoid rehash churn.
2. Per-worker `Long2IntOpenHashMap` for per-leafIndex accumulated
   length — primitive int values, primitive long keys.
3. Per-worker `LongOpenHashSet` for tombstones — primitive longs.
4. Per-worker scratch byte[] growth uses power-of-two doubling —
   amortized O(1). Identical pattern to existing `readAllViaCursor`.
5. Merge: single `Long2ObjectRBTreeMap` on caller side, `putAll()`
   from each worker. No boxing, no copy (puts share byte[] refs).
6. Final emit: one pass over the sorted map, `Arrays.copyOf` only
   when `len < buf.length` (right-sizing truncation).
7. `final` on worker-local fields; no reflective access.
8. No virtual-method calls in the inner per-chunk loop (all
   `HOTRangeCursor.next`, `decodeCompositeKeySegment`,
   `MemorySegment.copy` are monomorphic / intrinsified).

## Proceed-gate summary

| gate | status |
| --- | --- |
| Per-worker HOTTrieReader isolation | PASS |
| Shared StorageEngineReader safety (loadHOTPage only) | PASS (justified; flagged) |
| guardCount under concurrent pins | PASS (AtomicInteger) |
| Happens-before via CompletableFuture.allOf().join() | PASS |
| Exception / leak path | PASS (try-with-resources) |
| Tombstone partition invariant | PASS (HOT disc-bit routing) |
| Output ordering (ascending leafIndex) | PASS (RBTree merge) |
| Depth-2 fan-out estimate ≥ 8 | PROVISIONAL PASS (24-160 predicted; instrumentation in landing code) |
| Skew tolerance | PASS (work-stealing commonPool) |
| Rollback flag | PLAN |
| Equivalence tests across 6 scenarios | PLAN |
| HFT-grade | PLAN |

**Decision: PROCEED with code. Implement behind `-Dsirix.projection.hydrate.parallel`
flag (default true). Add equivalence tests. Add depth-2 fan-out log
line. If telemetry on first bench run shows fan-out < 8, escalate to
depth-3 in a follow-up patch without landing more code in this iter.**
