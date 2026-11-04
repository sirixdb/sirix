# Derived query-cache memory

Immutable numeric selections and packed interval-join sides are retained in allocator-backed
foreign memory, not on the Java heap. Each cached image owns one slab from the persisted-page
allocator (`MemorySegmentAllocator.allocateSlab`): a page-granular region whose `MemorySegment`
has the global scope, so reads carry no per-access liveness check, charged to the allocator's
physical budget before it is mapped and refunded exactly once when it is released. The heap
envelope (`HeapCacheBudget`) therefore takes no share for derived data; see the table below.

## Allowance

`DerivedCacheBudget` derives one combined allowance `D = A / 8` from the allocator's effective
allowance `A` (its first-initialization grant, including the physical-headroom clamp). At the
default 8 GiB persisted allowance that is 1 GiB: numeric snapshots receive `floor(3D / 4)`, packed
sides `floor(D / 4)`. `sirix.derivedCache.maxBytes` replaces the automatic target with an explicit
byte count, capped by `A`; zero disables both caches. The configuration is read at runtime in JVM
and native-image deployments.

Within a share, a single entry must fit it, and retained bytes plus in-flight construction bytes
plus retired-but-leased bytes never exceed it. Admission evicts the coldest entries of the same
share first and then asks the allocator; when the allocator refuses, the entry is rejected. The
allocator never evicts persisted pages on behalf of a derived cache.

## Lifetime and leases

Removing an entry from lookup never frees its storage. Every reader view (`MaskedColumns`) and
every background packing task holds one lease on its entry; the cache holds one while the entry
is published. The slab is unmapped and its bytes refunded when the last lease drops, so a reader
that acquired a lease before an eviction keeps reading valid memory until it closes, and a freed
entry can never be acquired again. Views are closed by the kernels that opened them (the joined
aggregate, the routed membership paths); a `Cleaner` backstop releases the lease of a view that
was never closed. A view's row masks are its own heap copy; the slab is immutable.

Entries are keyed by column-store identity (held weakly), the ordered field roster and the routed
key roster, compared exactly. Packed sides are keyed by the row identity of the numeric snapshot
they derive from and are evicted with it; a packed side built from a one-off selection is reaped
once that identity is unreachable.

## Admission

Numeric admission is reservation-first: the slab is reserved and mapped on the query thread, the
masks are copied at once (they are the only mutable input), and the record keys, presence words
and numeric lanes are bulk-copied into the slab on a background task. The query that decoded the
selection keeps its heap arrays and never pays the copy. Only immutable committed selections
qualify: write-transaction selections and views with changed masks are excluded, and sparse masks
keep the locality route. A background packing of an interval-join side counts its rows, reserves
the exact slab, and scatters the records off the query thread; the synchronous build side is
packed into a query-owned slab when the cache refuses, so a disabled cache changes no answer.

A snapshot stores, per kept leaf, its row mask, record keys and, per field, the presence words and
one contiguous numeric lane: the heap view's arrays laid out one after another, eight bytes per
row. Packed join records are five longs each. The kernels that read them (the dense build and
probe, the packed merge and chained fold, the membership filters) are the heap kernels: one loop
body whose representation is a loop-invariant choice, reading a resident lane through the typed
slab handle at a base offset hoisted once per leaf plus eight bytes per row (`SegmentAccess`, native
order, the unaligned layout because the slab is aligned by construction and the alignment check
would only repeat per access; the merge kernels invoke the handle at the read site itself, which the
compiler intrinsifies there instead of calling a helper). Nothing is copied per query, no per-row
call separates the row loop from its fold, and the slab read sites only ever see native segments.
Roster comparisons use the bulk `mismatch` intrinsic and mask comparisons a plain word loop, neither
allocating per leaf. A row-output join whose ordering keys are all long columns present on every row
orders its rows through one primitive buffer and an index sort rather than the generic tuple sort,
whose per-row allocations the optimized native image had compiled as calls into the collector's
slow path (the t250k row join read +16% in the first gate on that account). Measured cost of this form as native
images against the heap-array kernels of the base (one window, PGO): about +5% on the t250k
equi-join and +30% on the t250k interval merge, whose millions of reads each carry the handle's
bounds and session checks, while the t1M interval join runs at 0.18 of the base because the base
cannot cache there. Two earlier forms were measured and rejected: per-row vector loads from a
row-major block, and bulk copies into per-thread heap windows; both split the probe loop from its
fold into separate frames and the copies ran as a native-image word loop at about 3 GB/s.

## Heap envelope

Projection, dictionary and HOT heap ceilings come from `HeapCacheBudget`: the envelope is
`E = min(H / 2, configuredEnvelope)`, defaulting to `H / 2` of the runtime maximum heap `H`. The
other half of the heap is reserved for execution, output, GC and application/metadata objects.
The following disjoint shares sum to at most `189 / 192` of `E`; unused shares are not borrowed.

| Startup property | Default / maximum share |
| --- | --- |
| `sirix.heapCache.maxBytes` | Envelope `E`, at most `H / 2` |
| `sirix.projection.cacheBytes` | `5 / 8` of `E` |
| `sirix.projection.globalDict.recordCacheBytes` | `4 / 7` of `E / 6` |
| `sirix.projection.globalDict.verdictCacheBytes` | `1 / 7` of `E / 6` |
| `sirix.projection.segmentDict.verdictCacheBytes` | `2 / 7` of `E / 6` |
| `sirix.hotHeapCache.maxBytes` | `E / 8` |
| `sirix.heapCache.hotLookupBytes` | `E / 32` |
| `sirix.heapCache.hotMiniBytes` | `E / 48` |
| `sirix.heapCache.projectionBlobBytes` | `E / 64` |

At a 6 GiB heap these defaults equal or exceed the ceilings that preceded the envelope: compact HOT
images keep 384 MiB, the mini-page cap stays at 64 MiB, dictionary records, global verdicts and
segment verdicts stay above 256, 64 and 128 MiB, and the per-store projection fill
(`sirix.projection.eagerMaterializeBytes`, by default four fifths of the projection share) keeps
1.5 GiB. Each override accepts a non-negative decimal `long` byte count, is capped by its share and
can disable retention with zero; invalid values fail configuration. The existing HOT entry-count
limit and persisted-page allocation can further reduce the applicable HOT cache capacities.
GC-owned compact native images are conservatively charged to this envelope; allocator-frame pages
retain their separate native allocation.

These are retained payload-cache ceilings, not a process RSS guarantee. Operators must fit the
native arena (which now also holds the derived slabs), runtime heap, other native/direct memory
and OS/application reserve within the machine or container allowance.

## Observability

`DerivedCacheBudget.global().metrics()` exposes the configured target (`-1` for automatic), the
allocator allowance, the effective combined cap, both partition caps, retained bytes, in-flight
bytes, retired-but-leased bytes, cumulative rejected bytes (saturating at `Long.MAX_VALUE`) and
entry counts. `MemorySegmentAllocator.getSlabBytes()` reports the bytes committed to live slabs,
included in the allocator's physical bytes. `HeapCacheBudget.global().metrics()` exposes the
configured and effective envelope, workspace reserve and each cache share; `BufferManagerImpl`
logs the policy at initialization. These are cold-path observations, not per-row counters.

## Tests

Regression fixtures are `FrameSlotAllocatorSlabTest` (charging, exact-once refund, cross-thread
release, refusal without page-cache pressure), `DerivedCacheBudgetTest` (shares, simultaneous
construction, deferred freeing under a reader lease, dependents, stale entries, allocator refusal,
saturating arithmetic), `NumericMaskedColumnCacheTest` (identity fences, oversized and combined
rosters, alternating selections, concurrent readers, readers of an evicted snapshot, task leases,
native membership filters, mutable-transaction exclusion, failed copies) and
`IntervalRangeJoinWorkBudgetTest` (exact answers with the cache disabled, tiny and roomy, and a
concurrent reader joining an in-flight packing). `HOTGatherPublicationTest` races first readers of
shared HOT pages: the gather state is computed into final fields by the page constructors, one
construction per deserialized page and none per copy or lookup. Run the
full **Work budgets** block in [VERIFICATION.md](VERIFICATION.md) after changing admission or read
paths. Timing and memory acceptance additionally require paired native+PGO runs; unit tests assert
work and exact answers, never elapsed time.

The two cache fixtures carry the `native-image` tag and run in `:sirix-query:nativeSmokeTest`.
`META-INF/native-image/io.sirix/derived-cache/reachability-metadata.json` registers the Caffeine
weak-key configurations recorded by GraalVM's tracing agent over the query caches; when changing
cache builders, run with `-agentlib:native-image-agent=config-merge-dir=<output-directory>`, merge
the recorded reflection entries, and run the native smoke suite before performance acceptance.
