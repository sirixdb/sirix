# Derived query-cache memory

Decoded numeric column lanes, cached masked selections and packed interval-join sides are kept in
allocator-backed foreign memory, not on the Java heap. Each image owns one slab from the
persisted-page allocator (`MemorySegmentAllocator.allocateSlab`): a page-granular region whose
`MemorySegment` has the global scope, so reads carry no per-access liveness check, charged to the
allocator's physical budget before it is mapped and refunded exactly once when it is released. The
heap envelope (`HeapCacheBudget`) therefore takes no share for derived data; see the table below.
A released slab is unmapped (`VM.release`), so every reader of a slab must be provably finished
before its release: this document names the proof for each owner.

## Column lanes

A projection store (`ProjectionColumnStore`) decodes every long-lane column (`NUMERIC_LONG`,
`NUMERIC_DOUBLE`, `TIMESTAMP`, `DATE`, `STRING_GLOBAL`, `STRING_SEGMENT`) straight into one slab per
column: leaf after leaf, each lane at a 64-byte aligned base, written by the decoder itself
(`ProjectionIndexRowGroupCodec.unpackInto` and `ProjectionAlpEncoding.decodeInto` have segment
twins of their array forms). A `ColumnSlice` of such a column carries a `NumericLane` (segment, base,
rows) and no heap value array; presence words, boolean words, string dictionaries and the record
keys stay heap arrays in this step. Kernels read a lane through the slice's segment and base,
hoisted once per leaf, eight bytes per row at linear offsets (`SegmentAccess`), or through
`LongVector.fromMemorySegment`; `ColumnSlice.numericValues()` on a lane-backed slice copies the lane
to the heap once and counts it (`NumericLane.materializedCount`), the seam that keeps a consumer
not yet converted working — a converted kernel never calls it, and the integration step deletes it.

**Ownership.** The slab belongs to the store and the column (`ColumnSlabs`). It is charged to the
store's residency ledger exactly as a heap fill was (`decodedColumnResidentBytes` prices a long lane
at eight bytes per row), so the fill doors, the catalog weigher and the headroom share keep one
formula. It is released by exactly one of:

- the residency release at a query-scope exit over the headroom share
  (`ProjectionResidencyScope.close` → `releaseUnpinnedOverBudget`), and a fit door making room
  for a fill (`fitsMakingRoom`), both only with the headroom gate on;
- the handle's retirement (`ProjectionColumnStore.retire`), when it leaves the catalog's handle
  cache (weight pressure, invalidation, test isolation) or the bench/test registry: columns no query
  pins are released at once, pinned ones at their last unpin;
- a failed fill (decode error) and the loser of a same-column publish race release their own slab;
- a `Cleaner` backstop (`ColumnSlabs.run`) for a store that became unreachable with slabs still
  mapped — a bounded handle the catalog never cached. It never runs for a retired store.

**The proof that nothing reads a released slab is the pin count.** Every executor-driven read runs
inside a `ProjectionResidencyScope` — the vectorized executor opens one per top-level call, whether
or not the headroom gate is on — and a store pins a column in every open scope when it publishes a
fill or answers that the column is resident (`column`, `columnFilled`); a cache entry that reads a
resident column in place pins it for its own lifetime (`retainColumn` / `releaseColumn`). A release
touches only a column whose pin count is zero, and it is double-checked: the slot is unpublished
first, the pins are read again, and a pin that arrived in between republishes the column instead
of freeing it. Readers pin *before* they re-read the slot, so a slot still published after the pin
can no longer be freed under them. The one contract this leaves is that a reader of column slices
either runs inside a scope or holds an entry lease; a reader outside both (a direct store user in a
test) must not share its store with a catalog that may evict it.

**Masked views.** A masked fill (`columnMaskedView(col, fetcher, keep, LaneTarget)`) decodes a
non-resident long-lane column's surviving leaves into the caller's slab at the per-leaf bases of
`maskedLaneLayout`, and reports through the target whether it did so (`filled`) or served the
store's resident column pruned (`resident`). Without a target, or when the allocator refused the
memory (counted in `slabFillsDeclinedCount`), the fill decodes to heap arrays as before.

## Allowance

`DerivedCacheBudget` derives one combined allowance `D = A / 8` from the allocator's effective
allowance `A` (its first-initialization grant, including the physical-headroom clamp). At the
default 8 GiB persisted allowance that is 1 GiB: cached selections receive `floor(3D / 4)`, packed
sides `floor(D / 4)`. `sirix.derivedCache.maxBytes` replaces the automatic target with an explicit
byte count, capped by `A`; zero disables both caches. The configuration is read at runtime in JVM
and native-image deployments. Column lanes are outside this allowance: they are priced by the
projection residency budget above and charged to the allocator's physical budget like every slab.

Within a share, a single entry must fit it, and retained bytes plus in-flight construction bytes
plus retired-but-leased bytes never exceed it. Admission evicts the coldest entries of the same
share first and then asks the allocator; when the allocator refuses, the entry is rejected. The
allocator never evicts persisted pages on behalf of a derived cache.

## Lifetime and leases

Removing an entry from lookup never frees its storage. Every reader view (`MaskedColumns`) and
every background packing task holds one lease on its entry; the cache holds one while the entry
is published. The slab is unmapped and its bytes refunded when the last lease drops, so a reader
that acquired a lease before an eviction keeps reading valid memory until it closes, and a freed
entry can never be acquired again. An entry over resident columns (`publishResident`) owns no slab;
it holds the columns' pins until that same moment (`Entry.released`). Views are closed by the
kernels that opened them (the joined aggregate, the routed membership paths); a `Cleaner` backstop
releases what a view that was never closed held. A view's row masks are its own heap copy; the
lanes are immutable.

A query-local view — a mutable revision, no key roster, or a selection the share refused — owns
the slab its lanes were decoded into and returns it to the allocator when it closes; a view over
resident columns alone holds nothing, the query scope pins them. Only a cached view hands out a
task lease (`acquireTaskLease`), so a background packing never reads lanes that a closed view may
have released: an uncached selection is not packed in the background at all (nothing could find
the packed side again by its one-off identity).

Entries are keyed by column-store identity (held strongly, so the pins can be returned), the
ordered field roster and the routed key roster, compared exactly. An entry over a store whose
handle left the catalog is stale and reaped on the share's next admission. Packed sides are keyed
by the row identity of the cached selection they derive from and are evicted with it; a packed
side built from a one-off selection is reaped once that identity is unreachable.

## Admission

Admission is reservation-first and copy-free. The executor lays out the surviving leaves' lanes of
every non-resident long-lane field of the selection in one slab (`maskedLaneLayout`), reserves it
from the numeric share (`NumericMaskedColumnCache.reserve`), decodes straight into it, and
publishes the view as the entry (`publish`); fields the store already holds resident are read in
place and pinned by the entry (`publishResident` when nothing had to be decoded). Only immutable
committed selections with a key roster qualify: write-transaction selections stay query-local.
When the share or the allocator refuses the reservation, the lanes decode into a query-local slab
the view owns, and nothing is cached. A background packing of an interval-join side counts its
rows, reserves the exact slab, and scatters the records off the query thread under a task lease on
the selection's entry; the synchronous build side is packed into a query-owned slab when the cache
refuses, so a disabled cache changes no answer.

An entry stores, per kept leaf, its row mask and record keys (heap) and, per field, the slice whose
lane points into the entry's slab or the store's column slab. Packed join records are five longs
each. The kernels that read them (the dense build and probe, the packed merge and chained fold, the
membership filters) are one loop body per kernel reading the lane through the typed slab handle at a
base offset hoisted once per leaf plus eight bytes per row (`SegmentAccess`, native order, the
unaligned layout because the slab is aligned by construction and the alignment check would only
repeat per access). Nothing is copied per query and no per-row call separates the row loop from its
fold. A row-output join whose ordering keys are all long columns present on every row orders its
rows through one primitive buffer and an index sort rather than the generic tuple sort.

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
entry counts, and `lanes`: the allocator's live slab bytes, fills the allocator refused (decoded to
the heap), and lanes a not-yet-converted consumer copied to the heap. `MemorySegmentAllocator.getSlabBytes()`
reports the bytes committed to live slabs, included in the allocator's physical bytes. `HeapCacheBudget.global().metrics()` exposes the
configured and effective envelope, workspace reserve and each cache share; `BufferManagerImpl`
logs the policy at initialization. These are cold-path observations, not per-row counters.

## Tests

Regression fixtures are `FrameSlotAllocatorSlabTest` (charging, exact-once refund, cross-thread
release, refusal without page-cache pressure), `ColumnSlabLifetimeTest` (one slab per column read
in place; release at a scope exit over the share, under another query's pin, under an entry pin,
at retirement and at a retired column's last unpin; a failed fill, a lost publish race, masked
targets, the cleaner backstop), `LaneDecoderEquivalenceTest` (segment and array decoders agree for
every FOR width, for ALP with exceptions, and for every long-lane kind through the body-slice
decoder), `DerivedCacheBudgetTest` (shares, simultaneous construction, deferred freeing under a
reader lease, dependents, stale entries, allocator refusal, saturating arithmetic),
`NumericMaskedColumnCacheTest` (reservation publish and leased views, resident entries and their
pins, refused publishes, query-local slab ownership, readers of an evicted entry, task leases,
membership filters, a retired store's stale entry) and `IntervalRangeJoinWorkBudgetTest` (exact
answers with the cache disabled, tiny and roomy, and a concurrent reader joining an in-flight
packing). `SirixVectorizedExecutorLifecycleTest` pins the rule that every top-level call opens a
residency scope. `HOTGatherPublicationTest` races first readers of shared HOT pages: the gather
state is computed into final fields by the page constructors, one construction per deserialized
page and none per copy or lookup. Run the full **Work budgets** block in
[VERIFICATION.md](VERIFICATION.md) after changing admission or read paths. Timing and memory
acceptance additionally require paired native+PGO runs; unit tests assert work and exact answers,
never elapsed time.

The two cache fixtures carry the `native-image` tag and run in `:sirix-query:nativeSmokeTest`.
`META-INF/native-image/io.sirix/derived-cache/reachability-metadata.json` registers the Caffeine
weak-key configurations recorded by GraalVM's tracing agent over the query caches; when changing
cache builders, run with `-agentlib:native-image-agent=config-merge-dir=<output-directory>`, merge
the recorded reflection entries, and run the native smoke suite before performance acceptance.
