# Derived query-cache memory

Decoded numeric column lanes, cached masked selections and packed interval-join sides are kept in
allocator-backed foreign memory, not on the Java heap. Each image owns one slab from the
persisted-page allocator (`MemorySegmentAllocator.allocateSlab`): a page-granular region whose
`MemorySegment` has the global scope, so reads carry no per-access liveness check, charged to the
allocator's physical budget before it is mapped. Release removes its live-slab charge exactly once;
recycled backing remains committed until it is unmapped. The heap envelope (`HeapCacheBudget`)
therefore takes no share for derived data; see the table below.
Every reader of a slab must be provably finished before its release, which can recycle or unmap
the backing: this document names the proof for each owner.
A fresh slab's pages are populated in one `madvise(MADV_POPULATE_WRITE)` call
(`sirix.allocator.slabPopulate`, on by default; Linux only, a kernel without the call falls back to
lazy faults), so the lane fill that follows writes resident pages instead of taking one fault per
4 KiB page: the first-run admission of 180 MiB of join lanes paid 44k faults at 2.4 µs each, and
population halves that cost. A recycled slab (below) is resident already and is never populated again.

## Column lanes

A projection store (`ProjectionColumnStore`) decodes every long-lane column (`NUMERIC_LONG`,
`NUMERIC_DOUBLE`, `TIMESTAMP`, `DATE`, `STRING_GLOBAL`, `STRING_SEGMENT`) straight into one slab per
column: leaf after leaf, each lane at a 64-byte aligned base, written by the decoder itself
(`ProjectionIndexRowGroupCodec.unpackInto` and `ProjectionAlpEncoding.decodeInto` have segment
twins of their array forms). A `ColumnSlice` of such a column carries a `NumericLane` (segment, base,
rows) and no heap value array; presence words, boolean words and string dictionaries stay heap
arrays in this step, and the store decodes the KEYS chain to heap arrays (a masked selection moves
the kept leaves' keys into its own slab, below). Kernels read a lane through the slice's segment and base,
hoisted once per leaf, eight bytes per row at linear offsets (`SegmentAccess`), or through
`LongVector.fromMemorySegment`.

**One kernel per operation.** A consumer reaches a value only through the slice's lane, so each
operation has exactly one kernel rather than an array form and a segment form to keep in step: the
column-sliced predicate masks (`ProjectionColumnScan.evaluateMask` and the predicate-tree arm), the
record-key membership walk (`ProjectionRecordKeySet.andMembership`, `filterMembership`), the bounded
top-k heap (`TopKHeap.bindLeaf` hoists each sort key's lane once per leaf), the conjunctive numeric
aggregates and the field-value emission all read `(segment, base)`. The sliced group passes'
windowed access decodes each window's lanes into one buffer borrowed from the allocator and
returns it when the window's last slice leaves its cache, so those passes read native lanes too.
Windowed and leaf-set numeric access also borrow allocator memory and release it on close.
The KEYS chain remains a heap representation; masked selections copy retained keys to their slab.

**One segment class per process.** That representation difference is not free at the JIT level.
Every read through a segment handle casts the segment to the JDK's abstract segment class and calls
three of its abstract methods (base, offset, alignment mask), and C2 devirtualizes those from the
profile and the trap history of the JDK's own methods, which every caller in the process shares.
Once a heap-array segment has been read through that path four times at a site compiled for native
segments (`PerBytecodeTrapLimit`), the speculation is gone at those bytecodes for every kernel
compiled afterwards, which then pays a subtype check and a guarded virtual call per read: on the
integrated head the KEY_IN membership kernel's heap-array KEYS reads during a join's first execution
cost the join kernels compiled half a second later 3 % of t250k Q4. The join kernels therefore read
every per-leaf lane through a local holding a fresh native view of it
(`JoinKernels.nativeView`: `MemorySegment.ofAddress(address).reinterpret(size)`, one small object per
leaf), whose class the compiler sees being constructed, so no shared profile is consulted. The
durable cure is for no producer to hand a kernel a heap array as a segment: a heap array should be
read as an array, or decoded into a slab like every other lane.

**Reading numeric lanes.** Group folds, computed columns and membership filters read numeric
values directly through supported foreign-memory reads. Selection words supply row indices; the
plain numeric word fold hoists each column's segment and base and folds the selected rows as a
counted loop. There are no gathered heap windows or parallel array kernels in production.
`ProjectionComputedColumn` writes its derived lane directly to a query-owned slab, which is returned
after the route materializes its results. The fold, computed-column and membership differential tests
compare those reads with independent scalar expectations over full and sparse masks.

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
either runs inside a scope, holds its own resident-column pins, or holds an entry lease; a reader outside these lifetimes (a direct store user in a
test) must not share its store with a catalog that may evict it.

**Masked views.** A masked fill (`columnMaskedView(col, fetcher, keep, LaneTarget)`) decodes a
non-resident long-lane column's surviving leaves into the caller's slab at the per-leaf bases of
`maskedLaneLayout`, and reports through the target whether it did so (`filled`) or served the
store's resident column pruned (`resident`), including whole-column reads. Allocation refusal
increments `slabFillsDeclinedCount` and follows the existing budget-decline/restart path; it never
decodes numeric values to heap arrays. A query-local returned view pins each resident column until
it closes, independently of the executor's residency scope. A masked numeric fill without a supplied
target owns only the kept leaves' slab through an automatic-arena segment alias. Its cleanup returns
the allocation after its last segment reference is unreachable, independently of other queries'
residency scopes.

**Group routes.** A grouped aggregate reads its key, aggregate and conditional-key columns behind
the route's leaf keep mask, so the columns the store does not hold resident take that masked fill.
The route gives them ONE query-local slab (`SirixVectorizedExecutor.RouteLaneSlab`): the plan is
built from the route's column arrays, and the layout and the single allocation happen on the first
column the route actually resolves, so a route that declines earlier, or that delegates to another
arm, allocates nothing. Repeated and non-lane columns take no extra memory — a column already
resident gets no target at all and is served pruned from the store's own slab, pinned by the query
scope. The slices point into the route's slab, so `groupByAggregate` returns it in a `finally` once
its winners are materialized (every arm emits an `ItemSequence` over copied accumulator lanes and
strings, so nothing downstream reads a slice); the slab is charged to the allocator's physical
budget like every other, and refusal follows the budget-decline/restart path.

**Query-local slabs are recycled.** A view that is not cached, a group route's masked lanes and an uncached
join side decode into a slab that lives for one query. Mapping, zero-faulting and unmapping such a slab
on every query measured as +10 % on a 50 ms query once the kernels read the lanes in place, so
`FrameSlotAllocator.releaseSlab` keeps released slabs mapped for the next slab of the same page-rounded
size (`getRecycledSlabBytes`, bounded by `sirix.allocator.slabRecycleBytes`, by default one sixteenth
of the allocator budget and at most 512 MiB). Their pages stay committed and dirty: a consumer never
assumes zeroed slab memory (the test seam `sirix.allocator.poisonRecycledSlabs` fills a reused slab with
`0x5A`), and the recycled bytes stay in the allocator's committed total. Commitment admission
reclaims only the shortfall, after checking that idle backing can cover it; an impossible request
leaves resident buffers intact. Shutdown unmaps them all. A reuse hands out a fresh global-scope
segment over the same pages, so the instance released before cannot release the new owner's slab.

**Scratch owns separate mapped regions.** All scratch capacities, including 4–256 KiB, use
individually mapped regions under the allocator's shared commitment budget. Page-frame slots remain
separate because optimistic page readers require released frame addresses to stay mapped. A scratch
lease never lends its storage to a page frame. Any idle scratch capacity can be unmapped before a
scratch, slab, oversized, or page-frame commitment is refused; a lent region remains exclusively
owned and cannot be reclaimed. The small idle arrays keep their warm buffers without charging the
slab recycle allowance. Extra small buffers and all larger idle regions share that allowance, with
spares reused before mapping replacements. Exactly sized buffers above 16 MiB are unmapped on return.

With `FrameSlotAllocator`, a buffer's first `ScratchUse.READ` borrow at capacities through 256 KiB
attempts one `madvise(MADV_POPULATE_WRITE)` call to populate its pages. This is enabled by default; set
`-Dsirix.allocator.readScratchPopulate=false` at process startup to disable it. Population is
Linux-only and falls back to lazy faults on unsupported kernels and platforms. An `ENOMEM`
response also leaves the buffer to fault lazily rather than refusing the read. Larger capacities
serve coalesced runs and fault only as far as the read reaches; other scratch uses do not trigger
population. The buffer records the attempt while exclusively borrowed, so reuse does not repeat it.
`ReadScratchPopulateTest` covers the switch, capacity and use boundaries, and reuse.

**Group tables.** The accumulators a grouped aggregate folds into are allocator memory too. Each
grouped scan opens one `GroupWorkspace` (`io.sirix.index.projection`), which borrows regions from the
allocator's scratch facility (`ScratchUse.AGGREGATION`, 64 KiB doubling to 2 MiB) and carves them
into zeroed chunks: the stripes and index chunks of every `NumericGroupAggTable` of the scan, the
native slots of `DenseGlobalGroupAggTable`, the map-based kernels' accumulators, the spill's stripe
buffers, zero-key slots and the workers' standalone accumulators. A table that
rehashes or is released hands its chunks back to the workspace, so the passes of a multi-pass scan
reuse one another's memory; closing the workspace when the arm returns gives every region back, and
nothing of a scan's group memory outlives it. Winners are copied to heap arrays before they are
emitted, like the strings beside them. `GroupBuffer` retains its owning workspace and generation;
public access through `GroupLanes` checks every lane against the buffer's liveness and bounds. The
group kernels resolve a *record* instead: the buffer (`GroupBuffer.accumulator`) or the table
(`NumericGroupAggTable.acquireAccumulator`, `accumulatorAt`) validates the complete footprint once
and answers a primitive that names it, and `GroupKernelLanes` then reads and writes lanes relative
to that record through the supported foreign-memory handle, with no check and no allocation per
lane or per row. A table's probes read at addresses derived from its own chunk geometry. This access
form is confined to aggregation working memory; persisted page and column decoding retain segment
bounds checks. Set `-Dsirix.aggregation.debug=true` at JVM startup, or at **native-image build
time**, to make a record a bounded view of exactly its footprint and to check every probe address
against its chunk's segment and generation.

What a row costs depends on the lookup of an existing group being compiled into the row loop, which
HotSpot decides by method size. `NumericGroupAggTable` is therefore a leaf probe plus insertion and
indexed-acquisition methods that are deliberately too large to inline, and the lane accessors stay
within the size inlined at any call site; `GroupAccessorSizeTest` holds those sizes and the class
comments explain them. The plain kernels, which fold the rows of a word that have no group key into
the dense arm's one missing-key accumulator, take the word's rows at once and fold them column by
column (`foldNumericPlainWordLane`): a column's presence word is read
once, its sum and extrema stay in locals while its rows are walked, and its lane segment is the same
for all of them, so the per-row cost is one value read and three updates.

An allocator-owned `AggregationBudget` protects one eighth of native capacity from caches, including
cache-owned slabs. Each scan acquires an exclusive `AggregationGrant` before planning; contending
scans wait interruptibly when their minimum working set cannot fit. Actual rounded region capacity
is charged before allocation, including simultaneous old/new regions during growth. The planner
subtracts fixed worker and rounding overhead once and charges each group's complete stripe and
index/growth/merge peak; bounded top-k has the same limit. Closing a scan, after draining every
accepted worker even on errors, returns its regions and then its grant. Returned regions may stay
in the allocator's common scratch size classes only after moving their charge outside the protected
execution floor, and within the recycle bound (`sirix.allocator.slabRecycleBytes`, one sixteenth of
the budget by default): a scan returns its regions by the hundred, and the next scan would otherwise
map every one of them again and fault it in page by page. Dense slots are shared across workers;
their accumulating and monotone lanes use the supported atomic foreign-memory access modes through
the same resolved records, including bounded atomic access in debug mode.
`AggregationGrantTest`, `GroupRecordBoundaryTest`, `GroupWorkspaceTest`,
`DenseGlobalGroupAggTableTest`, `MapGroupWorkspaceTest`, and the group differential and work-budget
suites cover ownership, capacity, exact answers and cleanup.

## Allowance

`DerivedCacheBudget` derives one combined allowance `D = A / 8` from the allocator's effective
allowance `A` (its first-initialization grant, including the physical-headroom clamp). At the
effective allowance of 8 GiB that is 1 GiB: cached selections receive `floor(3D / 4)`, packed
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
is published. The slab is returned to the allocator and its cache-share charge refunded when the
last lease drops, so a reader that acquired a lease before an eviction keeps reading valid memory
until it closes, and a freed entry can never be acquired again. Every entry owns a slab (at least
its key lanes); an entry that
reads the store's resident columns in place also holds their pins until that same moment
(`Entry.released`). Views are closed by the
kernels that opened them (the joined aggregate, the routed membership paths); a `Cleaner` backstop
releases what a view that was never closed held. A view's row masks are its own heap copy; the
lanes are immutable.

A query-local view — a mutable revision, no key roster, or a selection the share refused — owns
the slab its lanes were decoded into and returns it to the allocator when it closes; a view over
resident columns also holds its own pins until it closes. Only a cached view hands out a
task lease (`acquireTaskLease`), so a background packing never reads lanes that a closed view may
have released: an uncached selection is not packed in the background at all (nothing could find
the packed side again by its one-off identity).

Entries are keyed by column-store identity (held strongly, so the pins can be returned), the
ordered field roster and the routed key roster, compared exactly. An entry over a store whose
handle left the catalog is stale and reaped on the share's next admission. Packed sides are keyed
by the row identity of the cached selection they derive from and are evicted with it; a packed
side built from a one-off selection is reaped once that identity is unreachable.

## Admission

Admission is reservation-first and copies no column. The executor lays out the surviving leaves'
lanes of every non-resident long-lane field of the selection and, behind them, the surviving
leaves' record keys in one slab (`maskedLaneLayout`, `KeyLanes.layout`); a routed selection whose
keys prune no leaf keeps every leaf explicitly rather than reading the store's whole columns
(which would fill them resident, one slab each, and spread the side's lanes over several
segments). It reserves the slab from the
numeric share (`NumericMaskedColumnCache.reserve`) only for NUMERIC_LONG/TIMESTAMP selections covering at
least one quarter of all rows and three quarters of leaves. String-bearing and sparse selections stay
query-local. The executor decodes the lanes straight into the slab, moves the
keys in once from the store's decoded KEYS arrays (`KeyLanes.copyInto`, eight bytes per row, the
arrays are not retained), and publishes the view as the entry (`publish`); fields the store already
holds resident are read in place and pinned by the entry. Only immutable committed selections with
a key roster qualify: write-transaction selections stay query-local. When the share or the
allocator refuses the reservation, the lanes and keys go into a query-local slab the view owns, and
nothing is cached. If the allocator also refuses that slab, the route declines through its existing
budget path. A background packing of an interval-join
side counts its
rows, reserves the exact slab, and scatters the records off the query thread under a task lease on
the selection's entry; the synchronous build side is packed into a query-owned slab when the cache
refuses, so a disabled cache changes no answer.

An entry stores, per kept leaf, its row mask (heap) and its key lane (`KeyLanes`: segment and
base, read like a numeric lane) and, per field, the slice whose lane points into the entry's slab or
the store's column slab. Packed join records are five longs each. The kernels that read them (the
dense build and probe, the packed merge and chained fold, the membership filters) are one loop body
per kernel reading the lane through the typed slab handle at a base offset hoisted once per leaf
plus eight bytes per row (`SegmentAccess`, native order, the unaligned layout because the slab is
aligned by construction and the alignment check would only repeat per access); the record keys of
a build row and of an emitted probe row come from the key lanes the same way. The join kernels'
handle is `JoinKernels.SLAB_LONG`, invoked directly at every read site (C2 compiles an inlined
`SegmentAccess.getLong` with that helper's process-wide, heap-polluted type profile: +60 % on t250k
warm Q10), and `JoinKernels` is listed for build-time initialization and holds nothing else: a
native image folds a `VarHandle` into a plain load only when it is a static final of a class
initialized at build time, and a handle held or aliased by a class the image initializes at run
time is invoked through the generic method-handle path on every read, 105 ns and a boxed
allocation against 2.3 ns, which cost the t1M warm joins Q4 44 % and Q9 39 % natively while C2,
which trusts every static final, showed a win (`SegmentHandleConstantsTest` guards both halves; see
`docs/NATIVE_IMAGE.md`, "Segment handles are build-time constants"). The dense build and
probe loops read a row's fields through the one segment the side's lanes share when the selection
was decoded into its own slab (bounds and scope loaded once per row, not once per field), each
lane's own segment otherwise; the packed merge is build-major: each build record of a bucket
is read once into locals and the bucket's probe records stream past it, two reads deciding a
non-overlapping pair (`JoinKernels`, measured against the heap kernels by `JoinKernelBenchmark`
and proven equal by `JoinKernelsTest`). A full mask word is walked as a counted run of 64 rows, a
partial word by its bits. Nothing is copied per query and no per-row call separates the row loop
from its fold. A row-output join whose ordering keys are all long columns present on every row orders its
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
entry counts, and `lanes`: the allocator's live slab bytes and fills the allocator refused
(declined through the budget path). `MemorySegmentAllocator.getSlabBytes()` reports the bytes
committed to live slabs, included in the allocator's physical bytes.
`HeapCacheBudget.global().metrics()` exposes the configured and effective envelope, workspace
reserve and each cache share; `BufferManagerImpl`
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
`NumericMaskedColumnCacheTest` (reservation publish and leased views, entries over resident columns
and their pins, refused reservations, query-local slab ownership, key lanes in the entry's slab,
readers of an evicted entry, task leases, membership filters, a retired store's stale entry) and `IntervalRangeJoinWorkBudgetTest` (exact
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
