# Derived query-cache memory

Decoded numeric column lanes, cached masked selections and packed interval-join sides are kept in
allocator-backed foreign memory, not on the Java heap. Each image owns one slab from the
persisted-page allocator (`MemorySegmentAllocator.allocateSlab`): a page-granular region whose
`MemorySegment` has the global scope, so reads carry no per-access liveness check, charged to the
allocator's physical budget before it is mapped and refunded exactly once when it is released. The
heap envelope (`HeapCacheBudget`) therefore takes no share for derived data; see the table below.
A released slab is unmapped (`VM.release`), so every reader of a slab must be provably finished
before its release: this document names the proof for each owner.
A fresh slab's pages are populated in one `madvise(MADV_POPULATE_WRITE)` call
(`sirix.allocator.slabPopulate`, on by default; Linux only, a kernel without the call falls back to
lazy faults), so the lane fill that follows writes resident pages instead of taking one fault per
4 KiB page: the first-run admission of 180 MiB of join lanes paid 44k faults at 2.4 µs each, and
population halves that cost. Huge-page advice for slabs from 2 MiB exists behind
`sirix.allocator.slabHugePages` but is off: with THP defrag in `madvise` mode every huge-page fault
compacts memory synchronously, and on a fragmented machine every attempt failed (261 stalls, 0
successes, +0.4 s system time on one cold join). A recycled slab (below) is resident already and is
never populated again.

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
`LongVector.fromMemorySegment`; `ColumnSlice.numericValues()` on a lane-backed slice copies the lane
to the heap once and counts it (`NumericLane.materializedCount`), the seam that keeps a consumer
not yet converted working — a converted kernel never calls it, and the integration step deletes it.

**One kernel per operation.** A consumer reaches a value only through the slice's lane, so each
operation has exactly one kernel rather than an array form and a segment form to keep in step: the
column-sliced predicate masks (`ProjectionColumnScan.evaluateMask` and the predicate-tree arm), the
record-key membership walk (`ProjectionRecordKeySet.andMembership`, `filterMembership`), the bounded
top-k heap (`TopKHeap.bindLeaf` hoists each sort key's lane once per leaf), the conjunctive numeric
aggregates and the field-value emission all read `(segment, base)`. The sliced group passes'
windowed access decodes each window's lanes into one buffer borrowed from the allocator and
returns it when the window's last slice leaves its cache, so those passes read native lanes too.
Producers that still decode to the heap — a windowed leaf access that does not borrow (top-k
resolution, one-leaf winner emission) and the KEYS chain — reach those same kernels as heap
segments (`MemorySegment.ofArray`), which is a representation difference, not a second code path.

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

**Reading a lane in a group fold.** A group fold's row index comes out of
`Long.numberOfTrailingZeros` over the selection word, so its loop is not counted over linear offsets
and no compiler hoists the segment's bounds check out of it: every surviving row would pay its own.
The group kernels therefore exist in two forms per accumulator shape. `foldSlicedLane`,
`foldSumsSlicedLane` and `foldNumericPlainLane` read the lane in place, one checked read per
surviving row. `foldSlicedWindow`, `foldSumsWindow` and `foldNumericPlainWindow` instead fold a
64-row value window that `ProjectionColumnGroupScan.gatherWindow` copied out of the lane with one
counted, intrinsified `MemorySegment.copy` — one bounds check per vector rather than per row — and so
index their values by the bit position rather than the row. `gatherPays` chooses per selection word
at the `ProjectionVectorKernels.FOLD_WALK_MAX_BITS` crossover, the same walk-versus-word crossover
the compare and fold kernels already dispatch on: below it a gather would read 64 rows to use a
handful, above it the per-row checks cost more than the copy. The window is per-thread scratch, 512
bytes per column reused for every word of every leaf — never a column representation, and nothing is
allocated per leaf, which is what distinguishes it from the `numericValues()` seam it removes.
`GroupFoldLaneEquivalenceTest` pins the two forms to identical accumulators, over native slab lanes
and over the heap-array lanes a still-heap windowed slice hands them, so the choice is about speed
alone.

**Reading operand lanes in a derived column.** `ProjectionComputedColumn` evaluates a program over
operand lanes for every row of a kept leaf, because the group kernel's own per-row mask decides later
which values count, so its walk is normally dense rather than selective. It has three read forms and
decides from one presence pass per leaf. When every row carries every operand — the shape of every
measured route — it stages each operand lane with ONE counted copy of the leaf's own rows and walks
the rows counted: a per-word gather would move the same bytes in 64-row pieces and pay sixteen times
the `MemorySegment.copy` calls for a full leaf, which is what that copy costs at this size, not its
bytes. When some row lacks an operand it falls back to the group folds' own per-word choice at the
same `gatherPays` crossover, so a selective leaf wastes nothing. Both buffers are flat — operand `c`
at `c * stride` in one array, not one array per operand — so a read is a single bounds-checked load of
a loop-invariant array, and neither is allocated per leaf: the window is 512 bytes per operand and the
staging buffer is allocated once per pass, at the row-group row bound, when the first fully present
leaf asks for it. A leaf above that bound reads per word instead. Each walk is deliberately its own
small method: the program interpreter runs once per row, and a caller it cannot be inlined into pays
that call for every row of the query. `ComputedColumnLaneEquivalenceTest` pins the three forms to an
independent oracle at every density and program width, over slab lanes and over the heap-array lanes
an unconverted producer still hands out.

**Reading a lane in a membership filter.** The two sides of a semi/anti join over an equality walk
their lanes by bit for the same reason a group fold does, and at a density that is the opposite of the
fold's: `MaskedColumns.presentLongValues` interns the build side's values and
`ProjectionRecordKeySet.filterMembership` tests the probe side's, and on the measured anti-join route
each reads about a quarter of a million rows per query with nine words in ten carrying more than the
crossover's eight. Both therefore decide a read form from ONE sizing pass per leaf — the
selection-AND-presence the walk would do anyway, popcounted — and have three. A leaf with a row to
read in more than one row in eight stages its whole lane with one counted copy and the walks index the
staged array: a projection leaf is sixteen words, so the per-word form would move the same bytes and
pay sixteen times the `MemorySegment.copy` calls, the same accounting the derived column's staging
rests on. Otherwise a word above the `FOLD_WALK_MAX_BITS` crossover is gathered into a reused
512-byte window, and a word at or below it is read in place. Nothing is allocated per leaf or per
word: one scratch per pass, the staging buffer at the row-group row bound taken when the first dense
leaf asks for it, and a leaf above that bound reads per word. Each walk is its own small method so
every dispatch arm keeps inlining it. `membershipStagedLeaves()`, `membershipGatheredWords()`,
`membershipInPlaceWords()` and the matching `MaskedColumns.value*` counters report which form ran;
`MembershipLaneReadFormsTest` and `MembershipValueSetReadFormsTest` pin all three to an independent
oracle, over slab lanes and over the heap-array lanes an unconverted producer still hands out, and
pin the gather's clamp on a partly valid last word.

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
budget like every other, and a refusal falls back to heap arrays and counts a declined slab fill.

**Query-local slabs are recycled.** A view that is not cached, a group route's masked lanes and an uncached
join side decode into a slab that lives for one query. Mapping, zero-faulting and unmapping such a slab
on every query measured as +10 % on a 50 ms query once the kernels read the lanes in place, so
`FrameSlotAllocator.releaseSlab` keeps released slabs mapped for the next slab of the same page-rounded
size (`getRecycledSlabBytes`, bounded by `sirix.allocator.slabRecycleBytes`, by default one sixteenth
of the allocator budget and at most 512 MiB). Their pages stay committed and dirty: a consumer never
assumes zeroed slab memory (the test seam `sirix.allocator.poisonRecycledSlabs` fills a reused slab with
`0x5A`), the recycled bytes stay in the allocator's committed total, any commitment that does not fit
the budget unmaps them first, and shutdown unmaps them all. A reuse hands out a fresh global-scope
segment over the same pages, so the instance released before cannot release the new owner's slab.

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
entry can never be acquired again. Every entry owns a slab (at least its key lanes); an entry that
reads the store's resident columns in place also holds their pins until that same moment
(`Entry.released`). Views are closed by the
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

Admission is reservation-first and copies no column. The executor lays out the surviving leaves'
lanes of every non-resident long-lane field of the selection and, behind them, the surviving
leaves' record keys in one slab (`maskedLaneLayout`, `KeyLanes.layout`); a routed selection whose
keys prune no leaf keeps every leaf explicitly rather than reading the store's whole columns
(which would fill them resident, one slab each, and spread the side's lanes over several
segments). It reserves the slab from the
numeric share (`NumericMaskedColumnCache.reserve`), decodes the lanes straight into it, moves the
keys in once from the store's decoded KEYS arrays (`KeyLanes.copyInto`, eight bytes per row, the
arrays are not retained), and publishes the view as the entry (`publish`); fields the store already
holds resident are read in place and pinned by the entry. Only immutable committed selections with
a key roster qualify: write-transaction selections stay query-local. When the share or the
allocator refuses the reservation, the lanes and keys go into a query-local slab the view owns, and
nothing is cached; only an allocator that refuses that slab too leaves the keys in their arrays
(viewed as heap segments) and the lanes in heap arrays. A background packing of an interval-join
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
