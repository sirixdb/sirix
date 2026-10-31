# Derived query-cache memory

Immutable numeric selections and packed interval-join sides share one process-wide heap
allowance. This is separate from the persisted-page allocator. The allocator allowance is a
scaling input, not storage from which Java arrays are allocated.

On first use, after the allocator is initialized, the automatic target is
`D = min(A / 8, H / 8, S)`, where `A` is the allocator's effective grant (including its
first-initialization and physical-headroom constraints), `H` is the runtime maximum heap,
and `S` is the operator's declared derived-cache heap allocation. Configuration is read at
runtime in JVM and native-image deployments.

Projection, dictionary, HOT heap and derived-cache ceilings come from `HeapCacheBudget`:
the envelope is
`E = min(H / 2, configuredEnvelope)`, defaulting to `H / 2`. The other half of the
heap is reserved for execution, output, GC and application/metadata objects. The
following disjoint shares sum to at most `E`; unused shares are not borrowed.

| Startup property | Default / maximum share |
| --- | --- |
| `sirix.heapCache.maxBytes` | Envelope `E`, at most `H / 2` |
| `sirix.projection.cacheBytes` | `E / 2` |
| `sirix.derivedCache.heapReserveBytes` | `E / 4`, the derived reserve `S` |
| `sirix.projection.globalDict.recordCacheBytes` | `4 / 7` of `E / 8` |
| `sirix.projection.globalDict.verdictCacheBytes` | `1 / 7` of `E / 8` |
| `sirix.projection.segmentDict.verdictCacheBytes` | `2 / 7` of `E / 8` |
| `sirix.hotHeapCache.maxBytes` | `E / 16` |
| `sirix.heapCache.hotLookupBytes` | `E / 32` |
| `sirix.heapCache.hotMiniBytes` | `E / 64` |
| `sirix.heapCache.projectionBlobBytes` | `E / 64` |

Each override accepts a non-negative decimal `long` byte count, is capped by its
share and can disable retention with zero. Invalid values fail configuration.
The existing HOT entry-count limit and persisted-page allocation can further
reduce the applicable HOT cache capacities. GC-owned compact native images are
conservatively charged to this envelope; allocator-frame pages retain their
separate native allocation.

`sirix.projection.eagerMaterializeBytes` defaults to half the effective projection
share. Its override is bounded by four fifths of that share, so a per-store fill
plus its quarter-fill payload-window allowance fit within the catalog ceiling.
The catalog charges the full estimated residency of each handle. Dictionary records include
their retained value, offset, routing and rank arrays in their byte weights. Windows may
stream with no retention when even one would exceed their allowance. `HeapHeadroom`
can further narrow fills and execution budgets using observed headroom; it does
not enlarge the declared envelope or derived reserve. The legacy
`sirix.projection.promoteMaxBytes` threshold is also bounded by the effective per-store fill
limit.

`sirix.derivedCache.maxBytes` overrides the automatic combined derived target;
it is capped by `S` and `H`, and zero disables both derived caches. This override
replaces the allocator/heap fraction target without granting extra heap reserve.

These are retained payload-cache ceilings, not a process RSS guarantee or a
reservation of every live object. Operators must fit the native arena, runtime
heap, other native/direct memory and OS/application reserve within the machine
or container allowance. Active readers, temporary execution and small cache
bookkeeping objects still consume the reserved execution/application half.
A free-heap or after-GC sample is not used to declare `S`.

`HeapCacheBudget.global().metrics()` exposes the configured and effective envelope,
workspace reserve, each configured/effective cache share and the per-store fill
ceiling. `BufferManagerImpl` logs the policy at initialization. Configuration is
fixed at runtime startup, including native-image deployments.

Numeric snapshots receive `floor(3D / 4)`; packed sides receive `floor(D / 4)`.
A single entry must fit its share, and retained entries plus reserved cache-construction
bytes cannot exceed that share. Caffeine supplies weighted replacement; numeric rosters
are evicted individually, so a combined roster that exceeds capacity does not discard
all snapshots belonging to that store. Long byte weights are rounded up to supported
integer weight units; exact byte reservations enforce the bound independently.

Numeric snapshot admission reserves before copying keys and masks, and conservatively
charges every retained numeric lane even if another owner shares it. Background packing
reserves its result, counting-sort scratch and all source lanes it pins. A refused
background build is skipped and the probe is streamed through the existing exact kernel.
The synchronous build side and decoded columns required to execute a query remain query
workspace even when caching is disabled. Their memory, active reader views and ordinary
query output are outside the retained-cache allowance. Temporary cache-construction
reservations are released on failure and reduced to the retained weight on publication.

Only immutable committed selections are eligible. Write-transaction selections and views
with changed masks are excluded. Store identity, exact field/key rosters and packed-side
signatures remain part of reuse checks. Packed reuse still depends on the numeric snapshot's
row identity; independent packed identity and chunk caching are not implemented here.

`DerivedCacheBudget.global().metrics()` exposes the configured target (`-1` for automatic),
effective allocator and heap allowances, declared reserve, effective combined cap, both
partition caps, retained bytes, in-flight bytes and cumulative rejected bytes. Rejected-byte
counters saturate at `Long.MAX_VALUE`. These are cold-path observations, not per-row counters.

Regression fixtures are `DerivedCacheBudgetTest`, `NumericMaskedColumnCacheTest` and
`IntervalRangeJoinWorkBudgetTest`. `HOTGatherPublicationTest` also races first readers of
shared HOT pages: lazy SIMD routing state must be fully constructed before publication.
Run the full **Work budgets** block in
[VERIFICATION.md](VERIFICATION.md) after changing admission or read paths. Timing and memory
acceptance additionally require paired native+PGO runs; unit tests assert work and exact
answers, never elapsed time.

The two cache fixtures also carry the `native-image` tag and run in
`:sirix-query:nativeSmokeTest`. This guards Caffeine's weak-key index and both weighted
partitions, including disabled and long-weight configurations. Native Caffeine registrations
in `META-INF/native-image/io.sirix/derived-cache/reachability-metadata.json` were generated
from GraalVM's tracing agent over these fixtures, the interval-join work-budget fixture,
changed core-cache fixtures and Q4/Q9/Q10. Only Caffeine entries missing from the existing
core/query metadata are included. When changing cache builders, run with
`-agentlib:native-image-agent=config-merge-dir=<output-directory>`, merge the recorded
Caffeine reflection entries, and run the native smoke suite before performance acceptance.
Do not infer generated cache/node class names from a single failing configuration.
