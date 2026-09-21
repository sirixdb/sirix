# JSON update-diff array positions

`JsonDiffSerializer` resolves array positions by memoizing a left-sibling walk. A lookup stops at
the first child or a cached sibling and assigns ordinals while unwinding the visited node keys.
The cache key is a node key in one read-only revision; it never depends on a path, field, or value.
This changes traversal work, not the path syntax, operation order, payload, or integrity metadata.

## Revision stability and lifetime

Each `serialize` call opens separate read-only transactions for its old and new revisions and
creates one cache for each. `AbstractResourceSession.createStorageEngineReader(revision)` passes
that explicit revision to `NodeStorageEngineReader`, whose revision number and revision root are
final fields and whose epoch ticket protects its reads. Moving a cursor does not mutate a node.
Later writes create a new revision; the old reader continues to follow its own revision root.

On the synchronous commit path, `AbstractNodeTrxImpl.commitInternal` invokes diff serialization
after the storage commit and publication of the committed uber page, before re-instantiating the
writer. On the pipelined path, page serialization and registration of the pending revision root
precede diff serialization; the successor writer is re-instantiated afterward. The diff therefore
does not inspect sibling links while a writer is still inserting or removing nodes in that epoch.
An insert or removal in a later revision can shift a surviving node's ordinal, which is why the
old and new caches must never be shared. `positionsAreIsolatedBetweenRevisions` guards that case.

The caches are local to one serialization and become unreachable when it returns (the production
test observer is absent). A long-lived transaction cannot accumulate caches from past commits.

## Work and memory bounds

For one array, let `K` be one plus the largest requested index and `M` the number of lookups.
The walk takes at most `K - 1 + M` sibling steps: a newly encountered node is memoized once, and a
lookup may take one final step onto a cached anchor. A single lookup takes no more steps than its
own index. No child count is narrowed to `int`, and an untouched suffix is neither traversed nor
used to size an allocation. The serialized ordinal retains the previous `int` arithmetic.

Across a revision, the primitive map retains the union of the touched prefixes, not each prefix
once per tuple. The reused primitive walk stack retains capacity for the longest uncached walk.
Fastutil grows both geometrically. Memory is therefore proportional to those touched nodes, plus
constant initial capacity; it can still be large when a diff actually names a far-tail element.
The same bound applies separately to the old and new revisions.

That map is allocated by the first lookup that walks, not when the cache is created: fastutil sizes
a default `Long2IntOpenHashMap`'s backing arrays in its constructor (396 payload bytes), while a
default `LongArrayList` defers its own. A serialization that resolves no array position therefore
allocates no cache backing storage - both the `buildPathSummary(false)` case, where `getNodePath`
returns before any cache is touched, and any document whose emitted paths carry no array step.
A revision whose cache is never asked for an ordinal costs nothing on the default commit path.

`JsonDiffArrayPositionWorkBudgetTest` is part of the ordinary `io.sirix.budget.*` lane in
[`VERIFICATION.md`](VERIFICATION.md). It covers forward, reverse, and shuffled tuple order, a
100,000-element array followed by a single head insert with default diff storage, and a diff that
resolves no array position at all. Both fixtures stay at the scale the budget package already uses,
so the memory-constrained cross-platform lanes run them; the guard comes from the bound, not from
the size of the data. The head-insert case resolves a second, non-head tuple through the same
counting session afterwards, so its zero move count is proven to be no work rather than a decorator
that has fallen off the serializer's cursor route.

`ArrayPositionCacheProbe` observes actual key, ordinal, and walk-stack backing-array capacities
after serialization. Their payload sizes are independent of JVM object headers and only grow
during a call, so the final retained payload is also the peak retained payload. This is a bound
on cache backing storage, not all heap allocation by commit. Its nonzero observation count and
entry count prevent a disconnected probe from passing with zero bytes.

The regressions were checked by mutation:

| Shape | Memoized walk | Reintroduced defect |
|---|---:|---:|
| All 10,000 positions, forward order | 9,999 sibling moves | 49,995,000 without memoization (20,000 ceiling) |
| Head insert into 100,000 elements, commit plus equivalent counted serialization | 0 sibling moves; 792 backing payload bytes; 2 cached entries across 4 revision caches | Eager preallocation alone still makes 0 moves and 2 entries, but sizes the cache from the array's length, overshooting the 1,024-byte ceiling by three orders of magnitude |
| Diff whose emitted paths carry no array step | 0 backing payload bytes across 2 revision caches | Allocating the ordinal map with the cache reports 792 |

Byte compatibility is checked by readable goldens only, so a break names the bytes that moved and
can be re-derived by reading the fixture. `JsonDiffSerializerArrayPositionTest` asserts one literal
whole-sidecar string for a document mixing a top-level array, a nested array, and an object-named
array, then re-serializes the same tuples out of document order and asserts every resolved path is
unchanged - the case where one cache serves several arrays. `BasicJsonDiffTest` holds the
`serialize` mode against literal fixtures that already carry array paths (`/[0]`, `/[1]`,
`/[0]/[0]`), and `positionsAreIsolatedBetweenRevisions` covers an ordinal that shifts between
revisions. These are output contracts, not snapshots of implementation source.

Run the relevant verification without a timed benchmark harness:

```bash
./gradlew --max-workers=1 -Dorg.gradle.jvmargs=-Xmx768m \
  -PtestHeapMin=256m -PtestHeapMax=2g \
  :sirix-core:test --tests 'io.sirix.budget.*' \
  --tests 'io.sirix.index.projection.BatchedSegmentReadWorkBudgetTest' \
  --tests 'io.sirix.diff.*' --tests 'io.sirix.property.*' \
  :sirix-query:test --tests 'io.sirix.query.budget.*'
```

The operation and capacity counts are the performance evidence. Ordinary JUnit report durations
can guide test-lane placement; commit-growth timing measurements belong to the separate benchmark
campaign, not verification on a shared machine.

## Which resources pay the cost?

Verified in `ResourceConfiguration`: `Builder.storeDiffs` defaults to `true`, and `pathSummary`
also defaults to `true`. Storage backend, versioning, hashing, Dewey-ID, and valid-time choices do
not change the `storeDiffs` default. Configuration serialization persists the flag and
deserialization restores it; an existing resource explicitly configured with `false` stays off.

Verified creation paths that leave this default intact include the core builder, CLI
`CreateResource`/`AbstractCreate`, JSONiq's `ResourceConfigurations.create`, and the REST JSON
creation/upload handlers. This is source verification, not a count of deployed resources.

The costly resolver is specifically JSON: both commit paths check `storeDiffs()` before invoking
`serializeUpdateDiffs`; `JsonNodeTrxImpl` writes the sidecar for eligible non-bulk commits from
revision 2 onward. Array-position resolution additionally requires a path summary and an array
step in an emitted path. `storeDiffs(false)` disables commit-sidecar generation;
`buildPathSummary(false)` leaves diff storage enabled but skips path resolution. Dewey IDs select
the tuple collection and add metadata without disabling path resolution. XML shares the builder's
flag default, but `XmlNodeTrxImpl.serializeUpdateDiffs` is empty and never runs this JSON resolver.
