# Versioned temporal aggregate indexes

A temporal aggregate index answers grouped aggregates at one half-open valid-time point in a
committed revision. It is opt-in and depends on an existing covering projection and the resource's
valid-time configuration. The same declaration API serves SH1 and other JSON resources.

```xquery
jn:create-temporal-aggregate-index(
  $document, '/[]', ('region', 'category'),
  ('count(*)', 'sum(quantity)', 'sum(price*quantity)', 'min(price)', 'max(price)'))
```

Commit the declaration through the ordinary transaction API. Declaration fields use paths relative
to the projection root, such as `region` or `address.zone`. A numeric operand is a projected
integer field or a checked product of two projected integer fields. A declaration supports 1–16
group fields and up to 16 distinct numeric operands. `count(field)` and `count(field*field)` count
present operands; `count(*)` counts records and is supplied by every declaration. Group keys may be
integers, strings, booleans, or missing values. `TemporalAggregateDeclarations.declare` is the
equivalent Java API.

The catalogue stores the source projection id, root, grouping columns, operand columns and required operations.
Catalogue reads retain a private copy of the aggregate declaration without decoding its lanes.
The first aggregate consumer decodes and validates those semantics once; unrelated queries and
catalogue copies do not initialize aggregate specifications. Creation validates the supplied
specification immediately. Malformed persisted operand declarations fail when first consumed.
Optional aggregate and change-log container classes are resolved only when those page types are used.
The shared page cache checks the `IndexRootPage` ownership marker instead of loading every index
container implementation to classify an ordinary page; revision-owned containers remain uncached.
`jn:drop-temporal-aggregate-index($document[, $id])` removes one declaration or all declarations.
Physical ids are never reused. Removing a source projection also removes its dependent aggregate
definitions from the new revision; historical catalogues and roots remain readable.
Aggregate CREATE and both aggregate DROP signatures are updating functions. REST checks MODIFY
permission before evaluating an updating query, including a commit-wrapped DROP.

Committing a historical document preserves pending DDL and edits when the writer already
represents that document's revision. Commit waits for an outstanding asynchronous publication
before checking the represented revision, and reverts only when a historical request needs a
different base. This applies to aggregate creation, both DROP signatures, and projection DROP
cascades.

Subsequent aggregate CREATE and aggregate or projection DROP operations on the same historical
document use that writer's pending catalogue and projection metadata. They retain earlier
declarations, drops and data edits. Stale or missing metadata, missing requested ids and unsupported
declarations reject before changing lineage; dropping nothing leaves pending work intact.

## Storage and updates

`TEMPORAL_AGGREGATE` has a dedicated HOT container under the revision root. Its values are opaque
slots with replacement semantics and zero-length tombstones, like projection slots. Page ownership,
rollback, fragment reconstruction and sliding-snapshot carry-forward use the normal Sirix storage
engine. By default these trees live in the hidden `<source>$derived` resource, with their own
writer, revision roots and catalogue. The source stores declarations and durable change logs.
The persisted byte layout is owned by [DISK_FORMAT.md](DISK_FORMAT.md#2-pages).

The `$derived` suffix is reserved: public database handles cannot create, open or remove these
internal resources, and `listResources()` omits them. Resource names are normalized to a single
relative path component before lookup, so aliases such as `records/` identify the same source and
cannot bypass the hidden-resource guard. Removing a source also removes its derived sibling,
including an incomplete directory. Derived sessions survive the last application session closing;
the database owns their lifetime.

Each typed group head points to one checkpoint and at most fifteen immutable delta parts per tier.
A part contains sorted valid-time endpoints with prefix count, presence and signed 128-bit sums, plus a compressed segment tree of
value-frequency vectors for declared extrema. Deltas with at most 64 extrema changes instead store
sorted `(from, to, value, sign)` candidates. A point read filters this bounded list into a signed
frequency prefix; checkpoints and larger deltas use the time tree. Equal frequency vectors within
an indexed part share their encoded bytes. The format uses 2,048-byte HOT chunks; indexed point
reads load only the chunks reached by binary search and the compressed tree path.

`TemporalAggregateDomain` uses a 27-bit millisecond-of-day radix under the UTC epoch day, keeping
day boundaries aligned without losing arbitrary millisecond precision. Its admitted instant range
matches `IntervalDomain`. Fixed coordinates allow new endpoints without changing the canonical
cover of older intervals. Each signed interval change contributes extrema frequencies to that
cover. A point query combines prefix sums and frequency vectors from the checkpoint and the tiered
delta parts. Extrema first check up to 32 candidates, then use weighted binary selection over the combined frequency
prefixes, bounding work even when a deletion removes a long run of minimum or maximum values.
Raw deltas inspect at most 64 candidates per extrema lane per part, independent of the stored record
population. The index never enumerates the matching records during a point query.

The projection listener coalesces dirty identities and column masks through page flushes. It captures
old cells from the decoded leaves or column slices before replacement, and new cells from the
extractor already maintaining the projection. A format-1 `CHANGE_LOG` slab persists the first old
and final new projected cells, changed masks and represented base revision. A slab-local string
dictionary avoids repeated endpoint text. Sparse row-local provenance disambiguates conservative
leaf flags without reopening historical document readers. The aggregate kernel consumes the same
decoded slab for every declaration, caching repeated endpoints while applying it; it performs no projection reads or
represented-revision reader opens. Unchanged maintenance barriers reuse the already-published batch;
each new covered revision still receives an explicit empty slab. Columns outside a declaration cause no aggregate I/O. A
transaction-local cache accounts for index creation and repeated explicit barriers; rollback
removes all unpublished changes through normal page ownership.

Changes are accumulated in sorted group, endpoint and value order. The database owns one lazy daemon
maintainer, which retains the source and derived sessions independently of application handles.
A shared revision listener wakes it only after the source revision is durable and published; a
one-second recheck also finds missed notifications. Ordinary source commits neither acquire the
derived writer lock nor write aggregate pages. The hidden resource uses `FILE_CHANNEL`,
`SLIDING_SNAPSHOT`, custom source timestamps, and no diffs, node history or path summary.
For an encrypted source, it inherits the source byte-handler pipeline and encryption key reference.
Opening an existing derived resource checks that it preserves the source's encryption configuration;
an incompatible resource is rebuilt rather than accepted.

The maintainer builds new declarations from the committed projection and applies subsequent slabs
without projection or document reads. It mirrors only aggregate declarations into the derived
catalogue, retaining their source ids. Drops affect new snapshots; historical snapshots keep their
catalogues and parts. Rebuilding a declaration after a revert resets its aggregate contents and
part allocator while preserving existing revision mappings. Ordinary maintenance does not rewrite
a catalogue file. Batches process at most eight revisions, stopping between revisions once they
reach 100,000 entries; an individual slab is not split. Each batch has one derived commit and exact
mapping at its end. Catalogue changes end a batch so declaring revisions have a snapshot.
Intermediate revisions use the exact fallback until query-time tail catch-up is available.

Catch-up appends immutable signed delta parts. Group heads also record checkpoint and delta byte
counts. Groups with more than sixteen parts or delta bytes above half the checkpoint become fold
candidates. A separate derived commit folds candidates when idle; sustained publication forces a
separate fold after 64 revisions, or earlier to leave room for batch replay after a crash, within
the existing 91-part representation. Catch-up never
folds old parts. A fold reads only its candidate groups and atomically republishes the mapping for
the same source revision. Signed intermediate parts retain negative counts and frequencies; only
complete checkpoints and query results require nonnegative counts.

Prefix-4 mappings bind source revisions to derived revisions and to a durable 128-bit source log
identity. Queries validate that identity and borrow the mapped derived snapshot for the lifetime
of their immutable source reader, which closes it. A query context instead owns up to 64 snapshots
on its coordinating thread, reusing them across source cursors from the same published history.
The cache keys include the captured source UberPage's object identity, so commit, truncate and
reopen require fresh identity validation even when revision numbers and timestamps are reused.
Eviction and context close release these readers; other threads keep source-owned snapshots.
Missing snapshots are retried after catch-up.
A cold query can read the durable mapping before the daemon has initialized its bounded memory
cache. Mapping-cache admission validates source identities, and rewinds evict discarded history.
Maintenance resolves its frontier from durable mappings when bounded cache eviction removes it.
Resolving a new snapshot declines to the existing exact masked/scan route if no exact mapping
exists, maintenance is poisoned, or the derived snapshot cannot be opened. A previously validated
snapshot remains exact for its immutable source view. The route never serves a stale summary. Query-time log-tail catch-up
and a query-language await function are not part of this change.

A revert uses the slab's represented base revision, reverts the derived writer to that source
mapping (replaying slabs from the preceding mapped revision for a batch interior), then applies
the new slab. Mapping entries for previously published branches survive the
revert. Truncation invalidates mappings above the surviving source frontier and serializes against
active maintenance, without putting a lock on normal source commits. Before replaying a replacement
history, the maintainer truncates its own discarded revisions. Equal source timestamps are safe:
revision identity, rather than timestamps alone, determines admissibility. After a process crash,
normal Sirix beacon/commit-marker recovery chooses the durable derived root; its mapping determines
where replay resumes.

A missing, incomplete or unreadable derived resource is recreated from the authoritative source.
Rebuilding scans committed projections for declarations and replays subsequent covered slabs;
queries use the exact fallback until a matching derived snapshot is available.

Failures roll back the derived transaction and poison only that resource. Maintained revision, lag,
poisoned revision and cause are exposed through `DerivedIndexMaintainer.Resource`; the source remains
usable and queries retain exact fallback behavior. Shutdown stops the worker before closing resource
sessions and allows up to 30 seconds for an in-flight batch to finish. Lag at shutdown is safe.

Capture and slab persistence run only while the projection has a declared aggregate consumer.
Dropping the last consumer stops logging; declaring a new one rebuilds sparse row provenance from
the current projection. Slabs from earlier covered revisions remain readable; revisions without
a consumer have no slab.

Initial builds and incremental aggregate writes run off the source commit path.

Long operands and long record counts bound sums within signed 128 bits. Products are checked before
entering the index; an overflowing product declines serving instead of wrapping. Removing an
unsupported row restores eligibility in the next revision.

## Admission and fallbacks

Group dictionary admission counts at most the current root array's row population plus one,
without copying keys or decoding heads. A dictionary larger than that population declines to
the masked projection route before fetching aggregate parts. This bounds admission after group
churn or deletion even though empty heads remain in the versioned dictionary. Both ordinary and
correlated grouped routes use this check against their selected revision; populated historical
revisions can still serve their aggregates.

The grouped and correlated grouped routes try this index before constructing the valid-time key
mask. Serving requires the selected revision's source to be the whole document-root array at
`/[]`; a nested array or replacement of the root array with another shape declines. They prefer
the narrowest declared grouping that covers the query keys; a wider grouping can
be folded into fewer keys. Each requested aggregate operand and operation must be covered. Residual predicates,
membership filters, transformed grouping keys and unsupported aggregate functions retain their
existing execution path. Ordinary grouped queries must order by every group key; otherwise the
existing path preserves first-seen group order. Correlated grouped admission already requires a
total order over its group keys.

Only exact millisecond instants inside the configured domain are admitted. Endpoint columns may
contain timestamp strings or typed timestamps. Typed endpoints follow the projection's
[epoch and UTC provenance contract](DISK_FORMAT.md#projection-indexes-segment--slot-layout).
Unsupported source values or non-exact interval endpoints are tracked in the revision's header and
conservatively decline index serving. Missing values retain aggregate presence counts. A missing
lower or upper bound makes that side open-ended; records with neither bound contribute nothing,
matching the valid-time index. These rules apply to initial builds and change-log maintenance.
The index is never served from a mutable transaction. Older queries select the catalogue and HOT
root belonging to their own revision.

Indexed document scans evaluate their source once per binding and share it between aggregate
admission and masked projection fallback. Missing aggregate declarations, uncovered lanes and
oversized dictionaries reuse that reader. Empty arrays defer point evaluation for all comparison
modes, including correlated scans.

SH1 declares `grade` and `(sid, grade)` groupings on contracts in
`BitemporalTemporalAggregates`. The benchmark's
[loader comparison controls](../bundles/sirix-query/bench/bitemporal/README.md#loader-comparison-controls)
own the switches that omit these declarations.

## Verification

`TemporalAggregatePartsTest` compares randomized signed parts and checkpoint folds with an independent scan oracle, checks
both interval boundaries and deletion of extrema, and preserves prior snapshots.
`TemporalAggregateIndexTest` exercises a non-SH1 resource, persistent historical roots, updates,
deletes, insertion, rollback, grouping rollup and exact-instant admission under all four versioning
types. `DerivedIndexMaintainerTest` covers lag, batching, declaration changes, revert and truncate,
mapping-cache eviction, encryption, resource aliases, poisoned maintenance and rebuilding missing
or damaged derived resources. `TemporalAggregateDerivedLagTest` checks query answers against an
independent `BigInteger` model before, during and after replay under all four versioning types.
`DerivedIndexCrashRecoveryTest` injects process death after source publication and before or after
the derived beacon; its opt-in command is in [VERIFICATION.md](VERIFICATION.md).

The counted-work inventory and bounds are owned by the
[work-budget README](../bundles/sirix-core/src/test/java/io/sirix/budget/README.md);
[VERIFICATION.md](VERIFICATION.md#running-the-layers) owns the commands; no budget test asserts
wall-clock latency.

Measurements recorded for commit `5a00aa171` describe the earlier part layout. The tiered layout and
subsequent review fixes require refreshed read, commit and storage evidence before those measurements
can describe the fixed head. Work-budget results establish counted work and exactness, not latency.
