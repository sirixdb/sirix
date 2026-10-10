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
engine. There is no separately published sidecar or process-local authority for committed answers.
The persisted byte layout is owned by [DISK_FORMAT.md](DISK_FORMAT.md#2-pages).

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
decoded slab for every declaration and shares parsed endpoints; it performs no projection reads or
represented-revision reader opens. Unchanged maintenance barriers reuse the already-published batch;
each new covered revision still receives an explicit empty slab. Columns outside a declaration cause no aggregate I/O. A
transaction-local cache accounts for index creation and repeated explicit barriers; rollback
removes all unpublished changes through normal page ownership.

Changes are accumulated in sorted group, endpoint and value order. Each publication or explicit
barrier appends a signed part with its own immutable id. Parts carry a size tier: sixteen parts of
one tier merge into one part of the next tier, leaving other tiers untouched. Encoded byte growth
can raise the result's tier, but cannot trigger a separate whole-group fold. The checkpoint's tier
is one above its byte-size tier, using 2,048-byte units and powers of sixteen. Only a cascade reaching
that top tier merges the checkpoint and remaining deltas into a new checkpoint. Signed intermediate
parts retain negative counts and frequencies; only complete checkpoints and query results require
nonnegative counts. At most six delta tiers fit the part format's signed-int byte-length limit, so a
head holds at most 91 parts. Folding work is amortized across changed records and tier promotions;
a top-tier cascade can still perform a large synchronous checkpoint rewrite.

The maintenance guarantee is amortized across publications, with an explicit exception to the
original per-publication changed-record bound. For fixed aggregate lanes and domain height, each
delta contribution is merged at most once per tier, giving an `O(log_16 N)` merge charge per changed
record over the sequence, where `N` is accumulated change volume. A checkpoint rewrite is charged
across the publications that fill its top tier. At fan-in 16, equal-sized tier-0 parts promote to
tiers 1, 2 and 3 after 16, 256 and 4,096
publications respectively. A stable tier-3 checkpoint therefore cascades once every 4,096 such
publications. Larger parts and encoded byte growth can start at, or jump to, a higher tier and reach
the checkpoint sooner; these frequencies describe small parts without byte-tier jumps.

A top-tier cascade reads all prior parts and re-encodes the entire group, including untouched
records. Its worst-case work is linear in the group's stored endpoints, summary nodes and value
frequencies, with chunk I/O proportional to the bytes read and rewritten in 2,048-byte units. A
single small publication can therefore incur resource-sized work when that group spans the resource.
The cumulative work budget includes this cost; it does not provide a per-publication work or latency
ceiling. This synchronous cascade is the approved tradeoff for retaining tiered compaction.

Capture and slab persistence run only while the projection has a declared aggregate consumer.
Dropping the last consumer stops logging; declaring a new one rebuilds sparse row provenance from
the current projection. Slabs from earlier covered revisions remain readable; revisions without
a consumer have no slab.
Maintenance remains synchronous. The log is groundwork for separately maintained derived resources;
there is no background maintainer or query-time lag protocol in this version.
Historical revisions retain their original group heads and parts through ordinary page copy-on-write.

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

Only exact millisecond instants inside the configured domain are admitted. Unsupported source
values or non-exact interval endpoints are tracked in the revision's header and conservatively
decline index serving. Missing values retain aggregate presence counts; missing temporal bounds
have the valid-time opener's open-ended semantics. The index is never served from a mutable
transaction. Older queries select the catalogue and HOT root belonging to their own revision.

Indexed document scans evaluate their source once per binding and share it between aggregate
admission and masked projection fallback. Missing aggregate declarations, uncovered lanes and
oversized dictionaries reuse that reader. Empty arrays defer point evaluation for all comparison
modes, including correlated scans.

SH1 declares `grade` and `(sid, grade)` groupings on contracts in
`BitemporalTemporalAggregates`. The loader's
`-Dsirix.bitemporal.disableTemporalAggregates=true` switch creates the comparison store without
these declarations. `SIRIX_COMMIT` output separates publication apply and commit costs.

## Verification

`TemporalAggregatePartsTest` compares randomized signed parts and checkpoint folds with an independent scan oracle, checks
both interval boundaries and deletion of extrema, and preserves prior snapshots.
`TemporalAggregateIndexTest` exercises a non-SH1 resource, persistent historical roots, updates,
deletes, insertion, rollback, grouping rollup and exact-instant admission under all four versioning
types. `TemporalAggregateMaintenanceWorkBudgetTest` also drives 528 small publications beside 256
and 4,096 rows, then 4,096 publications beside 4,096 distinct-endpoint rows to force the tier-3
checkpoint cascade. It checks the persisted part counts at every tier boundary, requires at most
one checkpoint cascade per boundary, bounds cumulative chunk, endpoint and summary-node reads/writes
per changed record, and checks all committed historical roots. Its first-publication case bounds copied aggregate leaf
pages for 20 changed rows beside differently sized untouched populations, and requires zero new
readers during maintenance. `ChangeLogWorkBudgetTest` bounds slab slots and verifies cold historical
reads and reverted bases under all four versioning types. The work-budget commands
in `VERIFICATION.md` include the maintenance budget; no test asserts wall-clock latency.

Measurements recorded for commit `5a00aa171` describe the earlier part layout. The tiered layout and
subsequent review fixes require refreshed read, commit and storage evidence before those measurements
can describe the fixed head. Work-budget results establish counted work and exactness, not latency.
