# Versioned temporal aggregate indexes

A temporal aggregate index answers grouped aggregates at one half-open valid-time point in a
committed revision. It is opt-in and depends on an existing covering projection and the resource's
valid-time configuration. The same declaration API serves SH1 and other JSON resources.

```xquery
jn:create-temporal-aggregate-index(
  $document, '/[]', ('region', 'category'),
  ('count(*)', 'sum(quantity)', 'sum(price*quantity)', 'min(price)', 'max(price)'))
```

Commit the declaration through the ordinary transaction API. Field names are relative to the
projection root; full declared field paths are also accepted. A numeric operand is a projected
integer field or a checked product of two projected integer fields. A declaration supports up to
16 group fields and 16 distinct numeric operands. Group keys may be integers, strings, booleans,
or missing values. `TemporalAggregateDeclarations.declare` is the equivalent Java API.

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

## Storage and updates

`TEMPORAL_AGGREGATE` has a dedicated HOT container under the revision root. Its values are opaque
slots with replacement semantics and zero-length tombstones, like projection slots. Page ownership,
rollback, fragment reconstruction and sliding-snapshot carry-forward use the normal Sirix storage
engine. There is no separately published sidecar or process-local authority for committed answers.

Each typed group head points to at most sixteen immutable parts. A part contains sorted valid-time
endpoints with prefix count, presence and signed 128-bit sums, plus a compressed segment tree of
value-frequency vectors for declared extrema. Deltas with at most 64 extrema changes instead store
sorted `(from, to, value, sign)` candidates. A point read filters this bounded list into a signed
frequency prefix; checkpoints and larger deltas use the time tree. Equal frequency vectors within
an indexed part share their encoded bytes. The format uses 2,048-byte HOT chunks; indexed point
reads load only the chunks reached by binary search and the compressed tree path.

`TemporalAggregateDomain` uses a 27-bit millisecond-of-day radix under the UTC epoch day, keeping
day boundaries aligned without losing arbitrary millisecond precision. Its admitted instant range
matches `IntervalDomain`. Fixed coordinates allow new endpoints without changing the canonical
cover of older intervals. Each signed interval change contributes extrema frequencies to that
cover. A point query combines prefix sums and frequency vectors from at most sixteen parts. Extrema
first check up to 32 candidates, then use weighted binary selection over the combined frequency
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
represented-revision reader opens. Columns outside a declaration cause no aggregate I/O. A
transaction-local cache accounts for index creation and repeated explicit barriers; rollback
removes all unpublished changes through normal page ownership.

Changes are accumulated in sorted group, endpoint and value order. A touched group appends one
compact signed part per revision. Repeated barriers fold into that revision's part. A batched fold
creates a checkpoint at fan-in sixteen, or when accumulated delta bytes exceed half the checkpoint
size (with a 2,048-byte floor for small groups). This bounds read fan-out but periodically performs
work proportional to the touched group's accumulated summaries; it is not constant-cost maintenance.
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

The grouped and correlated grouped routes try this index before constructing the valid-time key
mask. They prefer the narrowest declared grouping that covers the query keys; a wider grouping can
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

SH1 declares `grade` and `(sid, grade)` groupings on contracts in
`BitemporalTemporalAggregates`. The loader's
`-Dsirix.bitemporal.disableTemporalAggregates=true` switch creates the comparison store without
these declarations. `SIRIX_COMMIT` output separates publication apply and commit costs.

## Verification

`TemporalAggregatePartsTest` compares randomized signed parts and checkpoint folds with an independent scan oracle, checks
both interval boundaries and deletion of extrema, and preserves prior snapshots.
`TemporalAggregateIndexTest` exercises a non-SH1 resource, persistent historical roots, updates,
deletes, insertion, rollback, grouping rollup and exact-instant admission under all four versioning
types. `TemporalAggregateMaintenanceWorkBudgetTest` bounds node work and copied aggregate leaf
pages for 20 changed rows beside differently sized untouched populations, and requires zero new
readers during maintenance. `ChangeLogWorkBudgetTest` bounds slab slots and verifies cold historical
reads and reverted bases under all four versioning types. The work-budget commands
in `VERIFICATION.md` include the maintenance budget; no test asserts wall-clock latency.
