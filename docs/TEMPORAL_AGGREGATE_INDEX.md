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
`jn:drop-temporal-aggregate-index($document[, $id])` removes one declaration or all declarations.
Physical ids are never reused. Removing a source projection also removes its dependent aggregate
definitions from the new revision; historical catalogues and roots remain readable.

## Storage and updates

`TEMPORAL_AGGREGATE` has a dedicated HOT container under the revision root. Its values are opaque
slots with replacement semantics and zero-length tombstones, like projection slots. Page ownership,
rollback, fragment reconstruction and sliding-snapshot carry-forward use the normal Sirix storage
engine. There is no separately published sidecar or process-local authority for committed answers.

Each typed group head points to at most eight immutable parts. A part contains sorted valid-time
endpoints with prefix count, presence and signed 128-bit sums, plus a compressed segment tree of
value-frequency vectors for declared extrema. Equal frequency vectors within a part share their
encoded bytes. The format uses 2,048-byte HOT chunks; point reads load only the chunks reached by
binary search and the compressed tree path.

`TemporalAggregateDomain` uses a 27-bit millisecond-of-day radix under the UTC epoch day, keeping
day boundaries aligned without losing arbitrary millisecond precision. Its admitted instant range
matches `IntervalDomain`. Fixed coordinates allow new endpoints without changing the canonical
cover of older intervals. Each signed interval change contributes extrema frequencies to that
cover. A point query combines prefix sums and frequency vectors from at most eight parts. Extrema
first check up to 32 candidates, then use weighted binary selection over the combined frequency
prefixes, bounding work even when a deletion removes a long run of minimum or maximum values.
The index never enumerates the matching records during a point query.

The projection listener coalesces dirty identities and column masks through page flushes. Columns
outside the declaration cause no aggregate I/O. At a maintenance barrier the maintainer reads old
rows from the projection of the writer's represented revision and new rows from its installed
projection. A transaction-local cache accounts for index creation and repeated explicit barriers;
rollback discards it. Conservative projection leaf flags trigger row-local extraction so another
row's unsupported value cannot corrupt a subtraction. No duplicate aggregate row store is persisted.

Changes are accumulated in sorted group, endpoint and value order. A touched group appends one
compact signed part per revision. Repeated barriers fold into that revision's part. At the eight-part
limit, a batched fold creates a new checkpoint. This bounds read fan-out but periodically performs
work proportional to the touched group's accumulated summaries; it is not constant-cost maintenance.
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
pages for 20 changed rows beside differently sized untouched populations. The work-budget commands
in `VERIFICATION.md` include the maintenance budget; no test asserts wall-clock latency.
