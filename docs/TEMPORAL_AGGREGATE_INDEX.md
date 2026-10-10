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

Each group has a sparse segment tree over exact millisecond coordinates. `TemporalAggregateDomain`
uses a 27-bit millisecond-of-day radix under the UTC epoch day, keeping day boundaries aligned
without losing arbitrary millisecond precision. Its admitted instant range is the same as
`IntervalDomain`. Fixed coordinates avoid recompressing endpoints when an update introduces a new instant. An interval contributes to its canonical cover. Read links skip empty unary paths. A point
query folds the summaries on that path and never enumerates matching records.

Each record stores its interval and lane values once. Canonical cover nodes maintain count and
signed 128-bit sums by batched deltas. Only lanes declaring minimum or maximum maintain a persistent
Patricia tree of value frequencies; equal values share a leaf, and both extrema share one tree.
Deleting the last occurrence of an extremum updates its frequency path without scanning intervals. The summary count bounds the integer sums: long values over a long record count fit in
signed 128 bits. Products are checked before entering the index; an overflowing product declines
index serving instead of wrapping.

The projection listener supplies its authoritative dirty-record set after installing its new rows.
Changes to projection columns outside the declaration are filtered before any aggregate I/O.
The aggregate maintainer reads the previous contribution from its own persisted membership slot
and the current values from the covering projection. Changes are accumulated by group, canonical
cover node and value, then merged in sorted batches. Each frequency subtree is merged once
and each touched segment ancestor is written once per maintenance batch. Projection page flushes
coalesce identities and column masks; aggregate I/O waits until the commit maintenance barrier.
Thus structural edits and page flushes cannot turn a publication into per-segment updates. Explicit
index-maintenance barriers also publish pending changes. Maintenance does not rebuild an entire
group or resource.

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

`TemporalAggregateTreeTest` compares randomized batches with an independent scan oracle, checks
both interval boundaries and deletion of extrema, and preserves prior snapshots.
`TemporalAggregateIndexTest` exercises a non-SH1 resource, persistent historical roots, updates,
deletes, insertion, rollback, grouping rollup and exact-instant admission under all four versioning
types. `TemporalAggregateMaintenanceWorkBudgetTest` bounds node work and copied aggregate leaf
pages for 20 changed rows beside differently sized untouched populations. The work-budget commands
in `VERIFICATION.md` include the maintenance budget; no test asserts wall-clock latency.
