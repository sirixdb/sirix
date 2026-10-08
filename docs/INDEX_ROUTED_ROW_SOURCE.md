# Index-routed row source for projection scans

A secondary index can prove which rows a query reads; the projection (column) index holds those
rows' fields as columns. This mechanism joins the two: the index's answer becomes a **row mask** over
the projection, and the projection kernels fold only the masked rows. No record object is
materialised, and no leaf without a masked row is fetched.

The first consumer is the bitemporal opener: a grouped FLWOR over
`jn:open-bitemporal('db','res', T, P)` is served from the resource's projection at the revision
current at `T`, masked by the valid-time index's half-open stab at `P` (the SH1 queries Q7 and Q8;
see [Valid-time key slices](VALID_TIME_KEY_SLICES.md) for the stab). The mechanism itself is
general: any sorted set of record keys is a row source.

## The predicate: `Op.KEY_IN` on the virtual KEYS column

`ProjectionIndexScan.ColumnPredicate.recordKeysIn(long[] sortedKeys)` builds a predicate whose
column is `ProjectionColumnStore.KEYS_COLUMN` (`-1`, the KEYS lane every projection carries) and
whose literal is the strictly ascending key set. A row passes iff its record key is in the set.
There is no presence to AND: every row carries a key. The predicate's `keySetHash` is part of its
identity, so a plan memo keyed by predicate shape never confuses two key sets.

Every mask evaluator honours it:

| Evaluator | How |
|---|---|
| Sliced conjunctive and tree evaluators (`ProjectionColumnScan.evaluateMask*`) | the KEYS column's "slice" is built from the leaf's decoded record keys (`ProjectionColumnStore.recordKeyPredicateView`, `LeafColumnAccess.predicateSlice`) with the leaf's exact key range as its zone; `evalNumeric` runs the membership walk |
| Whole-leaf byte kernels (`ProjectionIndexByteScan.evalPredicateLeafMask`) | the walk over the payload's inline record keys; the presence AND is skipped |
| Leaf keep mask (`ProjectionColumnScan.pruneLeaves`) | a leaf none of whose keys (exact range from the retained KEYS chain, memoised in `recordKeyRanges`) is in the set is dropped before any column segment is fetched |
| Residency and sliceability gates | the virtual column is priced as the KEYS chain, never as a stored column |
| Page scan (`ProjectionIndexScan.evalColumn`) | refuses by name: the materialising reference path does not serve it |

The membership walk (`ProjectionRecordKeySet.andMembership`) is a merge over the leaf's keys, which
ascend except at **order exceptions** (rows stored out of key order), with a binary search for each
key that breaks the run: `O(rows + |set|)` on the common path and exact for both.

## Serving a grouped aggregate under a row mask

`SirixVectorizedExecutor.executeGroupByAggregate(..., GroupRouting)` takes the row source and the
computed lanes. The route appends the `KEY_IN` predicate to the conjunctive predicates, or ANDs it
over the root of a predicate tree, and otherwise runs the ordinary group arms. Under routing the
metadata routes that read no row mask (sorted top-K, scalar value summaries, any-K groups) are
skipped, and the request claims the sliced arms even after the handle's payloads were promoted to
whole-leaf scans: the row source prunes leaves there, and the derived lanes exist only as slices.

**Computed lanes.** A pre-group `let $v := $r.a * $r.b` (any `+,-,*` program over the loop var's
fields and integer literals, `ComputedProgram`'s encoding) is an aggregate operand `prog:<i>`. The
executor resolves its operand columns (NUMERIC_LONG, integral, null-free), evaluates the program once
per kept leaf into a query-local derived column (`ProjectionComputedColumn`; a row missing an operand
is missing in the derived column, exactly the interpreter's empty arithmetic), and hands that column
to the ordinary group kernels in place of a stored one. Exact arithmetic or decline: an overflow is an
`ArithmeticException`, which routes the query to the generic pipeline's decimal promotion.

**`count($let)`.** `count` of a let bound to a field is `fn:count` of the field's values in the
group — the lane's present count, not the row count — and is emitted from that lane. In-kernel
ordering on such an entry declines; the wrapper's sort applies the order-by.

## Admission

`IndexRoutedSourceStage` (before `GroupAggregateDetectionStage`) recognises a loop whose source is
`jn:open-bitemporal` with literal database and resource names, sets the source path to the array
members (the path the projection is declared on) and records the two instant expressions.
`GroupAggregateDetectionStage` then admits the pipeline exactly as it admits a document scan, plus
computed lets and `count($let)`. `SirixPipelineStrategy` compiles the instants at the pipeline's
entry scope (they may read prolog and outer variables, never a variable the pipeline binds before the
loop) and builds `SirixGroupAggregateExpr` with the routed source.

Per evaluation the expression evaluates the instants, resolves the document at `T` (the revision
current at that instant), takes the valid rows' record keys from the valid-time index
(`ValidTimeIntervalIndex.keys`, half-open, exact — inexact candidates are verified), acquires the
executor bound to that revision through the chain's per-source resolver, and serves. Anything on the
way that cannot be served — an instant that is not a dateTime, a resource without valid-time
configuration, a projection that does not cover the fields, an executor at another revision — falls
back to the generic pipeline, which evaluates the very same opener.

**Order.** The opener yields rows in record-key order; the projection folds them in physical row
order. The two agree except at order exceptions, so a routed grouping is served only under an
`order by` that names every group key: the wrapper applies it and the groups' order is total. An
unordered routed grouping stays generic.

## The other SH1 shapes

**Correlated grouping** (Q6, Q11; `CorrelatedGroupAggregateDetectionStage`,
`SirixCorrelatedGroupAggregateExpr`): an outer loop over a small table supplies the opener's
instants and some group keys. The outer prefix (the outer loop and its selections) runs as an
ordinary operator chain; per outer tuple the outer keys are evaluated by the interpreter and the
inner grouping — exactly the plain shape, over a synthetic pipe the stage builds and the plain stages
annotate — is served from the projection under that tuple's row mask; the groups merge on (outer
keys, inner keys) with `count` and `sum` added exactly and `min`/`max` compared (an `avg` or a
distinct count would need the lanes behind the emitted value, and declines). The order-by must name
every key. Executors are resolved per revision through the chain's per-source resolver; a query
touching more revisions than the resolver caches re-creates executors as it goes.

**Membership filter** (Q12): `where empty|exists(for $b in SRC2 where $b.f eq $r.g return …)`
under a routed loop, with `SRC2` a second routed opener (or a leading `let` bound to one). The filter
source's `f` values are read under its own mask, the main rows' `g` values decide which record keys
survive (a missing value matches nothing: it survives an anti-join and fails a semi-join), and the
grouping runs over the reduced key set. The hash-membership pipeline stays the fallback.

**Column-side equality join** (Q9; `JoinedGroupAggregateDetectionStage`,
`SirixJoinedGroupAggregateExpr`): Brackit's `Join` node over two single-loop branches, each over a
routed opener or a literal document, comparing one integral field of each side. Both sides' columns
are read under their row masks (`MaskedColumns`), the smaller side is hashed on its join values,
the other probes; every matched pair folds into a group keyed on fields of either side (string keys
are interned once per leaf dictionary into one id space shared by both sides) with
`count` (pairs, or present values of a field), `sum`, `min` and `max` over fields or `+,-,*` programs
of one side. No post-join predicate, no aggregate over both sides, an order-by naming every key; an
overflow declines to the generic `TableJoin` pipeline.

## Tests

- `RecordKeySetPredicateTest` (core): every kernel family — sliced resident and windowed, conjunctive
  and tree, whole-leaf bytes, the keep mask — against a brute-force walk, on leaves with order
  exceptions, alone and conjoined with column predicates.
- `IndexRoutedGroupAggregateTest` (query): the routed grouped shapes against the generic pipeline,
  byte-for-byte, over all four versioning types, at five transaction instants (old revisions
  included) and seven valid instants (both half-open boundaries included), with missing fields;
  the membership semi- and anti-joins; the correlated shapes (including an outer key that repeats,
  so groups merge); the joins (two openers, an opener and a document, duplicate hashed join values).
- `IndexRoutedGroupWorkBudgetTest` (query, work budget): a routed grouping materialises no object
  (no cursor move on the opener's document) and prunes the leaves that hold no admitted key; the
  generic reference over the same decorated cursor is the positive control.
