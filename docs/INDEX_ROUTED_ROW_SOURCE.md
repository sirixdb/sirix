# Index-routed row source for projection scans

A secondary index can prove which rows a query reads; the projection (column) index holds those
rows' fields as columns. This mechanism joins the two: the index's answer becomes a **row mask** over
the projection, and the projection kernels fold only the masked rows without materialising source
records. Temporal candidates requiring verification retain the checks described in
[Valid-time key slices](VALID_TIME_KEY_SLICES.md#admission-and-exact-fallbacks).
BODY segments of leaves whose exact row-source mask is empty are never fetched;
sparse selections map through the persisted record locator and read KEYS only for lookup candidates
and kept leaves.

The first consumer is the bitemporal opener: a grouped FLWOR over
`jn:open-bitemporal('db','res', T, P)` is served from the resource's projection at the revision
current at `T`, masked by the valid-time index's half-open stab at `P` (the SH1 queries Q7 and Q8;
see [Valid-time key slices](VALID_TIME_KEY_SLICES.md) for the stab). The mechanism itself is
general: any sorted set of record keys is a row source.

## The predicate: `Op.KEY_IN` on the virtual KEYS column

`ProjectionIndexScan.ColumnPredicate.recordKeysIn(sortedKeys, store, fetcher)` builds a predicate whose
column is `ProjectionColumnStore.KEYS_COLUMN` (`-1`, the KEYS lane every projection carries) and
whose literal is the strictly ascending key set. A row passes iff its record key is in the set.
There is no presence to AND: every row carries a key. The predicate's `keySetHash` is part of its
identity, so a plan memo keyed by predicate shape never confuses two key sets.

Every mask evaluator honours it:

| Evaluator | How |
|---|---|
| Sliced conjunctive and tree evaluators (`ProjectionColumnScan.evaluateMask*`) | the KEYS column's "slice" is built from the leaf's decoded record keys (`ProjectionColumnStore.recordKeyPredicateView`, `LeafColumnAccess.predicateSlice`) with the leaf's exact key range as its zone; `evalNumeric` runs the membership walk |
| Whole-leaf byte kernels (`ProjectionIndexByteScan.evalPredicateLeafMask`) | the walk over the payload's inline record keys; the presence AND is skipped |
| Leaf keep mask (`ProjectionColumnScan.pruneLeaves`) | the exact per-leaf membership mask drops leaves with no selected row before any BODY segment fetch, including order-exception leaves whose key ranges overlap the set |
| Residency and sliceability gates | the virtual column is priced as the KEYS chain, never as a stored column |
| Page scan (`ProjectionIndexScan.evalColumn`) | refuses by name: the materialising reference path does not serve it |

Row-source construction uses `ProjectionPersistedRecordLookup.find` to resolve sparse keys to
physical leaf slots and row positions through persisted normal fences and the sparse exception
locator. Pruning uses those physical slots rather than assuming slot numbers are document-order
leaf positions. A repeated sparse query never walks the full projection's rows.

Selections containing at least **25% of the descriptor-reported row count** use the explicit dense
path: one sequential KEYS-chain read and one monotone source-key cursor, with binary searches for
order exceptions. This threshold avoids a point lookup and exact leaf probe for each key when a
large selection would read most leaves anyway. The count requires no KEYS read. Dense sources
advance the key set at most once; a leaf never restarts a scan through preceding source keys.
Both paths produce the same masks, shared by sliced and byte kernels. The overload accepting
already decoded `leafKeys` is the explicit in-memory dense mapper.

## Serving a grouped aggregate under a row mask

`SirixVectorizedExecutor.executeGroupByAggregate(..., GroupRouting)` takes the row source and the
computed lanes. The route appends the `KEY_IN` predicate to the conjunctive predicates, or ANDs it
over the root of a predicate tree, and otherwise runs the ordinary group arms. Under routing the
metadata routes that read no row mask (sorted top-K, scalar value summaries, any-K groups) are
skipped, and the request claims the sliced arms even after the handle's payloads were promoted to
whole-leaf scans: the row source prunes leaves there, and the derived lanes exist only as slices.
Masked requests require resident sliced execution and do not trigger whole-projection background
promotion. Dense global-string grouping uses the same keep mask. A residency refusal declines
before operand fills; a later fill-budget refusal retains the mask on re-entry and declines before
entering a whole-leaf arm.

**Computed lanes.** A pre-group `let $v := $r.a * $r.b` (a supported `+,-,*` program over the loop var's
fields and integer literals, `ComputedProgram`'s encoding) is an aggregate operand `prog:<i>`. The
executor resolves its operand columns (NUMERIC_LONG, integral, null-free), evaluates the program once
per kept leaf into a query-local derived column (`ProjectionComputedColumn`; a row missing an operand
is missing in the derived column, exactly the interpreter's empty arithmetic), and hands that column
to the numeric and composite flat kernels in place of a stored one. The combined residency decision
prices every distinct operand and the derived value and presence buffers. Dictionary-string flat,
packed-substring, windowed and legacy multi-key arms decline computed lanes that they cannot consume.
Exact arithmetic or decline: an overflow is an
`ArithmeticException`, which routes the query to the generic pipeline's decimal promotion.
Computed lets are aggregate operands; grouping by one or using one under constant-only grouping
retains generic execution.

**`count($let)`.** `count` of a let bound to a field is `fn:count` of the field's values in the
group — the lane's present count, not the row count — and is emitted from that lane. A grouping
variable is scalar after grouping; aggregates over it retain the generic pipeline. In-kernel
ordering on such an entry declines; the wrapper's sort applies the order-by. Constant-key grouping
uses the same present-count rule for field counts, including double-wrapped counts.
For a computed let, a row contributes to its count only when every operand is present.

## Admission

`IndexRoutedSourceStage` (before `GroupAggregateDetectionStage`) recognises a loop whose source is
`jn:open-bitemporal` and folded valid-time point slices with literal database and resource names, sets the source path to the array
members (the path the projection is declared on) and records the two instant expressions.
`GroupAggregateDetectionStage` then admits the pipeline exactly as it admits a document scan, plus
computed lets and `count($let)`. `SirixPipelineStrategy` compiles the instants at the pipeline's
entry scope (they may read prolog and outer variables, never a variable the pipeline binds before the
loop) and builds `SirixGroupAggregateExpr` with the routed source.

Plain indexed FLWOR point slices fold before aggregate detection. Internal `open-bitemporal-slice`
and `scan-valid-time-index` sources supply their exact key sequences, including endpoint and
residual checks; unsafe coverage or an unavailable index declines. Both join sides and correlated
inner groupings use the same source admission.

Literal selector components containing `/` or starting with the reserved `prog:` token decline
projection admission, including in pre-group predicates; nested dereferences remain distinct path
steps. Joined and correlated returns must have unique names across the complete emitted record,
so duplicate names retain the generic `BIT_DUPLICATE_OBJECT_FIELD` error.

Per evaluation the expression evaluates the instants, resolves the document at `T` (the revision
current at that instant), takes the valid rows' record keys from the valid-time index
(`ValidTimeIntervalIndex.keys`, half-open, exact — inexact candidates are verified), acquires the
executor bound to that resource and revision through the chain's per-source resolver, and serves.
Lease acquisition enforces resource identity for every consumer. One leaf keep mask bounds every
key and operand fill; an empty source fetches no columns. Anything on the
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
ordinary operator chain; per outer tuple the inner grouping — exactly the plain shape, over a
synthetic pipe the stage builds and the plain stages annotate — is served from the projection under
that tuple's row mask. Outer keys are evaluated only when the inner grouping contributes a row;
the groups merge on (outer keys, inner keys) with `count` and `sum` added exactly and `min`/`max`
compared (an `avg` or a distinct count would need the lanes behind the emitted value, and declines). The order-by must name
every key. Dependent outer lets retain the generic pipeline because their bindings are not part of
the separately translated outer-key expressions. Executors are resolved per revision through the chain's per-source resolver; a query
touching more revisions than the resolver caches re-creates executors as it goes.

**Membership filter** (Q12): `where empty|exists(for $b in SRC2 where $b.f eq $r.g return …)`
under a routed loop, with `SRC2` a second routed opener (or a leading `let` bound to one). The filter
source's `f` values are read under its own mask, the main rows' `g` values decide which record keys
survive (a missing value matches nothing: it survives an anti-join and fails a semi-join), and the
grouping runs over the reduced key set. Both membership fields must be integral, null-free long
columns. The [hash-membership pipeline](QUERY_MEMBERSHIP_OPTIMIZATION.md#plan-and-invariants)
stays the fallback.

**Column-side equality join** (Q4, Q9; `JoinedGroupAggregateDetectionStage`,
`SirixJoinedGroupAggregateExpr`): Brackit's `Join` node over two single-loop branches, each over a
routed opener or a literal document, comparing one integral field of each side. Both sides' columns
are read under their row masks (`MaskedColumns`), the smaller side is hashed on its join values,
the other probes; every matched pair folds into a group keyed on fields of either side (string keys
are interned once per leaf dictionary into one id space shared by both sides) with
`count` (pairs, or present values of a field), `sum`, `min` and `max` over fields or `+,-,*` programs
of one side. Grouped output supports at most 64 keys and requires an order-by naming every key
and aggregates over one side. Wider groupings retain the generic pipeline. Q4 emits
[column-backed record answers](COLUMNAR_RECORD_SERIALIZATION.md) and evaluates its
equality/inequality residual over the paired long columns.
Only empty array selectors (`E[]`) admit document iteration. Unsupported selectors, operands,
residuals or arithmetic overflow retain the generic `TableJoin` pipeline.

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
  generic reference over the same decorated cursor is the positive control. BODY segment requests
  prove excluded leaves are not fetched, including an order-exception leaf whose key range overlaps
  the source but whose exact mask is empty. An empty source fills no columns.
