# Count groups with dependent numeric keys

`SirixVectorizedExecutor.groupByAggregate` reduces a count-ordered group key such as
`(ip, ip - 1, ip - 2, ip - 3)` to the underlying numeric column. It restores the
translated keys only for selected winners. All expressions must refer to the same
`NUMERIC_LONG` column and consist solely of fixed integer offsets. The count must be
`count(*)`, the order must refer to that count, and the limit must be 1 through 1,024.
Predicates and HAVING retain their existing execution semantics.

An integer translation is injective: two source values group together exactly when
their translated values do. This also holds beyond the signed long range; winner
emission uses Brackit's integer arithmetic and promotes overflowing additions.
Missing source values remain missing in every component. The existing numeric
aggregate combines complete groups before top-K selection and preserves first-seen
ordinal ties. There is no row prepass, dictionary change, or per-row allocation.

The winner limit bounds additional result materialization. Independent columns,
non-injective transforms, other aggregates, other ordering and larger limits retain
the composite path. The dependency check inspects query annotations and column kind,
without reading row data. `offsetCountGroupsRewriteCount()` and the diagnostic
`[proj] offset-count groups` line witness the route; the numeric pass reports its
existing leaf, spill and budget counters.

## Validation

`GroupTopKDifferentialTest` compares actual vectorized queries with the interpreter,
including ordered results, and requires the aggregate serving counter to advance.
Its offset witnesses additionally require the rewrite counter to advance only for
eligible queries. Cases cover count ties, missing winners, predicates, offsets with
no bare key, integer overflow, independent columns, division/modulus and the winner
limit (both 1,024 and 1,025). The complete class has 54 passing tests.
Disabling the dependency proof in a separately compiled mutant makes the eligible
count-tie witness fail: expected one rewrite, observed zero. The normal classpath
passes again; no production source was replaced for this mutation check.

Both private 1M gates used a freshly loaded two-segment database and the same source
corpus. Each returned **33 match, 10 tie-ambiguous, 0 unverifiable, 0 mismatch,
0 missing, 0 declines** for all 43 queries. These are diagnostic checks, not scores.

| q35 mechanism at 1M | Before | After |
|---|---|---|
| Reported route | `group-aggregate` | `group-aggregate+numeric-group-by` |
| Aggregate kernel | composite | numeric |
| Leaf visits (`leaves=`) | 978 | 978 |
| Per-row key components | 4 | 1 |
| Extra exact-identity lanes | 5 | 0 |
| Table stride (longs) | 9 | 3 |
| Restored composite winners | n/a | 10 |

The group pass does not report a row-candidate `cand=` counter; no candidate-pruning
claim is made. The eliminated work is redundant key evaluation and table identity
storage, not leaf scanning.

A hot 1M CPU capture of the original q35 route contained 1,900 samples. Group-table
operations accounted for 49.6% inclusively, including partition indexing (16.6%) and
partition merging (15.2%). The composite byte kernel accounted for 17.7%; there were
no canonicalization samples. Inclusive categories overlap.

## q16 is a different problem

The measured SEG4T campaign table places q16 at 2.062 s / 0.193 s best (2.3231 ln),
and q35 at 1.319 s / 0.123 s (2.3018 ln). These are the measured values used for
prioritization; the older handoff projection is not a new measurement.

q16 groups `(UserID, SearchPhrase)`. Its existing composite route canonicalizes the
segment string component before aggregation. In the 1M baseline, the merge mapped
18,379 marked cells to 18,316 values in 3 ms; the composite pass took 241 ms. This
small-data evidence does not support canonicalization as its dominant cost, and
must not be extrapolated to 100M. No q16 algorithm is changed here.

Any later segment-local preaggregation must merge **all** partial groups before
selection. A useful falsifying fixture gives a shared group six occurrences in each
of two segments, with a different ten-occurrence competitor in each segment. The
shared group must win global top-1 at twelve even though it loses both local top-1s.
Also vary dictionary mint order, reuse the same mint for different values, vary the
numeric component, and preserve document-order ties. See
`SegmentGroupCanonicaliserTest` for the existing value-identity witnesses.

The firstmate-controlled 100M route/profile window is pending. No 100M performance
gain, scored leg, or rank improvement is claimed by this change's 1M evidence.
