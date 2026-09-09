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

`ClickBenchQ16Q35RouteEvidenceTest` runs the **shipped** ClickBench q16 and q35 text
over 20,000 generated hits behind the projection index, once through the vectorized
executor and once through the interpreter, and requires byte-identical results in
emission order plus an advancing group-aggregate serving counter, so a silent decline
cannot pass as agreement. The fold counter must advance for q35 and stand still for
q16. That 20,000-row transcript reports `stride=3` for the rewritten q35 plan and
`stride=8` for q16's `(UserID, SearchPhrase)` composite plan: those are two different
queries, not a q35 before and after. q35's own measured stride is 9 before the
rewrite and 3 after it, at both 1M and 100M.

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

## 100M verification, 2026-09-07

The firstmate-controlled window compared the parent `ca4c34d38` executor with
`e9f0f5c76` on the same read-only, 148-segment database. `e9f0f5c76` is this change
before the rebases that renamed it `87a5f04be`: `ca4c34d38` is literally its parent.
The only later edit to that file is the campaign formatter's reflow of the new
block. `e9f0f5c76` is recorded here as the historical identity of the measured
build; it was left unreachable by those rebases, so a fresh clone will not have the
object and no check should assume it does. The parent executor and its
nested classes were compiled separately and prepended to the otherwise identical
runtime classpath. Each launch used 20 workers, a 14 GiB maximum
heap, a 10 GiB off-heap arena and the existing serving flags. The rig's `take_lock`
held the shared lock for the window; no benchmark or Gradle JVM was left running,
and every launch had at least 27.12 GiB MemAvailable. The lock was released
immediately afterward.

The rewrite **does help at 100M**. Two q35-only runs of 30 repetitions produced the
following last-ten medians. An additional 15-repetition parent run after the feature
run checked for timing drift. These are unscored single-query experiments, not a
replacement for the measured SEG4T campaign baseline or a rank claim.

| q35 measurement | Parent | Feature | Repeated parent |
|---|---:|---:|---:|
| Wall time, seconds | 0.896 | 0.3715 | 0.881 |
| Process CPU time, seconds | 16.8 | 7.0 | 16.5 |

This is approximately 58% less wall time against either local parent run. The
eight-second CPU profiles attached after try 1: through try 10 for the parent and
try 23 for the feature. Thus part of the feature's last-ten window overlaps
profiling; the last-five medians, entirely after profiling, are 0.896 versus 0.372 s.
The repeated parent run had no profiler. Diagnostic timings are excluded here.

| q35 diagnostic mechanism at 100M | Parent | Feature |
|---|---|---|
| `route=` | `group-aggregate` | `group-aggregate+numeric-group-by` |
| Completed passes | 1 composite | 1 numeric |
| Leaf visits (`leaves=`) | 97,737 | 97,737 |
| Key components / identity lanes | 4 / 5 | 1 / 0 |
| Table stride (longs) | 9 | 3 |
| Warm retained pool (`retainedMB`, MiB) | 2,798 | 466 |
| Restored composite winners | n/a | 10 |

There is no `visit=` or `cand=` field in these group kernels; `leaves=` is the
reported scan counter. Both still spill roughly 9.5M partial groups. The gain is
less key and table work, not fewer row visits or group candidates.

The prediction recorded before measurement was that q35's redundant numeric key
and table work dominated; unrelated setup or decode dominance would refute it.
The 14,609-sample parent CPU profile supports that prediction: group-table frames
account for 59.7% inclusively, the composite kernel 59.8%, with zero dictionary-merge
or file-read samples. These categories overlap. In the 14,558-sample feature
profile, `acquireExact`, `identityMatches` and the composite kernel disappear;
the numeric kernel and plain `acquire` replace them. Table work remains 69.1% of
the smaller CPU cost. The stronger hypothesis that both queries are memory-bound
is **unresolved**: CPU stacks alone do not establish hardware memory stalls.

Both q16 and q35 return byte-identical ordered JSONL results before and after at
100M. All requested `route=` and `[proj]` lines, including cold q16 restarts, are
preserved in the [diagnostic transcript](diagnostics/Q16_Q35_100M_2026-09-07.txt).
Raw logs, collapsed profiles, exact launch arguments, result hashes and parsed
statistics are retained outside the repository, in the measurement worktree's
`build/q16q35/100m-summary.json`, under
`/home/johannes/.treehouse/sirix-cdde48/4/sirix`, and the sibling artifacts it
names. The earlier 54 differential tests and full 1M gate remain applicable: after
the measured production commit only documentation, the
`ClickBenchQ16Q35RouteEvidenceTest` witness and that formatter reflow changed, so no
executor behaviour moved under the measurement.

### What this branch does and does not establish

These 100M numbers are **not reproducible from this repository**. They need the
148-segment 100M database, an exclusively quiet machine and the rig lock; a
repository test run has none of those, and remote CI has none of them either, so no
automated check re-derives the 0.896 s parent and 0.3715 s feature medians. Those
timings rest entirely on the committed transcript and the retained artifacts above.

They are also **not a score**. Those are unscored single-query diagnostic medians;
q35's scored basis is owned by the segment-lane handoff's lever queue
([§4 of the handoff](HANDOFF_SEGMENT_LANE_2026-09-06.md)), and it moves only with a
full-suite campaign leg, never with this branch's diagnostic.
A leg scores this rewrite only if its head contains the executor change. SEG5T does
not — it measured `de2724c5c`, which predates `87a5f04be`, so its q35 row rescores
the **unrewritten** query.

The committed **SEG6T** leg is the first that does: its head is `aa4d81d54`,
whose ancestry carries this executor change as `1cc53ec75` — the same rewrite
`87a5f04be` names on the sibling branch. `python3 rank.py SEG6T SEG5T` reads the
leg from `rig/legs/query-SEG6T.json` without a new run. That makes SEG6T the
scored row for the *rewritten* query — and nothing more. It is **not** a Δln for
the fold: `de2724c5c`…`aa4d81d54` also carries the composite fold guards, the
empty-lane reuse and the q16 work, so a SEG5T→SEG6T q35 difference has several
candidate causes and this document attributes it to none of them. Isolating the
fold's own scored contribution still needs a paired leg around it alone.

What a repository test run does establish is the route and the answers, at a size CI
can afford: see Validation above.

## q16 is a different problem

When this capture was taken, the measured SEG4T campaign table placed q16 at
2.062 s / 0.193 s best (2.3231 ln) and q35 at 1.319 s / 0.123 s (2.3018 ln); the
older handoff projection is not a new measurement. The values that order the lever
queue today are owned by [§4 of the handoff](HANDOFF_SEGMENT_LANE_2026-09-06.md), not
by this document.

q16 groups `(UserID, SearchPhrase)`. Its existing composite route canonicalizes the
segment string component before aggregation. In the 1M baseline, the merge mapped
18,379 marked cells to 18,316 values in 3 ms; the composite pass took 241 ms. This
small-data evidence does not support canonicalization as its dominant cost, and
the 100M capture now tests that hypothesis directly. No q16 algorithm is changed.

At 100M, q16 retains `route=group-aggregate`: 7,713,698 marked cells become
6,019,103 canonical values across 148 segments and 58 merge ranges. The cold
aggregate aborts its initial one-pass attempt and restarts with two hash-range
passes; the warm query completes those two passes, each visiting 97,737 leaves.
This mechanism is unchanged by the feature. Its 15-repetition baseline has a
last-ten wall median of 2.035 s, consistent with the measured SEG4T 2.062 s.

The 10,597-sample hot CPU capture refutes canonicalization as the dominant cost:
table and spill frames account for 53.9% inclusively, and `identityMatches` alone
for 33.3% self time. Canonicalization is still material at 21.8%, counting both
`SegmentGroupCanonicaliser` and its `SegmentValueMerge` worker stacks. Counting
only the canonicaliser class would incorrectly report 2.2% and miss the parallel
dictionary merge. No new q16 speedup is claimed.

Any later segment-local preaggregation must merge **all** partial groups before
selection. A useful falsifying fixture gives a shared group six occurrences in each
of two segments, with a different ten-occurrence competitor in each segment. The
shared group must win global top-1 at twelve even though it loses both local top-1s.
Also vary dictionary mint order, reuse the same mint for different values, vary the
numeric component, and preserve document-order ties. See
`SegmentGroupCanonicaliserTest` for the existing value-identity witnesses.

The 100M window ran only q16 and q35. No scored suite leg was run.
