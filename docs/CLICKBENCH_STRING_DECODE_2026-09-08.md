# Segment string decode and canonicalisation

This change moves dictionary decoding and representative equality checks onto
segment workers and maps settled canonical row IDs in disjoint ranges. It is
being delivered on correctness evidence. **Its performance effect is unverified
pending measurement resolution; no timing, speedup, rank improvement or delivered
ln gain is claimed.**

Firstmate's instrument study, communicated on 2026-09-08, measured this harness's
detection floor; that floor — in both its paired and its unpaired form — and the power
cap now in force are recorded in the
[rig README](../bundles/sirix-query/bench/clickbench/rig/README.md). That floor,
together with the unchanged baseline moving between nominally comparable runs (below),
is why an individual paired observation cannot establish this change's performance
effect. Both are properties of the measurement process, not results for this change.
Continuing JIT compilation during timed tries remains an unresolved hypothesis;
the study has not established the cause of the harness variability.

Firstmate has released the box for builds, tests and reviews. This lane still has
**no authorization for further 100M work**, and **Firstmate holds the merge**
pending measurement resolution. A released rig lock does not authorize a run.
These delivery constraints also apply to validation agents: complete correctness
checks and the PR, without adding a benchmark run or a performance claim.

## Implemented mechanisms

- The transformed dictionary walk reads its bytes through the storage resolver's
  segment cursor instead of translating a mint back to a position, and it
  snapshots one candidate representative per hash before publishing a batch, so
  representative resolution and equality run off the publication monitor.
  Publication, hash collisions, new arrivals and concurrently settled cells keep
  the original exact-value checks under the monitor.
  [Grouping by transformed segment values](SEGMENT_TRANSFORM_GROUPS.md) owns both
  mechanisms and their retention bounds.
- Whole-column row mapping reads already-settled canonical IDs on the caller's
  workers, with each task owning a disjoint output range. An unresolved cell
  causes the incomplete output to be discarded and the original serial mapping
  to run. Workers never issue IDs in this phase. A sealed value space skips
  redundant marking and uses the same fallback for previously unseen cells.

The dictionaries remain segment-local. Canonicalisation compares dictionary
values; a packed segment ID is never treated as a globally ordered value.
Missing rows, row masks, multiplicity, value ordering, stable ties, slice bounds
and immutable source lanes are preserved. No global dictionary, prepass, format
change or database rebuild is introduced. Hash aggregation and top-n source files
are outside this change.

The whole-column diagnostic `[proj] canonical lanes:` now includes `mapRanges`.
Allocation and reuse counters are totals over the column, while each mapping
range owns its shared empty canonical and presence lanes. Historical allocation
readings from serial mapping do not describe this parallel mapping.

## Profiling and rejected experiments

Profiles were captured before implementation. They identified transformed-value
canonicalisation, representative resolution under the publication monitor,
dictionary block reads, and value merging as shared work worth investigating.
Regex patterns were already compiled once with a reused thread-local matcher;
per-row pattern compilation was not the cause.

The profiling and benchmark observations from that work remain historical
artifacts. Their timing and sample figures are not performance evidence for
this delivery and are intentionally omitted from this report. A CPU profile
identifies where work occurs; it does not establish a wall-time or score gain.
Total suite time and the geometric-mean score are different measures, so saved
seconds cannot be substituted for score improvement.

Prefix-history merging, galloping, normalized ordering prefixes, an LRU
representative-cache experiment, and comparison shortcuts were removed. None has
an established performance benefit that warrants adding it here. The final
change retains the original merge algorithm and comparison implementation.

Review also corrected an existing documentation error in UTF-8 comparison:
validation is partial. A byte-identical prefix may settle ordering without
being decoded; malformed input in a sequence the comparison must decode still
fails closed. The suspected malformed-prefix defect did not reproduce, so
removing the comparison shortcut is not represented as a correctness fix.
Behavioral tests pin both sides of the partial-validation contract.

## Correctness evidence

The refreshed C2 1M oracle gate reports **33 matches and 10 strongly verified
legal tie windows**, with **zero mismatch, missing, unverifiable or route
declines**. All **43** serialized query outputs are byte-identical to the pristine
Sirix baseline. The strong DuckDB oracle uses
`duckdb_reference.py --candidate-reference` and
`compare-results.py --strong --bounded-oracle`; the
[operating manual](HANDOFF_SEGMENT_LANE_2026-09-06.md) describes those checks.

Previously completed 100M output comparisons also matched the pristine outputs
for all 43 queries. Those comparisons are retained as correctness evidence only;
no new 100M operation is required or authorized for this delivery. These are
checks over fixed corpora, not a proof over every possible input.

The recovered `ClickBenchStringDecodeRouteEvidenceTest` loads 300,000 generated
ClickBench rows into segment-scoped string columns. It runs the shipped q33
whole-column group, q13 predicated group with a distinct aggregate, and q28
transformed regex-key group through both the vectorized route and the independent
interpreter, asserting byte-identical serialized answers. It verifies the segment
column kind and that the group-aggregate route actually served the queries.
The fixture exceeds the parallel mapping threshold; diagnostics observed
`mapRanges=3`. q28 also runs with a fixture-scaled HAVING threshold to exercise
nonempty decoded output groups.

The fresh validation run before the instruction to idle passed that test and the
sibling `ClickBenchQ21Q22SegmentRouteEvidenceTest`, along with:

- `SegmentGroupCanonicaliserTest`: parallel/serial equivalence, sparse masks,
  range boundaries, immutable inputs, serial fallback arrival order, sealed
  values, hash-chain landings on duplicate values, simultaneous transformed walks
  and representative reads outside the publication monitor. No test forces two
  distinct values onto one hash, so the collision branch of the snapshot check is
  reasoned from the retained exact comparison, not witnessed.
- `RankTableReadViewTest` and `SegmentValueMergeTest`: physical cursor decoding,
  packed and spilled entries, sparse/range traversal, Unicode ordering and
  multiplicity.
- `ValueDictionaryComparisonTest`: shared UTF-8 prefixes, deciding-byte ordering
  and malformed deciding sequences on either side.

Earlier mutation checks failed when physical cursor decoding was disabled,
representative reads moved back under the publication monitor, or unresolved IDs
were allowed to be issued by mapping workers. These checks exercise behavior,
not the presence of particular source text.

## Open measurement questions

The q33 full-suite observation remains unexplained. It did not reproduce in
isolated q33 runs performed in alternating baseline/candidate order, and outputs
matched in every comparison. Removing the prefix experiment did not resolve the
suite-history sensitivity. This is an open measurement question, not an
established regression or an established gain.

The unchanged q31 baseline also varied between nominally comparable runs. That
discrepancy exposed the attribution problem before the instrument study quantified
the detection floor. q31 groups numeric keys and uses SearchPhrase as a predicate;
it does not exercise the same string-key canonicalisation as q13, q28, q33 and
q34. Excluding q31's unattributed movement was necessary for an evidence-based
decision. This lane claims no q31 or q32 performance effect.

Firstmate declined adding a query-shape gate or extending this change to resolve
suite-history sensitivity. The retained algorithm is being reviewed on its
correctness while measurement resolution and the merge decision remain with
Firstmate. An isolated observation must not be substituted into a full-suite
score or selected from different JVMs to construct a favorable result.

## Preserved records

The `STRDEC*` records in
[`rig/legs/`](../bundles/sirix-query/bench/clickbench/rig/legs/) preserve the original
historical observations and audit identities. Their numeric data has not been
rewritten, reselected or promoted into an accepted result. They are retained for
instrument diagnosis, not as a performance claim in this report or PR.

Raw logs, profiles, mutation checks, 1M oracle evidence, preserved runtimes and
local launchers are under
`bundles/sirix-query/build/diagnostics/strdec/` in the task worktree. The replacement
pair's original logs and dumps were in the pipeline's disposable worktree and
were unavailable after custody recovery; its committed audit summary preserves
the reported output checks. The frozen runtime, repeated q33 checks and refreshed
1M evidence remain in this worktree.

The recovered pipeline fixes and route test are committed. Documentation work
interrupted by the instruction to idle was preserved in
`strdec/stop-20260908-075759/`; its applicable diagnostic and test documentation is
retained here, while proposed historical score updates are superseded by the
correctness-only delivery instruction.

## Deferred follow-up

`docs/README.md` does not index the segment-lane arm of the campaign, this report
included, and 26 of the 79 `docs/*.md` files are unindexed in all. Firstmate declined
widening this delivery to fix it: one added row would make the index less consistent,
not more, and the sweep needs its own editorial pass over the whole index. The index
makes no completeness claim, so nothing in it is currently false. Recorded here so the
sweep is not lost.
