# Grouping by transformed segment values

`SegmentGroupCanonicaliser.transforming` builds per-segment cell-to-group maps for a deterministic,
thread-safe string transform. `SirixVectorizedExecutor.numericGroupAggregate` uses it for regex
replacement keys. This is query-time processing over existing segment dictionaries; the storage
format and load path are unchanged.

The physical resolver marks only present cells selected by the predicate, then reads those entries
in storage order on segment workers. Sparse selections sort their marked positions instead of
walking every dictionary entry. Workers transform and hash into bounded batches before publishing
canonical group ids. Rows retain their original positions, presence and multiplicity; the existing
aggregate tables retain the document ordinal that breaks ordering ties.

Physical order never becomes transformed value order. The value resolver deliberately exposes no
sorted-run cursor or source-position ordering. Original string MIN/MAX operands still use the
existing `SegmentValueMerge` and fold its canonical ranks, not packed cells or segment-local ids.

Equality compares the already transformed input with the group's representative. Identity results
are read directly from the original dictionary; their markers retain at most 4 MiB. Other
representatives use a cache charged conservatively at 128 bytes plus two bytes per UTF-16 unit,
capped at 64 MiB per canonicaliser. Over budget, equality reevaluates only the representative. Thus
cache refusal costs at most one extra transform per equality candidate; the old resolver transformed
both candidate values again after hashing the input. Batches flush at 4,096 entries or a 1 MiB
string charge, plus the single output crossing that byte limit. Ordinary untransformed keys allocate
neither transform cache nor transform batches.

Tests live in `SegmentGroupCanonicaliserTest` (parallel evaluation counts, selective predicates,
identity outputs, bounded cache refusal, missing values, transformed ordering, and cross-segment
MIN) and `SegmentLengthLaneQueryTest` (regex grouping with AVG length, MIN, multiplicity and stable
count ties against the interpreter). `GroupTopKDifferentialTest` covers the neighboring group shapes.
The physical-walk witness was mutation-tested by substituting the transformed resolver for the
storage resolver: it failed with 0 physical visits instead of 20,000, then passed after restoration.

## q28 diagnostic evidence

Base: `54b0a059b`, shared 100M segment database, 148 segments. The shared `leg.lock` guarded every
JVM; logs and the 1M gate database stayed in the task worktree. No scored suite leg was run.

Baseline `diag100m.sh 28`: `route=group-aggregate+numeric-group-by`, no group-aggregate decline.
The transformed resolver reported `storage-order resolve SKIPPED: segment 0 reports entryCount=-1`.
An instrumentation-only repeat counted `visit=62566260 cand=3009009`: visits are regex evaluations
during group-map construction, candidates are distinct transformed groups. The predicate selected
81,032,736 rows. The existing ordered MIN merge covered 22,861,426 selected cells and 19,720,796
original distinct values.

A 45-second async-profiler CPU capture during try 2, summarized with the rig's `collapsed.py`, found
61.9% of samples in `SegmentGroupCanonicaliser`, 32.0% in regex code and 27.6% in `sameValue`.
The hot profile run took 60.728 seconds with diagnostics enabled. These overlapping inclusive CPU
shares identify work; they are not wall-time shares or a leaderboard score.

Final two-try diagnostic: `route=group-aggregate+numeric-group-by`, `visit=23440370 cand=3009009`
on try 2. All 22,861,426 selected distinct cells were resolved across 148 segment walks before the
row loop. The remaining 578,944 transforms were bounded-cache representative rereads. Original
MIN still merged 19,720,796 distinct values, and the second length-table pass reused all 148 tables.
Diagnostic hot time was 8.616 seconds (cold 13.360 seconds), versus 60.728 seconds in the baseline
hot profile run. This is a diagnostic reduction, not a scored-suite result or a new campaign rank.

Validation: 45 canonicaliser tests, four segment length/query tests and 47 group top-K differential
tests passed. A freshly loaded, worktree-local 1M database contained two segments and five segment
string columns (445.0 MB). `seggate1m.sh` completed with query, DuckDB and comparison exit codes 0:
35 matches, eight strongly verified tie-ambiguous queries, zero unverifiable queries, zero
mismatches, zero missing results and zero declines across all 43 queries. q28 matched exactly.
