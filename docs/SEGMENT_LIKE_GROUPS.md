# Segment LIKE groups: remaining work

Status: 100M CPU/allocation comparison and correctness checks completed on 2026-09-07;
empty-lane optimization based on `ca4c34d38`. No new score is claimed. The measured SEG4T report
(`data/sirix-cb-measure-1/report.md` in Firstmate) gives:

| Query | Hot / board best | ln contribution | Change against projection |
|---|---:|---:|---:|
| q21 | 0.377 / 0.025 s | 2.4031 | -0.2799 |
| q22 | 0.522 / 0.021 s | 2.8427 | +0.1197 |

## What is already implemented

The handoff proposes sharing string-predicate verdicts and folding string MIN with canonical ids.
**Both mechanisms already serve q21/q22. They are not two unimplemented levers.**

- `SegmentCellVerdicts` adopts per-segment byte tables from `SegmentVerdictCache`, keyed by
  dictionary identity, revision, operation and literal. Commit `921c3f811` introduced this share;
  its own evidence records all 148 tables adopted on hot tries and q22's mask phase falling from
  5,123 to 67 ms. Adoption does not itself prove every entry is settled: hot profiling must also
  check for dictionary matching.
- `SirixVectorizedExecutor.numericGroupAggregate` computes predicate row masks, canonicalises the
  selected group keys, and calls `sealSegmentOperandShared` for each string extremum column.
  `SegmentGroupCanonicaliser.observeColumn` uses `SegmentValueMerge` to issue canonical collation
  ranks. `sealOrderPreserving` retains this order and `canonicaliseColumn` rewrites the selected
  row lanes. `ProjectionColumnGroupScan.aggregateByGroupNumericFlat` folds those numeric ranks;
  `fillAggEntries` materialises the winning rank's string. Raw segment cells are never globally
  ordered integers. The merge landed in `a3aed07ec`, with run cursors improved in `417c62ead`.
- Canonicalisation still costs marking selected cells, dictionary comparisons during the merge,
  and allocating rewritten row lanes. An integer MIN does not remove that preparation.

The query definitions in `ClickBenchQueries` also distinguish the work: q21 has one MIN and no
COUNT(DISTINCT); q22 has MIN(URL), MIN(Title), COUNT(DISTINCT UserID), and a predicate tree containing
NOT. q21 uses the same URL literal as q20; q22's Title and URL literals are different from q20's.

## Predictions and falsifiers, recorded before profiling

1. **Repeated predicate work contributes to the hot residue.** The row masks restrict the seals,
   but the aggregation kernel then evaluates the same predicates again. q22's
   `ProjectionColumnScan.evaluateMaskTree` evaluates the tree's operands and combines their masks;
   q21 uses the conjunctive evaluator. Reusing the existing immutable masks could remove the second
   evaluation without another prepass. Falsifier: the hot profile and phase timings put negligible
   work in the second evaluation. Any change must retain the existing path for callers without
   masks and cover NOT/missing semantics, tails, multiplicity and document-order ties.
2. **Empty rewritten lanes may be a larger q21 cost than MIN itself.** `rowKeepMasks` retains an
   all-zero bitmap when the conjunctive evaluator returns a positive leaf row count;
   `SegmentGroupCanonicaliser.canonicaliseMemoised` then allocates a full `long[cells.length]` even
   when no selected operand is present. In the 1M baseline, q21 retains masks for 949 leaves but
   selects rows in only one. With 1,024 rows per full leaf, its key and MIN lanes alone reserve
   roughly 15 MiB of long arrays for those leaves. A 100M profile must establish the actual count
   and allocation cost; linear extrapolation is not a measurement. Falsifier: hot allocation/CPU
   evidence shows few empty lanes or little allocation/GC cost. q22's tree already returns zero
   for an empty final mask, so this hypothesis predicts a larger effect on q21.
3. **Selected dictionary ranking or distinct state could dominate instead.** q22 seals two
   operand columns and maintains distinct UserID pairs. Falsifier: hot seal/merge phases and
   distinct-accumulator samples are small. Winner-only string extrema would be a wider change and
   should not be implemented without evidence that it removes substantial work.
4. **Cache eviction is a lower-confidence alternative.** q22 owns two literal tables and the
   cache has a byte budget. Falsifier: hot tries adopt all tables, settle no new values and spend
   negligible time matching dictionaries. Do not add another verdict cache.

## What the historical q22 increase establishes

`bundles/sirix-query/bench/clickbench/rig/legs/query-SEG3TB.json` contains q22
`[110.809, 0.462, 0.462]`; the handoff explicitly describes SEG3TB as a spliced leg. The later
projected row changes q17 and q27 only, so q22 keeps 0.462 s. SEG4T measures 0.522 s, a 60 ms
increase and `ln(0.532 / 0.472) = 0.1197` after the board offset. This comparison establishes the
increase against the planning value, not its cause. It is not a controlled before/after code
comparison. Cache state, allocation/GC, JIT state and changed code remain competing explanations;
a hot profile can locate today's cost but cannot alone prove a historical regression. The original
SEG4T `suite100m.log` adds concrete evidence:

```text
# q22 try 2: wall=1.520 s cpu=3.7 s util=2.4/20 gc=0 pauses 0.00 s
# q22 try 3: wall=0.522 s cpu=5.4 s util=10.4/20 gc=3 pauses 0.11 s
```

The winning try includes 110 ms of GC pauses, larger than the 60 ms projection delta. The preceding
try spends 1.520 s with no GC and only 2.4 busy cores. Pause totals cannot simply be subtracted
from wall time because they can overlap waits. No matching resource trace for the spliced 0.462 s
input was established. **The cause of the historical delta remains unresolved; calling it a code
regression is unsupported.**

## Baseline correctness

A freshly loaded worktree-local 1M database has two segments and five segment string columns
(445,057,968 bytes). `bundles/sirix-query/bench/clickbench/rig/seggate1m.sh` passed all 43
queries: 33 matches, 10 strongly verified tie windows, zero unverifiable results, zero mismatch,
zero missing and zero declines.

q21: `route=group-aggregate+numeric-group-by`, two selected rows in one of 978 leaves; its single
MIN ranks one URL. q22: `route=group-aggregate+numeric-group-by+group-distinct`, 217 selected rows
in 78 leaves; its minima rank 18 URLs and 10 titles. q21 adopts q20's two verdict tables; q22's
first try allocates two tables per literal. These cold 1M observations do not decide the hot 100M
hypotheses. Logs and the initial candidate note are under `build/firstmate-validation/` in the task
worktree.

## Measured 100M baseline

One authorized window selected only q21/q22, five tries each, fresh executors, 20 workers, the
rig's 14 GiB heap / 10 GiB arena envelope and serving flags, plus `sirix.projDiag=true`.
Async-profiler 4.2 sampled CPU at 1 ms and allocations at 512 KiB. Method tracing around
`ClickBenchRunMain.execute` delimited all ten queries without editing source; hot profiles exclude
the cold tries. This diagnostic is not a scored leg and its times do not replace SEG4T.

### q21: empty canonical lanes dominate allocation

Every hot try reports:

```text
route=group-aggregate+numeric-group-by
segment verdict tables for STR_CONTAINS: 148 adopted, 0 allocated, 0 without dictionary
predicate row masks: 1038 row(s) in 755 of 97737 leaves pass (96459 read)
segment value merge: 97 segment(s), 760 marked cell(s) -> 677 distinct in 1 range(s)
segment value merge: 97 segment(s), 593 marked cell(s) -> 507 distinct in 1 range(s)
```

The merges rank SearchPhrase groups and URL extrema. **99.2% of the retained leaves are empty**.
The earlier 949-leaf / roughly 15 MiB prediction concerned 1M; 100M selects 1,038 rows, not two.
Two 1,024-row lanes over the retained leaves imply roughly 1,507 MiB of payload. Independently,
the allocation profile estimates **1.493 GiB of canonical long lanes per hot try**:
6,411,517,952 sampled bytes at `new long[cells.length]` over four tries. Including presence arrays
and slice objects, `canonicaliseMemoised` accounts for 91.1% of estimated allocation, against
about 1.68 GiB total per try. G1 stacks account for 48.5% of 13,136 hot CPU samples.

| q21 try | Wall (s) | GC pauses (s) | Mask phase (ms) | URL canonicalise (ms) |
|---:|---:|---:|---:|---:|
| 2 | 0.783 | 0.21 | 26 | 224 |
| 3 | 0.679 | 0.11 | 27 | 325 |
| 4 | 0.457 | 0.06 | 25 | 138 |
| 5 | 0.264 | 0.03 | 30 | 71 |

Hot URL fill and order sealing take 0 ms throughout. Canonicalisation accounts for 7.6% of hot
CPU directly, with allocation consequences on GC threads. Predicate evaluation accounts for
12.0% in `rowKeepMasks` and 11.6% repeated in aggregation. No hot `matchesSlow` sample occurs.
CPU shares are not wall-time shares.

### q22: two predicate-mask evaluations dominate CPU

Every hot try adopts all 148 tables for each literal and allocates none:

```text
route=group-aggregate+numeric-group-by+group-distinct
predicate row masks: 7128 row(s) in 4964 of 97737 leaves pass (4964 read)
segment value merge: 128 segment(s), 4326 marked cell(s) -> 3673 distinct in 1 range(s)
segment value merge: 128 segment(s), 3263 marked cell(s) -> 2450 distinct in 1 range(s)
segment value merge: 128 segment(s), 2725 marked cell(s) -> 1925 distinct in 1 range(s)
```

The tree already drops empty final masks. Of 12,137 hot CPU samples, **68.9% is predicate-mask
evaluation**, split 34.46% in `rowKeepMasks` and 34.42% in `aggregateByGroupNumericFlat`.
The hottest `evalNumeric` lines walk packed string cells and read settled verdict bytes; the
method name does not imply a numeric predicate. The tree evaluates operands independently,
combines their masks, then runs again in aggregation. Sharing dictionary verdicts does not save
these row visits. Only one hot sample contains `matchesSlow`; cache churn is not a material
observed cost. **COUNT(DISTINCT), in `GroupDistinctAccumulator`, is just 0.13% of hot CPU.**

Canonicalisation is 0.46% of CPU and about 117 MiB allocation per try, against about 373 MiB
total. G1 is 11.2% of hot CPU, mainly around try 4's 90 ms pause. On try 5, predicate evaluation
is 83.1% and G1 0.04%.

| q22 try | Wall (s) | GC pauses (s) | Mask (ms) | Group/URL/Title merge (ms) | Aggregate scan (ms) |
|---:|---:|---:|---:|---|---:|
| 2 | 0.445 | 0.00 | 59 | 74 / 40 / 98 | 65 |
| 3 | 0.230 | 0.00 | 59 | 20 / 4 / 8 | 70 |
| 4 | 0.301 | 0.09 | 58 | 11 / 3 / 7 | 65 |
| 5 | 0.210 | 0.00 | 62 | 6 / 3 / 6 | 62 |

All hot MIN order seals take 0 ms; URL/Title rewrites take 6–7 ms each. The three merges load
the same 6,363 / 5,080 / 4,279 records each try, yet their combined duration falls from 212 to
15 ms. Try 2 stacks include dictionary overflow-page reads, decompression and filesystem reads;
`FileChannelReader` falls from 89/2,874 CPU samples to 5/2,486 on try 5. This locates a
warmup-sensitive dictionary-read component today, not the cause of the historical SEG4T delta.

## Bounded change and remaining target

The empty-lane hypothesis is supported for q21. `canonicaliseMemoised` now runs one prescan per
leaf for the first word that keeps a row, and **both** of the arrays it would otherwise mint for
that leaf are covered: the canonical value lane on either path, and the presence lane on the
row-masked path. A leaf that keeps no row shares an untouched zero array of the same length in each
covered lane within one rewrite; a leaf that keeps one owns its canonical lane, and owns its
presence lane too wherever a row mask made this pass build one. The kernel still receives full
slices with unchanged row counts, masks, zero bounds and canonical ranks. This avoids the
null-slice error documented in `rowKeepMasks` and changes no row traversal order. The prescan
doubles as the row loop's start and as the conjunction's first written word, so no leaf is crossed
twice. Dense leaves do one extra presence-word check and allocate exactly as before. Without a row
mask there is no presence array to save at all: the pass hands back the source slice's own words,
borrowed, as it always has, while its canonical zeroes are still shared among the leaves that hold
no present row. Empty lanes of alternating widths can allocate at most once per leaf, as before;
there is no global cache or new prepass.

Both shared arrays rest on the same read-only contract, now stated on the public entry points:
every consumer of a canonicalised lane reads it. Two lifetimes meet there and the contract names
both. A shared zero lane belongs to the rewrite, and the returned `ColumnSlice` objects are fresh
ones that never reach `SliceArrayPool.recycle`. A borrowed unmasked presence lane is a different
thing: it is the SOURCE slice's array, which a windowed access does hand to that pool on eviction,
so it must not outlive the caller's release of its window. The windowed arms satisfy this today —
each morsel is folded and released before the next fetch can evict anything. Tests exercise the
read-only half through real consumers — `ProjectionColumnScan.distinctLongs` and `distinctBitset`,
the exact count-distinct kernels — as well as the canonicaliser: mixed live/empty leaves, a 65-row
boundary, a shorter tail, a null mask, independent dense lanes, correct value inversion, unchanged
source arrays, empty leaves that contribute no visit at all, and shared value and presence lanes
that are still all-zero, and still the same objects, after both kernels have read every leaf that
shares them.

The repeated-mask hypothesis is supported for q22; distinct counting and verdict-cache churn
are disconfirmed as dominant hot costs. Reusing existing masks in the numeric aggregate arm is
the next candidate, deferred here to keep the change small. It must cover NOT/missing semantics,
tails, multiplicity and stable document-order ties, while retaining the path for callers without
masks. No global dictionary or format change is justified.

## Before/after at 100M (the canonical-lane build `c5870478e`)

**Every measurement in this section is of `c5870478e`, which shared the canonical lane only.** The
presence-lane extension (`b7be9d3db`) landed after these captures, so nothing below — not the
timing table, not the route and counter preservation, not the output hashes, not the 1M gate —
has been re-run against the code that actually ships. Those fresh captures are outstanding and are
required before this change is shipped; the paragraph after the counter block says what the
recorded counters do and do not already bound.

Firstmate authorized a second window after the private 1M gate passed. Both builds ran q21/q22
with the same five-try CPU/allocation capture, JVM envelope, flags and fresh-executor settings as
the initial diagnostic. Both additionally dumped the final serialized answers outside the timed
query body. The original core JAR was preserved before rebuilding; a local Gradle init script
substituted it in the before JVM's classpath. The after JVM used the rebuilt core JAR. This avoided
editing source or changing branches to reconstruct the baseline.

**q21's saving materialized.** Initial canonical-lane allocation was 1.493 GiB per hot try;
the controlled before repeated it at 1.457 GiB, and after is **14.5 MiB** by the same sampled
allocation method. Exact counters independently report, for each of the two q21 lanes:

```text
[proj] canonical lanes: sourceLongs=98700177 allocatedLongs=975761 reusedEmptyLeaves=95434
```

The old loop allocated a long for every source slot, so the unchanged source count implies
98,700,177 allocated longs per lane before. After allocates 975,761: **99.01% fewer slots**.
Combined payload falls from 1,579,202,832 bytes to **15,612,176 bytes (14.889 MiB)**. The retained
leaf count and row masks are unchanged; 95,434 empty leaves per lane reuse an existing zero array.
The other empty leaves initialize a shared array or change its width. This is allocation-work
evidence, not an inference from the wall-clock improvement.

That capture predates the presence lane, so its counter line names the canonical lane only. The
diagnostic now also reports `allocatedPresenceWords` and `reusedEmptyPresence`, and **the presence
lane's own measured figure is still outstanding** — it needs the next authorized 100M window and is
not claimed here. What the counters above already bound arithmetically: 96,459 retained leaves per
lane each took a fresh `long[16]` presence array (1,024-row leaves, 128 bytes), about 11.8 MiB per
lane and **23.6 MiB across the two**, of which the 95,434 empty leaves per lane are now shared —
leaving roughly 128 KiB per lane. Against the 1.493 GiB initial and the 14.5 MiB the canonical-lane
fix left, that puts the projected remainder near 15 MiB per hot try rather than 38 MiB. These are
derivations from the recorded counters, not a new measurement.

| q21 hot try | Before wall (s) | After wall (s) | Before GC (s) | After GC (s) |
|---:|---:|---:|---:|---:|
| 2 | 0.632 | 0.344 | 0.12 | 0.03 |
| 3 | 0.434 | 0.158 | 0.11 | 0.00 |
| 4 | 0.477 | 0.167 | 0.09 | 0.01 |
| 5 | 0.459 | 0.116 | 0.06 | 0.00 |

Hot CPU samples in G1 fall from 6,737/14,842 (45.4%) to 1,245/7,394 (16.8%); direct
canonicalisation falls from 905 samples to 69. The best of tries 2/3 changes from 0.434 to
0.158 s. These are diagnostic observations, not a new score or projected ln saving.

The q22 control retains **5,082,420 allocated longs per lane**, with zero reused empty leaves:
its tree had already dropped them. All three lane payloads remain unchanged. Its best of tries
2/3 is 0.356 s before and 0.278 s after; no q22 speedup is attributed to this mechanism.
This control and the dense-lane unit test establish that the allocation saving is bounded to
empty leaves. q22's duplicate predicate evaluation remains for a separate change.

In that comparison both queries retained the exact routes and marked-cell/distinct-rank counters
listed above, and both serialized outputs were byte-identical (`diff -r` exit 0), with SHA-256:

```text
q21 a6569917873542718e52cec662b1fad8dd035e904362abd7287770879423894f
q22 c6972e15129b9cd0ba18263b3206a8a1734a9b97c7864c7722d3018145558fe8
```

That build's final focused test run passed all 47 `SegmentGroupCanonicaliserTest` tests. It
compiled the changed source before the final
`bundles/sirix-query/bench/clickbench/rig/seggate1m.sh`, which then reported 33 matches, 10
strongly verified tie windows, **0 mismatch, 0 missing, 0 unverifiable, 0 declines**. The
two-segment private gate database was loaded by this lane. No scored 100M suite ran. The
after-check lock was released and no benchmark Java process remained.

**The shipping build is `b7be9d3db`, and only its focused tests have run so far:** all 48
`SegmentGroupCanonicaliserTest` tests pass, the 48th being
`sharedEmptyLanesAreNotWrittenByTheKernelsThatReadThem`, which reads shared value and presence
lanes back through the real count-distinct kernels. Its 100M allocation, route/counter and
byte-identity captures and its own `seggate1m.sh` gate have not been run; the hashes above are
`c5870478e`'s and must not be taken as covering presence-lane sharing.

## Evidence reproduction

The task worktree's `build/firstmate-validation/` contains `profile-window.sh`,
`profile-100m.log`, `q21q22-100m.jfr`, `trace-windows.json`, and per-query collapsed CPU/allocation
stacks. `jfr print --json --events jdk.MethodTrace` identifies query windows; use
`jfrconv --cpu` or `--alloc --total`, with `--from <epoch-ms> --to <epoch-ms> --dot --lines
-o collapsed`. Combined hot windows, with at most 1 ms boundary rounding, are:

| Query | From (epoch ms) | To (epoch ms) | CPU samples |
|---|---:|---:|---:|
| q21 tries 2–5 | 1788804607475 | 1788804609662 | 13,136 |
| q22 tries 2–5 | 1788804618503 | 1788804619691 | 12,137 |

Allocation figures are sampled estimates. Baseline totals: 10 numeric grouped serves, 5 distinct
grouped serves, zero windowed slices, zero row materializations and zero declines. This route
does not emit literal `visit=`/`cand=` counters; marked cells, distinct ranks and retained leaves
are its concrete work counters. The baseline lock was released cleanly with no Java remaining;
the database and corpora were read only, and all output stayed worktree-local.

The controlled comparison adds `after-window.sh`, `baseline-runtime.gradle`, `before-100m.log`,
`after-100m.log`, matching JFR files, `before-results/`, `after-results/`, `output-diff.txt`,
`summarize-ab.py` and `ab-summary.json`. The summary records exact hot-window bounds and sampled
totals for each build. `empty-lanes-test-final.log` and `gate-final.log` hold final validation.
