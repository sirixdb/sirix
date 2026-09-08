# Segment string decode investigation, 2026-09-08

Base: `aa4d81d547fb0e2353ede959786d6e8ba442edf2` (`SEG6T`).
Worktree: `fm/sirix-cb-strdec-1`.

The controlled exclusive-rig pair improves C6A hot geomean from **4.513 to
4.399**, a **1.0261x** geometric speedup and **1.1062 ln** reduction. Both rank
16 of 140. The earlier large late-query regression does not recur. All 43 result
files are byte-identical and all routes answer. The twofold string-query target
and top-10 campaign goal are not reached.

## Exclusive-rig acceptance measurement

Pristine `aa4d81d54` and candidate `562d0b522` run back to back under one
continuously held `$CB_RIG_WORK/leg.lock`, with identical fixed C2 settings,
three tries, all 43 queries, no profiler and no diagnostic flag. Firstmate had
stopped the other lane's 100M work before this pair. The launcher confirmed no
other database JVM before each leg; 310 audit samples throughout the pair found
at most one. The 41 external runtime dependencies match. The lock spans
2026-09-08 02:18:51 UTC through 02:24:06 UTC; the exact timestamps and checks are
in `legs/STRDECEXPAIR1-audit.json` under the rig directory.

| Leg | C6A hot geomean | Sum ln | Rank | Hot seconds |
| --- | ---: | ---: | ---: | ---: |
| STRDECEXBASE1 | 4.513 | 64.80 | 16 | 37.4 |
| STRDECEXCAND1 | 4.399 | 63.70 | 16 | 35.2 |

| Query | Pristine hot | Candidate hot | Ratio |
| --- | ---: | ---: | ---: |
| q5 | 0.624 s | 0.707 s | 0.883x |
| q12 | 0.738 s | 0.716 s | 1.031x |
| q13 | 2.401 s | 2.536 s | 0.947x |
| q28 | 8.625 s | 6.756 s | 1.277x |
| q33 | 2.324 s | 2.045 s | 1.136x |
| q34 | 2.179 s | 2.038 s | 1.069x |

This is a modest measured full-suite gain, not a uniform speedup. In particular,
q5 and q13 regress in this pair. No q31/q32 improvement is attributed to the
string path, and this lane's ln reduction must not be added to the aggregation
lane's overlapping gains. The combined head needs its own scored leg.

The earlier logs below remain for audit, but Firstmate identified potential
contention during those windows. They do not settle acceptance and were not used
to tune the retained implementation after exclusive access was granted. The
code measured here is unchanged from the candidate that passed 132 focused
tests and the complete 1M correctness gate.

This investigation owns dictionary decoding and value canonicalisation. It does not
change hash aggregation, top-N, query text, stored formats, or the database. All
representative state is query-local. Segment mints are translated through their
dictionaries; they are never compared as globally ordered values.

The corrected campaign target is 2.29x on the 14 queries above 0.5 s, or 1.37x
across all 43. Halving those 14 saves about 9.59 ln against an 11.45 ln gap.
Subset gains from parallel workers must not be added when they overlap; the
combined head needs its own scored leg. This lane claims no q31/q32 gain.

## Profile evidence

CPU profiles use async-profiler 4.2 at 1 ms over tries 2 and 3. Percentages are
inclusive CPU samples, **not wall-time shares**. The publication monitor is a
serial bottleneck even when its share of total worker CPU looks small.

| Profile | Hot, min(tries 2, 3) | Samples | `memoiseBatch` samples | `snapshotCandidates` samples |
| --- | ---: | ---: | ---: | ---: |
| Pristine q28 | 9.083 s | 155,754 | 10,542 | 0 |
| Optimistic representative q28 | 6.137 s | 154,609 | 2,947 | 1,749 |

The profiled q28 speedup is 1.480x. CPU work is approximately unchanged; moving
existing representative reads and equality checks onto segment workers increases
parallelism. The 100M q28 result is byte-identical. The regex is already compiled
once with a reused thread-local Matcher. Its engine, not pattern compilation,
accounts for substantial CPU.

The pristine q33 profile has 59,801 samples: `SegmentValueMerge` accounts for
32,666 (54.6%), including 12,457 in UTF-16 comparison. The comparison prototype
reduces merge samples to 27,919 of 56,521, but isolated q33 hot time is effectively
unchanged (2.174 to 2.170 s). Full-suite results are required to assess the broader
string path; a CPU reduction alone is not a claimed scored speedup.

q31 groups numeric keys. SearchPhrase is its predicate; it does not exercise the
same string-key canonicalisation as q13, q28, q33, and q34. Changes in its timing
cannot be attributed to this work without a separate profile.


## Historical six-query diagnostic (not the acceptance pair)

Both legs run q5, q12, q13, q28, q33, and q34, in that order, with four tries and
identical C2 diagnostic settings. Hot still means min(tries 2, 3). The fourth try
is not used. All six 100M result files are byte-identical.

| Query | Pristine hot | Shared rewrite hot | Speedup |
| --- | ---: | ---: | ---: |
| q5 | 1.356 s | 0.624 s | 2.173x |
| q12 | 1.606 s | 0.701 s | 2.291x |
| q13 | 2.457 s | 2.371 s | 1.036x |
| q28 | 8.657 s | 5.949 s | 1.455x |
| q33 | 2.228 s | 2.042 s | 1.091x |
| q34 | 2.182 s | 2.028 s | 1.076x |

The six-query geometric ratio with the scoring offset is **1.4329**, or 2.1583 ln
across these six measurements. **This is not a full-suite score gain.** In
particular, pristine q5/q12 take 0.718/0.782 s in the repeated full-suite leg;
subset/JIT warm-up changes make the larger narrow-run ratios unsuitable for
projecting onto the campaign's full-suite table. q13/q33/q34 remain below 1.3x
in this pair, and the twofold slow-query goal is not reached.

Artifacts: `baseline-c2-strings4.log`, `final6-c2-strings4.log`, and
`paired-final6.txt` in the local diagnostics directory. The final candidate
also guards an unresolved memo entry when translating arrival IDs through a
sealed rank table; repeated unresolvable inputs decline instead of indexing a
rank array with a negative ID. That guard does not change these corpus results.

## Historical full-suite legs and merge ablation

All legs below contain all 43 queries and all three scored tries. The C6A hot
board is the campaign filter, with 140 entries. These measurements have material
variation; they are reported individually rather than selecting per-query wins
from different JVMs.

| Leg | Implementation | C6A hot geomean | Sum ln | Rank |
| --- | --- | ---: | ---: | ---: |
| STRDECBASE1 | Pristine base | 4.839 | 67.80 | 16 |
| STRDECBASE2 | Pristine repeat | 4.679 | 66.35 | 16 |
| STRDECLCP2 | Early prefix/direct-cursor prototype | 4.580 | 65.43 | 16 |
| STRDECOPT1 | Optimistic representatives, serial row mapping | 4.827 | 67.69 | 16 |
| STRDECCAND4 | Parallel mapping and sealed-space shortcut | 4.775 | 67.23 | 16 |
| STRDECNOLCP | Same candidate, pristine merge | 4.309 | 62.81 | 16 |
| STRDECCAND5 | Prefix prototype with first-byte shortcut | 4.514 | 64.81 | 16 |
| STRDECFINAL6 | Cleaned candidate, original merge restored | 4.919 | 68.50 | 16 |

STRDECCAND4 has q28 at 6.120 s, but q34 at 4.342 s and regressions in several
later queries. STRDECNOLCP has q28 at 6.205 s and q34 at 1.965 s. All 43 100M
outputs in both legs are byte-identical to pristine. The no-prefix full leg
therefore improves the measured score while leaving the top-10 goal unreached.
These legs alone do not attribute its q31/q32 gains to dictionary work.

The final cleaned candidate again regressed late: q28 is 6.151 s, q33 2.540 s,
q34 4.693 s, and q35 0.986 s. Its 43 result files remain byte-identical, with no
route declines. Thus removing prefix history did **not** resolve the full-suite
problem, and the one favorable STRDECNOLCP leg cannot establish a reliable gain.
These pre-exclusivity runs did not establish a full-suite speedup. The exclusive
pair above supersedes that blocked verdict.

The historical exact three-try q34 diagnostic has 61,482 CPU samples, including 8,755
self samples in `ProjectionColumnGroupScan.aggregateByGroupNumericFlat`, versus
1,342 in the four-try full diagnostic. `NumericGroupAggTable.acquire` shifts
in the opposite direction, from 11,649 to 3,210 samples. Inlining and warm-up can
change sample attribution, so this does not prove an aggregation defect. It is a
concrete lead in files assigned to the parallel lane, which this worker has not
edited. This lead is unproven; the large regression disappeared in the exclusive pair.

The controlled six-query ablation gives the opposite ordering for the prefix
component: removing it changes q5/q12/q13/q28/q33/q34 from
0.519/0.571/2.223/5.806/1.846/1.745 s to
0.594/0.645/2.359/5.868/2.017/1.925 s. Prefix reuse improves that subset by
1.0883 with the scoring offset (0.5080 ln); all six outputs match byte for byte.
A four-try full-suite diagnostic with the same candidate did not reproduce
the late regression: q33 was 1.859 s and q34 1.874 s (profiled). Its q28 instead
varied from 12.068/9.609 s on tries 2/3 to 6.049 s on try 4. These diagnostic
results are not scored legs and the fourth try is not substituted into scoring.
The q34 profile has 55,191 samples, including 2,877 directly in
`SegmentRunCursor.commonPrefix`, 2,146 in `ArraysSupport.mismatch`, and 1,879 in
`vectorizedMismatch` beneath the merge. A first-differing-byte shortcut improved
its six-query subset by only 1.011x, with mixed individual results, and its scored
leg again regressed late: q33 4.291 s, q34 4.006 s. A further exact three-try
full-suite diagnostic reproduced q33 4.320 s and q34 4.348 s on its first try;
profiling q34 tries 2/3 then observed 2.420/1.798 s. That profiled run is excluded
from scoring. This evidence does not establish the underlying cause of the
variation, and the cleaned repeat shows the problem is not confined to the prefix experiment. The final
change restores the original merge and removes every prefix-history API.

## Mechanisms

- A transformed dictionary walk already knows each physical position. Decode it
  through the segment cursor directly, avoiding a mint-to-position lookup and
  read-view/cache lookup for every selected value. Unwalkable cells retain the
  original resolver fallback.
- Before publishing a transformed batch, snapshot an existing representative per
  hash under the canonicaliser monitor. Resolve and compare its immutable value
  on the segment worker. Publish a proven canonical ID under the monitor. Hash
  collisions, new arrivals, and concurrently settled cells still use exact
  equality and the original publication checks. The representative cache keeps
  its original 64 MiB bound; the snapshot arrays are bounded by the existing
  4,096-entry batch. No dictionary or row prepass is added.
- Rewrite the whole column's already-resolved row IDs in disjoint ranges on the
  caller's workers. Unresolved entries cause the incomplete output to be discarded
  and mapped by the original serial fallback; workers never issue new IDs during
  this phase. Missing rows, row masks, value ranks, per-slice min/max, and immutable
  source lanes are preserved. Sealed spaces also skip redundant row marking and
  go directly to the memo, with the same fallback for previously unseen cells.
- In ordinary byte-range comparison, different initial bytes bypass the mismatch
  scanner. ASCII differences compare directly; multibyte differences retain the
  validating UTF-16 decoder. The existing loser tree and its tie ordering remain
  in use.

## Rejected experiments

- Prefix-history loser tree and its first-byte shortcut: helped the six-query
  subset, but repeated full-suite regressions and the better no-prefix scored
  ablation did not justify keeping it. Removed, including its extra APIs. The
  final cleaned run still regressed, so removal is not credited as a fix.
- Galloping over consecutive wins: an exploratory diagnostic batched only 12.5%
  of q33 cells and increased record loads from 256,893 to 280,621. Dropped.
- Fixed eight-byte normalized ordering prefixes: correct on Unicode and spilled
  values, but C2 q33 hot was 2.238 s against 2.174 s pristine. Dropped.
- LRU representative cache within the existing byte budget: all correctness
  checks passed, but q28 hot was 8.890 s, and publication-monitor CPU remained
  10,687 samples. Eviction and cache maintenance replaced saved reads. Dropped.

The early galloping and LCP diagnostic launchers accidentally used Graal JIT and
omitted the 5 GiB eager-materialisation setting. Their timings are exploratory
only and are not compared with the campaign. All profiles and scored legs named
`*-c2-*` were rerun using the pinned task settings below.

## Delivery validation constraint

Firstmate accepted the exclusive measurement and assigned the 100M rig to the
aggregation lane. **No further 100M operation is authorized for this delivery:**
no profiling, ablations, benchmark legs, loads, or rebuilds. If validation appears
to require one, stop and escalate to firstmate before running it. Use the
committed exclusive pair as performance evidence and the focused unit/query
checks below for delivery validation. The existing 1M oracle result is complete;
no database rebuild is needed.

## Correctness and reproducibility

The final candidate passes 132 focused tests. Shared row-mapping coverage includes
real worker scheduling, sparse masks, range boundaries, unchanged input arrays,
serial fallback arrival order, already-sealed ranks, and repeated unresolvable
inputs. These cover UTF-16 ordering,
shared UTF-8 prefixes, malformed differing suffixes, packed and spilled entries,
sparse/range merges, multiplicity, physical cursor reads, simultaneous transformed
walks, and reads outside the publication monitor. Query checks include
`SegmentLengthLaneQueryTest`, `GroupTopKDifferentialTest`, and
`AnyKGroupsSegmentKeyRewriteTest`.

The final C2 1M validation reports 34 matches, 9 strongly verified legal tie windows,
zero mismatch, zero missing, zero unverifiable, and zero route declines. All 43
query dumps are byte-identical to the pristine Sirix 1M outputs. The strong
DuckDB oracle uses `duckdb_reference.py --candidate-reference` and
`compare-results.py --strong --bounded-oracle` as described in
[the operating manual](HANDOFF_SEGMENT_LANE_2026-09-06.md).

The 100M runs acquire `$CB_RIG_WORK/leg.lock` with `flock`, retain its inode, and
release it when the JVM exits. The database and both source corpora are read-only;
dumps, profiles, and oracle scratch files are inside this task worktree. The
launcher refuses an existing ClickBench JVM or less than 26,000,000 KiB of
MemAvailable. The fixed JVM settings are:

```text
-Xms6g -Xmx14g
-XX:+UnlockExperimentalVMOptions -XX:-UseJVMCICompiler
-Dsirix.offheap.bytes=10737418240
-Dsirix.projection.eagerMaterializeBytes=5368709120
-Dsirix.query.autoVectorize=true
-Dsirix.chunkedBody.enable=true
-Dsirix.chunkedBody.targetChunkBytes=16384
--add-opens java.base/sun.nio.ch=ALL-UNNAMED
--add-opens java.base/java.nio=ALL-UNNAMED
--add-modules jdk.incubator.vector
--enable-native-access=ALL-UNNAMED --enable-preview
```

Use `:sirix-query:printClickBenchRuntimeClasspath`, then invoke
`io.sirix.query.bench.clickbench.ClickBenchRunMain "$CB100M_DIR/db" --tries 3
--dump <worktree-local-directory>` with those settings and the lock. Scored legs
have no diagnostic flag or profiler. Isolated profiles additionally set
`-Dsirix.projDiag=true`, select one query, and use four tries, profiling only tries
2 and 3. The explicitly labeled full-suite diagnostics profile q34 after the
preceding queries; they are excluded from scoring. The fourth try is diagnostic steady state and never replaces the scored
hot definition.

Mutation checks killed three defects in the retained change: disabling the
physical cursor decode (one failed behavioral test), moving representative reads
back under the publication monitor (one), and allowing unresolved row IDs to be
issued from mapping workers (one). The rejected prefix prototype also killed a
reversed-prefix-order mutation with four failing tests.

Raw logs, preserved pristine/candidate runtimes, profiles, mutation logs, 1M
oracle results, and the local launchers are under
`bundles/sirix-query/build/diagnostics/strdec/` in this worktree. Scored triples
are persisted in `bundles/sirix-query/bench/clickbench/rig/legs/`; use that rig's
`mkleg.py` and `rank.py`. Use paired full-suite legs rather than summing apparent
wins from isolated profiles or selecting the fourth try.
