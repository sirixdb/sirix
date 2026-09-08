# Segment string decode investigation, 2026-09-08

Base: `aa4d81d547fb0e2353ede959786d6e8ba442edf2` (`SEG6T`).
Worktree: `fm/sirix-cb-strdec-1`.

> **EVERY 100M NUMBER IN THIS DOCUMENT IS PROVISIONAL.** Firstmate instructions
> 006 and 007 declared the campaign's measurement rig unreliable and handed
> exclusive 100M access to a separate rig-repair lane. That applies without
> exception to every profile, isolated q33 pair, full-suite pair, per-query time,
> ratio, geomean and summed ln below — including the legs this lane measured
> under a continuously held lock. None of them is an accepted performance result.
> None may be used to tune an implementation, to claim a campaign gain, or to
> score a leg until the rig is repaired and the figures are re-measured on it.
> They are retained unchanged as raw historical observations, nothing more.

## What this lane delivers

**A mechanism and its correctness evidence. No performance claim.** The change
decodes a transformed dictionary walk through the segment cursor, resolves and
compares representatives on segment workers outside the publication monitor, and
rewrites an already-resolved column's row IDs in disjoint parallel ranges with a
serial fallback. [Mechanisms](#mechanisms) states each in full.

Correctness is what has been established:

- The refreshed C2 1M oracle gate passed on the revised runtime: 33 matches,
  10 strongly verified legal tie windows, zero mismatch, zero missing, zero
  unverifiable, zero route declines, and all 43 query dumps byte-identical to the
  pristine Sirix 1M outputs.
- Every 100M output comparison passed: all 43 result files byte-identical to
  pristine in the exclusive pair, and all eight q33 runs byte-identical in the
  repeat check.

Those are corpus checks over two fixed ClickBench corpora. They are strong
evidence that the retained code answers identically on the data actually
exercised; they are **not** a proof of equivalence over all possible inputs, and
this document does not claim one.

**The performance effect is unverified, pending measurement-rig repair.** No ln
figure is delivered — not the 2.281 ln the exclusive pair observed, not the
1.637 ln that remains once q31 is subtracted. The reason is recorded in this
lane's own data: the *unchanged* pristine baseline measured q31 at 1.500 s in
`STRDECEXBASE1` and 3.191 s in `STRDECEXBASE2`. Identical code, identical flags,
2.1x apart. An instrument with that spread cannot resolve the effects this lane
is being asked to detect, so no gain and no regression is established here.

Declining to bank the unattributed 0.644 ln q31 credit was the judgement that
made an evidence-based decision possible at all. Had that credit been claimed,
the q31 baseline discrepancy would have been buried inside a headline number
instead of exposing the instrument. Record it as the right call.

The full-suite q33 result remains an **open question**, not a settled regression
and not a dismissed artifact. Its evidence and its unknown cause are preserved in
full below, together with the four isolated repeats that do not reproduce it.

## Provisional exclusive full-suite measurement (STRDECEXPAIR2)

Provisional, per the banner above. `STRDECEXPAIR2` observed C6A hot geomean
**4.782 to 4.535**, or **2.281 ln** lower, with both legs rank 16 of 140. All 43
result files are byte-identical and all routes answer. This single-pair
difference is not established causal credit for the lane, and is not accepted as
a measurement at all pending rig repair: it includes an unattributed 0.644 ln
q31 difference and a large q33 regression. Subtracting q31 alone does not
establish attribution for the remainder.

Pristine `aa4d81d54` and the revised candidate run back to back under one
continuously held `$CB_RIG_WORK/leg.lock` (inode 36438151, single acquisition),
with identical fixed C2 settings, three tries, all 43 queries, no profiler and no
diagnostic flag. The launcher confirmed no other database JVM before each leg;
336 audit samples throughout the pair found at most one, our own. The 41 external
runtime dependencies match, and the baseline leg uses the preserved pristine
runtime snapshot, not a rebuild. The lock spans 2026-09-08 02:57:53 UTC through
03:03:40 UTC. The candidate is `ed73b3a48` plus the review fix that removes the
ASCII comparison shortcut; its worktree diff is recorded by SHA-256 in
`legs/STRDECEXPAIR2-audit.json`, together with the per-query table and every
check named here.

The audit also records class digests proving which source each leg ran. In the
candidate runtime `SegmentGroupCanonicaliser` and `SegmentRunCursor` match the
superseded candidate and differ from pristine, so the lane's canonicalisation work
is present. `ValueDictionaryEntryNode` has a distinct class digest, but its source
diff against pristine now contains only the Javadoc clarification: the comparison
body is restored to pristine. A distinct class digest alone is not evidence of
different executable behavior.

| Leg | C6A hot geomean | Sum ln | Rank | Hot seconds |
| --- | ---: | ---: | ---: | ---: |
| STRDECEXBASE2 | 4.782 | 67.29 | 16 | 40.5 |
| STRDECEXCAND2 | 4.535 | 65.01 | 16 | 37.7 |

| Query | Pristine hot | Candidate hot | Ratio | Δ ln |
| --- | ---: | ---: | ---: | ---: |
| q5 | 0.610 s | 0.705 s | 0.865x | -0.143 |
| q12 | 0.754 s | 0.714 s | 1.056x | +0.054 |
| q13 | 2.731 s | 2.514 s | 1.086x | +0.083 |
| q28 | 9.365 s | 6.322 s | 1.481x | +0.392 |
| q31 | 3.191 s | 1.671 s | 1.910x | +0.644 |
| q32 | 6.815 s | 7.003 s | 0.973x | -0.027 |
| q33 | 2.311 s | 4.929 s | 0.469x | -0.755 |
| q34 | 2.279 s | 2.027 s | 1.124x | +0.117 |

Three caveats bound what this pair establishes.

- **q31 is not this lane's gain.** Its 3.191 s pristine hot is an outlier: the
  earlier pristine leg `STRDECEXBASE1` measured 1.500 s and this candidate 1.671 s
  for the same query. q31 groups numeric keys and this lane claims no q31/q32
  improvement. Excluding q31 leaves an observed sum-ln difference of **1.637**;
  that arithmetic is not a measurement of this lane's causal contribution.
- **q33 regressed by more than the whole lane's gain on any other single query**,
  2.311 s to 4.929 s. The previous pair measured 2.324 s to 2.045 s for a candidate
  that differs only by the removed shortcut, and the differential fuzz shows that
  shortcut did not change any comparison result in that check. This does not rule
  out a context-dependent performance regression in the retained code or its
  compiled execution. The cause of the late-query variation is unresolved.
- **The pristine baseline itself drifted between the two pairs**, 4.513 to 4.782
  geomean and 37.4 to 40.5 hot seconds for the identical `aa4d81d54` runtime and
  flags 40 minutes apart. Only within-pair deltas are meaningful here; absolute
  geomeans from different pairs are not comparable.

`STRDECEXBASE1`/`STRDECEXCAND1` measured a superseded candidate that still
contained the ASCII comparison shortcut. Their pair (4.513 to 4.399, 1.1062 ln)
is retained as history and no longer describes the delivered source.

## Provisional repeated q33 isolation check

Provisional, per the banner above. Firstmate requested repeated q33-only pairs
after the exclusive full-suite regression. The revised candidate first passed a refreshed complete 1M oracle
gate. The following eight JVMs then ran under one continuously held rig lock,
using the same preserved pristine and revised runtime snapshots as pair2,
identical external dependencies, the fixed C2 envelope, and three tries each.
Hot remains min(tries 2, 3). No profiler or diagnostic flag was enabled.

| Pair | Order | Pristine hot | Candidate hot | Ratio with 0.01 s offset |
| --- | --- | ---: | ---: | ---: |
| 1 | baseline, candidate | 1.945 s | 1.895 s | 1.026x |
| 2 | candidate, baseline | 2.213 s | 1.994 s | 1.109x |
| 3 | baseline, candidate | 2.160 s | 2.057 s | 1.050x |
| 4 | candidate, baseline | 2.219 s | 2.091 s | 1.061x |

The geometric ratio is **1.0611x**; median hot times are 2.1865 and 2.0255 s.
All eight outputs are byte-identical. The lock spans 03:11:52–03:13:35 UTC on
2026-09-08, with 104 audits showing at most one database JVM and no conflicts.
The evidence is persisted as `legs/STRDECQ33REPEAT1-audit.json`.

The full-suite 2.13x slowdown does not reproduce in any of the four isolated
pairs, in either run order. This supports sensitivity to suite history or JVM
state rather than a repeatable standalone q33 slowdown. It does not identify the
cause or exclude a context-dependent code regression. These isolated times must
not replace q33 in the full-suite score or be added to its observed gain.

The earlier logs below remain for audit, but Firstmate identified potential
contention during those windows. They do not settle acceptance and were not used
to tune the retained implementation after exclusive access was granted.

This investigation owns dictionary decoding and value canonicalisation. It does not
change hash aggregation, top-N, query text, stored formats, or the database. All
representative state is query-local. Segment mints are translated through their
dictionaries; they are never compared as globally ordered values.

The corrected campaign target is 2.29x on the 14 queries above 0.5 s, or 1.37x
across all 43. Halving those 14 saves about 9.59 ln against an 11.45 ln gap.
Subset gains from parallel workers must not be added when they overlap; the
combined head needs its own scored leg. This lane claims no q31/q32 gain.

**Hot suite seconds and the score are separate measures and can move in opposite
directions.** The score is a geometric mean of `(0.01 + ours) / (0.01 + best)`
per query, so it weighs each query against its board leader, and the campaign is
relatively furthest behind on the *fast* queries. A candidate last night cut
total hot time from 40.357 to 34.970 s — 13% faster — and scored 0.815 ln
*worse*. Read both columns of the tables below; neither substitutes for the
other, and seconds saved is not the campaign's objective.

## Provisional profile evidence

Provisional, per the banner above: these profiles and the q28 timings beside them
were taken on the same unrepaired rig and carry the same status as every other
100M figure here.

CPU profiles use async-profiler 4.2 at 1 ms over tries 2 and 3. Percentages are
inclusive CPU samples, **not wall-time shares**. The publication monitor is a
serial bottleneck even when its share of total worker CPU looks small.

A profile ranks where CPU is spent; it does not rank return on effort. A large
inclusive share is a reason to look at a path, never on its own a reason to
credit a change that touched it.

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


## Provisional historical six-query diagnostic (not the acceptance pair)

Provisional, per the banner above. Both legs run q5, q12, q13, q28, q33, and q34, in that order, with four tries and
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

## Provisional historical full-suite legs and merge ablation

Provisional, per the banner above. All legs below contain all 43 queries and all three scored tries. The C6A hot
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

## Rejected experiments

The 100M timings cited as grounds for each rejection are provisional, per the
banner above. The rejections themselves stand: none of these components earned
its place, and an unrepaired rig is not a reason to revive one.

- Prefix-history loser tree and its first-byte shortcut: helped the six-query
  subset, but repeated full-suite regressions and the better no-prefix scored
  ablation did not justify keeping it. Removed, including its extra APIs. The
  final cleaned run still regressed, so removal is not credited as a fix.
- First-byte shortcut and ASCII mismatch fast path in `compareUtf16Range`:
  bypassed `Arrays.mismatch` when the leading bytes already differed, and returned
  the byte difference directly when both mismatching bytes were ASCII. Its
  isolated q33 hot time was 2.174 s pristine against 2.170 s, i.e. noise.
  **Removed during review as unearned complexity.**
  A 3,000,000-case differential fuzz over random and deliberately malformed byte
  ranges found no divergence. The suspected malformed-input defect did not
  reproduce; removal is not claimed as a correctness fix. The review also found
  an inaccurate documentation claim, now corrected:
  `compareValueUtf16` promised `IllegalStateException` for any malformed payload,
  but the byte-identical prefix has been settled without decoding since
  `a3aed07ec`, well before this lane. `{C3,41}` against `{C3,42}` therefore
  orders by the deciding ASCII byte and does not throw, on the pristine base as
  much as here. `asciiMismatchAfterAMultibyteLeadIsSettledByTheDecidingByte` and
  `malformationAtTheDecidingSequenceFailsClosedWhicheverSideCarriesIt` pin both
  halves of the corrected contract.
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

**This lane no longer owns the rig, and must not take it.** A separate lane holds
exclusive 100M access on the captain's instruction while it repairs the
measurement rig itself. Any future 100M window for this lane — a leg, a profile,
an ablation, a rebuild — must be allocated by firstmate before it starts. Do not
begin one on the strength of a released lock or an absent lock file.

This lane's 100M operations are complete and its lock is released; the last one
ended 2026-09-08T03:13:35.547312+00:00, and none of the driver's 100M JVMs is in
flight. Firstmate's 02:50:48 UTC decision authorized the replacement full-suite
pair after removing the comparison shortcut, and their 03:08:32 UTC follow-up
authorized the repeated q33-only checks above. Both requests are **fulfilled**,
and neither authorizes any further 100M work now.

Correctness work continues without a window: focused tests and the 1M oracle gate
need no 100M run. The refreshed 1M oracle result is complete; no database rebuild
was performed or is needed.

**This change is banked on correctness, and its PR waits on the rig lane's
verdict before any merge.** Firstmate has handed the rig lane the q33 suite-order
anomaly below as a named target. No merge is authorized on the numbers in this
document.

## Correctness and reproducibility

The candidate that `STRDECEXPAIR1` measured passed 132 focused tests. The review
fix adds two comparison tests and re-runs the 64 tests covering the touched files
(`ValueDictionaryComparisonTest`, `SegmentValueMergeTest`,
`SegmentGroupCanonicaliserTest`), all green. Shared row-mapping coverage includes
real worker scheduling, sparse masks, range boundaries, unchanged input arrays,
serial fallback arrival order, already-sealed ranks, and repeated unresolvable
inputs. These cover UTF-16 ordering,
shared UTF-8 prefixes, malformed differing suffixes, an ASCII mismatch behind a
multibyte lead byte, malformation on either side of the deciding sequence, packed
and spilled entries, sparse/range merges, multiplicity, physical cursor reads,
simultaneous transformed walks, and reads outside the publication monitor. Query checks include
`SegmentLengthLaneQueryTest`, `GroupTopKDifferentialTest`, and
`AnyKGroupsSegmentKeyRewriteTest`.

The refreshed C2 1M validation on the revised runtime reports 33 matches and
10 strongly verified legal tie windows,
zero mismatch, zero missing, zero unverifiable, and zero route declines. All 43
query dumps are byte-identical to the pristine Sirix 1M outputs. The strong
DuckDB oracle uses `duckdb_reference.py --candidate-reference` and
`compare-results.py --strong --bounded-oracle` as described in
[the operating manual](HANDOFF_SEGMENT_LANE_2026-09-06.md).

The 1M oracle gate and the 100M output comparisons both passed. They are checks
over two fixed corpora and the tests listed above, not a proof that the retained
code answers identically on every possible input; read them as the strong
empirical evidence they are and no further. The performance effect of the same
code is unverified pending measurement-rig repair.

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
`bundles/sirix-query/build/diagnostics/strdec/` in this worktree;
`STRDECEXPAIR2`'s scored triples and audit summary are committed. Its raw logs
and dumps were created in the pipeline's disposable worktree and are no longer
available after custody recovery; the summary preserves their reported checks.
Before recovery, its revised runtime was frozen under `strdec/review1-runtime/`
for the repeated q33 and refreshed 1M checks, whose raw evidence remains in this
task worktree. All used new artifact names. Scored triples
are persisted in `bundles/sirix-query/bench/clickbench/rig/legs/`; use that rig's
`mkleg.py` and `rank.py`. Use paired full-suite legs rather than summing apparent
wins from isolated profiles or selecting the fourth try.
