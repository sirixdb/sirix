# q28 source attribution corrected; fresh-build provenance required

The alleged 9.9x q28 regression was a runtime-attribution failure. The slow run was
attributed to campaign head `98adc82eb` after compiling a reused worktree with
UP-TO-DATE tasks and without a frozen runtime manifest. Its [retained diagnostic](retained-slow-diagnostic.log), copied from the
preceding task, lacks
the `bounded=` field that both `47c32cdbc` and `98adc82eb` print unconditionally.
A fresh, frozen build of **unchanged `98adc82eb`** runs q28 at **5.716 s hot**, with
`bounded=true` on every try. The source does not have the alleged finite-limit bug;
**no production aggregation gate was changed**.

The retained slow diagnostic's final numeric aggregation pass took 0.342 s of
56.292 s wall time. That does not support the claim that the group merge caused the
delay. The evidence identifies a stale/different binary and invalidates its claimed
source attribution; it does not establish the exact earlier source of that binary.
Firstmate accepted this correction and amended delivery to gate safeguards, a rig
provenance fix, and the remaining validation.

## The actual limit path

`SortedScanDetectionStage.tryAnnotateSubsequenceLimit` records SORTED_LIMIT=25 for
`subsequence(pipe, 1, 25)`; it does not inspect HAVING.
`GroupAggregateDetectionStage` independently records the post-group count filter
as GROUP_AGG_HAVING. `SirixPipelineStrategy` passes both annotations through
`SirixGroupAggregateExpr` to `SirixVectorizedExecutor.executeGroupByAggregate`.
The executor's positive request limit passes unchanged into `selLimit`.

| Arm | Gate reads | Finite LIMIT after HAVING | Uncapped predicate-tree selection |
| --- | --- | --- | --- |
| Composite | request limit | 25, bounded | -1, unbounded |
| String | request limit | 25, bounded | -1, unbounded |
| Packed substring | selLimit, passed as method limit | 25, bounded | Long.MAX_VALUE, unbounded |
| Numeric | selLimit, passed as method limit | 25, bounded | Long.MAX_VALUE, unbounded |

The campaign's segment-string regex key takes `numericGroupAggregate`. It receives
25, filters with `havingPasses` before `sel.offer`, and selects up to 25 groups per
partition. The selector's size derives from that limit, independently of the budget
classification. `boundedSelection` already rejects Long.MAX_VALUE in both compared
heads. The executor diff from `47c32cdbc` to `98adc82eb` changes the composite/string
partial-worker argument to reuse the budget gate; it changes neither the four budget
gates nor the numeric arm's limit.

## Safeguards shipped

- Every runtime preparation forces `--rerun-tasks --no-build-cache`. Gradle exports
  actual compiler, resource and JAR task execution states; both engine Java
  compilations must have run. UP-TO-DATE, FROM-CACHE and missing compiler work refuse.
- HEAD and a digest of tracked and untracked, nonignored inputs are captured before
  and after compilation. Source movement during a build refuses. A scored run or
  paired comparison requires the on-disk frozen manifest, matching artifact hashes,
  the build witness and matching source-checkout identity at launch. Scored engine
  inputs must be committed; the explicit historical measurement-harness overlay
  remains recorded. Baseline and candidate each verify their own checkout.
- `mkleg.py TAG RUN/leg/leg.json` accepts only a complete runner receipt. It verifies
  the frozen build, runtime/source identity, successful collection verdict, exact
  suite-log hash and all 43 timing triples. Raw Gradle/Java logs and direct Java JSON
  cannot be converted to scored artifacts. Export retains steering scope and null
  unobserved metadata; publication remains an explicit curation of the observed regime.
- The four-arm HAVING + aggregate ORDER BY + finite LIMIT fixture observes bounded
  plans; its uncapped variants observe no bounded plan. The q28 composition also
  observes a bounded plan. **These gate fixtures pass on the original current head:
  they are safeguards, not reproductions of a gate defect.** The existing
  Long.MAX_VALUE and sparse-SUM ordering regressions remain enabled and passing.

The real Gradle fixture corrupts a reused compiled output, prepares again, verifies
actual compilation-task execution, and executes the frozen result to confirm it
matches the source. The launch/export fixtures reject absent manifests, stale task
outcomes, changed HEADs/inputs, dirty source, mutated frozen artifacts, spliced logs,
wrong runtime IDs and failed verdicts.

## Validation and allowance

**91 rig tests and 68 query tests pass**, zero failures/errors/skips. The query classes
are GroupTopKDifferentialTest, GroupHashRangePassTest, PartialGroupTopKTest and the
enabled SparseSumOrderingOriginTest.

At the existing 1,000,000-row database, all **43/43 lossless answer files are
byte-identical** between the freshly compiled `98adc82eb` runtime and the guarded
`d8f275788a6ac6add83db3c9a15ab5edae95016f` runtime. Both full suites were run once,
read-only, with fresh output directories. [Per-query SHA-256 proof](correctness-1m.json).

| Diagnostic | Source | Try 1 s | Try 2 s | Try 3 s | Hot s | Gate |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| q28 | unchanged 98adc82eb | 10.439 | 6.071 | 5.716 | 5.716 | bounded=true, all tries |
| q32 | d8f275788 | 11.701 | 3.512 | 3.218 | 3.218 | bounded=true, all tries |

The q32 diagnostic and SEG8T use the **same frozen runtime**:
`bec1bad6bf9f3e3bffc0587fdd33db28c9703753037b52aba1734a610affc35c`.
Its [build log](build.log) has all nine preparation tasks executed and its
[manifest](runtime.json) carries the task witness and source identity.

Every 100M run used the exclusive rig lease, one benchmark JVM, 20 workers, the same
read-only database and the pinned 6/14 GiB heap, 10 GiB off-heap, 5 GiB eager budget,
with JVMCI disabled. MemAvailable before q28, q32 and the score was respectively
28,911,608, 28,893,704 and 28,574,776 kB, each above 26 GiB.
No loader, corpus rewrite, database rebuild, power-setting change or global prepass
was introduced. Diagnostics and scored collection each returned exit code 0 with
no rig issues.

**The full task allowance is consumed: one q28 diagnostic, one q32 diagnostic and
one scored leg. Remaining 100M runs: zero, including pipeline fixers and verifiers.**
Further validation must use local fixtures and offline artifact checks. Campaign
branch CI runs no checks; delivery must be described as pushed and locally validated,
never as CI green.

## SEG8T standing

[query-SEG8T.json](../../legs/query-SEG8T.json) is the single authorized scored leg,
collected on 2026-09-09T20:38:37.945902+00:00 at source
`d8f275788a6ac6add83db3c9a15ab5edae95016f`. Its machine, load_time and data_size fields
are null. The publication scope records the campaign's authorized standing under
the retained physical regime; it does not turn a single leg into an effect estimate.

SEG8T hot total is **26.523 s**, versus SEG7T **27.890 s**.
C6A hot sum-ln is **58.034796**, versus **58.956827**:
delta **-0.922030 ln**, UNRESOLVED.
q28 is **6.214 s hot**; q32 is **3.296 s hot**.

**Sign convention:** delta seconds = SEG8T hot minus SEG7T hot; negative is faster,
positive is slower. Delta ln = ln((SEG8T hot + 0.01)/(SEG7T hot + 0.01)); negative
reduces the score. Hot = min(try 2, try 3). **Every one-leg query delta and the suite
delta is UNRESOLVED.** There is no paired noise estimate or causal gain claim.

| Query | SEG7T hot s | SEG8T hot s | Delta s | Delta ln | Status |
| --- | ---: | ---: | ---: | ---: | --- |
| q0 | 0.001 | 0.001 | +0.000 | +0.000000 | UNRESOLVED |
| q1 | 0.034 | 0.035 | +0.001 | +0.022473 | UNRESOLVED |
| q2 | 0.056 | 0.052 | -0.004 | -0.062520 | UNRESOLVED |
| q3 | 0.035 | 0.032 | -0.003 | -0.068993 | UNRESOLVED |
| q4 | 0.305 | 0.335 | +0.030 | +0.090972 | UNRESOLVED |
| q5 | 0.656 | 0.697 | +0.041 | +0.059741 | UNRESOLVED |
| q6 | 0.028 | 0.013 | -0.015 | -0.502092 | UNRESOLVED |
| q7 | 0.016 | 0.020 | +0.004 | +0.143101 | UNRESOLVED |
| q8 | 0.650 | 0.651 | +0.001 | +0.001514 | UNRESOLVED |
| q9 | 0.706 | 0.725 | +0.019 | +0.026190 | UNRESOLVED |
| q10 | 0.223 | 0.197 | -0.026 | -0.118320 | UNRESOLVED |
| q11 | 0.166 | 0.162 | -0.004 | -0.022990 | UNRESOLVED |
| q12 | 0.686 | 0.677 | -0.009 | -0.013015 | UNRESOLVED |
| q13 | 1.181 | 1.196 | +0.015 | +0.012516 | UNRESOLVED |
| q14 | 0.711 | 0.770 | +0.059 | +0.078655 | UNRESOLVED |
| q15 | 0.436 | 0.438 | +0.002 | +0.004474 | UNRESOLVED |
| q16 | 1.500 | 1.465 | -0.035 | -0.023452 | UNRESOLVED |
| q17 | 0.037 | 0.042 | +0.005 | +0.101096 | UNRESOLVED |
| q18 | 2.972 | 2.371 | -0.601 | -0.225074 | UNRESOLVED |
| q19 | 0.011 | 0.010 | -0.001 | -0.048790 | UNRESOLVED |
| q20 | 0.027 | 0.029 | +0.002 | +0.052644 | UNRESOLVED |
| q21 | 0.084 | 0.114 | +0.030 | +0.276987 | UNRESOLVED |
| q22 | 0.299 | 0.298 | -0.001 | -0.003241 | UNRESOLVED |
| q23 | 0.128 | 0.124 | -0.004 | -0.029414 | UNRESOLVED |
| q24 | 0.020 | 0.018 | -0.002 | -0.068993 | UNRESOLVED |
| q25 | 0.092 | 0.137 | +0.045 | +0.365460 | UNRESOLVED |
| q26 | 0.024 | 0.018 | -0.006 | -0.194156 | UNRESOLVED |
| q27 | 0.251 | 0.232 | -0.019 | -0.075583 | UNRESOLVED |
| q28 | 6.039 | 6.214 | +0.175 | +0.028520 | UNRESOLVED |
| q29 | 0.033 | 0.030 | -0.003 | -0.072321 | UNRESOLVED |
| q30 | 0.341 | 0.306 | -0.035 | -0.105044 | UNRESOLVED |
| q31 | 0.908 | 0.872 | -0.036 | -0.040005 | UNRESOLVED |
| q32 | 4.236 | 3.296 | -0.940 | -0.250238 | UNRESOLVED |
| q33 | 2.152 | 2.111 | -0.041 | -0.019146 | UNRESOLVED |
| q34 | 2.027 | 2.034 | +0.007 | +0.003431 | UNRESOLVED |
| q35 | 0.395 | 0.400 | +0.005 | +0.012270 | UNRESOLVED |
| q36 | 0.064 | 0.071 | +0.007 | +0.090384 | UNRESOLVED |
| q37 | 0.045 | 0.043 | -0.002 | -0.037041 | UNRESOLVED |
| q38 | 0.039 | 0.044 | +0.005 | +0.097164 | UNRESOLVED |
| q39 | 0.170 | 0.149 | -0.021 | -0.124053 | UNRESOLVED |
| q40 | 0.030 | 0.024 | -0.006 | -0.162519 | UNRESOLVED |
| q41 | 0.033 | 0.038 | +0.005 | +0.110001 | UNRESOLVED |
| q42 | 0.043 | 0.032 | -0.011 | -0.232622 | UNRESOLVED |

The scorer observed six firmware-managed MMIO power-limit changes
(76 → 45 → 49 → 57 → 65 → 74 → 45 W), retained under the existing rig policy.
No operator power setting was changed. The exact transitions remain in the verdict
and leg metadata.

## Retained evidence

[Validation summary](validation.json), [CSV comparison](queries.csv),
[q28 log](q28-diagnostic.log), [q28 frozen manifest](q28-runtime.json),
[q32 log](q32-diagnostic.log), [scored log](scored-suite.log),
[scored command](scored-command.json), [verdict](scored-verdict.json),
[runner receipt](scored-receipt.json), and [rig test output](rig-tests.log)
are retained with this report. Frozen manifests preserve their original paths and
hashes; these committed copies document the observed build, rather than claiming
that the full classpath or its scratch checkout is bundled in this evidence folder.
