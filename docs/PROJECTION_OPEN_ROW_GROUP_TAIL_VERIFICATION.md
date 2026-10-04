# Open projection row-group tails: verification

Measured on 2026-10-04 on the i7-12700H. The primary single-commit proof is the historical JSONBench
replay against main `cc042c494fdb648525bd833cdb14a5b7f82abd6e`. The earlier SH1 and generated-fixture
campaigns used main `3fb92436d`; their complete results remain below as supplementary evidence. The
production change contains the open-row-group tail, merged reader routing, and bounded writer-seeded
merge memo. The six changed existing classes and their nested classes were exported from the earlier
baseline and prepended to the common runtime for its main arm. Candidate runtime, main overlay and
dependencies were copied and hashed before measurement; the JSONBench shared query overlay is
identified below. No benchmark harness or permanent diagnostic counters are landed.

Every build and benchmark process used the prescribed memory limiter (at least 6 GiB available, two
shared flock slots). Benchmark JVMs were pinned to CPUs 0–11. Both arms use the same Brackit
snapshot, SHA-256 `4742c41b9408db48742255317b62a6c9cd64f8a9c7f779d5edfd2c4dea144366`. Gradle
resolved it with a fresh private `-Dmaven.repo.local` directory, leaving `~/.m2` untouched. Java
25.0.3; SH1 heap 2 GiB, commit-proof heap 6 GiB, direct cap 1 GiB.

## Correctness and work

The suite counts below are the captured campaign results before the subsequent
order-exception bitmap and memo-hit base-payload repairs; they are not current-head test counts.

- Full core suite: 12,327 tests, zero failures/errors, 77 skipped.
- Full query suite: 2,068 tests, two known external failures, seven skipped. The current Brackit
  snapshot changed array-navigation semantics; the maintainer identified
  `ArrayContainsScopeDifferentialTest.mixedArrayAndStringFieldRaisesInBothRoutes` and
  `SirixArraySizeTest.optimizerRecordsOnlySuccessfulStoredArrayCardinalities` as failing on main
  too. The [companion correction](https://github.com/sirixdb/sirix/pull/1251) is now on main. These
  tests are unchanged by the row-tail change.
- Final focused run: 1,410 core tests and all 18 query-budget tests, zero failures/errors or skips,
  plus `spotlessCheck`. It includes the complete projection package and the entire Work budgets
  block of `VERIFICATION.md`; no budget bound was widened.
- Full suites above ran before the final array-ownership repair. The final projection and budget run
  exercises that repair: public scalar, batch and directory outputs cannot mutate the shared merge
  memo, while internal maintenance borrows read-only raw bytes. The regression test failed all four
  versioning cases before the repair.
- The two new tail classes pass all 39 cases. Versioned-page cases cover all four `VersioningType`s:
  scalar, batch, bounded and actual parallel directory reads; same-transaction referenced-tail
  reads; cold reopen of each historical revision; global string dictionary ids;
  full/non-append/column-patch folds; tombstones and rollback; malformed row blobs and a descriptor
  that disagrees with its tail.
- Temporarily removing the writer's memo seed makes all four versioning cases fail `append seeds the
  writer memo` (expected zero cold merges, observed one). The observer is temporary test state and
  is restored; production maintains no diagnostic counter.

The subsequent repairs add regression coverage in
[`ProjectionIndexRowGroupPageTest`](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionIndexRowGroupPageTest.java),
[`ProjectionOpenRowGroupTailListenerTest`](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionOpenRowGroupTailListenerTest.java)
and [`ProjectionOpenRowGroupTailTest`](../bundles/sirix-core/src/test/java/io/sirix/index/projection/ProjectionOpenRowGroupTailTest.java).
Those cases cover hydrated bitmap growth across row 64, middle insertion followed by tail append,
and memo-hit/cold-route payload read work. They do not alter the frozen benchmark provenance below.

## SH1 t25k

The existing kit's 25 natural epoch publications (E0–E24), its immutable-metadata commit, and
unchanged twelve queries run against the read-only stream and oracle. Eight matched pairs reverse
arm order every other pair; fresh load and query JVMs, fresh stores. Every canonical answer must
match its oracle before an arm is accepted. Results are retained and stores deleted.

All 192 answers were exact. No load or query cell is a confirmed regression. The intervals are broad
on this shared laptop; the stated gate passes, rather than proving a tighter equivalence margin.
Times below are seconds.

| Cell | Main median | Candidate median | Paired median ratio | 95% interval |
|---|---:|---:|---:|---:|
| load | 26.262000 | 26.573500 | 1.0061 | 0.9909–1.0319 |
| q1 | 1.309408 | 1.342003 | 1.0296 | 0.9698–1.0888 |
| q2 | 0.515061 | 0.519161 | 1.0044 | 0.9629–1.1232 |
| q3 | 0.129806 | 0.121195 | 0.9671 | 0.8161–1.1039 |
| q4 | 0.822064 | 0.827176 | 0.9863 | 0.9647–1.0365 |
| q5 | 3.631277 | 2.980916 | 0.8705 | 0.7546–1.0666 |
| q6 | 3.711910 | 3.712910 | 1.0145 | 0.9895–1.0437 |
| q7 | 0.176859 | 0.182993 | 0.9941 | 0.9375–1.0885 |
| q8 | 0.185733 | 0.186662 | 1.0009 | 0.9682–1.0500 |
| q9 | 0.193051 | 0.195865 | 1.0159 | 0.9497–1.0561 |
| q10 | 0.368719 | 0.367542 | 1.0016 | 0.9753–1.0368 |
| q11 | 8.772588 | 8.923169 | 1.0148 | 0.9762–1.0243 |
| q12 | 0.240289 | 0.240432 | 1.0110 | 0.9664–1.0677 |

## Historical JSONBench single-record commit proof

The historical prototype result was 210,207 to 188,221 bytes per single commit (-10.5%), with commit
p50 35.53 to 35.95 ms after the memo was seeded. The historical 077/084 million-row Bluesky workload
has now been replayed on current main and the candidate. The original `prepare.py` reconstructed the
canonical replay from the preserved read-only `/var/tmp/jsonbench/data/file_0001.json.gz`. Its SHA-256,
`ad177298335065afe23beaef0ec5a4b5655f548d94052c94d491be5f10be6e9e`, exactly matches the historical
`source.json`. The root-single manifest, 617-request suite, independent `expected.json` and parameters
were preserved; input hashes are recorded below.

Each arm loaded 990,000 rows in 99 commits of 10,000, followed by 10,000 single-row commits with the
same long-lived writer. It used SLIDING_SNAPSHOT, revision window three, the original eight projected
fields and sorted specification, `storeDiffs(false)`, node history, custom timestamps, path summary
enabled and hash NONE. Each arm had a fresh store and JVM. Eight matched pairs alternated order:
main/candidate for odd pairs, candidate/main for even pairs. Load and correctness JVMs used the
prescribed memory limiter and `taskset -c 0-11`; running affinity was verified as CPUs 0–11. Both arms
retained the original 6 GiB heap/offheap configuration, 1 GiB direct cap and diagnostics/promotion
settings. No campaign-owned tests ran concurrently, and no tuning occurred during this replay.

The scratch driver removed three unavailable prototype-only telemetry snapshot calls and dumped
production `StorageProfile` totals outside commit timing at 990,000 and 1,000,000 rows. Commit service
time retained the original driver's `commit_seconds` interval. Bytes per single commit are the delta
of writer-path persisted disk-page bytes across those 10,000 commits divided by 10,000, the historical
metric. Every arm checked all 617 requests against the preserved independent oracle before deleting
its store: **9,872/9,872 exact comparisons** across sixteen arms. All task stores and canonical input
scratch were deleted afterward; the original data and prototype were unchanged. The campaign ran
07:32–10:05 UTC on 2026-10-04.

| Measurement | Main median | Candidate median | Paired median ratio | 95% interval |
|---|---:|---:|---:|---:|
| Bytes / single commit | 191749.591 | 164279.2227 | 0.856738321 | 0.856738321–0.856738321 |
| Commit p50 (ms) | 27.34341675 | 27.2227325 | 0.994417978 | 0.974112592–1.002198341 |

Bytes fell **14.326167872%**, with paired change interval [−14.326167872%, −14.326167872%]. Every
main arm measured 191,749.591 B/commit and every candidate arm 164,279.2227 B/commit; the zero-width
interval reflects deterministic bytes on this fixed corpus. The paired commit-p50 change was
**−0.558202177%**, interval [−2.588740832%, +0.219834123%]. There was no confirmed latency regression;
the upper p50 bound is also below 1.05. Per-pair p50 results are:

| Pair | Order | Main p50 (ms) | Candidate p50 (ms) | Candidate/main ratio |
|---|---|---:|---:|---:|
| 1 | Main, candidate | 27.7571305 | 27.0417270 | 0.974226316 |
| 2 | Candidate, main | 27.2017920 | 27.3267990 | 1.004595543 |
| 3 | Main, candidate | 27.1910765 | 26.4871700 | 0.974112592 |
| 4 | Candidate, main | 26.9582910 | 26.9816840 | 1.000867748 |
| 5 | Main, candidate | 27.1344965 | 27.1889515 | 1.002006855 |
| 6 | Candidate, main | 27.4850415 | 27.5454630 | 1.002198341 |
| 7 | Main, candidate | 27.5884520 | 27.2565135 | 0.987968209 |
| 8 | Candidate, main | 28.7674155 | 27.7164105 | 0.963465435 |

This is the primary proof on the historical corpus and protocol, with a larger byte saving than the
accepted -10.5% target and p50 at the current-main level within the reported interval. It does not
claim to reproduce the prototype's absolute 35.53/35.95 ms timings. Main's rounded 191,750 B/commit
matches the historical pre-A old-design control. The current implementation folds after 64 live
tail blobs; the historical prototype used the full-row-group bound.

The replay has no persisted order exceptions from middle inserts and does not cover the reported
bitmap-growth defect. Its production artifacts also predate the repair of eager base-payload reads
on merge-memo hits. The captured measurements describe the artifacts identified below and do not
validate either subsequent correctness/read-path repair or measure the resulting production head.

## Supplementary generated-fixture single-record commits

The earlier generated fixture used a different corpus and ordinary row-group-major projection
without a sorted view. Its measurements supplement the historical replay above.

Eight scalar lanes (`kind`, `did`, `time`, `collection`, `operation`, `valid`, `origin`, `id`) are
projected from `/[]` at load start. Rows 1–990,000 are loaded in 99 batches of 10,000, followed by
10,000 single-row commits. SLIDING_SNAPSHOT versioning, revision window three, diffs disabled,
ordinary row-group-major projection maintenance, no sorted view. Deterministic values:
`kind=commit`; `did=did:plc:` plus hex of `(id*0x9e3779b9)&0x3ffff`; collection `collection-` plus
`id%16`; operation update for `id%5==0`, otherwise create; origin `source-` plus `id%4`; time
`1700000000000000+id*1000`; valid `1700000000000000+id*3`. After the timed phase, all eight lanes of
all one million rows are read back against these formulas; strings resolve through the persisted
local or global dictionary.

`StorageProfile` disk-byte totals immediately before and after the single phase give bytes per
commit. Commit latency surrounds only `wtx.commit()`. A final 10,000-row/1,000-single-commit
diagnostic pilot was exact across all eight columns on both arms and is excluded from the matched
samples. Its byte ratio was 0.89624; no latency conclusion comes from that pilot.

Eight alternating matched pairs passed all sixteen million-row, eight-column readbacks. Bytes fell
5.98%. The commit-p50 interval is entirely below the 1.05 regression margin. Disk-byte totals were
identical within each arm across the eight runs, so the byte-ratio interval is degenerate.

| Measurement | Main median | Candidate median | Paired median ratio | 95% interval |
|---|---:|---:|---:|---:|
| Bytes / single commit | 71141.459 | 66887.318 | 0.94020 | 0.94020–0.94020 |
| Commit p50 (ms) | 13.157 | 13.359 | 1.01689 | 1.01227–1.02727 |

Before bounding live tail references, a complete eight-pair measurement on the same generated
fixture gave main 71,141.459 and candidate 72,598.106 B/commit (+2.05%, paired-bootstrap interval
[1.02048,1.02048]). Commit p50 medians were 13.467 and 14.190 ms, paired median ratio 1.05367,
interval [1.02625,1.06998]. It passed the defined latency gate but contradicted a byte-saving claim.
Page attribution showed overflow bytes falling by 4,545 B/commit while HOT leaf and indirect bytes
grew by 6,002 B/commit. The final implementation folds on the next append after 64 live tail-blob
references, then reuses the tail namespace. The boundary, rollback and every historical revision are
checked on all four versioning types; removing that policy fails all four cases.

The SH1 kit does not explicitly create a projection index. Its timings gate whole-engine
regressions; the projected single-record fixture exercises open-row tails directly.

The generated-fixture final first-pair page attribution was:

| Page kind | Main B/commit | Candidate B/commit | Change |
|---|---:|---:|---:|
| HOT leaf | 10,076.250 | 10,270.654 | +194.404 |
| HOT indirect | 2,411.938 | 2,446.942 | +35.004 |
| Overflow | 7,135.439 | 2,651.889 | −4,483.550 |
| Total | 71,141.459 | 66,887.318 | −4,254.141 |

All reported intervals use candidate/main ratios paired by run, the median of those ratios, and 95%
paired-bootstrap intervals with 5,000 resamples (fixed seed 20261004). A latency cell is a confirmed
regression when its lower bound exceeds 1.05. The paired median ratio need not equal the ratio of
the two arm medians.

## Frozen artifact provenance

### Historical JSONBench replay

The supplied local evidence is retained under
`/home/johannes/.treehouse/sirix-cdde48/10/sirix/build/pr6/jsonbench/`:
`verification-summary.txt`, `paired-analysis.json` (full-precision metrics and all per-pair values),
`artifact-provenance.json`, per-arm facts, commands, logs and exact comparisons under
`pairs/01-main` through `pairs/08-candidate`, plus scripts and original/adapted driver sources.

Main is `cc042c494fdb648525bd833cdb14a5b7f82abd6e`. Candidate production sources are from
`32592ea097a52b4f71e935875cc4b8eea3689e92`, rebased by the pipeline to
`224a668935645be73d8b86c26c26d559e58f16cc`. The replay did not rebuild that pipeline commit: its
object was absent in the campaign copy. It combined the prior frozen candidate/main compiled arms
with the two changed current-main query classes in one shared overlay. The campaign checked all six
main projection sources byte-identical between `3fb92436d` and `cc042c494`; core had no intervening
main changes. The two query source hashes below also match this review worktree. The prior frozen
runtime entries, including the candidate core jar, are unchanged. This documents the artifact
assembly and source continuity, not a new compilation of the pipeline head. Any subsequent change
to measured production paths needs validation and measurement of the resulting artifacts.

SHA-256 identifiers from `artifact-provenance.json`:

| Artifact or input | SHA-256 |
|---|---|
| Frozen runtime/source manifest (`final/runtime-manifest.json`) | `968ae58eb8b3645a6c47b4538dbafb2b399f76038060b8763dde6d791d56150e` |
| Adapted driver source | `029f7b321e61d173fd744189a9a70896880b807f10def9253dd357940c6fabbb` |
| Driver classes | `45527f8d27bc21471a704b9801944d097043e8bdbf14693d5eccd64b7c439af2` |
| Shared current-main query classes | `a7bf611c7f81dda6ddb3b5d9ee72d485f9a13cb62e7722957eb2e4fc7b3049b9` |
| `SirixArraySize.java` source | `119cae31115a39625a932a7909217b61ab67667a8e091df70a4cb2b5fef47896` |
| `SirixVectorizedExecutor.java` source | `3ec98b368212a66745c7d643fbb51e4b1a173db378c4b288915b73714e06b56f` |
| Reconstructed `replay.tsv` (historical `source.json`) | `ad177298335065afe23beaef0ec5a4b5655f548d94052c94d491be5f10be6e9e` |
| Root-single `manifest.json` | `f61f3a8ad02d635ab72d19dc2057566188c26ab2b2fe5f073133c7bc69a6d766` |
| `suite.json` | `d579dc25b40ff963f797bd046ea2b5e68c060b6020738602efac87de04c7f8ac` |
| Independent `expected.json` | `dcf086176306d9c9dad2852925438d19aed5c3af81336297f6f1c01c3fdb521d` |
| `parameters.json` | `c44fa7aa3b5fd4c34b3734ec7939c1f950bc8dba5b9c4dd852919d3755e27590` |

### Earlier SH1 and generated-fixture campaigns

The earlier local campaign evidence is retained under `build/pr6/final/` (ignored) in the supplied
campaign checkout: `runtime-manifest.json`,
`input-manifest.json`, `proof-manifest.json`, `paired-analysis.json`, per-arm commands, logs and
exact results. The manifests record source and classpath hashes before measurement. The main overlay
contains all nested classes of the six changed existing projection classes. No live build output was
used by either arm. SHA-256 identifiers:

| Artifact | SHA-256 |
|---|---|
| Final core jar | `ed3f2108f5398a984ad95749327faee082d61727862a40fd9bacc31f6cc4e0cf` |
| Main class overlay | `104a25fb1517bbc0fda94e8b7e8ca122b7ba71cd40f104790bdb65f3e133a9d8` |
| Runtime/source manifest | `968ae58eb8b3645a6c47b4538dbafb2b399f76038060b8763dde6d791d56150e` |
| Eight-column proof class | `1dc6721c38b12e8edd70c70fca0c67d3cb8acf23e5bc6b62114495aa378e1ad2` |
| Java executable | `7c4c7cf8156d47c85de69d9136efe042fefee946234b04e1f39d551e1bf4cff7` |
