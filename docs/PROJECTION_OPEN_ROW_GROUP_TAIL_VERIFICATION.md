# Open projection row-group tails: verification

Measured on 2026-10-04 on the i7-12700H. Main baseline: `3fb92436d`. The only production differences
are the open-row-group tail, merged reader routing, and the bounded writer-seeded merge memo. The
existing six changed classes and their nested classes were exported from the baseline commit and
prepended to the common runtime for the main arm. The candidate runtime, main overlay and
dependencies were copied and hashed before measurement. No benchmark harness or permanent diagnostic counters are
landed.

Every build and benchmark process used the prescribed memory limiter (at least 6 GiB available, two
shared flock slots). Benchmark JVMs were pinned to CPUs 0–11. Both arms use the same Brackit
snapshot, SHA-256 `4742c41b9408db48742255317b62a6c9cd64f8a9c7f779d5edfd2c4dea144366`. Gradle
resolved it with a fresh private `-Dmaven.repo.local` directory, leaving `~/.m2` untouched. Java
25.0.3; SH1 heap 2 GiB, commit-proof heap 6 GiB, direct cap 1 GiB.

## Correctness and work

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

## Single-record commit proof

The historical prototype result was 210,207 to 188,221 bytes per single commit (-10.5%), with commit
p50 35.53 to 35.95 ms after the memo was seeded. Its million-row Bluesky replay is unavailable: the
prototype replay symlink's target is missing. The measurements below use a fresh generated fixture
and do not reproduce that corpus.

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

The final first-pair page attribution was:

| Page kind | Main B/commit | Candidate B/commit | Change |
|---|---:|---:|---:|
| HOT leaf | 10,076.250 | 10,270.654 | +194.404 |
| HOT indirect | 2,411.938 | 2,446.942 | +35.004 |
| Overflow | 7,135.439 | 2,651.889 | −4,483.550 |
| Total | 71,141.459 | 66,887.318 | −4,254.141 |

All reported intervals use candidate/main ratios paired by run, the median of those ratios, and 95%
paired-bootstrap intervals with 5,000 resamples (fixed seed 20261004). A latency cell is a confirmed
regression when its lower bound exceeds 1.05.

## Frozen artifact provenance

The local campaign evidence is retained under `build/pr6/final/` (ignored): `runtime-manifest.json`,
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
