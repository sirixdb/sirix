# Repeated dense-group lookup: validated progress

`NumericGroupAggTable` remembers its previous dense handle and probe key. Repeating the
key can skip the hash-index lookup, but still compares every exact identity lane. Handles
survive index/storage growth; the code resolves the current storage chunk and invalidates
the remembered handle on release. This is a table-local aggregation mechanism with no
prepass, global cache, persisted-format change, or benchmark-specific answer handling.

The optimized native image improves both direct before/after 100M hot comparisons.
Sirix remains **9.23–9.29 times slower than the paired ClickHouse engine**. Rank one has
not been achieved, and the campaign remains open.

| Round | Hot after / before | Hot improvement | Cold after / before | Sirix / ClickHouse hot | Sirix / ClickHouse cold |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.96407 | 3.59% | 0.96450 | 9.29385 | 10.10237 |
| 2 | 0.97525 | 2.48% | 0.93996 | 9.22944 | 9.70927 |

Each ratio is `geomean((numerator_query_seconds + 0.010) / (denominator_query_seconds + 0.010))`.
Hot uses the minimum of attempts two and three; cold uses attempt one. The optimization
comparison is a direct, same-database AB/BA experiment against the preceding serial-directory
binary. A separate Sirix/ClickHouse paired experiment measures the remaining gap. Historical
normalized scores in the audit are explicitly unpaired context, not optimization proof.

## Workload and reproducibility

[ClickHouse/JSONBench](https://github.com/ClickHouse/JSONBench/tree/e6c7c98dc766394d51f7d506a3dd2b5d51165d70)
remained at `e6c7c98dc766394d51f7d506a3dd2b5d51165d70` on the
[2026-09-12 recheck](group-repeat-upstream-recheck.json). Its canonical 100M Bluesky tier exists;
the dashboard default is actually **1B**, hot, retains-structure=yes. At 100M the hot leader is
ClickHouse 25.11, with score 1.0229245566260303; the cold leader is StarRocks 4.0.1.
[upstream-pin.json](upstream-pin.json) retains the result-schema rankings, exact SQL, and
upstream score formula `geomean((engine_seconds + 0.010) / (fastest_seconds + 0.010))`.
The captain authorized local **ClickHouse 26.7.3.19** for the informal leader-engine comparison.

The five [canonical queries](../../queries.sql) count collections, count creations and exact
distinct users, group hourly activity for three collections, select the earliest three post
users, and select the three longest post spans. Both engines reused the same verified imports
of **99,999,968 cleaned rows** (32 rejected source lines), logical SHA-256
`f063d7db1d71009122ad4e99feb956423f52df3aa3f1d090a602cd44e121e538`.
No new database, migration, index rebuild, or dataset download was performed for this change.

The unchanged `jsonbench-isolated-v1` protocol keeps catalog construction inside query latency.
Setup, import, PGO training, correctness, and diagnostics are excluded from ranked timing.
Each attempt starts a fresh process; targeted file eviction followed by mincore verification
establishes cold OS cache, and later attempts retain warm OS cache. Both experiments run two
per-query AB/BA rounds, three attempts per engine: **60 timings and 20 verified cold blocks
per experiment**. Exact differential matches precede timing for all five queries in every arm.
Route counters verify 60 Sirix attempts in the before/after experiment and 30 in the leader pair;
native LZ77 decoding is also confirmed. Heap limits are 14 GiB maximum / 12 GiB initial,
off-heap is 8 GiB, projection promotion is disabled, and native queries use the available 20 CPUs.

Every load-bearing phase uses the fail-closed continuous guard: balanced profile,
`balance_power` EPP, temperature below 90 C, stable throttle counters, valid telemetry,
at most two seconds between samples, and an isolated 24 GiB/no-swap process scope.
The before/after pair recorded 567 clean samples, maximum 72 C and 0.743-second gap;
the leader pair recorded 498 clean samples, maximum 69 C and 0.787-second gap.
The existing 4 GiB additional-build allowance imposed a stronger 66 GiB disk floor for this lane.
The separate column-major v5 work and its unresolved space decision remain preserved.

## Validation and artifacts

- [Frozen source](group-repeat-v1-source.json): base `81cdec2d544f6829c5e9c3cd9fe56fe76b9984d8`
  plus exactly the production class and regression test. Uncommitted column-major work was excluded.
  Clean builds used no pooled outputs. [Toolchain continuity](group-repeat-toolchain-continuity.json)
  proves the same JDK and 40 dependency jars as the baseline, with only the two project jars rebuilt.
- [70 focused tests](tests.json), 11 suites, zero failures/errors/skips. Three new tests cover
  every identity lane, offset identities, hash collisions, zero-probe substitution, stable handles
  through index and storage growth, first-seen/count/aux state, and release invalidation.
- [Instrumented 1M gate](group-repeat-v1-instrument-1m-validation.json): all five exact in both arms,
  60 timings, 70 retained profiles. Hot after/before ratios 0.98591 / 1.06537 were mixed;
  no small-data speedup was claimed.
- Fresh 100M PGO training passed all five exact comparisons with expected aggregate routes.
  Profile SHA-256: `bc4ab1d4725fc0b894c90cca064c4ed74125177d5ac39da793aa667cdc900f12`.
  Optimized binary SHA-256: `d30dc2cf56dd0caebc7c11a11e8929be2944042f5e9675365d654431b23f9afb`.
  Before binary SHA-256: `50ccd4f1938c0d87a16d5a6a6ebca257175e4b9cbcbec34fe3d9f5f6f0cbfe6c`.
- [Direct before/after audit](group-repeat-v1-before-after-100m-validation.json) and
  [Sirix/ClickHouse audit](paired-group-repeat-100m-attempt1-validation.json) validate complete
  attempt sets, exact answers, routes, runtime hashes, cache classification, scores, and guards.

`raw-attempts.tar.gz` retains raw commands, answers, attempts, telemetry, logs, scripts,
test XML, and source/runtime manifests. `raw-files.json` hashes every member; the
packager rereads the archive and verifies each hash. `retained-large-files.json` lists binaries,
profiles, and source archives retained on the laptop with their sizes and checksums. Reproduction
uses the archived `group-repeat-lane.sh`, `advance-group-repeat-100m.py`, and shared protocol helpers,
with fresh output names and the recorded dataset/toolchain manifests. The
[preceding baseline](../20260912-serial-directory-100m/README.md) retains its own build and attempts.
