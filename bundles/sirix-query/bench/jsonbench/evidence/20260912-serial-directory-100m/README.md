# Serial projection directory traversal: validated progress, not rank one

The default directory walk now uses the existing serial HOT cursor. Parallel traversal remains
available with `-Dsirix.projection.parallelWalk=true`. Workers' speculative capture, transaction
leases, and coordinator replay cost more than their parallel decode saved on this laptop.
No persisted format, query answers, index contents, cache boundary, or query timer changed.

The optimized native result improves both paired 100M hot scores, but Sirix remains **9.58–10.06x
slower than ClickHouse**. This does not complete the campaign.

| Round | Prior native LZ77 hot score | Serial default hot score | Relative improvement | Serial default cold score |
| --- | ---: | ---: | ---: | ---: |
| 1 | 10.8187 | 10.0582 | 7.03% | 10.5781 |
| 2 | 10.6696 | 9.5836 | 10.18% | 9.9947 |

Scores are `geomean((Sirix query seconds + 0.010)/(ClickHouse query seconds + 0.010))`;
lower is better, and less than one would beat the paired engine. Hot selects the minimum of
attempts two and three; cold uses attempt one. Summed latency is not the ranking definition.
The before and after builds each have their own paired ClickHouse observations; the reported
improvement compares those normalized paired scores, rather than treating separate Sirix runs
as a simultaneous direct A/B experiment.

## Workload and protocol

[ClickHouse/JSONBench](https://github.com/ClickHouse/JSONBench/tree/e6c7c98dc766394d51f7d506a3dd2b5d51165d70)
was rechecked at revision `e6c7c98dc766394d51f7d506a3dd2b5d51165d70`; it remains unchanged.
The upstream 100M Bluesky tier exists, although its dashboard default is **1B**, hot,
retains-structure=yes. Its 100M hot leader is ClickHouse 25.11; its cold leader is StarRocks 4.0.1.
The captain authorized local ClickHouse **26.7.3.19** for this informal leader-engine comparison.

Both engines used the same cleaned corpus: **99,999,968 accepted rows**, 32 rejected input lines,
logical SHA-256 `f063d7db1d71009122ad4e99feb956423f52df3aa3f1d090a602cd44e121e538`.
The five [canonical queries](../../queries.sql) cover collection counts, creation counts and exact
user cardinalities, hourly activity for three collections, earliest post users, and longest post spans.
This is JSONBench, not the separate ClickBench/web-hits workload.

The unchanged `jsonbench-isolated-v1` rig ran all five exact differentials before ranked attempts,
then two per-query AB/BA rounds with three attempts: **60 timings, 30 Sirix route checks, 20 verified
cold blocks**. Each attempt starts a fresh process. Targeted file eviction is verified with mincore;
subsequent attempts have warm OS cache. Catalog construction stays inside the query timer.
Import, setup, PGO, correctness, and diagnostic timings remain outside the ranking.
The existing Sirix and ClickHouse imports were reused read-only; no dataset download or reimport
was needed. JVM/build workers are bounded separately; native queries use the available 20 CPUs,
with 14 GiB maximum heap, 12 GiB initial heap, and 8 GiB off-heap as in the baseline.

Every load-bearing phase ran under the fail-closed continuous guard: balanced profile,
`balance_power` EPP, temperature below 90 C, unchanged throttle counters, valid telemetry,
maximum 2-second sampling gap, and a dedicated 24 GiB/no-swap process scope. The paired run
recorded **489 samples, no failures, 71 C maximum, 0.747 seconds maximum sampling gap**.
No unrelated process or shared service was signalled.

## Validation and retained attempts

- Frozen source v2. No pooled build output was used.
- **23 tests across six suites**, zero failures/errors/skips.
  Coverage includes actual parallel engagement, default zero-worker traversal, writer isolation,
  side-reference versioning, corruption, frame ownership, and the five query shapes.
- V1 failed two tests because the newly explicit test entry point incorrectly requested an order
  header absent from its fixture. V2 supplies that fixture's identity order. Both source snapshots,
  exact test reports, and the original helper scripts are retained; no failed attempt was overwritten.
- Instrumented 1M: all five answers exact in each arm, native decoder confirmed in both,
  60 timings, and 70 separate profiles excluded from the final build. Hot after/before ratios
  were **0.80125 / 0.76748**.
- Fresh 100M PGO: all five exact, two tries, expected aggregate counters; 248 clean guard samples.
  Profile SHA-256 `4204abf57f1a94e65c777a9499c8cb2a42c62db0c54e483a58ee498c397625c5`.
- Optimized binary SHA-256 `50ccd4f1938c0d87a16d5a6a6ebca257175e4b9cbcbec34fe3d9f5f6f0cbfe6c`.
- Post-build Q1/Q4 diagnostics were exact and confirmed native decoding with no parallel directory
  engagement. Directory phases were **2048.5 / 1976.9 ms**. These diagnostics are unranked.

The preceding directory-batch candidate is **rejected**: despite its component-level gain,
its fresh optimized paired hot scores were **11.1845 / 10.7557**, worsening the prior lane.
Its source, focused tests, full PGO path, 1M/100M attempts and decision
are preserved. Selected-locator probes also remain explicitly unranked component measurements:
skipping unused captured values reduced captured bytes, but still visited every mixed HOT leaf.
The [architecture explanation](indexdef-trie-directory-explanation.md) records the IndexDef,
trie, revision ownership and possible write-maintained directory boundaries. It does not claim
that a new persisted directory has been implemented.

`raw-attempts.tar.gz` contains small raw outputs, guard telemetry, exact comparison files, scripts,
source overrides, and manifests. Packaging hashed every archive member and verified
those hashes by reading the completed archive back. Binary/profile/source-archive artifacts
are retained on the laptop. The prior baseline
and its build provenance are also documented in the [native LZ77 evidence](../20260911-native-lz77-100m/README.md).

The retained implementation is a generic traversal policy improvement. The next experiment must
address the remaining directory/serving cost without moving work outside the timer, introducing
benchmark-answer caches, or adding global precomputation.
