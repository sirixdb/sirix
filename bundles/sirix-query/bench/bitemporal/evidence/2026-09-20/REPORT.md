# SH1 implementation evidence, 2026-09-20 to 2026-09-21

## Scope

This is implementation and exactness evidence, not a timed head-to-head. No score, ranking or
performance winner is claimed. The target T100k pilot completed after two generic HOT repairs; the
later repair used by this run is commit `44fc2f4afe0194972a1899c315de1fe70b9a3da1`. Sirix runs on
JDK 25 and XTDB 2.1.0 on JDK 21.

The initial 1.85-GiB combined planning cap was superseded before target results by an approved
6-GiB cap per engine, 12-GiB total campaign cap and 20-GiB free-space floor. The adapters enforce
those gates after every publication. All generated input, stores, dependency caches and outputs
remain below `/var/tmp/sirix-bitemporal` on ext4.

## Development result

Two independent generator runs were byte-identical at 4,776 events with SHA-256
`bc0c819476a6ee4804ad246262ce223a04d044fcaf363b15e87d6bb542378fa2`.

Both stores were loaded, closed, reopened and queried. The dense-day oracle also replayed an
independent interval-list implementation and compared every development cell after each epoch.
For every Q1-Q12 result, literal `cmp` proved oracle = Sirix = XTDB. Row counts and hashes are in
`development-exactness.json`.

## T25k pilot result

The deterministic T25k stream contains 58,724 events (52,816 PUT and 5,908 DELETE), SHA-256
`0d7a24032cf76152e1fbbb00f9972890a608c1a42470db3ab036b35d6f03c597`.

The independent oracle, Sirix and XTDB all produced canonical answers for Q1-Q12. The comparator
invoked literal `cmp` for every oracle/Sirix and oracle/XTDB pair; all 24 comparisons passed. The
per-query row counts and hashes are in `t25k-exactness.json`; the complete comparator manifest in
`/var/tmp` has SHA-256
`9ea494127115cd6d7859b7579e4f1bdbd4c12194a0839272eab217b724d56cae`.

The Sirix query manifest asserted the persisted VALIDTIME definitions before execution and
recorded every route in `t25k-sirix-routes.json`. Q4, Q6-Q9, Q11 and Q12 required the strict
`valid < vt` residual because `jn:open-bitemporal` currently returns the valid-end boundary
inclusively while SH1 intervals are half-open. Q1-Q3, Q5 and Q10 used explicit point/range
predicates and did not require that residual.

The original pre-repair T25k attempt failed at E11 in `HOTLeafPage.rebuildForShorterPrefix`; its
unchanged log is retained at `/var/tmp/sirix-bitemporal/t25k/logs/sirix-load.log`, SHA-256
`67c6550ec0b3507a0d4a208a88a14bcdca5afb9021889801f5218d5146b41e1d`. The generic prefix repair
now on `main` cleared that failure without changing the event stream, indexes or queries, and the
rerun completed E24.

XTDB logged cancellation of an in-flight compaction job during normal node shutdown. The reopened
node nevertheless replayed and indexed all 25 committed transactions before querying, and all
answers matched. The retained store must not be described as an already compacted cold store in a
later measurement campaign.

## T100k pilot result

Two T100k generator runs were byte-identical at 234,884 events (210,814 PUT and 24,070 DELETE),
SHA-256 `fe3f025b5e143e75a6ec63badef4c2d830c91d1a0067cb41ba824a6e61f42c39`. The independent
oracle completed all twelve answers; its manifest SHA-256 is
`98cdefc938d1043210b2f1676ed6cfad012a282ea18ffba83cbf1972d1283c0b`.

The independent oracle, Sirix and XTDB produced canonical answers for Q1-Q12. The comparator used
literal `cmp` for every oracle/Sirix and oracle/XTDB pair; all 24 comparisons passed. The resulting
manifest has SHA-256 `30287636a849ff18b95a5f5d5b17d2b2912c95049e839d56e016b292c5e8477c`.
Per-query row counts and hashes are in `t100k-exactness.json` and `t100k-status.json`.

The Sirix query manifest again asserted the persisted VALIDTIME definitions and recorded every
route in `t100k-sirix-routes.json`. Q4, Q6-Q9, Q11 and Q12 required the strict `valid < vt`
residual because the index route exposes its high endpoint inclusively while SH1 intervals are
half-open. Q1-Q3, Q5 and Q10 used explicit half-open or strict-overlap predicates and did not need
that residual.

All five Sirix resources explicitly used `ResourceConfiguration.storeDiffs(false)`. An earlier
T100k attempt inherited the builder default of `true`; the optional update-diff serializer resolved
each array position by walking its left siblings and made publication work quadratic. The sidecars
are not queried by SH1 and have no XTDB counterpart. Ordinary revisions, custom commit timestamps,
path summaries and persisted VALIDTIME indexes remained enabled. This configuration fact must stay
attached to any result derived from this store. The stopped attempt's unchanged log is
`/var/tmp/sirix-bitemporal/t100k-hot-44fc2f4afe01/logs/sirix-load.log`, SHA-256
`4e3b8d3894fec07eb4315bb1396eb00a02ed0490c137e7b3aafcd36212e881a5`; E20 was its last completed
publication.

The first XTDB T100k load attempt reached E19, then failed. Its original `with-open` lifecycle
reported only a close-time Arrow allocator leak of 11,424 bytes, masking the primary exception.
GNU `time` recorded exit status 1, zero delivered signals and peak RSS 1,520,192 KiB under the 2-GiB
heap cap; no heap/OOM marker or kernel OOM/kill record was found. The unchanged log is
`/var/tmp/sirix-bitemporal/t100k-hot-44fc2f4afe01-nodiffs/logs/xtdb-load.log`, SHA-256
`696a79c63181b48366866a0d74c8fddb7a47a4d6aa22ef5d4e7a3cfe5d5a214c`. The cause is therefore
unexplained: the evidence cannot distinguish an XTDB fault, an adapter fault or leftover store
state. The adapter now preserves a primary exception and attaches any close failure as suppressed.
A single authorized retry used a fresh store, completed E0-E24, reopened, and answered Q1-Q12.

For provenance, the earlier pre-repair Sirix E1 validator failure remains at
`/var/tmp/sirix-bitemporal/t100k/logs/sirix-load.log`, SHA-256
`7225bb1dcca18b92780bc32f00fd5ad97728dae83a020571fa6f850f373e759f`. The generic HOT repair
cleared it without changing the event stream, indexes or queries.

## Resource facts

`resource-facts.json` records load wall time, closed-store logical/allocated bytes and observed
memory facts. At T100k, Sirix load completed in 66.81 seconds outer wall time (62.846 seconds
reported by the loader), with 358,405,091 logical and 358,850,560 allocated bytes. The successful
XTDB fresh-store load completed in 1,052.79 seconds outer wall time (1,047.559 seconds reported by
the loader), with 138,731,164 logical and 143,749,120 allocated bytes at loader close. XTDB's direct
process peak RSS was 1,514,324 KiB. Sirix's GNU `time` RSS covers the outer Gradle invocation, not
the forked engine process; a 5,696,500-KiB live child observation is recorded separately and is not
called a peak. These are capacity and reproducibility facts from shared-machine correctness runs.
They were not gathered with the paired ten-round protocol and must not be used to claim a
performance winner.
