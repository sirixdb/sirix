# SH1 implementation evidence, 2026-09-20

## Scope

This is implementation and exactness evidence, not a timed head-to-head. No score, ranking or
performance winner is claimed. The completed pilot uses T25k because the T100k Sirix load exposes
a validator-detected generic HOT structural-splice defect on current `main`, commit
`858d0bb8a5a055db902a22e402c7eda9fcc264cd`. Sirix runs on JDK 25 and XTDB 2.1.0 on JDK 21.

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

## T100k status

Two T100k generator runs were byte-identical at 234,884 events (210,814 PUT and 24,070 DELETE),
SHA-256 `fe3f025b5e143e75a6ec63badef4c2d830c91d1a0067cb41ba824a6e61f42c39`. The independent
oracle completed all twelve answers; its manifest SHA-256 is
`98cdefc938d1043210b2f1676ed6cfad012a282ea18ffba83cbf1972d1283c0b`.

Sirix completed E0 and failed while applying E1. The HOT publication validator reported
`I8-children-sorted-by-firstkey`: child 7's first key
`018000018d000000008000018d2e77800100000000` was not greater than a preceding child's first key
`018000018d000000008000018d339ddc0100000000`. The unchanged log is
`/var/tmp/sirix-bitemporal/t100k/logs/sirix-load.log`, SHA-256
`7225bb1dcca18b92780bc32f00fd5ad97728dae83a020571fa6f850f373e759f`.

No index was disabled, no input or interval was changed and no alternative query was substituted.
T100k therefore does not yet have a three-way exactness result and must be rerun unchanged after
the separately routed generic HOT structural-splice repair. `t100k-status.json` records the
machine-readable finding together with the oracle's row counts and hashes.

## Resource facts

`resource-facts.json` records load wall time, closed-store logical/allocated bytes and observed
peak RSS. Sirix RSS measurements cover the outer Gradle invocation rather than an isolated engine
process; they are labeled accordingly. These are capacity and reproducibility facts only. They
were not gathered with the paired ten-round timing protocol and must not be used to claim a
performance winner.
