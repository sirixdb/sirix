# Supply History 1 bitemporal benchmark

Supply History 1 (SH1) is a deterministic, reduced supply-contract workload for exact
bitemporal queries. It is an adaptation of temporal operator categories and supply-domain
ideas; it is not TPC-BiH, TPC-H, or an audited TPC result.

This kit establishes correctness before measurement. It does not contain a score, ranked
rounds, a native image, PGO, or a claim that either engine is faster. A later campaign must use
the repository's ten-round campaign rig and may find that XTDB wins.

## Logical model

All valid-time intervals are half-open `[from,to)` at exact UTC midnights. Day zero is
`2024-01-01T00:00:00Z`; day 366 is `2025-01-01T00:00:00Z`. Amounts are integer cents and all
query arithmetic must remain exact signed 64-bit arithmetic.

| Relation | Identity and payload | Initial interval |
|---|---|---|
| `contracts` | `id,pid,sid,cost,qty,grade` | `[0,366)` |
| `products` | `id,category,retail` | `[0,366)` |
| `suppliers` | `id,region,tier` | `[0,366)` |
| `epochs` | `id,epoch,ts` | immutable publication schedule |
| `days` | `id,day_no,ts` | immutable 366-day calendar |

There are 25 atomic logical publications, E0 through E24, at `day(15 * epoch)`. A PUT replaces
an identity's payload only within its interval, preserving/splitting outside pieces. A DELETE
removes only its interval. Adjacent equal pieces may coalesce. Sirix stores the three business
relations as revisioned JSON arrays with explicit `vf`/`vt`; XTDB uses native system and valid
time. Neither adapter stores precomputed query answers or flattened system history.

Contract 1 is a public boundary fixture: E6 puts `[90,210)` at base cost +100, E12 puts
`[150,180)` at +200, E18 deletes `[160,170)`, and E20 puts `[164,168)` at +300. Random changes
are selected by the first eight bytes of SHA-256 over seed `20260920`; no runtime PRNG or map
iteration order affects the stream.

All five Sirix resources explicitly set `ResourceConfiguration.storeDiffs(false)`. The builder
default is `true`, which the first T100k attempt inherited; at commit time that default serializes
an update-diff sidecar and resolves each array position by walking its left siblings. The sidecars
are not read by any SH1 query, are not part of the revision or valid-time model being measured, and
have no corresponding XTDB work. They are therefore disabled as benchmark configuration, while
ordinary revision storage, custom commit timestamps, path summaries and VALIDTIME indexes remain
enabled. Any later correctness or timed result from this kit must explicitly disclose that
diff-sidecar storage is off.

## Tiers and capacity

| Tier | Contracts | Products | Suppliers | Events |
|---|---:|---:|---:|---:|
| `development` | 2,000 | 200 | 100 | 4,776 |
| `t25k` | 25,000 | 2,500 | 500 | 58,724 |
| `t100k` | 100,000 | 10,000 | 2,000 | 234,884 |

The development stream SHA-256 is
`bc0c819476a6ee4804ad246262ce223a04d044fcaf363b15e87d6bb542378fa2`; T25k is
`0d7a24032cf76152e1fbbb00f9972890a608c1a42470db3ab036b35d6f03c597`; T100k is
`fe3f025b5e143e75a6ec63badef4c2d830c91d1a0067cb41ba824a6e61f42c39`.

The original design used a 1.85-GiB combined planning cap. On 2026-09-20 the approved envelope
was raised, before target results were available, to 6 GiB per engine and 12 GiB for the complete
campaign, with a 20-GiB free-space floor. Both loaders enforce the applicable gates after every
publication. Input, databases, dependency cache, temporary files and outputs live only below
`/var/tmp/sirix-bitemporal` on ext4. Existing `/var/tmp/sirix-jsonbench-*` trees are never read or
modified by this kit.

T25k and the target T100k tier are complete: the independent oracle, Sirix and XTDB produced
byte-identical answers for all twelve queries at both scales. T100k uses the generic HOT repair at
commit `44fc2f4afe0194972a1899c315de1fe70b9a3da1` and the explicitly documented diff-sidecar
configuration above. The earlier validator-detected T100k failure and the intervening diagnostic
attempts remain in the evidence record; neither indexes, events nor query bodies were changed to
obtain the passing result.

## Queries and Sirix routes

The literal JSONiq bodies are in `BitemporalQueries.java`; the paired XTDB 2.1.0 SQL is in
`xtdb.clj`. Both adapters execute their `ORDER BY`, validate schema/integer/key order, materialize
raw results, and emit the same canonical TSV bytes.

| Q | Operator shape | Sirix serving route | strict end residual |
|---:|---|---|:---:|
| 1 | point belief at E24 | revision scan + half-open predicate | no |
| 2 | point belief at E12 | revision scan + half-open predicate | no |
| 3 | valid-range price extrema | revision scan + strict overlap | no |
| 4 | corrections, system-time self-join | VALIDTIME + generic join | yes |
| 5 | one entity's publication history | revision scan per publication | no |
| 6 | grouped publication evolution | VALIDTIME + generic group | yes |
| 7 | latest grouped exposure | VALIDTIME + generic group | yes |
| 8 | supplier/grade distribution | VALIDTIME + generic group | yes |
| 9 | supplier temporal join/group | VALIDTIME + generic join/group | yes |
| 10 | interval-overlap product join | revision scans + generic join/group | no |
| 11 | daily temporal aggregation | VALIDTIME + generic group | yes |
| 12 | disappearance anti-join/group | VALIDTIME + generic anti-join/group | yes |

`jn:open-bitemporal` currently treats the index's high endpoint as inclusive. Q4, Q6-Q9, Q11
and Q12 therefore use `local:slice`, which first uses the ordinary persisted VALIDTIME index and
then applies `valid < vt`. This strict residual was required to preserve SH1's half-open model.
Q1-Q3, Q5 and Q10 use explicit half-open or strict-overlap predicates. The Sirix runner refuses
to run when the persisted VALIDTIME definitions are absent and records the route and residual flag
in its manifest; these labels describe generic engine/query paths, not benchmark-specific code.

## Runtime prerequisites

- Sirix runs from this checkout on its configured JDK 25 toolchain.
- XTDB is exactly 2.1.0 on `/usr/lib/jvm/java-21-openjdk-amd64`.
- `pom.xml` resolves XTDB/Clojure dependencies into `/var/tmp/sirix-bitemporal/m2`; no jars are
  vendored in the repository.
- Python 3, Maven, GNU `time`, `cmp`, `du`, `findmnt`, Docker CLI and ext4 `/var/tmp` are required.

Prepare XTDB once:

```bash
bundles/sirix-query/bench/bitemporal/setup-xtdb.sh
```

## Reproduce an exactness run

The orchestrator accepts a tier and a fresh run name. It refuses to overwrite evidence:

```bash
bundles/sirix-query/bench/bitemporal/run-exactness.sh development dev-20260920
```

Equivalent explicit commands, shown for development, are:

```bash
ROOT=/var/tmp/sirix-bitemporal/dev-20260920
mkdir -p "$ROOT/input" "$ROOT/logs"

./gradlew --console=plain :sirix-query:bitemporalGenerate \
  "-Pbitemporal.args=development $ROOT/input"

python3 bundles/sirix-query/bench/bitemporal/oracle.py \
  --tier development --events "$ROOT/input/events.jsonl" \
  --out "$ROOT/oracle" --cross-check-intervals

./gradlew --console=plain :sirix-query:bitemporalSirixLoad \
  "-Pbitemporal.args=development $ROOT/input/events.jsonl $ROOT/sirix"

bundles/sirix-query/bench/bitemporal/run-xtdb.sh \
  load development "$ROOT/input/events.jsonl" "$ROOT/xtdb"

./gradlew --console=plain :sirix-query:bitemporalSirixRun \
  "-Pbitemporal.args=$ROOT/sirix $ROOT/sirix-results"

bundles/sirix-query/bench/bitemporal/run-xtdb.sh \
  run "$ROOT/xtdb" "$ROOT/xtdb-results"

python3 bundles/sirix-query/bench/bitemporal/compare-results.py \
  --oracle "$ROOT/oracle" --sirix "$ROOT/sirix-results" \
  --xtdb "$ROOT/xtdb-results" --out "$ROOT/exactness.json"
```

The query commands reopen closed stores. XTDB startup explicitly waits until all 25 committed
transactions are visible; this is essential because node construction may return before replay has
indexed the log tail. The XTDB adapter preserves a primary load or query exception if node close
also fails, attaching the close failure as suppressed instead of masking the actionable cause. The
comparator invokes `cmp` for every Q1-Q12 pair and reports the first differing row. SHA-256 is an
integrity aid, never the equality decision. Development additionally replays a second interval-list
oracle and compares every dense `(epoch,table,id,day)` cell.

Run the fast kit tests with:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest \
  bundles/sirix-query/bench/bitemporal/test_bitemporal_protocol.py
./gradlew :sirix-query:spotlessCheck :sirix-query:test \
  --tests io.sirix.query.bench.bitemporal.BitemporalCanonicalizerTest
```

## Measurement protocol boundary

This exactness runner is intentionally not the timed head-to-head. Before later scoring, freeze the
event hash, query hashes, engine revision/version, index manifest and capacity tier; then use the
three-try/ten-round paired rig, one heavy process at a time. Each timed try must use a new process,
include compile/prepare plus full canonicalization in its query timer, preserve raw and canonical
answers, verify cache eviction where claimed, and byte-match the oracle. Do not infer a performance
winner from the load facts or one untimed correctness execution in `evidence/`.

## Recorded evidence

`evidence/2026-09-20/` records the exact commands, three-way proofs for development, T25k and
T100k, route assertions, resource facts and diagnostic provenance. The original T25k E11
prefix-rebuild failure and the original T100k E1 malformed structural splice are retained; generic
HOT repairs cleared both. A later T100k load inherited Sirix's default update-diff sidecars and was
stopped after diagnosing their quadratic sibling walk; the final, disclosed configuration disables
those unused sidecars for all five resources. The first XTDB T100k load then failed after E19, but
its primary exception was masked by a close-time Arrow leak error. No external kill or OOM evidence
was found, so its cause remains unexplained; a fresh-store retry completed and supplied the passing
answers. None of these single executions is a timed head-to-head or supports a performance winner.
