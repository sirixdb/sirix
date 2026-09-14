# Constant-bucket dictionary counting, 100M rows

The candidate measured **2.395× and 2.341× behind ClickHouse** on the five-query native
JSONBench hot geometric score. All Q1–Q5 results matched exactly. The earlier v55 rounds measured
2.506× and 2.670×, but ran at a different time: the score change is an observation, not an isolated
measurement of the code change. ClickHouse parity remains unachieved. Changes are uncommitted.

## Implementation

`ProjectionColumnGroupScan.aggregateByGroupCompositeFlat` now specializes count-only grouping
with one local dictionary string key and otherwise numeric keys. Verified column min/max values
prove whether each numeric key becomes constant after its offset and positive division. Numeric
presence is checked for every selected row. Equal modulus residues alone do not establish a
constant bucket, and inconclusive blocks use the existing row loop.

Eligible blocks count primitive dictionary IDs in reusable arrays, then prove exact string identity
and probe the group table once per used ID. This removes per-row hashing, division, and table probes.
Missing strings retain their separate identity unless the query requests a literal substitution.
When missing and stored empty strings merge, the earliest selected document ordinal wins, regardless
of dictionary order. Overflow, masks, discarded partitions, and collision checks retain their
existing semantics. No database format or write path changed; revisioning and fine-grained writes
remain available.

## Measurements

The isolated JVM test prepended either the baseline or candidate group-scan classes to the same
frozen runtime. It used baseline/candidate/candidate/baseline order, 12 Q3 tries per process,
20 processors, a 12–14 GiB heap, and 8 GiB off-heap budget on the retained 99,999,968-row database.
Every result matched the retained ClickHouse answer. The mean of each run's last-six median fell
from **478.75 ms to 300.75 ms**, a **37.18% time reduction**. This warmed-JVM result is separate
from the fresh-process native score.

Native v56 contains this change plus the earlier
[sorted-directory prefix jump](../20260914-directory-prefix-jump-100m/README.md). All 2,053 base
class hashes were verified against v55; compiler flags, configuration, and five PGO profiles were
retained. The binary SHA-256 is
`f6e84d439d83a5c58fb32593ebc867b0bb625c2c1dcbe300561cf16e2562f8c5`.

The native comparison alternated engine order across two rounds. Every query had one verified
cold-file attempt and one warm-OS-cache attempt, each in a fresh process. The score is the geometric
mean of `(Sirix seconds + .010) / (ClickHouse seconds + .010)`. Correctness ran separately.
The 24 GiB, zero-swap scope recorded no OOM or memory-limit events, and the thermal guard passed.

| Query | Sirix hot seconds, rounds 1 / 2 | ClickHouse hot seconds, rounds 1 / 2 |
| --- | --- | --- |
| Q1 | .011 / .011 | .078 / .088 |
| Q2 | 2.896 / 2.913 | 1.785 / 1.702 |
| Q3 | 3.047 / 3.164 | .557 / .594 |
| Q4 | 1.888 / 1.895 | .304 / .299 |
| Q5 | 1.974 / 1.985 | .307 / .326 |

Separate native diagnostics measured Q3 aggregation at 371 ms before and 65 ms after. The candidate
spent 1,242.5 ms reconstructing the directory and 535.5 + 575.9 ms loading two columns. Four young-GC
pauses totaled 279.788 ms; peak RSS was 4,304,728 KiB. These single, logged samples identify remaining
costs; phases can include GC pauses and must not be added as independent costs or treated as ranked
speedup estimates.

## Regression checks and rejected tuning

The targeted core/query suites passed 95 tests, followed by an additional missing-value case.
Coverage includes both dictionary-key positions, dense and ordinary tables, table growth, filtered
and partial words, multiple dictionaries, empty dictionaries, missing numeric blocks, exact first-row
references, signed transforms, overflow, collisions, and partition discard behavior.

All four 200K-row synthetic ClickBench runs passed 43 exact interpreter comparisons. Against DuckDB,
each gave 33 exact matches and 10 strongly verified legal tie windows, with no missing result. The
four-try full-suite screen was essentially flat in summed hot medians (+0.80%). A forty-try follow-up
on Q4/Q10/Q11/Q12/Q14/Q16/Q17 did not reproduce the apparent Q11 slowdown: its last-twenty medians
were 12/12 ms for the baseline and 12/11.5 ms for the candidate. This small synthetic screen does
not establish performance on the full real ClickBench corpus.

A native ABBA screen of the existing `sirix.filechannel.coalesceGapBytes` setting compared 256 KiB
with 4 KiB. Smaller gaps improved cold Q2/Q3 but regressed warm Q2 by about 4%; the default remains
256 KiB. All 16 results matched exactly and thermal monitoring passed. Coalesced-span diagnostic
bytes exclude individual reads, so their reduction must not be presented as total I/O savings.

## Evidence and reproduction

[summary.json](summary.json) retains source/build hashes, all timing summaries, protocol, scope
limits/events, and the rejected I/O screen. Complete commands, dumps, diagnostics, accepted source
snapshots, and test logs are retained locally under
`build/jsonbench-campaign/source-flag-summary-v47/constant-bucket-count-v1/`.
The native engine comparison is under the adjacent
`paired-constant-bucket-v56-100m-attempt1/`; build inputs are in
`build/jsonbench-campaign/pgo-constant-bucket-v56-100m/build-outcome.json`.
These large raw artifacts are ignored by Git. Per-run `command.json` files preserve the frozen
classpath and runtime flags; the scope/thermal verdict preserves the native comparison command.
