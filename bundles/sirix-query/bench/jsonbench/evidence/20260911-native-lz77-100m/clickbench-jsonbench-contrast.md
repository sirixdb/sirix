# Why the earlier ClickBench result looks better

The strongest directly verified difference is the serving/timing boundary. The historical
ClickBench run opened its projection catalog **before** starting query timers, then reused one
process and resource session across 43 queries and three tries. Its fresh executor per try did not
clear the shared catalog, page, column, or OS caches. This campaign's JSONBench protocol starts the
timer before catalog warm and starts a fresh process for every attempt, including hot attempts
(warm OS cache). After the latest fix, catalog lookup alone still costs about 3.2 seconds; its
directory traversal costs about 2.9 seconds. Moving that work outside the timer would change the
protocol, not improve the measured engine mechanism.

The historical command was **JVM/C2**, using Graal JDK 25.0.3 with `-XX:-UseJVMCICompiler`, 14 GiB
heap, 10 GiB off-heap and a 5 GiB eager-materialization budget. It was not a native executable.
Therefore the native-image foreign-memory specialization and missing embedded decoder resource
cannot be claimed as causes of that historical JVM result. Native decoder activation was not
established by the historical score report; a library's presence in a JAR would not prove use.

Evidence: historical commit `465f208946549e8a264598e986585942e1c8868c`,
`ClickBenchRunMain.java:219` (open-time catalog), `:447` (per-try executor), `:458` (timer);
retained command `bundles/sirix-query/build/diagnostics/score4-leg-20260909T105540/leg/suite/command.json`.
Current `JsonBenchRunMain.java:309` starts timing before `warmCatalog`.

| Evidence | Prior ClickBench web-hits | Current JSONBench Bluesky |
|---|---|---|
| Workload | 43 queries over flat web-hit fields | Five queries over kind, operation, collection, DID and time |
| Actual fast serving | Projection routes, including aggregates and sorted scans; prior validated SEG4T recorded no NONE/declines and zero row materializations | All five use aggregate + sliced routes; Q4/Q5 also numeric + dense; exact counters retained |
| Storage used by queries | Imported projection columns; shared cached catalog/columns across tries | Imported covering projection in HOT; 97,657 row groups, up to 1,024 rows each; query-selected column decoding |
| Strong score evidence | SEG7T 100M hot GM 3.939639, rank 15/140 against the C6A board; one unpaired leg | Two paired AB/BA rounds against local ClickHouse 26.7.3.19; current hot ratios 10.818661 and 10.669560 |
| Meaning of score | 43-query cross-machine leaderboard comparison; report marks per-query changes unresolved | Five-query paired ratio using the same 10 ms offset; 5/5 exact answers before accepting timing |

The old score was useful progress, not proof of beating the same-laptop leader. Both use geometric
means with a 10 ms offset, but their query sets, comparator populations, runtime/cache policy and
machine envelopes differ. Ratios and ranks cannot be compared directly.

Nested JSON affects import and missing-field representation, but it does not explain this timing
gap by itself: the current fast route reads typed projection columns, not arbitrary nested JSON
objects per row. Disconfirming evidence also exists for an always-cheap ClickBench scan: its older
Q25 profile visited 96,459 leaves and evaluated 13,172,392 candidates. Prior optimization covered
group acquisition, dense accumulators, merge/spill, dictionary identity and indexed-source reuse;
many of those generic mechanisms also serve these JSONBench queries. The main-line 1M Q18/Q32
control used aggregate/sliced routes and still showed HOT directory and decode costs during startup.

Historical reports (read as evidence, never reused as build output):
`/home/johannes/IdeaProjects/firstmate/data/sirix-cb-score-4/report.md`,
`sirix-cb-measure-1/report.md`, and `sirix-cb-q18-q32-profile-1/report.md` under the same data root.

# What ClickHouse actually builds here

The paired binary is 26.7.3.19, SHA-256
`d2a2611ed56bc6d1544fafe8ece7e888ae2af6e13ea7095993a5105d8c74d4f3`.
Guarded, read-only introspection of the existing 99,999,968-row table is retained in
`inspect-ch-indexes-attempt1/`: SHOW CREATE, system tables, defaults and all five EXPLAIN plans.
The guard passed (21 samples); no schema change or reimport occurred.

| Mechanism | Observed configuration and effect |
|---|---|
| Physical sorting and sparse primary index | MergeTree ORDER BY `(kind, operation, collection, did, timestamp)`; actual primary key equals sorting key. Default and persisted index granularity 8,192 rows; adaptive threshold 10,485,760 bytes. |
| Secondary skipping indexes | None in `system.data_skipping_indices`. The primary index does prune granules; that is a different mechanism. |
| JSON column storage | Five declared typed paths remain subcolumns even with `max_dynamic_paths=0`; remaining paths use shared JSON storage. Actual serialization is object v3, dynamic v3 and advanced shared data. Typed fields avoid reparsing the complete JSON object. |
| Dictionary encoding | kind, operation and collection are LowCardinality(String); DID is String; time_us is UInt64. These local column dictionaries are not external dictionaries. |
| Other materialization | No ClickHouse projections or external dictionaries in system tables. No materialized view or aggregate column appears in the table DDL. Column codec is ZSTD(1); the table retains the JSON structure. |

The [MergeTree documentation](https://clickhouse.com/docs/reference/engines/table-engines/mergetree-family/mergetree)
explains how sorting keys and sparse marks select granules. The
[JSON type documentation](https://clickhouse.com/docs/reference/data-types/newjson) distinguishes
typed subcolumns from shared dynamic-path storage, and
[LowCardinality documentation](https://clickhouse.com/docs/reference/data-types/lowcardinality)
describes dictionary encoding. Defaults above were read from the actual paired binary, not inferred
from current documentation.

Actual primary-index plan selections are Q1 **12,216/12,216**, Q2 **11,665/12,216**,
Q3 **7,146/12,216**, Q4/Q5 **1,040/12,216** granules, across 18 active parts. These are plan-level
granule selections, not measured row counts. In particular Q1 has no primary-index pruning and
still substantially outperforms Sirix. Active-part bytes reported now are 19,658,021,120; this is
not the earlier whole-directory allocated storage measurement of 29,010,276,352 bytes.

Sirix already builds a five-path covering projection during streaming import, with HOT lookup,
column encoding/dictionaries, row-group descriptors, presence information, fences and Bloom data.
See `JsonBenchProjection.java`, `ProjectionIndexBuilder`, `ProjectionIndexHOTStorage` and
`ProjectionIndexCatalog`; importing the original JSON remains separate from ranked query latency.
Generic import-built indexes are a fair prospective option with setup/storage disclosed and query
answers computed during the query. There is no measured justification yet for a new value index
as the first intervention: simply discovering the existing directory costs about 2.9 seconds on
both Q1 and Q4. A persisted directory locator could avoid that discovery, but would require format,
revision/update and recovery design plus a separately justified rebuild. The smaller immediate
experiment is to reduce or batch that existing traversal safely, preserving torn-read recovery and
side-reference ownership. No answer cache, global prepass or destructive rebuild is proposed.

# Current measurements and limits

The scalar-handle lane reduced the preceding native hot score by 31–36%. The subsequent native
LZ77 packaging fix reduced it a further 53–55%, with all canonical answers exact and serving
counters unchanged. Its paired hot score remains **10.67–10.82 times slower** than ClickHouse;
rank one is not achieved. Both lanes use fresh source/runtime manifests and fresh 100M PGO training.

The native LZ77 fix reduced Q1 metadata parsing from about 3 seconds to 0.126 seconds, while
directory traversal remained 2.900 seconds (Q4 2.933 seconds). Native perf and async-profiler CPU
captures independently identify `captureLeafDirectorySlots` as a major cost. Async CPU self shares
are 17.23% for Q1 and 15.23% for Q4; Q1 also spends 12.63% in composite grouping and 9.07% in dense
group acquisition. These whole-process, unranked samples include startup and GC.

Async-profiler 4.2 native CPU/wall/native-memory checks and six 100M captures succeeded with exact
results (guard 65 samples, no failures). Native Java-allocation `check` crashed during profiler
initialization, exit 139, before the query; retained as unsupported probe evidence. Java monitor
events were instead collected on a labeled JVM control, not claimed as native coverage. Four JVM
allocation/lock controls passed exact answers (guard 67 samples). JVM allocation samples implicate
directory leaf loading and column decode; JVM lock-event counts implicate the file-reader buffer
pool. Their relative shares do not establish native critical-path wait time. Native wall samples
include many idle GC/pool threads; native malloc profiles are dominated by G1 startup and do not
measure Java object allocations. Version 4.2 resolves native leaf symbols but often stops unwinding
at Graal Java frames; perf raw DWARF captures remain available for deeper attribution.

Rejected/limited diagnostic attempts are preserved: profiler debug logging interleaved with route
counters in the first probe; the allocation probe above terminated before query execution; optional
perf source-line rendering was deliberately stopped after six minutes. Q1 and Q4 symbol reports
had completed before that stop. The attempted follow-up renderer correctly refused to overwrite
Q4. These are not accepted timing samples. Every ranked phase and successful profile phase has its
own guard verdict; the stopped/error probes retain their separate cleanup diagnostics.
