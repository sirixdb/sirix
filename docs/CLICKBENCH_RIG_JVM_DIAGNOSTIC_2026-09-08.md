# Unchanged-runtime JVM diagnostic

One full 43-query, three-try process (`jvm50-01`) ran under exclusive access at flat
50 W using the original `aa4d81d54` compiled runtime and the unchanged 6/14 GiB
heap, 10 GiB arena, 5 GiB eager-residency and C2 flags. JFR, compilation logging,
GC/safepoint logging and an external resource observer were added for this run.
It is diagnostic evidence, not a scored calibration leg. No query engine changed.

The trace supports incomplete JVM warmup and intermittent GC as mechanisms to
investigate. It does not establish a universal hardware noise floor or allocate
the measured suite variance causally. The preceding
[40-leg decomposition](CLICKBENCH_RIG_VARIANCE_ATTRIBUTION_2026-09-08.md) determines
which queries deserve attention. Its query covariance shares already include
cross-query covariance; the two percentages must not be added as separate causes.

| Whole-process observation | Value |
|---|---:|
| JFR compilations | 8,516 (5,736 C1; 2,780 C2) |
| Summed compilation elapsed time, overlapping threads | 49.524 s |
| Sampled compiler CPU time, lower bound | 43.76 s |
| JFR GC phase pauses | 255, totaling 7.945 s |
| `sirix-vec-exec` thread starts | 2,160 |
| Main-thread execution samples in `Query.<init>` | 16 / 2,230 |
| Minimum host MemAvailable | 11,869,280 KiB |
| Maximum JVM RSS | 16,598,476 KiB |
| Maximum observed JVM VmSwap | 0 KiB |
| JVM physical read bytes, final minus first sample | 18,998,251,520 |
| Host memory PSI some / full | 0.767 / 0.677 s |
| Host I/O PSI some / full | 5.997 / 5.270 s |

q3 fell from 0.138 s on try 2 to 0.038 s on try 3 with no reported collection in
either try. C2 compilation of its numeric aggregate kernel occurred around that
transition. q6 also had no collection on its hot tries, while C2 compilation of
its string min/max path was still active. q21 and q42 still had relevant C2 work
near their hot tries. These are warmup clues, not proof of an exact causal overlap.

q39 took 0.298 s on try 2 without a reported collection and 0.394 s on try 3 with
four collections and 0.12 s reported collection time. The JFR pause trace agrees
approximately with that interval. This proves that GC can affect an individual
attempt here; it does not explain the earlier three-leg q39 distribution alone.
The harness's historical `pauses` field comes from `GarbageCollectorMXBean`:
its collection time is approximate and is not itself an exact safepoint trace.
See the [Java 25 API definition](https://docs.oracle.com/en/java/javase/25/docs/api/java.management/java/lang/management/GarbageCollectorMXBean.html).

Four full allocation-compaction pauses lasted 349.041, 224.047, 194.123 and
253.739 ms. A separate explicit `System.gc()` pause lasted 166.143 ms. JFR's
`jdk.SystemGC` stack identifies the existing
`SirixVectorizedExecutor.GroupPasses.refreshBudget` path. The source conditionally
requests collection when a new live-heap estimate might save grouping passes.
This is an execution-policy lead for the engine lane, not a change made by this
instrumentation task. Turning it off would change execution and requires a
separate experiment and owner.

JFR identifies exactly one `jdk.SystemGC` call and collection id 91 with cause
`System.gc()`. Its matching phase pause is 0.166127531 s out of 7.945428022 s:
**2.09%** of traced pause time. The other 7.779300491 s includes allocation-driven
collections and metadata-triggered collections. This explicit event falls within
q18 try 1 (7.034 s), after q2/q3/q6 had already finished. It cannot directly pause
those earlier tries in this process. Removing it could still change subsequent
heap budgets, number of grouping passes and later allocation-driven collections.
The proposed `-Dsirix.projection.groupPasses.refreshBudgetByGc=false` comparison
was canceled by Firstmate after this attribution. One flag-on leg completed;
the flag-off controller stopped during cooldown before launching Java. There is
no paired result or variance claim. Its effect on budget sizing, spills and
grouped-query performance remains a follow-up. The original engine was unchanged.

The fresh executor per try remains necessary to avoid memoized result reuse.
Its worker pool starts threads lazily during execution. q6 started no vector
workers, so pool startup cannot be the sole explanation for the largest variance
contributor. Query construction, execution and full serialization remain inside
the timer. `Query(CompileChain, String)` compiles the query on construction, but
only 16 main-thread samples included that constructor. Runtime expressions also
live in a `compiler` package; classifying all such frames as front-end compilation
would incorrectly assign lazy database work to parsing or optimization.

Host reclaim occurred (471,090 direct pages scanned, 5,133,969 kswapd pages scanned,
75 allocation stalls), while the query process showed no swapped resident pages.
The host swapped 38 pages in and 675 out over the observed interval. Occupied swap
at launch is therefore not evidence that this JVM was actively swapping. The
process's write counters include JFR and compilation output and must not be read
as database writes. The page cache was inherited naturally; no cache drop occurred.

The Java child exited 0 and produced all 129 unique timings and a complete JFR.
The observer then hit `PermissionError` reading `/proc/3066744/io` during teardown,
so the original controller verdict retains an issue and the controller exited 1.
No successful scored verdict is substituted. The observation is usable with that
missing final process sample disclosed. Production observation now distinguishes
an exited child from a live permission failure.

The diagnostic observer also performed heavy sampling while reading query output;
one snapshot took 0.109 s. Inferred short-query windows can consequently be delayed
by more than the query duration. Exact JIT/GC overlap counts are exploratory.
The new reader records receipt timestamps only and leaves resource sampling to
its observer thread. Receipt timestamps still are not exact JVM phase events.

[Committed evidence](../bundles/sirix-query/bench/clickbench/rig/evidence/jvm-diagnostic-20260908/README.md)
retains the original failed verdict, the derived `analysis.json`, and `raw-members.json`, which names
every member of the raw archive and its hash. The raw JFR/log archive itself was discarded from the
deliverable and cannot be regenerated, so the JFR replay is no longer runnable; the archive whitelist
and every member hash were checked while it was present. See
[evidence retention](../bundles/sirix-query/bench/clickbench/rig/evidence/RETENTION.md).
