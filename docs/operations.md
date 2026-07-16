# Sirix — Operations Guide

Production deployment, tuning, and troubleshooting for `sirix-core` and the
`sirix-rest-api` server. This document focuses on the operational surface — JVM
flags, cache budgets, OS limits, observability, backups — rather than on API
usage. For API documentation, see the project README and JavaDoc; for storage-
format internals, see `docs/ARCHITECTURE.md`.

> **Status.** Sirix is currently at `1.0.0-alpha10`. The wire format is on
> `BinaryEncodingVersion.V0`; bumps are stamped into the page header and rejected
> on read with a clear "version not known" error. There is **no migration tool
> yet** — when V1 is introduced, a one-shot upgrader will ship alongside.

---

## 1. Supported environment

| Dimension | Value |
|---|---|
| **JDK** | Java 25 LTS (sourceCompatibility / targetCompatibility = 25). Earlier JDKs are not supported. |
| **OS / arch** | Linux x86_64 — fully supported, including the bundled native LZ77 decoder. macOS and Windows run on the pure-Java LZ77 fallback (correct, slower). |
| **Other JVMs** | OpenJDK HotSpot is the reference. GraalVM Community / EE work; the perf-campaign baseline runs on a recent EA build for the MemorySegment fixes (see `graal-issue-13377.md` in project memory). |
| **Native image** | Supported via GraalVM `native-image` for `sirix-rest-api` and `sirix-kotlin-cli`. See `docs/NATIVE_IMAGE.md`. |
| **Cluster** | Single-node only. No replication, no consensus. Multi-tenancy at the database level (one resource session writer per resource). |

---

## 2. Mandatory JVM flags

Sirix uses Foreign Function & Memory (FFM), the Vector API, preview features, and
several JDK-internal exports that must be opened. These flags **are not optional**
— omission produces `IllegalAccessError` at startup.

```
--enable-preview
--enable-native-access=ALL-UNNAMED
--add-modules=jdk.incubator.vector
--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED
--add-exports=java.base/sun.nio.ch=ALL-UNNAMED
--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED
--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED
--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED
--add-opens=java.base/java.lang=ALL-UNNAMED
--add-opens=java.base/java.lang.reflect=ALL-UNNAMED
--add-opens=java.base/java.io=ALL-UNNAMED
--add-opens=java.base/java.util=ALL-UNNAMED
```

The same set is applied in the project's Gradle build (`build.gradle:215`) and
in the REST API CI workflow (`.github/workflows/gradle.yml`).

Notes:

- `jdk.unsupported/sun.misc` is required only because of a transitive
  `net.openhft/zero-allocation-hashing` dependency. Sirix code itself no longer
  uses `sun.misc.Unsafe` directly.
- The `jdk.compiler` opens are needed when using the Brackit query stack with
  ahead-of-time AST compilation; they are harmless when not exercised.

---

## 3. Heap sizing and GC choice

Sirix is built around **off-heap** `MemorySegment`-allocated page memory. The
on-heap budget covers (a) the JVM and Brackit's query state, (b) per-thread
buffers and caches, (c) intermediate query result objects, and (d) on-heap
references to off-heap pages held by transactions. A typical production sizing:

| Workload | `-Xms` | `-Xmx` | `-XX:MaxDirectMemorySize` |
|---|---|---|---|
| Embedded library, single resource, ~1 GB working set | 2 GB | 4 GB | 1 GB |
| `sirix-rest-api` server, mixed workload | 4 GB | 8 GB | 1 GB |
| Analytical workload over multi-GB data (Chicago-scale) | 5 GB | 12 GB | 2 GB |

Defaults inside the gradle `:test` JVM are `-Xms5g -Xmx12g` (build.gradle:251) —
not because tests need 12 GB, but because they pre-touch the heap (`AlwaysPreTouch`)
to make GC behavior comparable across runs.

### GC

The reference GC is **ZGC** with always-pretouch and large-pages, configured in
the project's gradle test-JVM as:

```
-XX:+UseZGC
-XX:+AlwaysPreTouch
-XX:+UseLargePages
-XX:+UseStringDeduplication
-XX:+HeapDumpOnOutOfMemoryError
-XX:ReservedCodeCacheSize=1000m
-XX:EliminateAllocationArraySizeLimit=1024
```

Z is preferred because:

- Sirix's hot path is largely off-heap, so old-gen pressure is dominated by long-
  lived caches, not transient objects. Z's region-based collector handles this
  well.
- Sirix expects sub-second pause budgets; G1's 50–200 ms pauses on a 12 GB heap
  with deep object graphs are too disruptive.

Generational ZGC (`-XX:+ZGenerational`) is supported but currently commented out
in the build because some workloads regress versus single-gen Z; benchmark
before flipping it on.

### Direct memory

`-XX:MaxDirectMemorySize` should be at least **1 GB**. Sirix uses direct buffers
for FFI (LZ4), file-channel reads, and certain serialization paths.

### Other flags worth knowing

- `-XX:-UseJVMCICompiler` — workaround for a Graal JIT speculation bug
  (oracle/graal#13387) that caused 27% wall-clock regressions on
  `conjunctiveCountByGroup` queries. See `graal-jit-speculation-bug.md`.
- `-Xlog:gc*=debug:file=gc.log` for production GC tracing.
- `-Ddisable.single.threaded.check=true` — **obsolete, do not set.** This is a
  Chronicle-Bytes property; Chronicle Bytes is not a SirixDB dependency on the
  release line, so nothing on the classpath reads the flag. It was carried in
  the Dockerfile/Gradle test args as cargo cult and has been removed. Passing
  it is harmless but pointless.

---

## 4. Cache budgets

Sirix's `BufferManager` is a multi-tier cache. The defaults are computed as
fractions of the **memory budget** (the off-heap allocator's max segment size),
and can be overridden via system properties.

| Cache | Default | Property | Purpose |
|---|---|---|---|
| `RecordPageCache` | 50% of budget | `sirix.cache.recordPage` | Most-recent record-page versions — primary data cache |
| `RecordPageFragmentCache` | 18.75% of budget | `sirix.cache.recordPageFragment` | Older revision fragments needed to reconstruct historical records |
| `PageCache` | 6.25% of budget (min 100 MB) | `sirix.cache.page` | Index pages, RevisionRoot pages — metadata, not records |
| `RevisionRootPageCache` | 5,000 entries (fixed count) | — | Revision root pointers |
| `RBTreeNodeCache` | 50,000 entries (fixed) | — | RB-tree index nodes |
| `NamesCache` | 500 entries (fixed) | — | Interned QName / property-name strings |
| `PathSummaryCache` | 20 entries (fixed) | — | Per-resource path-summary readers |

Set explicit byte counts when you know your working set:

```
-Dsirix.cache.recordPage=8589934592   # 8 GB
-Dsirix.cache.recordPageFragment=3221225472   # 3 GB
-Dsirix.cache.page=536870912   # 512 MB
```

Initial sizing log line (look for it in startup output):

```
INFO  io.sirix.access.Databases - Initializing global BufferManager with memory budget: 16 GB
INFO  io.sirix.access.Databases -   - RecordPageCache: 8589934592 bytes (8192 MB) (default: 25% of budget)
INFO  io.sirix.access.Databases -   - RecordPageFragmentCache: 3221225472 bytes (3072 MB) (default: 12.5% of budget)
INFO  io.sirix.access.Databases -   - PageCache: 1073741824 bytes (1024 MB) (default)
```

---

## 5. Native libraries

### `libsirix_lz77.so`

A bundled native LZ77 decoder for Linux x86_64. Embedded as a JAR resource at
`/native/linux-x86_64/libsirix_lz77.so` and extracted to a temp file at the
first decode call.

- **If present:** ~2× decompression throughput versus the pure-Java fallback.
- **If absent or platform mismatch:** falls back to `SirixLZ77Codec` pure-Java
  decoder, which is correct but slower.
- **Override:** `-Dsirix.lz77Codec.native.disable=true` forces pure-Java for A/B
  testing.

To rebuild from source: `./gradlew :sirix-core:buildNativeLz77` (requires `gcc`
on `PATH`). The build step is no-op when `gcc` is missing — the JAR ships only
the prebuilt `.so`.

### LZ4 (FFM)

The default `FFILz4Compressor` invokes the system `liblz4.so.1` via FFM. On
modern Linux distros this is in `apt install liblz4-1` / `dnf install lz4` and
present by default. macOS: `brew install lz4`. Windows: build / install
`liblz4.dll`.

If `liblz4` is unavailable the constructor throws at first compress/decompress.
Page writes succeed only when the compressor is functional; there is no
runtime fallback for LZ4 (unlike LZ77).

---

## 6. OS-level requirements

| Setting | Value | Why |
|---|---|---|
| `ulimit -n` | ≥ 65,536 | Each storage engine reader holds an open file handle to the resource; a busy server with hundreds of concurrent transactions will exceed the default 1024. |
| `vm.max_map_count` | ≥ 262144 | MemorySegment-backed allocations + memory-mapped file I/O can use many mappings. |
| Huge pages | enable `vm.nr_hugepages` (or `transparent_hugepage=always`) | `-XX:+UseLargePages` is the JVM default and falls back silently if huge pages aren't available, but you give up TLB efficiency on hot pages. |
| Disk | local NVMe SSD strongly preferred | Sirix's read path is page-random; spinning disks are roughly 100× slower per page read. |
| Filesystem | ext4 or xfs | btrfs and ZFS work but add their own copy-on-write layer that interacts oddly with Sirix's CoW page format. |
| Time source | NTP-synced | Sirix records commit timestamps; clock skew shows up as out-of-order revisions. |

---

## 7. Observability

The `sirix-rest-api` server exposes Prometheus-format metrics at `GET /metrics`
via [Micrometer](https://micrometer.io). Wired in
`bundles/sirix-rest-api/src/main/kotlin/io/sirix/rest/MetricsHandler.kt`.

### HTTP-level metrics

| Metric | Type | Labels | Notes |
|---|---|---|---|
| `http_request_duration_seconds` | Timer | method, path, status | per-request latency histogram |
| `http_requests_total` | Counter | method, path, status | request rate |
| `http_active_requests` | Gauge | — | in-flight requests |

### Sirix-internal metrics

Bridged into the same Prometheus registry via `SirixMetricsRegistry` (no
dependency from `sirix-core` to Micrometer). Embedders calling
`MetricsHandler.install(router)` on the REST API get these for free; standalone
embedders can wire the same gauges into their own registry by implementing
`SirixMetricsRegistry.Bridge`.

| Metric | Type | Source |
|---|---|---|
| `sirix_record_page_cache_hits_total` | counter | `ShardedPageCache` |
| `sirix_record_page_cache_misses_total` | counter | `ShardedPageCache` |
| `sirix_record_page_cache_evictions_total` | counter | `ShardedPageCache` |
| `sirix_active_node_read_only_transactions` | gauge | `TransactionMetrics` |
| `sirix_active_node_read_write_transactions` | gauge | `TransactionMetrics` |
| `sirix_node_read_only_transactions_opened_total` | counter | `TransactionMetrics` |
| `sirix_node_read_write_transactions_opened_total` | counter | `TransactionMetrics` |
| `sirix_record_page_cache_size_bytes` / `_max_bytes` | gauge | `BufferManagerImpl` |
| `sirix_record_page_fragment_cache_size_bytes` / `_max_bytes` | gauge | `BufferManagerImpl` |
| `sirix_hot_leaf_page_cache_size_bytes` / `_max_bytes` | gauge | `BufferManagerImpl` |
| `sirix_allocator_physical_memory_bytes` | gauge | `LinuxMemorySegmentAllocator` |

Still on the production-readiness backlog: commit-queue depth and GC pause
attribution. For these, use JFR (`-XX:StartFlightRecording`) plus the Sirix
logback appender at `INFO` level.

For the embedded-library use case (no REST), Sirix logs cache initialization,
storage allocator decisions, and ClockSweeper progress at INFO. Logger names:

- `io.sirix.access.Databases` — startup, BufferManager init.
- `io.sirix.cache.BufferManagerImpl` / `io.sirix.cache.ShardedPageCache` — cache lifecycle.
- `io.sirix.cache.ClockSweeper` — eviction sweeps (PostgreSQL bgwriter pattern).
- `io.sirix.cache.LinuxMemorySegmentAllocator` — off-heap allocator events.
- `io.sirix.access.Databases$Databases` — close/cleanup warnings.

---

## 8. Backup and restore

Sirix has **no streaming or incremental backup tool**. Resource directories are
self-contained; the operational pattern is:

1. Stop the writer for the resource (close any active `NodeTrx`).
   Read-only transactions can continue.
2. `cp -a` or `rsync -a --inplace` the resource directory to the backup target.
   Sirix's append-only page format means this is consistent without additional
   coordination.
3. Verify the backup by opening it as a read-only resource:
   ```java
   try (var db = Databases.openJsonDatabase(backupPath);
        var session = db.beginResourceSession("...");
        var rtx = session.beginNodeReadOnlyTrx()) { /* ... */ }
   ```

Restoring is a directory move/copy back; no replay is required.

**Caveats:**

- Hot backup (writer running) is **not** safe — the in-flight Transaction Intent
  Log can leave the on-disk image inconsistent. Wait for `wtx.commit()` /
  `wtx.close()` first.
- Snapshot-based backups via filesystem snapshots (LVM, ZFS) are safe **iff** the
  snapshot is atomic across all files of the resource. ext4 + LVM is fine; per-
  file snapshots are not.

A point-in-time recovery is possible via Sirix's revision system: open the
resource at the desired revision number or timestamp via
`session.beginNodeReadOnlyTrx(revision)` /
`session.beginNodeReadOnlyTrx(Instant)`. No external tool needed.

---

## 9. Supported workloads

| Dimension | Supported | Notes |
|---|---|---|
| **Document model** | JSON, XML | one or the other per resource; no mixing |
| **Document size** | up to 64 KiB per LZ77 block, unlimited overall | LZ77's 16-bit offset caps the back-reference window; documents larger than 64 KiB fall back to a literal-only token stream (no compression) |
| **Page size** | 256 KiB ceiling | all in-memory page buffers use this as the practical max |
| **Concurrency** | many concurrent readers, exactly one writer per resource | the writer lock is a `Semaphore(1)` per resource |
| **Bitemporality** | system-time (revisions), valid-time (configurable paths via `validTimePaths`) | both queryable via `jn:all-times`, `jn:open-bitemporal`, `sdb:timestamp`, `sdb:valid-from` |
| **Versioning strategies** | FULL, INCREMENTAL, DIFFERENTIAL, SLIDING_SNAPSHOT | choose at resource creation; `SLIDING_SNAPSHOT` is the production default |
| **Indexes** | name index, path index, CAS index, HOT (height-optimized trie) | configured at resource creation |
| **Query language** | JSONiq via Brackit; XQuery via Brackit | the cost-based optimizer (M1–M5) is wired in for JSONiq |

---

## 10. Known limitations and operational caveats

1. **Single-writer-per-resource.** A second `beginNodeTrx()` on a resource with
   an active writer throws after a 5-second `tryAcquire` timeout. Plan for
   serialised writes; do batch ingestion in one writer.

2. **Brackit dependency.** Sirix depends on the released `io.sirix:brackit:1.0-alpha1`,
   so builds are reproducible from Maven Central with no local install or commit-hash
   pinning required. (Brackit is itself in its 1.0 alpha series alongside Sirix.)

3. **No on-disk format migration tool.** `BinaryEncodingVersion.V0` is the only
   shipping version. When V1 lands, an upgrader will ship; today, opening a
   resource written by an incompatible Sirix version raises
   `IllegalStateException: <n> not known.`

4. **Auto-commit features are in flight on multiple branches**
   (`feature/warm-auto-commit-v1`, `feature/async-auto-commit`,
   `feature/eager-serialize-gc-fix`). The `AfterCommitState.KEEP_OPEN_ASYNC_FLUSH`
   path on `main` now passes a basic round-trip test (3000 inserts crossing
   the auto-commit threshold, final commit + read-back). Runtime guards in
   `AbstractResourceSession.beginNodeTrx` reject misuse: `KEEP_OPEN_ASYNC_FLUSH`
   requires `FILE_CHANNEL` + count-based auto-commit; the `AsyncAutoCommitTest`
   suite covers both the happy path and the two fail-fast guards. The branch
   consolidation (merging the three feature branches' design improvements
   into one) remains a multi-session effort.

5. **Chicago-scale ingestion tests are `@Disabled`.** The reference 3.6 GB
   Chicago dataset is not in CI; large-scale ingestion regressions are caught
   manually by removing the `@Disabled` annotation and running locally on a
   machine with ≥ 16 GB RAM.

6. **Crash-recovery test coverage.** `CrashRecoveryTest` exercises seven
   scenarios: stale-marker no-op, marker-driven partial-write truncation
   (single + multi-revision), missing marker normal case, **torn-write
   without `.commit` marker (orphan tail bytes don't lose committed data)**,
   **dual-beacon torn write in `sirix.revisions` (recovery falls back to
   the second beacon at offset 512)**, and **concurrent reader survives
   writer crash + recovery**. A full Writer-decorator fault-injection
   harness for testing failure DURING the commit flush sequence (vs. after)
   is still on the backlog.

---

## 11. Quick-start: launch the REST API server

```bash
java \
  --enable-preview \
  --enable-native-access=ALL-UNNAMED \
  --add-modules=jdk.incubator.vector \
  --add-exports=java.base/jdk.internal.ref=ALL-UNNAMED \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED \
  --add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED \
  --add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED \
  --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.lang.reflect=ALL-UNNAMED \
  --add-opens=java.base/java.io=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED \
  -Xms4g -Xmx8g \
  -XX:+UseZGC -XX:+AlwaysPreTouch -XX:MaxDirectMemorySize=1g \
  -Dsirix.cache.recordPage=4294967296 \
  -Dsirix.cache.recordPageFragment=1610612736 \
  -jar bundles/sirix-rest-api/build/libs/sirix-rest-api-1.0.0-alpha10-fat.jar \
  -conf bundles/sirix-rest-api/src/main/resources/sirix-conf.json
```

`/metrics` will be available on the configured port immediately; database
directories are created lazily under the path configured in `sirix-conf.json`.

---

## 12. Where to look when something is wrong

| Symptom | First place to check |
|---|---|
| `IllegalAccessError` on startup | mandatory JVM flags (§ 2). |
| `<n> not known.` on resource open | resource was written by an incompatible Sirix version (§ 1, § 10.3). |
| `OutOfMemoryError: Direct buffer memory` | raise `-XX:MaxDirectMemorySize` (§ 3). |
| `OutOfMemoryError: Java heap space` | raise `-Xmx`, OR shrink record-page cache (§ 4). |
| Page cache hit rate < 50 % | look at the working-set size in the startup log; raise `sirix.cache.recordPage`. |
| Long GC pauses | confirm ZGC is engaged (`-Xlog:gc*=info`); avoid G1 on heaps > 8 GB. |
| Slow LZ77 decompression | confirm `libsirix_lz77.so` extracted (look for `SirixLZ77NativeDecoder loaded` at INFO). |
| `No read-write transaction available` (5s timeout) | another writer is open on this resource session — close it first (§ 10.1). |
| Process-level slowdown after writer churn | check whether a writer was orphaned without `close()`; the deprecated `finalize`-based detector was replaced by Cleaner — leak warnings now appear at WARN with `NodeStorageEngineWriter FINALIZED WITHOUT CLOSE`. |
| Concurrent reader-open contention | Sirix 1.0.0-alpha5 onwards drops `synchronized` on `beginNodeReadOnlyTrx`; if you see throughput plateau, profile with `jfr`. |

---

## 13. Project memory

For deeper context, see:

- `docs/ARCHITECTURE.md` — page format, versioning, transaction model.
- `docs/cost-based-optimizer-design.md` — JQGM, histogram selectivity, DPhyp.
- `docs/NATIVE_IMAGE.md` — GraalVM native-image build/deploy.
- `CLAUDE.md` — internal developer expectations (HFT-grade hot path,
  no-Claude-in-commits, etc.).
- `ROADMAP.md` — open work items and target order.
