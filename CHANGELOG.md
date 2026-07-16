# Changelog

All notable changes to SirixDB are documented in this file.

## [Unreleased]

### Added

- **Asynchronous durable commits** (`AfterCommitState.KEEP_OPEN_ASYNC_COMMIT`) — the middle
  ground between synchronous auto-commits and the async pre-flush: every threshold crossing
  creates a real, durable, queryable revision, but the durability barriers (index-catalogue
  fsync, buffered-tail flush, data force, uber-beacon writes) run on a background thread while
  the transaction keeps inserting into the next epoch. Depth-1 pipeline with backpressure;
  readers see a revision exactly when it hardens (durable-before-visible); a hardening failure
  poisons the transaction. FILE_CHANNEL backend, count-based auto-commit only. See
  `docs/ASYNC_COMMIT_DESIGN.md`.
- `BasicJsonDBStore.Builder#useAsyncFlushForImports` — bulk imports (e.g. `jn:store`) now use
  the asynchronous background pre-flush by **default** on the FILE_CHANNEL backend: one
  semantically meaningful revision per import instead of parser-progress checkpoint revisions,
  with leaf I/O overlapped with parsing and memory still bounded by
  `numberOfNodesBeforeAutoCommit`. Pass `false` (or `-Dsirix.import.asyncFlush=false`) to
  restore synchronous intermediate auto-commits.

### Changed

- **Breaking rename:** the async intermediate "commit" is now called what it is — an async
  flush. `AfterCommitState.KEEP_OPEN_ASYNC` → `KEEP_OPEN_ASYNC_FLUSH`,
  `StorageEngineWriter.asyncIntermediateCommit()` → `asyncFlush()`,
  `awaitPendingAsyncCommit()` → `awaitPendingAsyncFlush()`. The mechanism creates no revision
  and no commit record; the old names asserted otherwise.

## [1.0.0-beta7] — 2026-07-15

### Fixed

- **Every write transaction leaked its three writer file descriptors** (#1109) —
  `NodeStorageEngineWriter.close()` never closed the underlying storage writer, and the
  storage-engine reader deliberately skips its page reader for write transactions, so nobody
  closed the `FileChannelWriter`: the buffered-data, SYNC-revisions and DSYNC-beacon channels
  leaked per commit until the GC's channel cleaner happened to reclaim them. Long-running,
  auto-committing workloads leaked 3 descriptors per commit and hit sporadic
  `Too many open files` failures. The writer is now closed when the page write transaction
  closes.
- **Optimizer walkers closed the shared database on every compile** (#1109) — the CAS/path
  index walker and the valid-time index walker closed the store's *cached* collection (which
  closes the whole database and evicts it from the store) and potentially the cached shared
  resource session after every compiled query. Besides the churn, a transient I/O failure
  during the forced reopen was silently swallowed and disabled the VALIDTIME index rewrite for
  that compile — the source of the flaky `windows-latest`
  `ValidTimeIndexOptimizerRewriteTest` failures. The walkers now borrow the store-owned
  objects, close only the transactions they open, and log resolution failures.
- **Unbounded reader file-descriptor growth** (#1109) — `FileChannelStorage.createReader()`
  opened two fresh `FileChannel`s per reader while read-only transactions stay cached in their
  session, so descriptor usage grew with every query evaluated against a long-lived session.
  Readers now share a striped, lazily-opened, reference-counted channel pool (up to
  `min(availableProcessors, 8)` pairs per storage, closed when the last borrowing reader
  closes): serial workloads keep the previous footprint, concurrent readers are capped at the
  stripe count, idle sessions hold zero descriptors, and striping keeps positional reads
  uncontended on Windows. As a side effect the valid-time optimizer gate test dropped from
  ~8 minutes to ~30 seconds.

## [1.0.0-beta6] — 2026-07-11

### Added

- **macOS and Windows support** — the Umbra-style off-heap frame-slot allocator now runs on
  all three platforms via a `VirtualMemory` SPI (POSIX `mmap` on Linux/macOS, `VirtualAlloc`
  reserve/commit with guaranteed-zero decommit-recommit reuse on Windows); the legacy Windows
  pool allocator stays reachable via `-Dsirix.allocator=windowspool` for rollback. Cross-platform
  CI lanes (macOS, Windows) back the claim; known limitation: crash-recovery re-initialization
  with `MEMORY_MAPPED` storage is unsupported on Windows (see `docs/KNOWN_LIMITATIONS.md`).

### Fixed

- **Path-summary corruption on nested-array removal** (#1099) — removing an array element whose
  subtree held the only references to nested `__array__` path entries left stale in-memory
  references; a later `removeField` in the same transaction crashed with
  `Failed to move to nodeKey: N`. Cursor fast paths now validate in-memory references against
  the authoritative node mapping, and a removed subtree root that is a plain array releases its
  own `__array__` path entry (previously leaked).
- **RevisionEpochTracker poisoning on double-close** (#1102) — a concurrent or reentrant
  transaction close deregistered its epoch ticket twice, permanently corrupting the tracker's
  free stack so no further transactions could be opened in the process. Tickets are now
  generation-tagged and ABA-safe, deregistration is idempotent, and both close paths run
  exactly once via a CAS latch.
- **Large values crashed the commit** (#1076) — string values beyond the largest slotted-page
  size class (~512 KB) failed with `IndexOutOfBoundsException` instead of diverting to an
  overflow page; multi-megabyte values now round-trip (regression-tested at 200 KB, 600 KB
  and 2 MB).
- **LZ4 decompression buffer leak** (#1074) — the allocator-owned output buffer leaked when the
  native decompression call itself threw; repeated corrupt reads could drain the frame-slot
  budget into an `OutOfMemoryError`.
- macOS startup failure of the off-heap allocator (`__errno_location` binding, Linux-only mmap
  flags) fixed; allocator symbols bind lazily and per-OS.

## [1.0.0-beta5] — 2026-07-07

### Fixed

- The standalone launchers generated by `installDist`/`distZip` (`sirix-shell`, `sirix-cli`, `sirix-mcp`) now bake in the required JVM flags; previously `sirix-query` shipped none and `sirix-mcp` lacked `--add-modules=jdk.incubator.vector`, so any write operation (e.g. `jn:store`) crashed with `NoClassDefFoundError: jdk/incubator/vector/Vector`.
- `sirix-shell` starts the interactive REPL when stdin is a terminal instead of requiring the undocumented `-iq` flag (piped input still executes as a single query), and Control-D/Control-C exit the shell cleanly instead of printing `Error: null` with a non-zero exit code.
- README quick-start corrections: the JSONiq field-access example uses `$$.name` (the previous `.name` failed to parse), the update example no longer produces an object with a duplicate key, and the `sirix-shell` transcript matches the actual prompt and empty-line query termination.

## [1.0.0-beta4] — 2026-06-19

### Changed

- Bumped brackit to `1.0-alpha7`, fixing the sequence functions (`fn:subsequence`/`reverse`/`remove`/`insert-before`) over JSON arrays and objects.

## [1.0.0-beta3] — 2026-06-19

### Added

- **Valid-time interval index** — a persistent HOT-backed Relational-Interval-Tree that accelerates `jn:valid-at` / `jn:open-bitemporal` with an `O(h)` point stab, plus a CAS-index narrowing path and a linear-scan fallback. Bumped brackit to `1.0-alpha6`.

## [1.0.0-beta2] — 2026-06-13

### Changed

- Version bump and packaging fixes.

## [1.0.0-beta1] — 2026-06-12

### Added

- **V0 on-disk format** with write-through (preallocated, buffered-beacon) commits.
- **Typed, fail-closed vectorized analytics** — a columnar group-by/aggregate path that lands within a small factor of DuckDB at 100M records.

### Fixed

- Durability and operational hardening across core, query, and the REST API; the REST read/query hot path no longer serializes concurrent requests (unordered `executeBlocking`).

## [1.0.0-alpha11 – alpha22] — 2026-06-04 … 2026-06-10

A rapid correctness, durability, and performance hardening series on the way to beta. Highlights:

### Fixed

- **Serializer correctness** — invalid JSON on single-named-scalar projections; unescaped object keys; number round-trip (exponent-without-dot, overflow, subnormal). (alpha12–alpha14, alpha21)
- **Query semantics** — int/double comparison (`XPTY0004`) over mixed-numeric fields via brackit; a predicate-over-unwrapped-array optimizer that destructively returned empty; `jn:open`/`xml:open` before the first revision now returns an empty sequence. (alpha14, alpha16, alpha19)
- **Latency** — node-history/query latency (event-loop blocking + an uncached history path); array-unbox `O(n²)`. (alpha20)
- **Durability / IO** — streaming-shredder back-pressure deadlock; flaky `UberPageCorruptionTest` (page data-length bounded to the file size); Docker fat-jar glob for versioned release builds. (alpha15, alpha21, alpha11)

See the [GitHub releases](https://github.com/sirixdb/sirix/releases) for full per-version notes.

## [1.0.0-alpha10]

The first 1.0 alpha series — the API is stabilizing toward a production 1.0 release.

### Added

- **MCP Server** — Model Context Protocol server module for AI agent integration, with all 13 tool handlers wired to the SirixDB API
- **Vector Embeddings** — HNSW index for semantic search, with tombstone deletion, query-time efSearch tuning, and serialization versioning
- **Cost-Based Query Optimizer** — Multi-milestone optimizer with PathSummary statistics, selectivity estimation, cardinality propagation, predicate pushdown, DPhyp join ordering, and cost-driven pipeline routing
- **Columnar Vectorized Execution** — Zero-copy columnar extraction with late materialization, SIMD filters, ColumnBatch pipeline, and Mesh data structure for join fusion
- **`sdb:explain()` function** — Inspect query plans from JSONiq
- **Comprehensive JSON test suite** — 199 tests across 14 files
- **Fuzz tests** — Structural correctness fuzz tests for JSON mutations, DeltaVarIntCodec, DeweyIDEncoder, and PageLayout

### Changed

- **HOT (Height Optimized Trie)** — PEXT-routed HOTLeafPage with prefix compression, zero-alloc MSDB, compact-first splits, atomic split+insert
- **Removed heavy dependencies** — Eliminated Guava, Dagger, Checker Framework, lz4-java, snappy-java, and brownies-collections
- **CI pipeline** — Parallelized test jobs and native image tests with matrix strategy
- **Code quality** — Replaced all star imports with explicit imports project-wide

### Fixed

- Production-readiness hardening across cost-based optimizer, HOT implementation, and MCP server
- UberPage dual-beacon fallback for corruption recovery
- Resource cleanup: try-with-resources for read-only transactions, defensive cleanup in snapshots
- Lock leak prevention, bounds checks, and input validation audit
- FSST compressed-domain comparison fixes and thread-safe extractors

## [0.11.0] — Previous Release

### Highlights

- Bitemporal query support with `jn:valid-at()`, `jn:open-bitemporal()`, and valid time configuration
- Sliding snapshot page versioning strategy
- Native binary builds via GraalVM (sirix-cli, sirix-shell, REST API server)
- Interactive JSONiq/XQuery shell (sirix-shell)
- Kotlin CLI with full database operations
- REST API with Keycloak OAuth2/OpenID Connect authentication
- Merkle hash tree verification for tamper detection
- Path, CAS, and Name indexes

---

For the full commit history, see [GitHub Commits](https://github.com/sirixdb/sirix/commits/main).
