# Changelog

All notable changes to SirixDB are documented in this file.

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
