# Changelog

All notable changes to SirixDB are documented in this file.

## [Unreleased] — 0.11.1-SNAPSHOT

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
