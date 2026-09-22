# Changelog

All notable changes to SirixDB are documented in this file.

## [Unreleased]

### Added

- **Projection indexes (experimental, analytical)** are maintained **incrementally** by the
  transactions that touch the record set: inserts, updates, deletes and moves resolve each dirty
  record through exact locators or a bounded fence probe and rewrite only the touched persistent
  units (row groups, order/fence chunks, Bloom chunks, set summaries, dictionary radix paths).
  There is no dirty-record cliff, no whole-index rebuild, and no update-time invalidation — an
  unattributable or corrupt touched unit fails the owning transaction (rollback-only) instead of
  silently degrading the index. Column lookup is by the declared path relative to the record set,
  so nested columns sharing a trailing name with another path are accepted. See
  `docs/PROJECTION_INDEXES.md` and `docs/PROJECTION_INDEX_INCREMENTAL_MAINTENANCE.md`.
- **Load-time projection builds** — `BasicJsonDBStore#create(collection, resource, reader|parser,
  ProjectionSpec)` catalogues a projection on the still-empty resource and lets the load fill it,
  so a load plus its projection is ONE pass instead of a shred followed by a full
  `jn:create-projection-index` walk. Until the load's final commit the projection's metadata slot
  holds the stale tombstone, so an interrupted load leaves queries on the generic pipeline rather
  than on a half-filled index. See `docs/PROJECTION_INDEXES.md`.
- **Sorted projection views** — a projection may declare sort columns (`ProjectionSortedSpec`, a
  `ProjectionSpec` for load-time builds, or the optional fifth argument of
  `jn:create-projection-index`) and then keeps every projected record ordered by them, like a
  table's `ORDER BY` key. A grouped extremum whose string equality filter covers a leading run of
  those columns is answered from that key range; commits maintain the view per touched leaf.
  Projection indexes built by this code cannot be opened by earlier releases; downgrading means
  dropping and rebuilding them. See `docs/PROJECTION_READ_PERFORMANCE.md` and the Compatibility
  section of `docs/DISK_FORMAT.md`.
- **Bulk JSON loaders for fresh resources** — `BulkJsonTreeAssembler` (sequential) and
  `ParallelBulkJsonImporter` (feeder scan + worker page builders + ordered adoption, for corpora
  whose top level is an array; NDJSON rides `NdjsonAsArrayInputStream`). Both build trees
  structurally identical to cursor-based insertion, enforced by a full-field differential oracle,
  and refuse up front what they do not reproduce faithfully (`hashType` other than `NONE`, stored
  DeweyIDs, node history, a non-empty target). Path statistics are **built during the load** by both
  loaders, through the very accumulator the cursor path defers through, so the arms cannot drift.
  (A resource written WITH path statistics now carries the versioned `PathStats` record, a
  one-directional format break recorded in `docs/DISK_FORMAT.md`.) The parallel importer
  **maintains PATH, CAS and NAME definitions and armed projection builds in the load's single
  pass** — the workers extract each family's tuples from the primitives they already hold and the
  coordinator drains them into the families' ordinary bulk loaders; a catalogued projection with
  no armed load-time build is refused rather than silently left unmaintained, and valid-time
  interval maintenance is the one family that still refuses. API, scope, verification, tuning and
  measured numbers: `docs/BULK_IMPORT.md`.
- **Asynchronous durable commits** (`AfterCommitState.KEEP_OPEN_ASYNC_COMMIT`) — the middle
  ground between synchronous auto-commits and the async pre-flush: every threshold crossing
  creates a real, durable, queryable revision, but the durability barriers (index-catalogue
  fsync, buffered-tail flush, data force, uber-beacon writes) run on a background thread while
  the transaction keeps inserting into the next epoch. Depth-1 pipeline with backpressure;
  readers see a revision exactly when it hardens (durable-before-visible); a hardening failure
  poisons the transaction. FILE_CHANNEL backend, count-based auto-commit only. See
  `docs/ASYNC_COMMIT_DESIGN.md`.
- `BasicJsonDBStore.Builder#useAsyncFlushForImports` — bulk imports (e.g. `jn:store`) now use
  the asynchronous background pre-flush by **default** on the FILE_CHANNEL and MEMORY_MAPPED
  backends (the latter is the store's default on 64-bit Linux/macOS; both append through the
  file-channel writer): one semantically meaningful revision per import instead of
  parser-progress checkpoint revisions, with leaf serialization and I/O overlapped with
  parsing (parallel double-buffered background flush) and memory still bounded by
  `numberOfNodesBeforeAutoCommit`. Crash-durability semantics change accordingly: nothing
  of an in-flight import is durable until its single final commit (the sync mode's
  checkpoint revisions each survived a crash). Pages whose serialization spills overlong
  records into OverflowPages are exempted from the background flush and committed by the
  final recursive commit (their durable image needs the overflow disk keys). Pass `false`
  (or `-Dsirix.import.asyncFlush=false`) to restore synchronous intermediate auto-commits;
  `-Dsirix.asyncFlush.parallelism` sizes the shared background-serialization pool.
- **Work-budget tests** — performance regression tests that run in the ordinary suites and fail when
  a load or query starts doing materially more *work*, which a result check cannot see because the
  answer is unchanged. They capture the engine's own counters around one operation (`WorkCapture`,
  `EngineWorkCounters`, `QueryWorkCounters`) and assert a budget on them; they assert no wall-clock
  time, so they are exact on any CI machine and a broken budget names the path that grew. Covered:
  projection queries keep their route (value-count summary, column slices, sorted view) and read no
  more leaves than it needs; a sorted view over an optional aggregate field declines a range after
  one walk without declining clean ranges of the same view; a projection bulk load spills and keeps
  its pinned pages bounded on `FILE_CHANNEL` and `MEMORY_MAPPED`; a batched page read coalesces and
  covers its region once. `NativeImageDowncallConfigTest` guards the
  `-Pnative.preinitializeDowncalls` build configuration (it builds no image, so it cannot measure
  one). Each budget was proven by putting the guarded defect back. See
  `bundles/sirix-core/src/test/java/io/sirix/budget/README.md` and `docs/VERIFICATION.md`.
- **Always-on batch-read counters** — `FileChannelReader.runCount()`, `runSpanBytes()`,
  `runFallbacks()` and the new `runSingletons()` (batch members read one page at a time). The first
  three were previously counted only under `-Dsirix.projDiag` and readable only as a formatted
  string; each event is at least one positional read, so counting is free at that granularity. A
  batch that stops coalescing returns the same bytes, so these are the only way to tell. They are
  process-wide running totals read as a difference across an operation, never reset in place.

### Changed

- **Wide projection string columns serve through windowed leaf loads** instead of whole-column
  eager materialization. A projection whose worst-case resident size exceeds
  `-Dsirix.projection.eagerMaterializeBytes` (default: the smaller of half the projection cache
  budget and a quarter of the heap) is served from bounded 128-leaf windows, and a column fill
  that would exceed the budget declines — the query re-enters the windowed whole-leaf route where
  one exists, otherwise the record path answers. A decline is a routing decision, not a corruption
  signal, so the index stays valid. Whole-column materialization is unchanged below the budget.
  See `docs/PROJECTION_INDEXES.md`.
- **A projection abandoned mid-load reports unconditionally.** The one silent load-degradation —
  a resource-wide value dictionary breaching its byte budget, which abandons the projection while
  the load still succeeds — now prints `[proj] PROJECTION ABANDONED` on stderr with the quantity
  that breached the budget (the shipped log configuration discards the warning), and the stale
  tombstone records a machine-readable `StaleReason` plus its remedy. The reason rides previously
  reserved flag bits, so the `PIXM` wire format is unchanged for existing tombstones
  (`docs/DISK_FORMAT.md`).
- **JSON revision-diff sidecars carry an integrity envelope** (`sirix-diff-format`,
  `operation-count`, `operations-sha256`). Readers (`jn:diff`, the REST diff handler,
  multi-revision resource copy) validate identity, count and digest before use and fall back to
  recomputing the authoritative diff otherwise; sidecars written without those fields are not
  accepted by this build. The REST diff response shape is unchanged — the envelope fields are
  internal and stripped before serving. See `docs/DISK_FORMAT.md`.
- **Breaking rename:** the async intermediate "commit" is now called what it is — an async
  flush. `AfterCommitState.KEEP_OPEN_ASYNC` → `KEEP_OPEN_ASYNC_FLUSH`,
  `StorageEngineWriter.asyncIntermediateCommit()` → `asyncFlush()`,
  `awaitPendingAsyncCommit()` → `awaitPendingAsyncFlush()`. The mechanism creates no revision
  and no commit record; the old names asserted otherwise.

### Fixed

- **A HOT leaf-page rebuild could write past its page** — inserting a key that shortens a leaf's
  common prefix rewrites every resident entry with the reclaimed prefix bytes, and that rewrite was
  performed without checking that the grown entries still fit the 64 KiB page. Loading a JSON
  resource with a declared valid-time index failed mid-load with
  `IndexOutOfBoundsException: Range [64580, 64580 + 1165) out of bounds for length 65536` out of
  `HOTLeafPage.rebuildForShorterPrefix`. The rebuilt image is now sized before anything is written
  and the shrink is refused when the rebuilt entries plus the pending entry do not fit, leaving the
  page exactly as it was so the caller takes its ordinary "leaf is full" path (split, skipped
  consolidation merge, multi-page half). Results, on-disk format, revision visibility and write
  granularity are unchanged. Specified in `docs/HOT_INDEX_SPECIFICATION.md` §3.2.2.
- **A HOT structural insert could place part of a leaf's key range past a sibling** — a HOT node's
  mask is shared by all its branches, so a bit one branch discriminates on is an unused, zero column
  for every child elsewhere, and a multi-value leaf there may hold keys on both sides of it. Two
  handlers assumed it could not. Folding such a leaf's split into the node inserted its upper half at
  that half's partial-key position, which lies past any sibling told apart by a less significant
  bit: the children were no longer ordered by first key, and that sibling's keys were routed to the
  inserted child. The complete-frontier splice, which takes over when a fold is declined, built its
  block around sides the block's own bits cut through: it gave up on a side straddling the block's
  most significant bit and did not look at the second bit at all, where a straddling side's keys are
  routed to the new key's leaf. Loading a JSON resource with a declared valid-time index at 100,000
  records stopped with `IllegalStateException: HOT published structural splice is malformed (first:
  I8-children-sorted-by-firstkey …)`, raised by the writer's own validation, so nothing was published
  in that state. The fold is now declined unless the upper half lands beside its slot, and the
  complete-frontier splice splits a side where a bit of its block cuts through it, so that every
  child of the block is one-sided on the bits of its path. A leaf overflow whose fold is declined at a
  parent the cascade would fold into rather than nest under has no second placement of its own, so it
  is now discharged through that same complete-frontier splice: the parent's subtree is split
  immediately before the key and the key gets its own leaf. When the overflow was a byte overflow on a
  key the leaf already holds, that leaf carries the merged value and the split drops the stale entry;
  a side reference the dropped entry owned has no home in either half, so the split refuses before
  publication rather than orphaning a segment page and the load stops. Only a projection index can
  reach that — side references are attached to a HOT leaf by `ProjectionIndexHOTStorage` alone, so
  path, CAS, name and valid-time leaves never carry one — every entry to that route can, not just the
  declined fold. What the two entries did before differs, and neither committed anything wrong: a
  fold declined at the leaf's own parent already failed closed without publishing, because the
  cascade reached the same refusal and the transaction was marked rollback-only, so the load stopped
  at that insert exactly as it stops now; a cascade the trie-condition pre-check now declines instead
  completed the insert and published a half that broke the condition latently, so the load stopped
  only later. For that second entry a projection index with a dropped side reference now stops at the
  insert rather than later. Carrying the dropped entry's reference onto the key's fresh leaf is a
  separate task. Further into the same load, splitting a full node published a half that broke the
  trie condition (I11) against its own child: a half keeps only the bits that still vary within it, so
  a child that sat safely below the node's most significant bit can sit above the half's. Only the
  half the new key joins was checked and lies on the key's route, so the other went out unseen,
  committed, and stopped the load three publications later with `HOT published structural path is
  malformed`, when an insert was first routed through it. A full-node decomposition, and the
  integrate cascade behind it, are now declined when a half would break the condition. The merge
  path's own capacity cascade is pre-checked with the same predicate, at both of its entries to the
  integration: an ancestor that would publish such a half, and one that would refuse the fold
  outright because the cascaded split bit's straddle partial is taken or would not land beside the
  slot, both hand the overflow to the complete-frontier splice before anything is allocated. Only
  where the cascade would fold — a parent taller than the split keeps nesting the halves under a node
  of their own, which touches no block. The same question on the other branch-path decomposition of
  a full node, and a cascade whose split bit is the node's own most significant bit, were considered
  and left unguarded: no test enters the one or reaches the other, so a guard there could not be
  shown to fire; both are stated as known limits in the specification. A failed index write on the
  merge path's split arm poisons the transaction exactly as it always did — before, inside or after
  the integration — because the key's document node is already written there while its index entry is
  not, so a caller that caught the failure and committed would hold a document with no posting. The
  new routing takes no exception to that rule: the overflow handed to the complete-frontier splice,
  a split the handler could not construct, a fault while the halves are retired and a failure inside
  the integration itself are all covered by it. The two pre-checks are believed to make a failure
  inside the integration unreachable, and no test exercises one. The 100,000-record valid-time correction stream the
  regression test replays (25 publications, 1,080,574 index-writer operations) never starts a
  merge-path capacity cascade, so both entries are covered by constructed scenarios instead, each of
  which fails without its own pre-check; the trie-condition reason is decided by the same predicate
  call but is not reached through the merge path by any test. One decomposition is still measured before its insert and re-split after it: on a combination
  collision the full-node branch arm sub-inserts the key into the affected child and re-splits the
  same node without re-asking the guard, which can publish a half above a child that breaks the trie
  condition. That break sits on the key's own route, so the published-structure validation catches
  it, the transaction is poisoned and the load stops with nothing wrong committed; the 100,000-record
  stream takes the arm four times without reaching it, no test constructs it, and closing it is a
  separate task. Results, on-disk format, revision visibility, write granularity and the validation
  itself are unchanged. Specified in `docs/HOT_INDEX_SPECIFICATION.md` §4.5.2–§4.5.4, §4.5.6 and
  §4.5.7.
- **A HOT structural insert could lose a key with every invariant intact** — when a full node's most
  significant bit splits it 1:31, the new key's half comes back bare, as the node's *own* child
  reference. Folding the key into that half published the fold by re-pointing that reference, and it
  already names the unfolded child in the transaction log, where registration stops at any reference
  that carries an identity: the folded page was never logged. The writer, which resolves the log
  first, and the commit both kept the unfolded child, so the key was absent from the revision that
  was committed while the trie stayed well-formed. No invariant, validator or detector over the trie
  can see that, because nothing in the trie is wrong; in a valid-time index it shows only as a record
  answered over part of its span, by the store that kept its other endpoint. The folded page is now
  published under a fresh `PageReference`, with the split's BiNode rebuilt around it, so registration
  reaches it and the commit carries it. A fold into such a half is also declined when the half is
  already at `MAX_NODE_ENTRIES` — a half the split compressed always has room, a lone child sized by
  its own inserts need not — and the complete structural frontier places the key instead, where the
  fold previously assembled a 33-child node and failed the put with `MultiNode must have 1-32
  children`. As defense in depth, a structural put now re-reads its own key on the route the
  transaction log resolves; a route that does not produce it throws and marks the transaction
  rollback-only (`STRUCTURAL_PUT_NOT_READABLE`), so a publication the log cannot carry fails at the
  put instead of answering wrongly later. On-disk format, revision visibility and write granularity
  are unchanged; answers change only where a key was previously lost. Specified in
  `docs/HOT_INDEX_SPECIFICATION.md` §4.5.4 case 2 and §4.8.

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
