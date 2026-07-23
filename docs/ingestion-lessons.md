# Ingestion / Write-Path Lessons from Umbra, ClickHouse, DuckDB, and simdjson

*Last updated: 2026-07-23*

How to speed up SirixDB **ingestion** (write/insert throughput, including the
JSON parsing/shredding layer), grounded in Sirix's actual write path and in
verified techniques from Umbra/HyPer, ClickHouse, DuckDB, and simdjson. This is
the write-side companion to [`storage-lessons.md`](./storage-lessons.md) (which
covers read-side/query lessons); read-side material is not repeated here.

Each finding was cross-checked against primary sources and adversarially
verified (3-vote). Facts are high-confidence; the *transfer to Sirix* is
interpretive and grounded here by a code walk of the write path. Invariant
conflicts (append-only immutability, durable bitemporal time-travel,
single-writer-per-resource) are flagged.

## Sirix write path — where ingest time actually goes

The node-write path is already heavily optimized: **zero per-node heap
allocation** (flyweight singletons in `JsonNodeFactoryImpl` writing directly into
the page `MemorySegment`), fused object-field records halving node count, and a
`mostRecentPageContainer` cache so ~1024 in-order inserts share one copy-on-write
leaf. The real bottlenecks are therefore:

1. **Serial per-resource writer.** `AbstractResourceSession` holds a
   `Semaphore(1)` for the whole write transaction — single-document ingest is
   fundamentally single-core. The only parallelism is `ParallelJsonShredder`
   sharding into *separate* resources, which fragments the logical document.
2. **O(depth) rolling-hash per node** (default `HashType.ROLLING`): `rollingAdd`
   in `AbstractNodeHashing` walks inserted-node → root, modifying every ancestor
   slot. Dominant per-node CPU for deep JSON. Gated off by bulk mode
   (`repairBulkInsertHashes` → one postorder pass) or `HashType.NONE`.
3. **Per-node maintenance fan-out**, synchronous and default-on: PathSummary
   cursor walks, CAS/Name/Path index listener notifications, DeweyID sibling-reads
   + `SirixDeweyID.toBytes()` (the main residual allocation), name-dictionary
   lookups.
4. **Commit-time CPU on the writer thread** (`NodeStorageEngineWriter`
   phase 1): FSST symbol-table *training*, PAX region build, codec bake-off
   (amortized via a sticky-winner election), and outer LZ4/deflate — all
   synchronous. Async commit backgrounds only **phase 2** (durability barriers);
   `ASYNC_COMMIT_DESIGN.md` lists backgrounding phase-1 serialization as *future
   work*.
5. **Durability barriers per epoch**: index-catalogue fsync + data force + two
   DSYNC beacon writes. The measured dominant cost of sync auto-commit (design
   doc: 556 ms → 141 ms async-flush → 106 ms single-commit for 100k inserts).
6. **COW write amplification for small/interactive inserts**: a single-node
   insert rewrites the indirect spine to a new `RevisionRootPage`/`UberPage`;
   only sequential bulk order amortizes it (≈1 leaf page / 1024 nodes).
7. **Stock single-threaded pull parser** (Gson for `Reader`, Jackson for
   `InputStream`), one token → one `wtx.insert`. No SIMD, no columnar staging.

Anchor files: `service/json/shredder/{JsonShredder,JacksonJsonShredder,ParallelJsonShredder}.java`,
`access/trx/node/json/{JsonNodeTrxImpl,JsonNodeFactoryImpl}.java`,
`access/trx/node/AbstractNodeHashing.java`, `access/trx/node/AbstractDeweyIDManager.java`,
`index/path/summary/PathSummaryWriter.java`, `access/trx/page/NodeStorageEngineWriter.java`,
`page/{PageKind,KeyValueLeafPage}.java`, `access/trx/node/AbstractResourceSession.java`,
`access/ResourceConfiguration.java`, `docs/ASYNC_COMMIT_DESIGN.md`.

## Summary

Four transferable axes, ranked by value × safety:

1. **Background the commit-time serialization/compression** (FSST + codec
   bake-off + PAX + LZ4) off the writer thread — the highest-value transfer, and
   already on the Sirix roadmap. Pivotal feasibility question: byte-reproducible
   historical reads after a deterministic background pass.
2. **Replace the single-threaded Gson/Jackson parser** with a SIMD/lazy
   front-end and morsel-parallelize the parse/pre-materialize layer.
3. **Batch into large commits** + a group-commit / write-behind buffer.
4. **Defer per-node maintenance** (hashing already has bulk mode; extend to
   secondary indexes and the path summary).

Plus a safe incremental buffer-manager win (LeanEvict eviction), and one clear
non-transfer (Umbra's ephemeral in-memory MVCC versioning).

---

## 1. Move commit-time serialization/compression off the writer thread
**Addresses bottleneck 4. Confidence: high (3-0). Highest-value transfer.**

DuckDB's core write-path idea is **optimistic writing with deferred/incremental
compression**: transaction-local data is streamed to new blocks and compressed
**per ~120K-row row group as it is appended**, not accumulated in memory and
compressed at commit; the compressed columnar layout is flushed at a **background
checkpoint**, not on the insert path (large changes reference the WAL instead of
routing all bytes through it).

For Sirix: move FSST training + codec bake-off + LZ4/deflate off the commit
critical path into a background pass, and incrementally flush filled
`KeyValueLeafPage`s during long ingests. `ASYNC_COMMIT_DESIGN.md` already scopes
this ("deep-copy the trie so FSST/codec runs off-thread") as future work.

**Safety flag (pivotal):** safe under bitemporal time-travel **only if** the
background compression pass is deterministic and leaves already-committed
revision content byte-reproducible. Prove this against the codec bake-off design
before building it.

Sources: [DuckDB PR #4996 (optimistic write / streaming compression)](https://github.com/duckdb/duckdb/pull/4996),
[DuckDB — analytics-optimized concurrent transactions](https://duckdb.org/2024/10/30/analytics-optimized-concurrent-transactions).

## 2. SIMD/lazy JSON parser + morsel-parallel front-end
**Addresses bottleneck 7 (and the parallelizable part of 1). Confidence: high (3-0).**

- **simdjson** — >4× faster than RapidJSON (25× vs nlohmann), ¾ fewer
  instructions; multithreaded NDJSON parsing exceeding **3 GB/s**; used on the
  ingest path of ClickHouse, Meta Velox, Apache Doris, StarRocks, QuestDB.
- **On-Demand (lazy) parsing** — ~70% faster than DOM simdjson (>2.5× yyjson),
  materializes values only on access, `string_view` thin wrappers + a reused
  parser instance minimize per-value allocation — a direct fit for Sirix's
  low-allocation shredding hot path. (Stage-1 SIMD structural indexing + UTF-8
  validation still scans the document once.)
- **DuckDB** parses with yyjson (ANSI C) and **parallelizes NDJSON reads across
  threads and across globbed files**, shredding nested JSON to STRUCT/LIST
  columns on load.
- **Morsel-driven** parallelism: parse/tokenize/pre-materialize nodes in per-core
  thread-local buffers, then feed the ordered insert phase.

**Safety flag:** the parse/tokenize/pre-materialize layers parallelize cleanly
and touch no invariants; the **ordered node-linking phase stays serial** (DeweyID
assignment, sibling pointers, rolling-hash have sequential document
dependencies). So this breaks the *parser* bottleneck, not the single-writer
ceiling. **JVM caveat:** needs a Java SIMD parser (simdjson-java / jsoniter) or a
JNI binding.

Sources: [simdjson](https://github.com/simdjson/simdjson),
[On-Demand parsing — Keiser & Lemire 2024](https://arxiv.org/pdf/2312.17149),
[DuckDB — Shredding Deeply Nested JSON](https://duckdb.org/2023/03/03/json),
[Morsel-Driven Parallelism — Leis et al. SIGMOD 2014](https://db.in.tum.de/~leis/papers/morsels.pdf).

## 3. Batch into large commits; add a group-commit / write-behind buffer
**Addresses bottlenecks 5 and 6. Confidence: high (3-0).**

ClickHouse's hard-won lesson: **many small synchronous inserts are pathological**
— each insert creates a part, causing "too many parts" errors and poor
performance; batch **10,000–100,000 rows** at roughly one insert per second so
background merges (deferred write work) can keep up. The mechanism differs from
Sirix's COW commits, but the *shared cost* is identical: small-frequent writes
each pay a structural amplification tax (a Sirix commit rewrites the indirect
spine to a new UberPage, analogous to part creation). ClickHouse **async inserts**
(server-side buffering) are the closest analog to a group-commit / write-behind
buffer for Sirix's single-writer model.

For Sirix: bias defaults toward larger batches (`beginNodeTrx(autoCommitNodeCount)`
with a high count, explicit single commit for bulk load), and design a
write-behind buffer that batches many nodes/documents into large commits while
preserving durability and crash recovery. Safe under all invariants.

Source: [ClickHouse — Selecting an Insert Strategy](https://clickhouse.com/docs/best-practices/selecting-an-insert-strategy).

## 4. Defer per-node maintenance during bulk load
**Addresses bottlenecks 2 and 3. Confidence: high (3-0) on the pattern.**

The morsel pattern's "separate per-core storage, insert pointers in a second
phase" and Umbra/HyPer's **deferred index build during load** map onto Sirix's
per-node maintenance fan-out. Sirix already does the hash half:
`repairBulkInsertHashes` runs **one postorder hash + descendant-count pass** over
the imported subtree instead of O(depth) per node. The extension is to defer
**secondary-index (CAS/Name/Path) and PathSummary construction** to a post-load
batch pass rather than per-node listener notifications — they are derived
structures, safe to rebuild, and not needed until the load completes.

Source: [Morsel-Driven Parallelism — Leis et al. SIGMOD 2014](https://db.in.tum.de/~leis/papers/morsels.pdf).

## Buffer manager (incremental, safe)

### LeanEvict eviction swap
**Confidence: high (3-0). Unconditionally safe.**

Replace clock-sweep with LeanStore's low-overhead strategy: speculatively
unswizzle random pages into a ~10%-of-pool cooling FIFO — near-LRU hit rate
(92.8% vs LRU 93.1% / 2Q 93.8% at 5 GB data / 1 GB pool, Zipf 1.0) at near-zero
per-access tracking. Safe because it only decides which *immutable* pages stay
resident; it cannot alter persisted revisions. (Pointer-swizzling's hot-page win
is *reduced* for Sirix because COW rewrites the indirect path each commit,
invalidating swizzled child pointers — see `storage-lessons.md` §6.)

Source: [LeanStore — Leis et al. ICDE 2018](https://www.semanticscholar.org/paper/LeanStore:-In-Memory-Data-Management-beyond-Main-Leis-Haubenschild/3590f4d44434c6a9d27441546cf6cbd003a632ce).

### Umbra monotonic-fill (validation) / variable-size pages (bigger redesign)
**Confidence: high (3-0).**

Umbra guarantees strictly monotonic tuple IDs and **completely fills nodes before
allocating new ones** (split-free inserts). Sirix already does this effectively —
doc-order monotonic node keys, 1024 records/page, the `mostRecentPageContainer`
short-circuit — so it is validation, not new work. Umbra's **variable-size pages**
(store large objects / dictionary tables natively and consecutively) are a
candidate for FSST symbol tables and large string values, but would require
redesigning the fixed 1024-fanout trie / fixed-record page model.

Source: [Umbra — Neumann & Freitag CIDR 2020](https://www.semanticscholar.org/paper/Umbra:-A-Disk-Based-System-with-In-Memory-Neumann-Freitag/88a0ce733f712e742733362623aa0a27710a5b9f).

## Does NOT transfer — recorded for the record

**Umbra's write-amplification win** — keep only the newest version in-place on the
page, and hold older versions as **ephemeral, in-memory, garbage-collected**
before-image deltas in per-transaction version buffers that are *never persisted*
— is fundamentally **incompatible** with Sirix. Umbra's old versions exist only
for MVCC isolation and are discarded once no active transaction needs them;
Sirix must **durably retain every historical revision** for bitemporal
time-travel. It is the cleanest contrast to Sirix's full COW page rewrite, but it
fails the immutability/durability transfer test.

Source: [Memory-Optimized MVCC — Freitag et al. VLDB 2022](https://www.researchgate.net/publication/364046355_Memory-optimized_multi-version_concurrency_control_for_disk-based_database_systems).

## The hard ceiling & open questions

The `Semaphore(1)` per-resource writer means single-document ingest is
single-core; only the parse/pre-materialize front-end parallelizes — the ordered
node-linking (DeweyID / sibling / hash) is inherently serial.
`ParallelJsonShredder`-into-separate-resources is the only intra-document
parallelism today, at the cost of fragmenting the tree.

- Can commit-time FSST + codec bake-off + LZ4/deflate move to a deterministic
  background checkpoint that produces **byte-identical** page content, so
  historical bitemporal reads stay reproducible? (Pivotal for §1.)
- What is the realistic morsel-parallel speedup ceiling for shredding given the
  ordered per-node dependencies?
- Can COW indirect-page write amplification be cut via partial-path rewrite or
  commit batching without breaking the fixed-fanout-1024 radix-trie addressing
  and UberPage/RevisionRootPage invariants?
- What is the right group-commit / write-behind design for the
  single-writer-per-resource model that batches large commits while preserving
  durability and crash recovery?

## Suggested sequencing

1. **Background phase-1 serialization/compression** — biggest win, already
   scoped; needs the determinism/byte-reproducibility proof first.
2. **SIMD/lazy JSON parser** (simdjson-java / jsoniter / JNI) + morsel-parallel
   parse layer.
3. **Larger-batch commit defaults + write-behind buffer.**
4. **Deferred secondary-index + path-summary build in bulk mode.**
5. **LeanEvict eviction swap** — low-risk buffer-pool improvement.

## Provenance & method

Five decomposed search angles, 25 primary sources fetched and deduplicated, 117
candidate claims extracted, per-claim 3-vote adversarial verification. 25 claims
confirmed, 0 killed, merged into the findings above — all high-confidence on the
underlying facts (peer-reviewed papers, official vendor docs, and the DuckDB PR).
The transfer-to-Sirix assertions are interpretive analogies grounded by the
write-path code walk, not source-attributed; treat them as engineering
hypotheses to benchmark, not settled results.
