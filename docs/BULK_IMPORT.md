# Bulk JSON Import

SirixDB ships two bulk loaders for building a **fresh resource** from JSON input, both producing
trees structurally identical to cursor-based insertion — same node keys, kinds, names, values,
pointers, child counts, path summary and reference counts. Equivalence is enforced by a
full-field differential test (`BulkAssemblyEquivalenceOracleTest`), not assumed.

## Sequential: `BulkJsonTreeAssembler`

```java
try (var session = database.beginResourceSession(name);
     var wtx = session.beginNodeTrx(autoCommitNodes, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
  BulkJsonTreeAssembler.assemble(wtx, reader);
  wtx.commit();
}
```

Append-only page assembly: every pointer is computed from a container stack *before* a record is
written, so leaves are created with final pointers and each container takes exactly one in-place
fixup at close. Node keys are predicted and asserted at every mint — a violated prediction is a
hard error, never a silent mis-pointer.

## Parallel: `ParallelBulkJsonImporter`

```java
ParallelBulkJsonImporter.assemble(wtx, inputStream);          // raw UTF-8 bytes (preferred)
ParallelBulkJsonImporter.assemble(wtx, reader);               // char sources (bridged)
```

For inputs whose **top level is a large array** — the shape of every bulk corpus — members are
independent subtrees, so the importer parallelizes: a writer-free feeder thread slices the raw
byte stream into member-aligned chunks and scans their metadata in one pass; the coordinator
resolves names and path-summary steps in document order, reserves each chunk's exact contiguous
node-key range, and dispatches builds to a bounded pool; workers emit **final record bytes** into
standalone pages (records are delta-encoded against their own key, so pre-reserved ranges need no
rebasing); the coordinator stitches page-sharing chunk boundaries and adopts whole pages into the
transaction. Any other input shape falls back to the sequential assembler automatically.

NDJSON (one JSON value per line) rides an exact adapter:

```java
ParallelBulkJsonImporter.assemble(wtx, new NdjsonAsArrayInputStream(inputStream));
// bounded prefix of a large corpus:
new NdjsonAsArrayInputStream(inputStream, recordLimit)
```

### Scope

Both loaders refuse, up front, configurations they do not faithfully reproduce: `hashType` other
than `NONE`, stored DeweyIDs, node history, a non-empty target document.

**Path statistics are built during the load** by both loaders. Observations are accumulated through
the very class the cursor path defers through (`PathStatsAccumulator`), so the classifier, the
NaN/infinity policy, the 128-bit integral sum, the fraction carry and the HLL hashing cannot drift
between the arms; the parallel importer collects one partial per (chunk, path) and the coordinator
merges them in document order. Every lane but `sumFraction` is order-free — end to end, not just
within a batch: the integral sum accumulates in 128 bits all the way into the persisted
`PathStats.sumHi`, so the same values persist the same statistics however many flushes or commits
the load takes (`PathStatsPersistedSumOrderIndependenceTest`). `sumFraction` is never served —
`BulkPathStatsDifferentialTest` compares four arms field by field after commit, close and cold
reopen, including exact HLL bytes.

The parallel importer **maintains PATH, CAS and NAME** index definitions during the load: each
chunk's build worker collects that family's tuples from the primitives it already holds, the
coordinator drains them per chunk into the families' ordinary bulk loaders, and one flush per family
materialises each trie before the caller's commit — the same entries the sequential path's per-node
change notifications produce, matching a sequential oracle's postings exactly across 20 probes, at a
measured +10.2% import cost for four real definitions (`:sirix-query:clickBenchPrimitiveIndexCost`).
The gate is postings equality rather than byte equality on purpose: a bulk-built trie is canonical
while an incrementally built one is insertion-order-dependent, so the two are read-equivalent but
not structurally equal — see
[`HOT_BULK_BUILD.md`](HOT_BULK_BUILD.md) §1. **Projection indexes are supported** too (see
below). Only **valid-time** interval maintenance is still refused, and the refusal is def-based with
an exact-family message: that family is resolved by a configured-path visitor over whole records,
which has no chunk-local equivalent yet. Mutating existing resources remains the cursor's job.

### One-pass projections riding chunks

A projection index declared BEFORE the data is built by the parallel load itself, in the same single
pass, instead of by a second full walk of the finished resource. One pass is also the FASTER route
(see the cost section below), and the index is byte-identical either way.

```java
try (var wtx = session.beginNodeTrx(nodes, AfterCommitState.KEEP_OPEN_ASYNC_FLUSH)) {
  session.getWtxIndexController(wtx.getRevisionNumber())
         .createProjectionIndexAtLoadStart(projectionDef, wtx, expectedRows);
  ParallelBulkJsonImporter.assemble(wtx, inputStream);
  wtx.commit();   // the final commit closes the build: dictionaries, fences, metadata
}
```

Rows are extracted WHERE THE DATA IS: each chunk's build worker fills a columnar row batch
(`ProjectionChunkRowBatch`) from the same primitives it writes into page bytes — field matching by
path class, cell classification by the row extractor's own shared helpers — so extraction costs one
hook per node instead of a per-record re-read through the transaction. The coordinator replays the
batches into the armed build in document order at adoption, and mints each record's structural-order
label through the directory's in-order append lane, which is pure label algebra: no document lookup
anywhere on the feed path. The rows are then PACKED by exactly the machinery the post-pass build
uses — leaves, dictionary election, fences, Bloom chunks, metadata — which is what makes the two
indexes identical rather than merely equivalent: `ParallelBulkProjectionEquivalenceTest` compares
them slot for slot (row-group descriptors and leaves, fence chunks and the physical-order header,
per-column Bloom manifests and chunks, set-summary chunks, the record locator, the structural-order
directory and the dictionary blobs) and fails on a single differing byte.

The in-flight representation is bounded in the same units as the storage path. Raw JSON chunks keep
the 4 MiB scheduling target but are chains of reusable 256 KiB slabs, decoded through a streaming
reader; there is no chunk-sized `byte[]`, chunk copy, or chunk-sized `char[]`. Projection row counts
also close a chunk before any row-indexed lane can exceed a 256 KiB payload. String bytes use 64 KiB
arena chunks, and pending record roots/ends use FIFO blocks with two 256 KiB primitive lanes. A very
large single record may link more slabs while it is processed, but excess slabs are trimmed before
the buffer returns to the pool. This keeps the general path non-humongous without reducing the
scheduling granularity that amortizes dispatch and page stitching.

Under the async-flush pipeline, a record's row is fed only once its whole subtree has entered the
intent log — the chunk's tail page is deliberately held out of the log until its successor's head
merges into it, and the feed drains against that adopted-key watermark. The batch rows themselves
read nothing back, so the feed discipline exists to keep document order and to land each row's
dictionary interns in the storage epoch its record was adopted in, not to keep pages readable; the
old design's second invariant — withholding the epoch's record accounting while a record straddled
the held tail page — is gone with the re-read that needed it.

Arming is optional and changes nothing when absent: an unarmed parallel import runs the same code
path as before, and the feeder does not even collect per-member node counts. A projection that is
catalogued but has NO load-time build armed for the transaction is refused rather than silently left
unmaintained.

The load-time build is **append-only**: rows are appended in document order exactly once, so an
update that reaches back into an already-indexed record fails loudly. Build the index with
`jn:create-projection-index` afterwards if the write order is not document order.

One reproduction caveat, deliberate and narrow: the read-back extractor's work-list visits nested
sibling subtrees in reverse order, while a worker streams document order. For any field matched at
most once per record — every gate corpus, and ClickBench — the two are indistinguishable. A
duplicate match poisons the cell identically in both orders; only the residual value bytes of such a
poisoned cell (and the element order of a set column fed by several arrays of one record) can
differ, and no consumer reads through a poisoned cell.

#### Cost: measured — one pass beats bare-then-post-pass by 2.8×

Three arms over the first 1M REAL ClickBench hits rows (2.36 GB JSON, array-wrapped), interleaved
bare/onepass/postpass per rep, minimum of 3, `-Dsirix.projection.globalDict=never`, `-Xmx6g` —
re-derivable through `:sirix-query:clickBenchProjectionCost` (arguments in
[`CLICKBENCH.md`](CLICKBENCH.md#import-and-decomposition-harnesses)):

| Arm | min | median | vs bare |
|---|---|---|---|
| parallel import, no projection | **12.54 s** | 12.88 s | — |
| parallel import, projection armed one-pass | **23.77 s** | 24.47 s | +11.2 s (+89.5%) |
| parallel import bare, then post-pass build | **66.98 s** | 67.42 s | +54.4 s |

Both projection arms produce the identical index — 977 row groups, live metadata, verified per rep.
**One pass is 2.82× faster end to end than loading bare and building afterwards**, at 1.9× the bare
import's wall. The previous design — the coordinator re-reading every record through the write
transaction at drain time — measured 497 s one-pass on this workload's synthetic twin, ~8× SLOWER
than the post-pass; moving extraction into the build workers and order labels onto the in-order
append lane removed the entire regression (the rotation witness's 147k-record one-pass arm fell
from 67 s to 3 s in the same change).

#### Why one pass also lifts a hard ceiling

A resource-wide value dictionary is appended in generations, and one generation admits at most
16,384 distinct entries. The post-pass build appends the whole corpus as a SINGLE generation, so any
elected string column with more distinct values than that kills the build outright — the standing
workaround being `-Dsirix.projection.globalDict=never`. A load-time build flushes a generation per
storage epoch, so generations rotate with the load. Measured on one corpus of 147,456 records with
36,864 distinct values in the elected column: the one-pass parallel load publishes a live
`STRING_GLOBAL` column over 144 row groups, while the post-pass build on the same data dies with
`append entry 16,385 exceeds the safe per-append limit of 16,384`.

This raises the ceiling from "the whole corpus" to "one storage epoch"; it does not remove it. A
column that is ~100% distinct still saturates a generation inside the dictionary's own 16-leaf
sample window, whatever the epoch size.

### Verification guarantees

- Per-chunk builds verify their reserved range exactly (final minted key == range end AND
  populated slots == reserved count) — a count/build divergence refuses loudly per chunk.
- With a projection armed, THREE independent derivations of the record roots must agree: the
  feeder's per-member node counts, the range arithmetic that names the chunk's last member, and the
  build worker's own parent-key record detection (checked per row at feed time). The build's
  end-of-load check additionally compares rows emitted against the record-set array's child count —
  so a load that mis-attributes records refuses instead of publishing a short index.
- The equivalence oracle compares parallel imports against sequential loads field-by-field,
  including path-summary reference counts, across adversarial chunkings (one-member chunks,
  boundaries mid-page, names first appearing in late chunks).

### Tuning

| Property | Default | Meaning |
|---|---|---|
| `sirix.parallelImport.builders` | `cores/4` | Build threads. Builds are cheap relative to the flush pipeline's parallel serialization — an oversized build pool *starves* it. |
| `sirix.parallelImport.chunkBytes` | `4 MiB` | Chunk size. Larger chunks reduce pipeline overlap; 4 MiB measured best. |
| `sirix.asyncFlush.maxLogEntries` | `16` | Intent-log entries per flush epoch. Bulk imports benefit from `128`–`256` (fewer epoch round-trips); the default is tuned for interactive writers. |
| `sirix.asyncFlush.maxNodeCount` | `16384` | Record-count epoch bound; raise together with `maxLogEntries` (e.g. `262144` with `256`). |
| `sirix.asyncFlush.parallelism` | `cores/2 - 1` | Serialize-pool width, shared JVM-wide. This is the load's throughput limit (see roadmap), but it is already at its optimum: on a 20-core box, 4 measured 15.2 s and 14 measured 14.0 s against 13.1 s at the default, because serializers past that point take cores from the insert threads. |
| `sirix.hft.telemetry` | `false` | Print per-run flush phase totals (`serializeJoinWaitTotalNs`, `kvlAppendTotalNs`, permit waits) at writer close. Off by default; the counters are the only honest way to attribute an import's wall between insert and flush. |
| `sirix.offheap.bytes` | `24 GiB` | Off-heap arena budget. **Cap this on smaller machines for large imports** (e.g. `8g` on a 32 GB box) — the default assumes a query-serving footprint. |

### Measured (20-core box, NVMe)

Arms selected through `:sirix-query:clickBenchParallelLoadProbe`; its switches are tabulated in
[`CLICKBENCH.md`](CLICKBENCH.md#import-and-decomposition-harnesses).

| Corpus | Sequential | Parallel |
|---|---|---|
| 1M ClickBench-shaped rows (2.0 GB JSON) | 30.4 s | **12.1 s** |
| 10M real ClickBench rows (gz-streamed NDJSON) | — | **119.6 s** (12.0 s/1M, linear) |
| 100M real ClickBench rows (full 23 GB `hits.json.gz`) | — | **1145.2 s** (11.5 s/1M, 87.3k rows/s) |

The 100M run is the full official corpus streamed through the NDJSON adapter: 99,997,497 records
(the corpus's exact row count) verified by post-commit read-back, ~10.6 billion nodes minted,
119.4 GiB on disk — scaling stays linear and per-row cost actually improves slightly as epochs
amortize.

The 119.4 GiB figure was measured on 2026-08-26 (`faff8547f`) with overflow payload compression
**off**, which was the default then. `sirix.page.overflow.compress` has defaulted to `true` since
`53f33cfa7`, so a current import writes fewer bytes; the run has not been repeated since the flip.
Treat 119.4 GiB as an upper bound for a fresh import at this scale, not as a comparable baseline
against a compressed build. The wall-clock and GC numbers on this page come from that same run and
were likewise not re-measured under the new default.

GC profile under the parallel importer: zero full collections at every scale. The current chunk
path uses the fixed-size slab/streaming representation described above, so it does not rely on
pooling multi-megabyte heap arrays to avoid allocation churn. Peak RSS for the 10M
import: 3.5 GB (heap 10 GB budgeted, off-heap capped at 8 GB). At 100M: 3,953 young pauses
totaling 4.4 s of a 1,145 s wall (0.38%), max single pause 3.7 ms.

### Known limits and roadmap

- The remaining wall is the flush's **parallel page serialization**, not the pipeline's depth.
  The depth-1 reading is accurate — consecutive epochs' serialize and append never overlap, and
  the insert side really does park on the flush permit for most of a load (8.8 s of a 12.1 s 1M
  import) — but the overlap that a two-generation intent log would buy is worth far less than
  that number suggests. Run with `-Dsirix.hft.telemetry=true` and the per-run phase totals say
  why: of 10.9 s of worker time across 395 epochs, **8.4 s is stalled on the serialize pool and
  2.5 s is the append**, and only the append is strictly serialized per writer. Overlapping the
  append is the entire prize.
  That prize was collected the cheap way to price it. The flush's sliding window already
  double-buffers serialize against append and simply never gets to, because its width is defined
  as the epoch size — one window per epoch. Sizing it independently pipelines several windows per
  epoch: the stall fell to 6.8 s and worker time to 9.7 s, and **wall time did not follow**
  (interleaved min-of-3: 12.047 s at one window per epoch, 12.412 s at four, 13.8 s at
  seventeen). Freed flush capacity goes straight back into contention with the insert threads —
  the same reason raising `sirix.asyncFlush.parallelism` to 14 measured slower end to end than
  the default 9 despite strictly less worker time. A multi-generation intent log aims at the same
  23%, against a class that has twice produced silent cross-generation use-after-free bugs; the
  serialize stage is where the load's throughput actually lives.
- PATH/CAS/NAME maintenance is **not streaming**: each family's bulk loader keeps its whole entry
  set resident (roughly `key bytes + 20` per indexed node) until the single flush at load end —
  the price of building each trie in one canonical pass. Selective definitions like the four
  measured above are cheap at any scale; a definition that indexes *every* node of a 100M-row
  corpus has no bound, and the flush cannot be split because a family is one tree. Budget for it
  or build that family afterwards. The unbuilt options are in
  [`HOT_BULK_BUILD.md`](HOT_BULK_BUILD.md) §3.
- XML bulk import is a planned follow-up. Projection, PATH, CAS and NAME maintenance riding the
  parallel load has landed (see above); valid-time is the one family that still refuses.
- Import into existing resources is out of scope for both loaders.
