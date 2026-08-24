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

### Scope (v1)

Both loaders refuse, up front, configurations they do not faithfully reproduce: `hashType` other
than `NONE`, stored DeweyIDs, node history, path statistics, a non-empty target document. The
parallel importer additionally refuses primitive-index maintenance during the load. Mutating
existing resources remains the cursor's job.

### Verification guarantees

- Per-chunk builds verify their reserved range exactly (final minted key == range end AND
  populated slots == reserved count) — a count/build divergence refuses loudly per chunk.
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

| Corpus | Sequential | Parallel |
|---|---|---|
| 1M ClickBench-shaped rows (2.0 GB JSON) | 30.4 s | **12.1 s** |
| 10M real ClickBench rows (gz-streamed NDJSON) | — | **119.6 s** (12.0 s/1M, linear) |

GC profile under the parallel importer: zero full collections; chunk buffers and decode scratch
are pooled, so no humongous-allocation-triggered cycles. Peak RSS for the 10M import: 3.5 GB
(heap 10 GB budgeted, off-heap capped at 8 GB).

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
- XML bulk import and projection maintenance riding the parallel load are planned follow-ups.
- Import into existing resources is out of scope for both loaders.
