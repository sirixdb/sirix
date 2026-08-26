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

- The remaining wall is the flush pipeline's depth-1 epoch chain (serialize and append of
  consecutive epochs never overlap); a two-generation intent-log pipeline is designed but not
  built.
- XML bulk import and projection maintenance riding the parallel load are planned follow-ups.
- Import into existing resources is out of scope for both loaders.
