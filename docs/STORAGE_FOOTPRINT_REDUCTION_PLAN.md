# Storage Footprint Reduction Plan

Status: measurement-gated design, 2026-08-28. No storage-code change is authorized by this
document. The 100M ClickBench correctness and HFT work remains the current gate.

## 1. Non-negotiable constraints

1. **Lightweight encoding remains the query representation.** Numeric FOR/truncation/ALP, packed
   dictionary ids, presence bitmaps, zone maps and the keyed-trie global dictionary must remain
   directly consumable by the existing SIMD kernels. General-purpose compression must never replace
   these encodings.
2. **Every mutation stays incremental.** An insert, update, delete or move may rewrite only the
   smallest existing copy-on-write unit that contains the changed information, plus its bounded
   routing/descriptor metadata. No complete projection, complete column, complete dictionary or
   complete database rebuild is an update mechanism.
3. **All `VersioningType`s remain correct.** Unchanged projection segments and trie nodes remain
   referenced from prior revisions. A storage wrapper must not make `FULL`, `INCREMENTAL`,
   `DIFFERENTIAL` or `SLIDING_SNAPSHOT` silently share mutable state or read a different logical
   payload.
4. **HFT constraints apply to ingestion and maintenance.** No per-row compression allocation, no
   unbounded work queue, no humongous heap buffer, and no major/full/mixed collection. Compression
   happens only after a bounded unit has been encoded and uses reusable bounded native/chunked
   workspace.
5. **Savings must be demonstrated.** A codec is stored only when its net persisted size, including
   its envelope, is at least 20% smaller than RAW. Codec choice is per unit, not a database-wide
   belief.

## 2. What CedarDB demonstrates—and what it does not prove for Sirix

CedarDB's 2026 article, [Encoding or Compression: Why not
both?](https://cedardb.com/blog/encoding_vs_compression/), separates the two layers:

- dictionary, single-value, frame-of-reference and truncation encodings are the in-memory/query
  representation and can often be scanned without materializing decoded values;
- zstd is optionally applied *after* encoding, only to cold persisted bytes;
- CedarDB uses the general compressor only when it saves at least 20%; and
- on its 100M ClickBench `hits` table, zstd reduced the already encoded 21.4 GiB to 7.88 GiB
  (2.72x).

The follow-up string design is similarly layered: dictionary ids remain the comparison domain and
FSST compresses dictionary values, preserving integer predicate execution. See [Efficient String
Compression for Modern Database Systems](https://cedardb.com/blog/string_compression/).

This validates the *architecture*, not an expected Sirix ratio. Sirix already ran the corresponding
offline experiment over 53 MB of lightweight-encoded projection segments: zstd level 9 produced
0.91x, while decode cost 3–4x LZ4. That negative is recorded in
`docs/BENCHMARK_CAMPAIGNS.md` §4.9. The likely reasons are smaller independent Sirix segments and
encodings that have already removed most byte-level redundancy. Therefore the primary JSON store,
each projection segment kind, dictionary payloads and framing must be measured separately.

## 3. Establish the byte ledger before changing the format

The database file is an append-only mixture of the primary versioned JSON tree, the projection
index, dictionaries, HOT routing pages and revision/catalog metadata. File size alone cannot say
which layer is responsible.

Run a representative 1M corpus first and the completed 100M corpus second with:

- `-Dsirix.storage.profile=true` for serialized and persisted bytes per page class;
- `-Dsirix.pageSectionDiag=true` for `KeyValueLeafPage` header/body/PAX-region attribution; and
- an extended projection probe that totals descriptor, KEYS, BODY, DICT, bloom, fence, global
  dictionary and overflow payload bytes by segment kind and column.

The profiler must close at least 99% of `sirix.data` growth after record framing/alignment is added.
For each candidate unit it records count, raw encoded bytes, persisted bytes, p50/p95/max unit size,
cardinality/bit width where applicable, and the current lightweight scheme.

The offline codec matrix then tests the exact persisted encoded bytes with:

| Candidate | Purpose |
|---|---|
| RAW | Direct-access baseline; always available |
| existing inner scheme | Separates encoding gain from general compression gain |
| LZ4 fast / HC | Existing low-latency general compressor |
| zstd levels 1 and 3 | Cold-storage candidate; higher levels are not ingestion candidates |
| FSST dictionary payload | String-dictionary-specific candidate, not a whole-segment wrapper |

Report size including envelope bytes, compression MiB/s, single-core and parallel decompression
MiB/s, allocation count/bytes, and cold/hot scan time. Concatenating unrelated segments before
compression is not a valid result because it would hide update amplification.

## 4. Proposed persisted representation

### 4.1 Projection index

The existing `(rowGroupId, slotKind)` segment remains the maintenance and copy-on-write unit. Wrap
the already lightweight-encoded segment in a small, self-describing storage envelope:

```text
u8  codec       // RAW, LZ4, or ZSTD
u32 logicalLen  // bounded and validated before allocation
u32 storedLen
u8[storedLen] payload
```

The descriptor's content hash continues to cover the **logical encoded bytes**, so an identical
re-encode remains a no-op regardless of codec choice. The normal page checksum covers the exact
stored envelope. RAW exposes its payload as the current byte/`MemorySegment` slice. A compressed
unit is inflated once into a bounded reusable native frame and cached in its ordinary logical
encoded form; every existing SIMD kernel then sees exactly the same bytes it sees today.

Compression is selected separately for KEYS, BODY, DICT and auxiliary segments. Tiny segments and
high-entropy columns stay RAW. Large logical segments are framed in independently bounded chunks so
no heap array crosses the HFT allocation cap; the row-group column segment remains the mutation
unit.

### 4.2 Primary JSON pages

Do not blindly enable the legacy whole-page outer pipeline. It can defeat ranged region reads and
forces unrelated regions to be decompressed together. Measure and apply a storage wrapper at the
existing independently addressable encoded-body/PAX-region boundary. Zone maps, directories and
routing metadata remain readable without inflating value bodies.

The existing per-region lightweight encodings stay first. General compression is eligible only for
a region whose measured net saving clears the threshold. This is the most plausible source of a
whole-database gain because the earlier zstd negative covered projection segments, not the complete
primary tree.

### 4.3 Global dictionaries

The keyed trie remains the lookup structure and is updated by bounded append generations. Its keys,
routing metadata and integer ids must remain directly searchable; compressing the whole trie would
turn one lookup into a block inflate and is rejected.

Measure these independently:

- packed row id streams (normally keep directly executable);
- distinct UTF-8 value payloads (FSST, and optionally outer compression if it still saves 20%);
- keyed-trie leaf/bucket framing; and
- repeated generation metadata.

An insert of a new distinct value appends only its bounded dictionary generation and patches the
header/trie path. Existing generations are never rebuilt or renumbered. Deletes do not renumber ids;
revision reachability and existing reference sharing remain unchanged.

## 5. Mutation and revision behavior

For a changed record:

1. locate its row group through the existing record locator/order structures;
2. re-extract only affected projected columns;
3. encode and, if profitable, wrap only each affected BODY/DICT segment;
4. persist only those segment slots and the bounded descriptor/fence/locator units they change; and
5. carry every untouched reference forward exactly as today.

An insert at the document tail, an ordered middle insert, an update, a delete and a move must each
have a test asserting both results and write locality. The assertions count touched row groups,
column segments, descriptor chunks, locator entries and global-dictionary generations. A format
wrapper is not allowed to turn any of them into a projection rebuild.

`FULL` may emit the full *versioned page fragment* required by its established semantics, but it
must still carry unchanged referenced column segments rather than duplicate their payloads.
Non-FULL types must preserve their existing sparse fragment behavior. Cold reopen at every revision
is part of the differential, not an optional smoke test.

## 6. HFT implementation rules

- Codec work begins only after a row-group/page unit is complete; never on the per-value path.
- Reuse per-worker native/chunked input and output frames. Reject a corrupt `logicalLen` before
  reserving memory.
- Bound compression jobs by the existing serializer/append queue; no second unbounded executor.
- A failed or unprofitable compression attempt writes RAW without retaining both representations.
- Codec sampling must be deterministic and bounded. No background whole-store cooling or compaction
  is required for correctness or normal updates.
- The load gate requires zero concurrent-mark, mixed, full, evacuation-failure, pinned and humongous
  events in the measured region. Throughput alone cannot waive this.

## 7. Acceptance gates and sequence

1. Finish the 100M correctness baseline and compare all 43 results with DuckDB.
2. Fix the general ingestion retention spike and the global-dictionary build front; establish a
   clean no-major-GC AUTO run.
3. Produce the closed byte ledger and codec matrix. Do not implement a general codec before this.
4. Add the projection storage envelope behind focused RAW/LZ4/zstd differential tests. Ship a codec
   for a segment class only if its aggregate size and latency gates pass.
5. Apply the same measured process to primary encoded regions and dictionary value payloads.
6. Run insert/update/delete/move and cold-reopen differentials for every `VersioningType`, followed
   by the complete core/query suites and all 43 ClickBench queries.

Initial pass/fail bars:

- 43/43 query outputs equal DuckDB after normalization;
- byte-identical logical encoded payloads before and after the storage wrapper;
- no query hot-path regression for RAW and no hot-cache regression after a compressed cold read;
- no more than 10% ingestion-throughput loss for a default-on codec;
- at least 20% net saving for every stored compressed unit and a material whole-database saving;
- no new allocation above 256 KiB payload on the heap and no major/full/mixed GC; and
- mutation locality counters unchanged from the uncompressed representation.

The eventual database-size target must be set from the completed, attributed 100M baseline rather
than from DuckDB's table size alone: Sirix also persists the versioned primary JSON tree and
incrementally maintained projection. The comparison remains useful, but only after those components
are reported separately.
