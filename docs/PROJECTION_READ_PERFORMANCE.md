# Projection read performance

These read paths depend on storage metadata, column encodings, and query semantics. They do not
recognize benchmark names, query numbers, particular field names, or dataset values. Existing
revisioned storage formats and fine-grained mutation paths remain unchanged.

## Storage reads

`FileChannelReader` captures the data-file size once for a batch of durable references. It refreshes
that bound for every batch, so a later committed append is visible to the same reader. Declared
length validation, short-read detection, and configured checksum verification remain in place.

A bounded prefix read can fetch a small page's header and body together. The checksum and
deserializer receive exactly the declared body, excluding following bytes. Larger pages fetch
their remaining body through the existing pooled buffer. With batch-input borrowing enabled,
coalesced runs decode bounded views from the exclusively owned buffer when the byte pipeline
supports memory segments. Stream-only handlers
retain their page-copy path. Checksum views have independent positions and exact per-page limits.

With an empty byte pipeline and input borrowing enabled, overflow decoding borrows its serialized
input directly: both raw and compressed overflow decoders finish with independently owned payload arrays. This removes a
temporary native-frame allocation, copy, and release. Other page kinds still use the owned-buffer
pipeline because record and HOT pages can retain that storage. Configured byte handlers always run.

Batches with multiple page references can submit Linux `POSIX_FADV_WILLNEED` hints for upcoming
requested offsets before reading and decoding the current run: one hint per coalesced run, covering
the run plus a bounded tail for its last page. The default window of 1,024 offsets hints an ordinary
batch whole, up front, with 32 KiB tails. The advisory `prefetch` route that HOT, directory and
column-store scans use hints sorted offsets the same way; a page followed closely by another is
hinted exactly up to it. Hints do not queue Java tasks, retain page objects, or change the normal
integrity checks. Descriptor discovery is lazy; single-page readers and unsupported platforms keep
ordinary reads. Whole-file access hints remain separate opt-in policies.

Scratch buffers use a bounded unordered pool. Padded atomic slots transfer exclusive ownership
without a common queue lock or per-thread retention. Each operation makes one bounded pass over
the slots. An exhausted pool can allocate a temporary buffer; a full pool discards returned extras.
Returned pages retain independently owned data, never a borrowed pool view.

Frame recycling scans only the bitmap prefix that can contain previously allocated slots. Each
bounded retry refreshes the monotonically advancing allocation limit; the scan wraps with an
increment and comparison. Atomic free-bit claims, count-credit fallback, and slot versions still
control ownership.

Fresh immutable projection side pages can accumulate in a separate bounded native window before
append. Their HOT parents remain pinned, so ordinary record snapshots can flush without scattering
each column's payloads. A reusable primitive permutation groups writes by index and segment kind;
references and result offsets retain their original positions. Immediate record dependencies use
the original queue. Both queues share one append owner and validate completed writes before either
publishes offsets. Explicit final drains include both queues. The default lazy active/frozen pairs
reserve 128 MiB for immediate pages and 8 MiB for grouped pages; larger individual payloads use the
immediate path. Existing formats, logical commits, and incremental mutations are unchanged.

## Projection execution

- Packed scalar dictionary predicates compare encoded IDs in word-sized groups. Dense consumers
  can materialize an immutable `int[]` representation on demand. Presence masks, partial final
  words, and string comparison semantics still apply.
- Numeric grouping of `(value + offset) / divisor`, optionally followed by a modulus, can reuse
  representative values when verified metadata proves one quotient for a block. The derived
  slices are local to the query and never replace ordinary cached column values. Overflow,
  unproven blocks, and exhausted representative caches take the ordinary decode path. When extra
  representative scratch space does not fit, an ordinary column fill remains available at its
  original budget.
- Sorted grouped extrema can read summary windows through independent readers at the same
  committed revision. Windows remain bounded to 1,024 summaries, and folding preserves key order.
  Each worker reuses one trie reader across its bounded summary window; payloads still use the
  ordinary scalar read and per-blob length/hash checks. Writer transactions retain the serial path.
  Started workers finish and close their readers
  before failures propagate; existing tie and unsupported-shape fallbacks remain intact.
- Grouped span top-K scans can visit leaves by a conservative upper bound. Equal first-group keys
  in the directory identify groups crossing leaves; combined revisioned leaf extrema bound their
  complete spans. A crossing group needs only its edge summaries and single-group interior bounds.
  The cutoff comes from completed groups, retains K+1 entries, and uses a strict comparison so ties
  still reach the existing fallback. Endpoint division remains separate and exact-score overflow
  still fails. A packed key arena, primitive candidate arrays, and 128 cached summaries bound memory
  below 48 MiB. Views above 262,144 leaves or 16 MiB of first-group keys keep the existing scan.
  Larger views also fall back after a bounded number of summary-cache misses when pruning is poor;
  missing acceleration metadata falls back, while malformed metadata is rejected. Writer-local
  queries retain their existing serial route.
- Bloom pruning stays serial when physical chunks can write different bits in the same logical
  output word. Parallel execution requires disjoint bitmap-word ownership.
- Narrow packed integers decode eight IDs from each loaded word. General widths and partial tails
  preserve the existing encoding and bounds.
- Composite dictionary grouping reuses one bounded string-identity proof cache per worker and
  pass across sequential subchunks. Canonical byte comparison and the registry's terminal collision
  check remain active. Rebinding to another registry invalidates cached proofs; there is no
  thread-local retention, and fully pre-proven components need no cache.
- Dictionary counting has a small, separately compilable row loop. It preserves counts, missing
  groups, and first-row ordinals. Cold JVM compilation and fully warmed execution must be measured
  separately when changing these method boundaries.
- Composite COUNT over numeric, temporal and global dictionary ID keys has a smaller primitive
  loop. It loads presence words once per 64-row block and compares complete identity tuples after
  hashing. Conditional global keys use the executor-resolved literal ID; missing fields and absent
  conditional alternatives keep their distinct identity markers. Local dictionaries, segment IDs,
  arithmetic/string transforms, aggregates and DISTINCT retain the general kernel.

Cached BODY bytes pass the store's checksum and descriptor gate before a verified-body capability
is created. Temporary masked buffers go directly to checked decoders. These paths remove repeated
verification of the same immutable bytes without admitting unchecked data into shared caches.

## Controls

Set system properties before opening resources. Defaults apply to both JVM execution and native
executables; the bounded overrides also support controlled comparisons on other workloads.

| Property | Default | Effect |
| --- | --- | --- |
| `sirix.filechannel.pagePrefixBytes` | `1024` | Prefix bytes, clamped to 4–65,536; 4 restores a separate header read. |
| `sirix.filechannel.coalesceGapBytes` | `65536` | Maximum gap between adjacent references in a coalesced run. |
| `sirix.filechannel.batchReadAhead` | `1024` | Requested-offset hint window, clamped to 0–4,096; 0 disables advice. |
| `sirix.filechannel.batchReadAheadBytes` | `32768` | Tail hinted past a page whose length is unknown, clamped to 4 KiB–1 MiB. |
| `sirix.filechannel.prefetchBatch` | `32` | References per advisory `prefetch` batch, clamped to 0–1,024; 0 disables the advisory route. |
| `sirix.projection.reuseWorkerProofCache` | `true` | Retain worker proof caches across subchunks; false restores invocation-local caches. |
| `sirix.filechannel.batchFileSize` | `true` | Reuse one allocation bound within each batch. |
| `sirix.filechannel.lockFreeBuffers` | `true` | Use atomic buffer slots; false restores the bounded queue. |
| `sirix.io.borrowOverflowInput` | `true` | Borrow overflow input only for an empty byte pipeline; false restores the owned temporary frame and, unless the batch switch is set, owned batch input. |
| `sirix.filechannel.borrowBatchInput` | value of `sirix.io.borrowOverflowInput` | Decode coalesced page views while the read buffer remains exclusively owned; an explicit value overrides the overflow switch. |
| `sirix.projection.batchPhysicalOrder` | `true` | Batch fence reads for bounded, dense committed physical orders. |
| `sirix.projection.overlapDirectoryLoad` | `true` | Overlap dense committed document-order and column-descriptor reads using independent revision-bound readers. |
| `sirix.projection.columnDirectoryWorkers` | `32` | Column-major descriptor-walk worker ceiling, clamped to 1–64 and further limited by CPUs and one worker per 1,024 leaves. |
| `sirix.projection.parallelWalk` | `false` | Opt in to the parallel row-group-major directory walk; the serial cursor is the default. |
| `sirix.projection.prefetchAll` | `false` | Opt in to the background read-ahead sweep of every sliceable column on the first projection lookup. |
| `sirix.projection.bloomFetchWindowChunks` | `16` | Bloom chunk payloads fetched per ranged read of one pruning call, clamped to 1–64. |
| `sirix.projection.coalesceBlobBatches` | `true` | Coalesce bare durable blob offsets after capturing verified leaf state. |
| `sirix.projection.packedStringSlices` | `true` | Keep eligible scalar dictionary IDs packed until needed densely. |
| `sirix.projection.constantBucketSlices` | `true` | Enable query-local numeric grouping representatives. |
| `sirix.projection.numericProofs` | `true` | Read optional revisioned full-presence numeric bucket proofs. |
| `sirix.projection.packedDictionaryCounts` | `true` | Count eligible packed dictionary IDs without a dense ID array. |
| `sirix.projection.longLaneCompositeCounts` | `true` | Count composite keys with exact long identities in a separate primitive loop. |
| `sirix.projection.dictionaryCountBatches` | `true` | Accumulate plain local-dictionary COUNT groups in reusable per-entry counters before folding the group table. |
| `sirix.projection.parallelSortedSummaries` | `true` | Permit independent readers for committed sorted summaries. |
| `sirix.projection.batchSortedSummaries` | `true` | Reuse traversal workspace within each summary window; false restores per-blob readers. |
| `sirix.projection.sortedSummaryMaxWorkers` | `4` | Worker ceiling, clamped to 1–8 and further limited by CPUs and one worker per 1,024 leaves of the queried key range. |
| `sirix.projection.sortedLookahead` | `8` | Candidate leaves the sorted bound walk and span scan fetch as one batch, clamped to 1–64; 1 restores one read per step. |
| `sirix.projection.sortedSpanBounds` | `true` | Enable bounded best-first grouped spans on committed revisions. |
| `sirix.projection.heapSpanPriority` | `true` | Reuse the candidate permutation as a max-heap for span views with at least 1,024 leaves. |
| `sirix.projection.denseSpanBounds` | `true` | Index validated dense physical leaf bounds directly; sparse IDs retain binary search. |

The retained-buffer population and summary windows are bounded; decoded column fills also obey the
existing projection memory budget. Input properties do not weaken revision or integrity checks.

Build and write controls apply while a projection is built or a load flushes pages:

| Property | Default | Effect |
| --- | --- | --- |
| `sirix.projection.sortedRun.budgetBytes` | `min(heap/16, 512 MiB)` | Heap ceiling for resident sort keys while a sorted view is built; larger builds spill sorted runs. |
| `sirix.projection.sortedRun.spillDirectory` | `projection-sort-spill` in the resource's directory | Root for the spilled runs of a sorted-view build; each resource spills into its own subdirectory, named after the resource and a digest of its path. |
| `sirix.asyncFlush.groupSidePages` | `true` | Group fresh immutable side pages whose parents remain independently pinned. |
| `sirix.asyncFlush.sideGroupTargetBytes` | `4194304` | Positive grouped-window byte limit, capped by the ordinary side-page limit. |
| `sirix.asyncFlush.sideGroupTargetCount` | `1024` | Positive grouped-window page limit, capped by the ordinary side-page count limit. |

## Validation entry points

`FileChannelReaderBatchBoundsTest` checks body boundaries, checksum rejection, buffer reuse,
append/truncation visibility, and stream-handler compatibility. `FileChannelBufferPoolTest` checks
bounded retention, concurrent ownership, and memory publication with heap and direct buffers.
`FileChannelReadAheadTest` exercises real files, caller order across advisory windows, appends,
corruption and truncation, plus unsupported-channel and invalid-hint handling.
`ProjectionStringIdentityRegistryTest` forces fingerprint collisions, including after cache reuse
and registry changes. `FrameSlotAllocatorTest` checks concurrent ownership, recycled-bitmap wrap,
and stale versions.
`BorrowedOverflowDecodeTest` checks raw and compressed input lifetime on heap/native storage,
configured handlers, malformed payloads, and the diagnostic off switch. Mixed HOT/overflow batch
tests also overwrite both span and final-body buffers before reading previously returned pages.
`ConstantBucketGroupCountTest`, `PackedDictionaryPredicateTest`, and `PackedIntegerWordDecodeTest`
use independent result or encoding oracles. `ProjectionSortedGroupSummaryTest` exercises revision
history, edits, asynchronous flushes, and worker cleanup. `ProjectionBlobBatchReadTest` checks
ordered sparse and duplicate requests, inline/overflow transitions, writer-local state, historical
revisions, and per-payload integrity while reusing traversal workspace.
`LongLaneCompositeCountTest` checks 1–64 numeric/temporal keys, conditional global IDs, missing
values, partial bitmap words, predicate trees, forced hash collisions, partition merges, first-seen
ordinals, discard handles, metadata errors and checked-transform fallbacks against row oracles.
`DictionaryCountBatchTest` checks dictionary cardinalities, dense/packed ID paths, missing values,
predicates and trees, discarded partitions, reused scratch, and earliest source ordinals against a
row oracle. Merged representatives must decode to the exact group key; the first-seen ordinal is
checked independently because an existing table merge may retain any valid key representative.
`ProjectionSortedSpanScanTest` compares an independent grouped oracle across split/deleted leaves
and historical revisions, and checks ties, signed division, overflow, pruning, missing metadata,
corruption and unselective fallback. `ProjectionSidePageLocalityTest` checks physical grouping
across mixed record snapshots and reopened edits for both slot layouts and all versioning types;
`AsyncFlushFailurePathTest` covers both native queues, their bounds, rollback and append failures.

Measure cold data-cache reads, fresh processes with warm data caches, and repeated executions within
one process separately. Run profilers separately from ranked timing. Retain exact-result and query
serving-route comparisons alongside elapsed time, allocations, GC, and filesystem input.

## Dense physical-order reads

Fence chunks still contain 32 physical leaves, keeping a fine-grained write local to a small stored
unit. A committed order with equal live and physical counts must visit every chunk in any valid
permutation. For 1,024 through 1,048,576 leaves, the reader fetches windows of 64 chunks and retains
only packed predecessor/successor links. Additional scratch is bounded by 8 MiB of links plus one
488 KiB window of decoded chunks; the output permutation is also required by the scalar reader.
Owner ranges, lengths, links, slot bounds, tail and total live count remain checked. Sparse orders,
smaller/larger orders and writer readers keep the existing demand-driven traversal.

Opaque blob batches capture marker bytes after validating each HOT leaf's stamp. Bare durable
offsets can then use the backend's coalesced reads; references carrying a checksum or an in-memory
page retain scalar reads. Every payload still requires its own marker length and content hash.
Writer-local blob reads retain their previous lifecycle. Coalescing can read gaps between requested
pages within the existing bounded span/gap policy, trading some bandwidth for fewer serial reads.

`ProjectionPhysicalOrderBatchTest` checks bounded batches, reordered document links and historical
revisions under every versioning strategy, semantic corruption, and a sparse order whose unused
physical chunk is missing. `ProjectionIndexFencesTest` retains the unchanged-chunk sharing checks
for local updates. `ProjectionBlobBatchReadTest` exercises scalar/coalesced integrity checks, mixed
inline/overflow windows, duplicate and missing slots, writer state, and reopened history.

## Sorted views

A projection may declare a sorted view: every projected record, ordered by a list of key columns
and then by record key, like a table's `ORDER BY` key. The declaration names columns only
(`ProjectionSortedSpec`, the load-time `ProjectionSpec`, or the optional fifth argument of
`jn:create-projection-index`); it carries no filter literal. A grouped extremum whose string equality
filter names exactly a leading run of the key columns, grouping by the next key column and
aggregating the last one, is served from the one contiguous key range those literals encode. Every
route — per-leaf group summaries, leaf bounds, the span scan and the full-key scan — is restricted
to the leaves that can hold that range; boundary leaves contribute only their in-range groups, and
their wider bounds only weaken pruning. Equality columns must be string columns, so a literal
compares as the interpreter compares it; any other shape falls back to the generic route.

A row whose key cannot be represented exactly (an unrepresentable or non-integral cell, or a whole
encoded key longer than 4 KiB, record key included) is still stored, under a reserved key that
sorts after every ordinary key. The directory header counts such rows, and the sorted route
declines — the generic route then answers — while the count is nonzero. The key bound keeps room for
at least fifteen keys in every leaf and directory node. The header also records the view's key
layout. If the configured column kinds no longer produce that layout (for example after a
`sirix.projection.temporalKinds` change for a timestamp key column), maintenance writes every row
it touches under the reserved key rather than mixing two encodings, so the view declines until
those rows are rewritten under matching kinds. This has a write cost: in that state, every commit
that touches records still stored under their old key scans the view in key order until it has
found them, which on a large view can mean reading most of it. Results stay exact. An index
declaration never rejects a valid document; a full or unwritable disk fails the load as any write
does.

When the aggregated field is optional, a query over a prefix range holding rows without that field
may walk the range up to twice before it falls back to the generic route: once through the leaf
summaries and once key by key. Results stay exact; only such queries pay the extra walk.

Memory and maintenance cost:

- The initial build keeps at most `sirix.projection.sortedRun.budgetBytes` of keys and references
  on the heap. Beyond that it sorts the resident run, writes it to a run file in its own build
  directory under the resource's spill directory and reuses the blocks. That directory is
  `projection-sort-spill` inside the resource's directory, never `java.io.tmpdir`;
  `sirix.projection.sortedRun.spillDirectory` replaces its root and keeps one subdirectory per
  resource. Runs pass through the resource's byte handlers exactly as its pages do, so a resource
  configured with the `Encryptor` never writes a plaintext sort key to a run. Publishing merges the
  runs with one 128 KiB buffer per run (plus a 64 KiB stream buffer for stream handlers) and encodes
  leaves as keys stream past. Run files are deleted after the merge or when the build is released or
  aborted. A run file that cannot be deleted is logged and never fails the build or masks another
  failure; like the runs left by a process that died, it is deleted the next time the resource is
  opened, while live builds in this or another running process are kept. An I/O error while
  spilling or merging fails the load with an error naming the spill directory and its cause.
- Per commit, maintenance takes each touched record's old key from the key it last wrote for the
  record in the open transaction. That memory survives queries served inside the transaction,
  failed commits and the drop of another index. Otherwise it derives the key from the revision the
  writer represents (after `revertTo`, the reverted-to revision). It uses that key directly, with
  no read, when the view already existed at that revision with the current layout and no reserved
  rows. Otherwise the derived keys are checked against the view in one key-ordered pass that reads
  each leaf once. Any record the view does not hold under its derived key, such as one whose row was
  built inside the open transaction, is found with one ordered scan. The commit's removals and
  insertions are applied as one sorted batch: each touched leaf is rewritten, and its group summary
  re-encoded, once; each touched bounds chunk is copied and published once. A leaf that overflows
  splits into balanced leaves, and only directory nodes whose entries change are rewritten.

## Sorted span candidate priority

For span views with at least 1,024 leaves, `ProjectionSortedSpanScan` builds a primitive max-heap
inside the existing candidate permutation. It removes only the highest remaining upper bound until
the unchanged strict K+1 cutoff proves the remaining leaves cannot win. Smaller views keep indirect
sorting. Heap ordering can change the visit order of equal-bound leaves; the exact tie fallback and
summary-read budget still apply. Fewer comparisons do not guarantee fewer reads, so both elapsed
time and filesystem input belong in the comparison.

After `readUnordered` validates sorted, unique positive physical IDs, endpoints 1 and N prove the
N-entry bounds array covers the complete dense range. Such views can use `leafId - 1`; all other
views keep binary search, and missing IDs still raise. Neither change allocates another candidate
array or changes stored formats, revision visibility, write granularity or memory ceilings.

`ProjectionSortedSpanScanTest` exercises both switches in all four combinations against independent
row folds, including large cross-leaf groups, deletions, inserted splits, reordered histories, signed
division and different K values under every versioning strategy. An unselective tied view checks
that both priority algorithms retain the existing bounded fallback.

## Overlapping committed directory loading

`ProjectionDirectoryLoad` can overlap descriptor discovery with document-order loading for committed
column-major views whose validated order header declares equal physical and live leaf counts. It
uses the same 1,024–1,048,576-leaf bound as dense physical-order reads. Descriptors can be discovered
in physical-ID order before the complete document permutation is available; they are reordered only
after the full chain and every descriptor have been validated. The additional identity and reference
arrays are bounded by 12 MiB, with no persisted representation change.

Every worker leases an independent reader at the caller's exact revision. All started work is joined
before returning or propagating failure, and worker readers close within their lease. Small, sparse,
oversized and writer-local views retain existing loading behavior. Cached handles bypass this work
on repeated in-process queries. The property can disable overlap for controlled latency and CPU
comparisons; a lower elapsed load time need not imply less total CPU work.

`ProjectionDirectoryLoadTest` compares descriptor bytes, physical order and independent encoded
oracles across every versioning strategy, cold historical reopen, reordered and sparse leaves. It
also checks actual input overlap, reader independence, corrupt links/descriptors, orphan and missing
leaves, parameter bounds and cleanup before failures return. Existing directory and fence tests
continue to cover the underlying formats and local-update sharing.

## Optional numeric bucket proofs

`ProjectionNumericProofs` stores a 64-row-group chunk of canonical numeric bounds, full-presence evidence, row count, BODY length, and BODY content hash. The initial builders derive this evidence from verified integral BODY segments. Chunks occupy a separate opaque slot region beyond sorted-leaf metadata; existing descriptors, per-column segment ids, and old-reader formats are unchanged.

`ProjectionColumnStore.numericBucketKeyColumn` may use these chunks only for its query-local grouping view. Signed offset arithmetic must not overflow, and the endpoint quotients must agree before applying a modulus. Missing, sparse, unsupported, or changed entries read their ordinary BODYs. A changed hash or row count invalidates an old entry without adding work to fine-grained column writes. Proof views never populate the ordinary column cache. The caller's revision-bound fetcher reads proofs, preserving historical isolation even when an unchanged build shares a cached column-store handle.

The read path caps proof scratch at 20 MiB and requests proof chunks only when candidate BODY bytes exceed their encoded proof bytes by a factor of four. Full-size presence masks and representative numeric arrays are reused per worker. `-Dsirix.projection.numericProofs=false` disables this optional read path; it does not change the stored format or disable ordinary column reads.

`ProjectionNumericProofsTest` verifies residual BODY fetch counts, reordered physical groups, missing/sparse/stale evidence, ordinary-read cache isolation, signed arithmetic, representative-capacity fallback, corruption, async storage epochs, rollback, and reopened history under both slot layouts and all versioning strategies. The v74/v75 campaigns retain source/class/native snapshots, historical-revision checks, and isolated before/after measurements. The final v75 JVM and native ClickBench aggregates improved slightly; earlier slower JVM runs and compilation diagnostics remain part of the evidence, so runtime variability must still be monitored.

The composite COUNT kernel also defers dense dictionary-ID materialization when all other key components are constant for a selected block. `PackedDictionaryIds.countSelected` streams verified 0/1/2/4-bit IDs into worker-local counters, preserves missing values and first selected row ordinals, and checks dictionary bounds. Each packed word is loaded once; full selections shift through consecutive IDs, while partial selections visit surviving bits. A block that needs the general grouping loop materializes its dense lane as before. `-Dsirix.projection.packedDictionaryCounts=false` selects the previous dense histogram path for controlled comparisons.

`PackedDictionaryCountTest` compares all supported widths, partial words, dense/sparse masks, missing cells, invalid IDs, and repeated accumulation against an independent row oracle. The broader composite-group tests retain conditional keys, transformations, exact identity/collision proofs, and winner ordering. The first alphabet-scan counter was rejected after a same-binary warm-time regression; the tested v75 checkpoint uses the measured streaming algorithm. An additional oracle case covers fully present, fully selected blocks through the full 1,024-row boundary.

## Runtime defaults for input borrowing

Borrowed overflow input and coalesced-buffer views default on for every runtime, including
ahead-of-time native-image executables and JVMs using the native LZ77 codec. Both modes are
supported and preserve the same ownership and checksum contracts. Two switches control them; only
the literal value `false` disables a path, and both are resolved once per reader:

- `sirix.io.borrowOverflowInput` (default `true`) decodes overflow pages straight from the read
  buffer when the byte pipeline is empty. `false` restores the owned temporary frame. It is also
  the default for the batch switch below, so one `false` restores owned input on both paths.
- `sirix.filechannel.borrowBatchInput` (default: the value of `sirix.io.borrowOverflowInput`)
  decodes coalesced batch members from views of the batch read buffer. When set, it wins:
  `-Dsirix.io.borrowOverflowInput=false -Dsirix.filechannel.borrowBatchInput=true` keeps batch
  borrowing with owned overflow input, and `-Dsirix.filechannel.borrowBatchInput=false` alone
  disables only batch borrowing.

The selection is independent of schema, query text, row count, and persisted format. Controlled
measurements found lower JVM allocation and better JVM aggregate query time with borrowing. An
earlier native hot-query suite over a small database slightly favored owned input; on a 100M-row
column scan the owned path's per-page frame-slot allocation, copy and release were the largest
single cost of the column fills, so native images now borrow by default as well.


## Local dictionary COUNT batches

Plain, untransformed local-dictionary COUNT grouping accumulates selected rows into reusable
primitive counters indexed by the leaf-local dictionary ID. A separate missing-value bucket keeps
its existing identity. The fold hashes each used dictionary entry once and adds its whole count to
the group table, preserving the earliest source ordinal, zero-hash side entry and partition discard
contract. Packed IDs can feed the counters without materializing a dense ID array.

This read-only specialization applies by key and aggregate semantics, independently of field names,
query text, or dataset. Regex keys, global keys and queries with other aggregates retain their
existing kernels. Counter scratch is bounded by the persisted leaf dictionary; no revisioned format,
fine-grained update unit, or transaction ownership changes. The diagnostic property
`-Dsirix.projection.dictionaryCountBatches=false` selects the general row kernel for comparison.

The v89 campaign preserves same-binary off/on trials and full-suite before/after trials separately.
Its aggregate gains are small, with individual query regressions still being investigated; this
specialization is not evidence that every workload improves. CPU, wall-time, allocation and JIT
profiles are separate from the ranked measurements.
