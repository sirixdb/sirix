# Bulk Ingestion — Intent Spec & Staged Plan

**Status:** v1 in implementation (fast lane); v2 designed (byte-level builder)
**Scope:** throughput of document-order bulk loads (`jn:load`, `jn:store`,
`insertSubtreeAsFirstChild` of large parsed subtrees) into JSON resources.
**Non-goals:** mutation performance (single-node updates are O(depth) and already in a
different complexity class), non-document-order inserts, XML shredding (same skeleton
applies later), parallel partitioned shredding (v2+, sketched in §8).

---

## 1. Problem statement (measured)

Same 4-core box, JDK 25, medians of 3, identical binaries (2026-07-21):

| Workload | DuckDB `read_json` | sirix `jn:load` | Ratio |
|---|---|---|---|
| 2M rows / 78 MB JSON (~22M nodes) | 3.7 s | 15.6 s | 4.2× |
| 100M rows / 3.9 GB JSON | 87.9 s | 371.5 s | 4.2× |

The 15.6 s figure already includes the async background pre-flush (intermediate
commit serialization off the insert thread — landed with PR #1131). JFR profiling of
that state shows the insert thread busy ~100% of wall time, with **~76% in per-node
insert machinery** and no single dominant hotspot: parser+value conversion ~8%,
FFM bounds/liveness checks ~5%, cursor bookkeeping (`moveToSingletonWrite`) ~3.5%,
`RecordToRevisionsIndex` trie maintenance ~3%, neighbor adaptation ~2%, varint/record
serialization ~4%, name-dictionary lookups ~2%, and a long tail. A structural
comparison explains the 4.2×: DuckDB appends 5 column vector cells per row; sirix
creates ~11 identity-bearing tree nodes per row, each with parent/sibling links,
dictionary references, and an index entry — through machinery built for arbitrary
random edits, used here for a purely sequential append.

## 2. Intent

Cut bulk-import wall time by exploiting what a document-order load knows that the
general editing path cannot assume: **the entire neighborhood of every new node is
determined by the parser's stack**, and **every structural fact about a container is
final the moment its closing token arrives**. The imported resource must remain a
full sirix resource — node identity, counts, hashes, dictionaries, path summary,
revisions index, versioned page trie — indistinguishable from a cursor-built one.

Targets: v1 (fast lane, this document's plan) ≥1.3× import throughput with zero
semantic change; v2 (byte-level page builder) 2–3× cumulative, reaching ~1.5–2× of
DuckDB ingest, which is the estimated floor while writing ~11 identity-bearing
records per row.

**S1 measurement (2026-07-21):** the cursor-layer elimination alone landed
parity-proven but performance-neutral on the 4-core box (2M-row shred medians
16.9 s fast lane vs 16.8 s classic — within noise). The cursor reads/moves it
removes are singleton binds against hot pages, cheaper than their profile
attribution suggested. The lane's value at this stage is architectural: explicit
anchors and container-close events are the prerequisite for the two measured
levers — the ~24% hash-maintenance share (S3 finding below) and the v2 byte-level
builder — and the differential parity gate now exists to hold every next step to
bit-equality.

## 3. Equivalence contract (the acceptance gate for every stage)

A resource built through the bulk lane MUST be indistinguishable from one built by
the cursor path:

1. **Node-level equality:** identical nodeKeys (same assignment order), kinds,
   names, values, parent/firstChild/lastChild/leftSibling/rightSibling links,
   childCount, descendantCount, pathNodeKeys, and — where hashes are configured —
   bit-identical hash values (subsequent incremental update math builds on them).
2. **Serialization equality:** byte-identical JSON serialization of the revision.
3. **Behavioral equality:** any subsequent operation (update, query, projection
   serving, diff) produces identical results on both stores; a post-import edit
   applied to both must leave them node-equal again.
4. **Physical layout is explicitly NOT part of the contract** (page bytes/offsets
   may differ, as they already do run-to-run via codec election and flush timing);
   readers resolve through the trie, so layout is below the abstraction line.
5. **Decline-to-cursor:** whenever a precondition is unmet, the importer silently
   uses the existing cursor path. The fast lane may only change cost, never
   behavior — the same discipline as the vectorized serving layer.

Enforced by differential tests: shred fixture documents through both lanes,
assert (1)+(2) via full node-walk and serialize comparison, then apply identical
edits and re-assert. Fixtures cover: nested objects/arrays, empty containers,
single-element documents, all primitive kinds, long strings crossing page
boundaries, overlong (>150 KB) values, documents crossing many auto-commit
rotations, and field-name sets larger than the memo capacity.

## 4. Applicability gates (v1)

The fast lane engages only when ALL hold (checked once at subtree-insert start):

- Document-order bulk append: `insertSubtreeAsFirstChild` into an empty resource
  (the `jn:load`/`jn:store` bootstrap path).
- DeweyIDs disabled (default for the JSON store).
- Timed auto-commit not configured (count-based or none).
- Hash mode `NONE` or `ROLLING` with the bulk-insert deferred-hash behavior the
  cursor path already uses (see §6 stage S3).

Anything else — including all mutation APIs — takes the existing path unchanged.

## 5. Architecture v1 — a fast lane through the existing record layer

The guiding altitude decision: **reuse the record layer, bypass the cursor layer.**
Record serialization (heap-bound write singletons, `writeNewRecord`,
`DeltaVarIntCodec`), page allocation, the TIL, the async pre-flush, dictionaries,
and commit machinery stay untouched — they are correct, reviewed, and shared with
every other path. What the fast lane removes is work the cursor path does *because
it cannot assume document order*:

| Cursor path (per node) | Fast lane (per node) |
|---|---|
| `moveToSingletonWrite` cursor maintenance | none — no cursor during the streamed subtree |
| `adaptForInsert`: prepare parent singleton (childCount, first/lastChild) + prepare left-sibling singleton (rightSibling) — 2 record resolutions | stack-frame arithmetic; sibling/parent links written once with known values; the left sibling's record is (nearly always) in the current append page and patched in place |
| `getPathNodeKey` / name-dictionary lookup per node | memoized per (parent path node, field name, kind) — O(distinct fields) total |
| `RecordToRevisionsIndex` keyed-trie maintenance per node | sequential append (keys ARE the monotonically increasing nodeKeys) |
| per-insert threshold/state checks | hoisted per-container |

**The BulkInsertContext** (one per streamed subtree, pooled):

- An explicit frame stack mirroring parser nesting. Frame:
  `{nodeKey, patchHandle, childCount, descendantCount, hashAccumulator, lastChildKey}`.
  Frames are pooled primitive arrays — zero allocation per node, no boxing.
- `BEGIN_OBJECT`/`BEGIN_ARRAY`: emit the container record (placeholder counts),
  push a frame holding a patch handle to the record's heap location.
- Value/field events: emit the record with parentKey/leftSibling taken from the top
  frame; patch the previous sibling's `rightSiblingKey` through its handle; update
  frame accumulators.
- `END_*`: pop; write final childCount/descendantCount (and, S3, the folded hash)
  through the patch handle; fold `descendants+1` into the new top frame.
- **Patch handles and page pinning (v2 design, not in the v1 lane):** a handle is
  (page, slot) while the record's page is live in the TIL. Pages containing
  open-container records or the current tail sibling are pinned against the async
  flush rotation (the flush already tolerates exempted pages via the
  promote-to-TIL mechanism from #1131). Pinned page count is bounded by nesting
  depth + 1; a depth guard would decline to the cursor path beyond a threshold.
  The v1 lane needs none of this — its frame stack simply grows (doubling
  primitive arrays, exercised by the depth-200 parity fixture) because deferred
  patching is not yet in play (parent/sibling adaptation stays the classic
  singleton-based mechanism).
- Auto-commit rotations keep working: the threshold check runs per node as today
  (a counter increment + compare), rotation snapshots the TIL minus pinned pages.

## 6. Staged plan (review + tests gate every stage)

- **S0 — this document.**
- **S1 — fast lane core:** `BulkInsertContext`, shredder integration behind the
  gates, neighbor patching + counts via the stack. *Gate:* differential fixtures
  (§3), full async/shredder/projection suites green.
- **S2 — dictionary memoization + revisions-index sequential append.** *Gate:*
  S1 gate + index/dictionary parity assertions.
- **S3 — hashes:** keep the cursor path's bulk-mode hash timing (deferred
  commit-time computation) for v1 — identical outputs by construction; evaluate
  the streaming Merkle fold (hash-at-close, bit-equal to rolling semantics) and
  land it only if the differential gate proves bit-equality. *Gate:* hash
  bit-equality fixtures incl. post-import single-node update on both stores.

  **S3 LANDED (2026-07-21, streaming fold):** the fold engages exactly when the
  classic lane would run the end-of-subtree postorder repair — `ROLLING` with
  `repairBulkInsertHashes`, or `ROLLING` without auto-commit — and computes every
  hash/descendantCount during the shred, at the moment a node's structure becomes
  final (right sibling linked, container closed, or subtree end). Values are
  bit-identical to the repair (differential suite enforces it, including across
  auto-commit rotations), but the repair's full re-read traversal disappears.
  Measured on the 2M-row shred, identical config (`repairBulkInsertHashes`,
  jn:load's auto-commit threshold): classic repair **~59 s**, streaming fold
  **~19.4 s** — **3×**, and within noise of a default import on the same box
  (~19–20.5 s that session). Complete hashes are now effectively free for
  fast-lane imports that opt into the repair semantics. One deliberate
  visibility difference: with auto-commit, INTERMEDIATE import revisions carry
  the hashes of nodes already finalized in that epoch (classic leaves all
  hashes 0 until the final epoch's repair); the head revision — the contract's
  scope — is bit-identical.

  Two honest corrections to the earlier S3 finding: (a) the "~24% hash lever"
  conflated two different baselines — the auto-committing DEFAULT only pays
  per-insert adaptation for epoch 1 (~5% of a 2M import; the rest is the
  documented unmaintained-hashes gap), so *completing* the hashes can never be
  cheaper than that partial work, no matter how it is folded. The fold therefore
  deliberately does NOT replace the default path: auto-committing
  `repairBulkInsertHashes=false` shreds keep the classic per-insert adaptation
  bit-for-bit (§3 gate 5 — the lane changes cost, never semantics). Making
  complete hashes the import default remains a resource-configuration decision
  (`repairBulkInsertHashes`), now with its cost reduced from +40 s to ~0 on the
  fast lane. (b) The repair traversal itself was far more expensive than its
  24% share suggested (it re-reads and re-dirties every page the async pre-flush
  already wrote) — the fold's 3× shows most of that cost was the traversal, not
  the hash arithmetic.

  **S3 finding (2026-07-21, measured + blocked on a semantics decision):** with the
  default ROLLING hashes, hash maintenance costs **~24% of import wall time**
  (2M-row shred: 16.9 s → 12.8 s with `-DhashType=NONE`) — the end-of-subtree
  postorder pass re-reads and re-dirties every page the async pre-flush already
  wrote, which is the single biggest known lever left in the import path. A
  streaming hash-at-close fold could recover most of it, BUT bit-equality has a
  landmine: the current semantics are MIXED. While auto-commit hashing is active
  (before the first rotation), `rollingAdd` stores each node's hash computed from
  its CREATION-time bytes — `rightSiblingKey` still NULL — and the final postorder
  pass REUSES any non-zero hash instead of recomputing, while nodes from later
  epochs (hash still 0) are hashed from their FINAL bytes. Stored hash values
  therefore depend on where the auto-commit threshold fell during the import. A
  streaming fold can either (a) replicate this mixed behavior exactly
  (creation-time own-data hashes, matching fold order), or (b) ride a deliberate,
  separately-reviewed change that makes hashing uniformly final-bytes — which
  alters stored hash values and must be assessed against the hash-based diff
  machinery for stores that mix old and new revisions. Decision owner: maintainer.
- **S4 — wire-up + validation:** enabled by default on the `jn:load` path when
  gates hold; adversarial review agents on the full diff; 2M/100M before/after
  benchmarks; async-flush interplay (rotations mid-subtree, overlong records,
  rollback mid-import).

Each stage ends committed, pushed, and green; a stage that misses its gate is
reverted or gated off (system property `sirix.bulkInsert.fastLane`, default on)
rather than merged weakened.

## 7. Performance rules (HFT discipline)

Zero per-node allocation in the fast lane (pooled frames, fastutil memo maps,
primitive keys); no boxing; `final` everywhere applicable; no virtual dispatch in
the per-event switch beyond the parser's own; patch handles are primitive
(page-ref + slot offset), resolved without map lookups in the common
current-page case; memo maps pre-sized; all counters `long`/`int` locals folded
into frames only at push/pop.

## 8. v2 outlook (not in scope tonight)

- **Byte-level page builder:** construct `KeyValueLeafPage` heaps directly from
  parser events (records serialized straight into the slotted heap, pages sealed
  and handed to the flush as immutable), skipping the TIL round-trip for interior
  pages entirely. Estimated cumulative 2–3×.
- **Parallel partitioned shredding:** chunk the top-level array by raw-byte
  bracket scan, per-chunk builders with pre-reserved nodeKey ranges, seam
  stitching (sibling links, counts, hash folds) + dictionary merge at join.
- Both keep the §3 contract verbatim.
