# Writer-side `getRecord` caller audit (revised)

**Date:** 2026-05-28 (revised twice: post-review + post-implementation-bisect)
**Branch:** `perf/hot-drop-child-snapshots` @ `4712ea8cb`

> **Status (2026-05-28): pool is NOT viable as designed.** Implementation attempt at `FlyweightShellPool` (ring of 4 per kind) was reverted after `UpdateTest.testRemoveDescendantTextConcat2` and `testReplaceElementWithTwoSiblingTextNodesWithTextNode` regressed. Root cause: the audit's invariant 2 ("pool checkouts must not be cached in any tx-lifetime collection") was met for `records[]` but **missed `AbstractNodeReadOnlyTrx.currentNode`** — set by `moveToLegacy` which dispatches to `NodeStorageEngineWriter.getRecord` in write transactions. The `XmlNodeTrxImpl.remove` PostOrderAxis loop calls `removeRecord → getRecord` per descendant; after RING_SIZE checkouts the cursor's `currentNode` pool slot gets rebound to a different record, silently corrupting any subsequently-captured `node = getStructuralNode()` reference. Ring rotation cannot satisfy the "stable reference across N engine calls" contract that the cursor implicitly relies on. A working pool requires explicit checkout/checkin (audit's original recommendation) — an API change, not a perf drill. See section 8.
**Scope:** Every call-site in `sirix-core` that — directly or by dynamic dispatch through a `StorageEngineReader`-typed reference — invokes `NodeStorageEngineWriter.getRecord(...)`.
**Goal:** Document the identity-lifetime expectation at each site so a future per-tx kind-keyed shell pool (replacing the current "fresh shell per call" model) can be designed without silently breaking move / copy / index flows.
**Companion:** `MEMORY.md → writer-side-flyweight-pool-analysis.md`.

> **Revision history.** First pass (2026-05-27 AM) caught only the 11 direct `storageEngineWriter.getRecord(...)` call-sites and gave a *wrong* justification for the 2-slots-per-kind requirement (premised on cursor competition that doesn't actually exist on the common write-path). Independent review surfaced indirect callers through `RBTreeReader`, `PathSummaryReader`, `Names`, `DeweyIDPage`, `JsonVectorIndexImpl`, and `AbstractNodeReadOnlyTrx.moveToLegacy`, plus a critical `records[]`-caching invariant. This revision rebuilds the analysis from the verified dispatch model.

## 1. Dispatch model (the actual call paths into `NodeStorageEngineWriter.getRecord`)

Three classes of caller reach `NodeStorageEngineWriter.getRecord(...)`:

**Direct calls on `storageEngineWriter`** (the field is typed `StorageEngineWriter`):

- `XmlNodeTrxImpl.java:180, 345`
- `JsonNodeTrxImpl.java:2298, 2378`
- `PageBackedVectorStore.java:175, 193, 203, 213, 223, 318`

**Indirect calls on a `StorageEngineReader`-typed reference** that *is* the writer at runtime (because `NodeStorageEngineWriter` extends `AbstractForwardingStorageEngineReader implements StorageEngineWriter`, and `StorageEngineWriter extends StorageEngineReader`). Sites where the reader-field was constructed with `storageEngineWriter`:

- `RBTreeReader.java:136, 529` — held in `storageEngineReader` field; `RBTreeWriter.java:124` passes the writer into the constructor. Active on every CAS / PATH / NAME index modify.
- `PathSummaryReader.java:159, 620` — held in `storageEngineReader` field; `PathSummaryWriter.java` constructs with writer. Active on every `adaptPathForChangedNode` (called from move flows at `JsonNodeTrxImpl:2337, 2411`, `XmlNodeTrxImpl:210, 376`).
- `Names.java:95, 105, 141, 152` — `storageEngineReader` parameter; reached from `NamePage` reconstruction during read of any name dictionary in the write tx.
- `DeweyIDPage.java:145` — `storageEngineReader` parameter to `getDeweyIdForNodeKey(...)`. Reached during DeweyID-bearing flows.
- `RecordRevisionsLookup.java:53` — `rtx.getStorageEngineReader()`; in write-tx context, returns the writer.
- `JsonVectorIndexImpl.java:237` — vector search loop; uses the trx's reader, which is the writer.

**Cursor fallback** — only one indirect path here, but it's load-bearing:

- `AbstractNodeReadOnlyTrx.java:1026` — `moveToLegacy` is called when `moveToSingletonWrite`'s page-resolution or kind-singleton lookup fails (lines 769, 802, 816, 833) or when SINGLETON_ENABLED is false (line 474). On the common write path, `moveToSingletonWrite` does NOT touch the pool — it reads from `records[]` (line 782) or from `singletonByKindId[]` (lines 814, 820–827) instead. The pool is only reached when the page isn't in TIL.

### Critical aside — `records[]` and the cursor

`moveToSingletonWrite` at line 781–795 reads `page.getRecord(slotOffset)` BEFORE anything else, and if a record is present, binds `currentNode = thatRecord`. The records[] array is populated by `NodeStorageEngineWriter.prepareRecordForModification(...)` (lines 397, 445), which stores the kind-singleton there. So under load, the cursor often binds to the prepare-singleton via records[], not its own `singletonByKindId[]`.

**Hard invariant for the pool design:** if pool checkouts ever land in `records[]` via `modifiedPage.setRecord(record)`, the next `moveTo` to that slot would set the cursor's `currentNode` to a pool slot. The next pool checkout for the same kind would silently rebind the cursor's node. This is the most dangerous failure mode in the entire design. Pool checkouts MUST flow only into the immediate caller's local variable; the pool path must not write into records[].

## 2. Per-site analysis

### Cluster A — `XmlNodeTrxImpl` (2 sites)

**A1 — `moveSubtreeToFirstChild` (line 180).** The returned `node` is downcast to `StructNode toMove` and held across the entire move:
- `checkAncestors((Node) nodeToMove)` (line 188) — only reads `node.getNodeKey()`; no rebinding.
- `adaptSubtreeForMove(toMove, DELETE)` (line 198) — descendant walk via cursor + `prepareRecordForModification` of attrs/namespaces.
- `adaptHashesForMove(toMove)` (line 201) — sets cursor's currentNode, no rebinding of `toMove`.
- `adaptForMove(toMove, nodeAnchor, ...)` (line 204) — many `prepareRecordForModification` calls and `moveTo`s.
- `adaptSubtreeForMove(toMove, INSERT)` (line 215).

Re-reads of `toMove`/`moved` after engine calls: lines 209 (`toMove instanceof NameNode moved`), 210 (`moved.getURIKey()`, etc.). Re-reads of `fromNode` inside `adaptForMove` at lines 444 (`fromNode.getNodeKey()`), 445, 450, 461, 467.

**Classification:** POOL-HAZARD. The `toMove` reference must survive any intervening same-kind pool checkout.

**A2 — `moveSubtreeToRightSibling` (line 345).** Structurally identical to A1 (same helper sequence).

**Classification:** POOL-HAZARD.

#### Text-merge sub-trace (A1/A2 → `adaptForMove` lines 466–518)

This is the path that triggered the reviewer's "could need 3 slots" concern. Verified manually:
- `fromNode` is in pool slot A for kind X (ElementNode or TextNode).
- The cursor's `moveTo` at lines 469, 472, 476, 488, 492, 494, 496, 508, 512, 514 dispatches to `moveToSingletonWrite`. If the target page is in TIL, the cursor uses records[] or `singletonByKindId[]` — no pool competition.
- If the cursor's target page is NOT in TIL, `moveToLegacy` fires → writer.getRecord → pool slot (kind Y, may equal X).
- `currentLeftNode` (line 477) and `currentRightNode` (line 497) are `nodeReadOnlyTrx.getStructuralNode()` — they ALIAS the cursor's `currentNode`. So they're either records[] / `singletonByKindId[]` / pool depending on the same moveTo branch.
- `prepareRecordForModification(...)` at lines 480, 489, 500, 509 returns the `reusable*Node` kind-singleton from the factory — a separate reference channel from the pool.
- `persistUpdatedRecord(...)` does NOT call getRecord; it caches the prepare result into records[] (writes singleton-as-records[]-entry, not a pool checkout).

Max concurrent same-kind pool shells in this block:
- Slot A: `fromNode` (kind X, held throughout).
- Slot B: cursor's `currentNode` IF `moveToLegacy` fires AND kind matches X.

I find no third concurrent pool shell. `leftSibling`/`rightSibling` are kind-singletons, not pool. `currentLeftNode`/`currentRightNode` are aliases to the cursor's `currentNode`, not independent allocations.

**Verdict on the reviewer's 3-slot concern:** 2 slots per kind remain sufficient for the text-merge path. The 3-slot scenario doesn't materialize because the only second-pool consumer is the cursor's legacy fallback, and there isn't a third independent pool allocation in this stack frame.

### Cluster B — `JsonNodeTrxImpl` (2 sites)

**B1 — `moveSubtreeToFirstChild` (line 2298).** Same shape as A1. Note line 2320–2322's comment ("toMove may be a flyweight singleton that gets mutated during adaptForMove") — the author already partially anticipated this hazard, captured `originalParentKey` defensively at line 2322, but did not extend the discipline to other reads of `toMove`. The hazard is real if a pool weren't sized for the held reference.

**Classification:** POOL-HAZARD.

**B2 — `moveSubtreeToRightSibling` (line 2378).** Structurally identical to B1.

**Classification:** POOL-HAZARD.

### Cluster C — `PageBackedVectorStore` (6 sites)

All six are textbook `getRecord → read field → return`. Lifetime is instantaneous; the shell is released at method exit.

| Site | Pattern |
|---|---|
| line 175 (`loadMetadata`) | Reads `getEntryPointKey()` + `getMaxLevel()`, no intervening call |
| line 193 (`getVector`) | Reads `getVector()`, returns |
| line 203 (`getNeighbors`) | Reads `getNeighbors(layer)`, returns |
| line 213 (`getNeighborCount`) | Reads `getNeighborCount(layer)`, returns |
| line 223 (`getMaxLayer`) | Reads `getMaxLayer()`, returns |
| line 318 (`isDeleted`) | Reads `isDeleted()`, returns |

**Classification:** POOL-SAFE under 1 slot per kind.

### Cluster D — `RBTreeReader` (2 sites, indirect)

**D1 — constructor (line 136):** `getRecord(DOCUMENT_NODE_KEY, indexType, indexNumber)` stored as `currentNode`. The full-tree-walk loop at lines 144–150 fires only when `!(storageEngineReader instanceof StorageEngineWriter)` (line 143) — i.e., NOT on the write path. On the write path, the constructor stores `currentNode` and returns.

**D2 — `moveTo` (line 529):** Cursor pattern. `getRecord(nodeKey, indexType, index)` replaces `currentNode`. No prior reference of the same kind is held at the RBTreeReader instance level.

**Classification:** POOL-SAFE under 1 slot per kind for RBNodeKey / RBNodeValue.

**Cross-instance concern:** A single write tx may hold multiple `RBTreeReader` instances simultaneously (one per active index: CAS, PATH, NAME). Each holds its own `currentNode` field. If those fields hold pool shells of the same kind, the pool must service them in distinct slots. **Maximum concurrent RBNodeKey shells across all RBTreeReaders in a tx ≤ number of active index types (CAS / PATH / NAME) = at most 3.**

Pool sizing for RBNodeKey, RBNodeValue: must accommodate **3 concurrent shells per kind** (one per index type).

### Cluster E — `PathSummaryReader` (2 sites, indirect — with a separate hazard)

**E1 — constructor (line 159):** `getRecord(DOCUMENT_NODE_KEY, PATH_SUMMARY, 0)` stored as `currentNode`, then the constructor walks the entire path summary at lines 173–192, populating `pathNodeMapping[]` (line 179) and `qnmMapping` (line 182).

**Critical hazard at line 179: `pathNodeMapping[(int) pathNode.getNodeKey()] = pathNode;`**

The walk yields `pathNode` references obtained via `moveTo(pathNode.getNodeKey())` → ultimately a `getRecord(...)`. Storing those references in a long-lived array means any later same-kind pool checkout would rebind the array's entries. **The `pathNodeMapping[]` cache is incompatible with a naive pool.**

**E2 — `moveTo` (line 620):** Cursor pattern, identical to D2. Independent of E1's cache, except that E1's cache `pathNodeMapping[]` is consulted FIRST at line 591, returning a cached `StructNode` reference. **If `pathNodeMapping[]` holds pool shells and they've been silently rebound, this method returns the wrong record.**

**Classification:**
- The cursor pattern (E2 by itself) is POOL-SAFE under 1 slot.
- The cache (E1) **forces a design constraint**: either PathNode kind is excluded from the pool, or `pathNodeMapping[]` must be refactored to store node keys (not shells), or the pool's checkout must mark the slot as "pinned indefinitely" until explicitly released (but PathSummaryReader never explicitly releases — its lifetime ≈ transaction).

The cleanest fix is to **never pool the PathNode kind**: pool-exclude lookup is cheap (a kind-keyed boolean array) and PathNode is read-heavy with long-lived caches that intentionally outlive any single operation.

### Cluster F — `Names` (4 sites, indirect)

**F1 — constructor scan path (lines 95, 105):**
```java
for (long i = 1; i < maxNodeKey; i += 2) {
  final var nameNode = storageEngineReader.getRecord(i, IndexType.NAME, indexNumber);
  if (nameNode != null && nameNode.getKind() != NodeKind.DELETE) {
    final HashEntryNode hashEntryNode = (HashEntryNode) nameNode;
    final int key = hashEntryNode.getKey();
    nameMap.put(key, hashEntryNode.getValue().getBytes(...));           // extract bytes, copy out
    final long nodeKeyOfCountNode = i + 1;
    final var countNode = storageEngineReader.getRecord(nodeKeyOfCountNode, IndexType.NAME, indexNumber);
    final HashCountEntryNode hashKeyToNameCountEntryNode = (HashCountEntryNode) countNode;
    countNameMapping.put(key, hashKeyToNameCountEntryNode.getValue()); // extract int
    countNodeMap.put(key, nodeKeyOfCountNode);
  }
}
```

`nameNode` is fully consumed (line 102 reads `getValue().getBytes(...)` — a byte[] copy) BEFORE line 105's second getRecord. `countNode` is consumed at line 111 before the loop body ends. Across iterations, the previous `nameNode` is no longer referenced.

**Classification:** POOL-SAFE. Concurrently-live same-kind shells per loop iteration: 1.

**F2 — live-bitmap reconstruction (lines 141, 152):** Same structure as F1 but iterates a Roaring bitmap. Same classification.

**Classification:** POOL-SAFE under 1 slot per kind for HashEntryNode, HashCountEntryNode.

### Cluster G — `DeweyIDPage`, `RecordRevisionsLookup`, `JsonVectorIndexImpl` (3 sites, indirect)

**G1 — `DeweyIDPage.getDeweyIdForNodeKey` (line 145):**
```java
final DeweyIDNode node = storageEngineReader.getRecord(nodeKey, DEWEYID_TO_RECORDID, 0);
if (node == null) return null;
return node.getDeweyID();
```
Immediate consumption. **POOL-SAFE under 1 slot for DeweyIDNode.**

**G2 — `RecordRevisionsLookup.revisionsFor` (line 53):**
```java
final DataRecord record = rtx.getStorageEngineReader().getRecord(nodeKey, RECORD_TO_REVISIONS, 0);
if (!(record instanceof RevisionReferencesNode rrn)) return null;
final int[] revs = rrn.getRevisions();
return (revs == null || revs.length == 0) ? null : revs;
```
Returns a primitive int[] read from the shell. **POOL-SAFE under 1 slot for RevisionReferencesNode.**

> Note: `rrn.getRevisions()` returns the **live array stored on the node** (see the comment at line 41–43). Callers treat it as read-only. The int[] outlives the shell — that's fine; the bytes are independent of the FlyweightNode's MemorySegment binding.

**G3 — `JsonVectorIndexImpl.searchKnn` (line 237):**
```java
for (int i = 0; i < resultCount; i++) {
  final long hnswKey = hnswNodeKeys[i];
  final VectorNode vectorNode = reader.getRecord(hnswKey, VECTOR, indexNumber);
  documentNodeKeys[i] = vectorNode.getDocumentNodeKey();
  distances[i] = distFn.distance(query, vectorNode.getVector());
}
```
Loop body fully consumes `vectorNode` before next iteration. **POOL-SAFE under 1 slot for VectorNode.**

### Cluster H — `AbstractNodeReadOnlyTrx.moveToLegacy` (line 1026)

The cursor's fallback when `moveToSingletonWrite` can't bind via records[] or `singletonByKindId[]`. The returned `newNode` is stored as the cursor's `currentNode` (`setCurrentNode((N) newNode)` at line 1040). Only one cursor `currentNode` per trx; previous value is replaced atomically.

**Classification (revised 2026-05-28): POOL-INCOMPATIBLE under ring-rotation design.**
`currentNode` is read by `getStructuralNode()` (line 1156) which simply returns it. Callers — e.g. `XmlNodeTrxImpl.remove` at line 1329 — capture the reference into a local (`final StructNode node = nodeReadOnlyTrx.getStructuralNode();`) and hold it across many subsequent engine calls. If `currentNode` was set from a pool checkout, and any of those subsequent calls re-checkout the same kind enough times to wrap the ring, the held `node` reference silently rebinds to a different record. `node.getNodeKey()` then returns the wrong key, `prepareRecordForModification(wrongKey, ...)` returns whatever's in records[] for that other key (often `DeletedNode`), and `getHash()` throws.

This is the same hazard class as records[] aliasing — but for the `currentNode` single-cell cache, which the first audit pass missed. The single-cell cache makes it *worse* than records[] in one sense: every cursor moveTo that hits `moveToLegacy` overwrites it.

**Empirical failure:** observed during pool implementation. `XmlNodeTrxImpl.remove` PostOrderAxis loop on a > RING_SIZE subtree corrupts the captured `node`; subsequent `adaptHashesWithRemove` reads the wrong key, hits a DeletedNode from records[], and throws `UnsupportedOperationException` from `NodeDelegate.getHash` (line 170). Tests: `UpdateTest.testRemoveDescendantTextConcat2`, `testReplaceElementWithTwoSiblingTextNodesWithTextNode`.

## 3. Pool sizing — consolidated table

Per kind, the max concurrent live pool shells across all caller stack frames in any one tx operation:

| Kind | Max concurrent | Drivers |
|---|---|---|
| `ElementNode` / `ObjectNode` / `ArrayNode` / `TextNode` / other `StructNode`-subkind | **2** | `toMove` in A/B + cursor `moveToLegacy` fallback in H |
| `RBNodeKey<K>` | **3** | one cursor per active RBTreeReader instance (CAS + PATH + NAME indexes possible) |
| `RBNodeValue<V>` | **3** | same logic as RBNodeKey |
| `PathNode` | **excluded from pool** (or pool-pin without release) — `PathSummaryReader.pathNodeMapping[]` retains shells for tx lifetime |
| `HashEntryNode` | **1** | only `Names.java` iterates; immediate-consume |
| `HashCountEntryNode` | **1** | same |
| `DeweyIDNode` | **1** | immediate consume |
| `RevisionReferencesNode` | **1** | immediate consume |
| `VectorNode` | **1** | `PageBackedVectorStore` + `JsonVectorIndexImpl` loop both immediate-consume |
| `VectorIndexMetadataNode` | **1** | only `loadMetadata` uses getRecord |
| `AttributeNode` / `NamespaceNode` | **0 from pool** | only used via `prepareRecordForModification` (kind-singleton, not pool) |

**Recommended pool ring size per kind:** 4 (covers the highest sizing of 3 with one slot of headroom). Per-tx memory overhead: 4 × ~12 kinds × ~256 B ≈ 12 KB. Trivial vs. the ~6 MB allocation it replaces.

**PathNode is excluded** — the simplest valid design ignores PathNode for pooling, letting it continue to allocate fresh shells. Cost: ~unknown small fraction of the 6 MB. Future work could refactor `pathNodeMapping[]` to cache node-keys + lazy-load on access (already mostly done — `childLookupCache: Long2LongOpenHashMap`).

## 4. Hard invariants for any pool implementation

1. **Pool checkouts MUST NOT be written into `KeyValueLeafPage.records[]`** (via `modifiedPage.setRecord(...)` at `NodeStorageEngineWriter.java:397, 445` or anywhere else). The cursor reads from records[] at `AbstractNodeReadOnlyTrx.java:782` and would otherwise alias a pool slot.
2. **Pool checkouts MUST NOT be cached in any tx-lifetime collection.** Specifically: `PathSummaryReader.pathNodeMapping[]`, `PathSummaryReader.qnmMapping`, RBTreeReader's `currentNode` field (already short-lived per moveTo, but verify), and anywhere a FlyweightNode reference gets stashed in an instance field of a non-stack-bound object.
3. **PathNode is excluded from pooling** (Cluster E forces this). The pool factory must accept a kind-allowlist or kind-denylist.
4. **Pool is anchored to the writer instance, not the transaction.** `AbstractNodeTrxImpl.reInstantiate` (line 467, replaces `storageEngineWriter` at lines 479–483) creates a fresh writer per epoch; a trx-anchored pool would survive into a closed writer reference and have stale page bindings.
5. **Async commit doesn't touch the pool.** `NodeStorageEngineWriter.executeSnapshotWrite` (line 765) deep-copies pages and serializes — never calls `getRecord`. No multi-thread synchronization needed on the pool.

## 5. Suggested pool design

- **Per-writer, per-kind ring of size 4.** Indexed by `NodeKind.getId()`, sized at 64 (matches `singletonByKindId[]` allocation).
- **Checkout API:** `pool.checkout(kindId, page, offset, recordKey)` → bind ring's next-slot FlyweightNode and return. No explicit checkin (ring rotation = implicit release once the caller frame returns).
- **Bypass for excluded kinds.** PathNode dispatches to the existing `FlyweightNodeFactory.createAndBind` (fresh allocation) — pool-aware codepath returns null / falls back.
- **Invariant assertion (debug mode).** Each shell read asserts `slot.recordKey == expectedKey`; rebind detection logs the offending kind, ring position, expected vs. actual key. Run the HOT + sirix-core + sirix-query test suites with the assertion enabled before disabling it for benchmarks.
- **Routing changes:**
  - `NodeStorageEngineWriter.getRecordForWriteAccess` (line 645) on records[] cache miss: replace `storageEngineReader.getValue(page, recordKey)` with `pool.checkout(kind, page, offset, recordKey)`.
  - `NodeStorageEngineWriter.getRecord` (line 631) reader-fallback path: same replacement, but must NOT subsequently store the pool checkout in records[].
- **The existing `WriteSingletonBinder` (used by `prepareRecordForModification`) remains untouched.** It uses kind-singletons (`reusable*Node` fields on the factory), a separate reference channel from the pool.

Estimated implementation: ~200 LOC plus the invariant-assertion test mode (~50 LOC).

## 6. Risks (revised)

- **R1 (high) — `records[]` aliasing.** Mitigated by Invariant 1. The cursor's `moveToSingletonWrite` reads from records[] and assumes it holds stable singletons. Any breach of Invariant 1 is catastrophic.
- **R2 (high) — PathSummaryReader cache.** Mitigated by Invariant 3 (PathNode excluded). Long-term cleaner fix: refactor `pathNodeMapping[]` to node-keys.
- **R3 (medium) — RBTreeReader cross-instance concurrency.** Mitigated by pool size 3+ per kind for RBNodeKey/RBNodeValue (recommended size 4).
- **R4 (medium) — `currentNodeKey` desync.** If a pool slot is rebound to a different `recordKey` while the cursor was using that slot's `currentNode`, `currentNodeKey` becomes a lie. Invariant assertion catches this in tests.
- **R5 (low) — Future indirect callers.** Any new `StorageEngineReader`-typed field constructed from `storageEngineWriter` inherits the pool semantics. Re-run the audit after any refactor that introduces such a reference.
- **R6 (low) — async commit.** Confirmed clean. No mutex / volatile / Atomic* needed on the pool.

## 7. Next step (superseded — see section 8)

~~The audit is now ready to drive implementation.~~ Implementation attempt failed. See section 8.

## 8. Implementation outcome (2026-05-28)

**Attempted:** `FlyweightShellPool` per-writer kind-keyed ring of 4. Wired into `NodeStorageEngineWriter.getRecordForWriteAccess`. Compiled clean; JSON moves/inserts/removes passed; XML `UpdateTest` regressed with 2 failures on text-merge / replace-with-text flows.

**Failure mechanism (verified):**

1. `XmlNodeTrxImpl.remove` (line 1329) does `final StructNode node = nodeReadOnlyTrx.getStructuralNode();` — captures the cursor's `currentNode` reference into a local.
2. If the most-recent `moveTo` hit `moveToLegacy` (e.g., page not yet in TIL), then `currentNode` IS a pool shell (was issued by my pool path via `storageEngineReader.getRecord` dispatching to `NodeStorageEngineWriter.getRecord`).
3. PostOrderAxis loop calls `removeRecord(currentNodeKey)` per descendant. Each `removeRecord` internally calls `getRecord(...)` → another pool checkout. The pool ring rotates.
4. After `RING_SIZE` checkouts of the same kind, the pool slot holding `node`'s underlying shell is re-bound to a different record. `node.getNodeKey()` now returns the *wrong* key.
5. `setCurrentNode(xmlNode)` (line 1387) sets `currentNodeKey = xmlNode.getNodeKey()` = wrong key.
6. `rollingRemove` (line 251) calls `prepareRecordForModification(wrongKey, ...)`. records[] for that key holds a `DeletedNode` (from step 3's `removeRecord`). `getHash()` on it throws `UnsupportedOperationException`.

**Why the audit's invariant 2 didn't catch this:** the invariant said "MUST NOT be cached in any tx-lifetime collection" and listed `pathNodeMapping[]`, `qnmMapping`, `RBTreeReader.currentNode`. **It missed `AbstractNodeReadOnlyTrx.currentNode`** — the cursor's single-cell cache. The cursor isn't really a "collection" in the data-structure sense, but it has the same lifetime problem.

**Why unbinding before return doesn't fix it:** unbind materializes fields to Java primitives but **doesn't change object identity**. A future checkout for the same ring slot re-binds the SAME object to a different key. Any caller holding the previous reference sees the new key on next read.

**Why a larger RING_SIZE doesn't fix it:** can't predict the maximum length of a held-reference + checkout cascade. A PostOrderAxis over a large subtree will always exceed any finite ring size.

**The audit's *original* recommendation (per-tx pool with checkout/checkin) WOULD work** because callers would have to explicitly release — they wouldn't be implicitly holding references past their lifetime. But that's an invasive API change touching every `storageEngineWriter.getRecord` caller's lifecycle. It's not a perf drill.

**Bottom line:** ring-rotation pooling is incompatible with the implicit "hold reference across arbitrary engine calls" contract that `getRecord` callers rely on (the cursor most notably). The 6 MB / 1.93% allocation is the structural cost of that contract. Reverted to fresh-shell allocation.

**Separate finding (shipped as a fix):** during diagnosis, bisected to discover `commit 6eaa56d25` ("perf(hot): kill write-path slice + scalar partial-key scratch allocations") had introduced a regression in `setSlotToHeapDirect` — it skipped the 2-byte DeweyID trailer that `setSlotToHeap` writes when `areDeweyIDsStored`, causing `getRecordOnlyLength` to misread the directory and return null from `getSlot` in the SLIDING_SNAPSHOT page-combine path. Fixed by adding the trailer handling to `setSlotToHeapDirect`.

## 9. Future directions

If the 6 MB ever becomes a real budget concern:

- **Per-tx checkout/checkin pool** (invasive API change). Every `getRecord` call returns a token; caller must call `release(token)` at end of scope. Stack discipline checked in debug mode. Touches every call site listed in section 2.
- **Refactor the cursor** to not store its `currentNode` as a long-lived reference — instead, materialize fields into primitive fields on the trx itself, treat the shell as transient. Removes the implicit "stable reference" contract that makes ring-rotation unsafe.
- **Accept the 6 MB.** It's < 2% of canary allocation, structurally bound, and not the worst remaining lever per the JFR profile.

## Related

- [[writer-side-flyweight-pool-analysis]] — memory entry: why a one-line binder addition isn't the fix.
- [[alloc-profile-hot-leaf-slice]] — JFR canary that surfaced the 6 MB.
- [[feedback_hft_hot_path]] — HFT alloc rules.
- [[hot-i8-scale-bug]] — multi-seed fuzz lesson.
