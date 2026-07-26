# HOT Incremental Insert — Detailed Port Plan

**Goal.** Replace SirixDB's HOT secondary-index insert path with a faithful port of
Binna's incremental insert + split + integrate + cascade, adapted for Sirix's
persistent (copy-on-write) page structure and multi-value leaf pages. Acceptance:
`HOTVersionedLeafStressTest` + `HOTFormalVerificationTest` green with I8 enforcement
**on**, zero invariant violations across all revisions and versioning types.

**Companion docs.** *Why it is correct:* `HOT_INCREMENTAL_SPLIT_VERIFICATION.md`
(termination, I11 through the cascade, reconciliation with Foundation Theorem 3).
*What is wrong today:* `HOT_EXISTING_CODE_AUDIT.md` (5 divergence classes).
*Reusable verified primitives:* `HOTBulkBuilder` (R(S) construction, MSDB,
sparse-path encoding, SingleMask/MultiMask assembly). *Reference algorithm:*
`HOT_INCREMENTAL_SPLIT_VERIFICATION.md` §2; C++ at `~/IdeaProjects/hot-reference`.

---

## 1. Design overview

### 1.1 The two structural levels

Binna's reference has *compound nodes* + *single-entry leaves*. Sirix has:

- **`HOTIndirectPage`** = Binna's compound node (≤ 32 children; a flattened block of
  ≤ 31 BiNodes; SingleMask or MultiMask discriminative-bit layout; per-child
  partial keys; `mostSignificantBitIndex`; `height`).
- **`HOTLeafPage`** = a *multi-value bucket* of ≤ `C = 512` sorted entries. This is
  **not** in Binna's model. A leaf page stores a *complete `R(S)`-subtree* flat
  (`HOT_FORMAL_FOUNDATION.md` §3.2).

Consequence: there are **two** overflow events, both feeding the *same*
`integrateBiNodeIntoTree`:

| event | trigger | produces |
|-------|---------|----------|
| leaf-page overflow | a bucket exceeds `C` entries or page bytes | split at the bucket's key-set MSDB → 2 leaf pages → one `BiNode` |
| indirect-page overflow | a compound node already has 32 children | Binna's compound split at `node.MSB` → one `BiNode`, then capacity cascade |

### 1.2 Merge-vs-branch — the multi-value-leaf decision

Pure Binna creates a BiNode for *every* inserted key. With multi-value buckets, a
key whose branch point lies *inside* its routed leaf's `R(S)`-subtree simply
**merges into the bucket** — no BiNode. Precisely: descend to leaf page `L`, compute
the mismatch bit `β = MSDB(K, L)`.

- **β below `L`'s level** (K agrees with L's keys on every ancestor disc bit) ⇒
  **merge** K into L. If L overflows ⇒ leaf-page split.
- **β at an indirect's level** (K branches off above L) ⇒ K becomes a new
  single-entry leaf page; a BiNode on `β` is added at the owning indirect — Binna's
  ordinary insert.

### 1.3 The copy-on-write adaptation

Binna mutates nodes in place and walks a parent stack upward. Sirix is persistent:

- Every `addEntry` / `split` / integration **produces a new page** and
  `log.put`s it into the `TransactionIntentLog` — no page is ever mutated in place.
- `prepareLeafOfTree` already copy-on-writes every indirect on the descent path.
  The cascade **continues that discipline upward**: each integrated level is a
  fresh `HOTIndirectPage` CoW'd into the TIL; the chain stays path-copied to the
  index root.
- The **new-root** step re-points the index-page slot (`PathPage`/`CASPage`/
  `NamePage` `getOrCreateReference(indexNumber)`).
- Leaf-page splits respect versioned fragment chains and **carry tombstones**
  (`hot-tombstone-preservation`).

### 1.4 What is reused vs. replaced

*Reused as-is:* `HOTBulkBuilder` (and its `msdb`, `bitAt`, sparse-path encoder,
`assembleIndirect`); `HOTIndirectPage.computeDensePartialKey`; `findChildIndex`
routing (made pure highest-subset-match); the `prepareLeafOfTree` descent skeleton.
*Replaced:* `HOTTrieWriter`'s `handleLeafSplitAndInsert`, `splitParentAndRecurse`,
`updateParentForSplitWithPath`, `createNodeFromChildren`, `buildFlatNonStrict`,
`computeDiscBits`, `computeSparsePathRecursive`, and the entire
`extend`/`lift`/`reconcile`/`phase7*` family.

---

## 2. New / changed components

- **`BiNode`** (new): a **virtual** node — a transient `record BiNode(int
  discBitIndex, int height, PageReference left, PageReference right)` nested inside
  `HOTIncrementalInsert`, *not* a class in the `io.sirix.page` model. It mirrors the
  reference's ephemeral `split` return value: produced by a split, consumed
  immediately by integration, then discarded. It has **no** page key, TIL entry, or
  serialized form. When a BiNode must stand alone (the root or intermediate-node
  case) it is *materialized* as a 2-entry `HOTIndirectPage`. Physically the trie is
  only compound nodes (`HOTIndirectPage`) and leaf pages; a compound node internally
  flattens up to `k−1` BiNodes.
- **`HOTIndirectPage.split` / `addEntry`** (new methods, pure): `split` partitions a
  node at its own `MSB` into a `BiNode` of two fresh `HOTIndirectPage`s
  (faithful sparse-path re-encoding); `addEntry` returns a fresh `HOTIndirectPage`
  with one additional child slot for a new BiNode bit. No in-place mutation.
- **`HOTLeafPage` split** (new helper): split a bucket at its key-set MSDB into two
  fresh leaf pages; place the new `(K,V)`; carry tombstones; recurse a half that
  still overflows (rare byte-capacity case) via `HOTBulkBuilder`.
- **`HOTIncrementalInsert`** (new, `index/hot/`): the orchestration — descent-result
  processing, the merge-vs-branch decision, `integrateBiNodeIntoTree`, the cascade,
  the pull-up, and the CoW/TIL plumbing. This is the clean replacement for
  `HOTTrieWriter`'s insert path.
- **`AbstractHOTIndexWriter`**: `doIndex` drives `HOTIncrementalInsert`; the interim
  `rebuildOverflowedLeaf` family and the `STRICT_BINNA` block are removed.
- **`HOTTrieWriter`**: the dead/anti-pattern insert machinery is deleted (step 6),
  *after* confirming it has no non-secondary-index responsibilities.

---

## 3. Step-by-step implementation

Each step ends at a **compiling, test-green** state. Steps 1–2 touch no live insert
path (safe, isolated). Steps 3–5 touch `doIndex`.

### Step 1 — `BiNode` + leaf-page split

- Add `BiNode`.
- Add `HOTLeafPage` split: `splitWithNewEntry(L, key, value, allocator, revision,
  indexType) → BiNode`. Compute `β = msdb(firstKey, lastKey)` of the union of L's
  entries + `(K,V)`; partition the sorted union at `β` (a clean prefix/suffix cut);
  build two leaf pages; `BiNode{β, height 1, leftRef, rightRef}`.
- **Test** `HOTLeafPageSplitFaithfulTest`: adversarial buckets — both halves are
  valid leaf pages, `β` is the true key-set MSDB, every entry incl. tombstones
  preserved, union of halves == input ∪ {new}.

### Step 2 — indirect-node split + addEntry

- `HOTIndirectPage.split(insertContext) → BiNode`: partition the node's children at
  `node.MSB`; `compressEntries` each half into a fresh `HOTIndirectPage` (drop the
  consumed root bit, re-encode partials MSB-first, pick SingleMask/MultiMask);
  `BiNode{node.MSB, node.height+1, leftRef, rightRef}`.
- `HOTIndirectPage.addEntry(biNode, insertContext) → HOTIndirectPage`: a fresh node
  with the BiNode's bit folded into the mask and one extra child slot.
- **Test** `HOTIndirectPageSplitFaithfulTest`: validate each result with
  `HOTInvariantValidator` (I3/I4/I5/I7/I8/I11) over adversarial nodes; cross-check
  routing against `HOTBulkBuilder` of the same key set (Theorem IV corollary).

### Step 3 — descent: mismatch bit + insert depth

- Extend the descent (`prepareLeafOfTree` or a sibling pass) to also return: the
  resident key it diverged from, the mismatch `β`, the insert depth `d*`
  (`while (index(β) > stack[d+1].MSB) ++d`), and the affected-subtree info
  (`getInsertInformation` equivalent on Sirix's mask layout).
- Keep the existing top-down CoW of the descent path unchanged.

### Step 4 — `integrateBiNodeIntoTree` + cascade + pull-up

- `HOTIncrementalInsert.integrate(path, depth, biNode)`:
  - `depth == 0` → wrap in a 2-entry `HOTIndirectPage`; `log.put`; **re-point the
    index-page root slot**; height grows by one.
  - `parent.height > biNode.height` → intermediate node: a fresh 2-entry indirect
    in the old slot.
  - else `parent` not full → `parent.addEntry(biNode)` → CoW the parent into the
    TIL.
  - else `parent` full → assert `index(parent.MSB) < index(biNode.discBit)`;
    `B' = parent.split(...)`; **recurse `integrate(path, depth-1, B')`**.
- **Pull-up:** when `β` is more significant than the target node's MSB, wrap the
  whole node under a fresh `BiNode{β}` and integrate at the same depth.
- Every created page → `log.put`; every cascade level path-copied to the root.

### Step 5 — wire into `doIndex`

- `doIndex`: descend (step 3) → merge-vs-branch (§1.2). Merge path: `mergeWithNodeRefs`;
  on overflow → leaf-page split (step 1) → `integrate` (step 4). Branch path: new
  leaf page + BiNode at `d*` → `addEntry`/`split` → `integrate`.
- Remove the interim `rebuildOverflowedLeaf` family and the `STRICT_BINNA` block.
- `handleInsertFailure`'s compaction attempts are kept (they precede any split).

### Step 6 — delete dead code

- Delete the confirmed-dead `HOTTrieWriter` members (`commitTimeLiftAllChildMsbs`,
  `liftChildMsbsForI11`, `phase7kRecursiveCommit`, `phase7jExtendWithAllClosureBits`,
  `reconcileRootMaskI11Safe`) and the now-unreferenced `extend`/`split`/`integrate`
  cruft. Verify each has no caller (incl. the document trie) before removal.

### Step 7 — verify

- `HOTVersionedLeafStressTest` + `HOTFormalVerificationTest` green, I8 on.
- Build `HOTIncrementalInsertVerificationTest` — the executable model V1–V5 from
  `HOT_INCREMENTAL_SPLIT_VERIFICATION.md` §10 (incremental == canonical; invariants
  after every insert; cascade coverage; termination bound; end-to-end).

---

## 4. Copy-on-write / TIL discipline (invariant for every step)

1. No `HOTIndirectPage` or `HOTLeafPage` is mutated in place. Every op returns a new
   page; `log.put(ref, PageContainer.getInstance(page, page))` registers it.
2. The descent path is already CoW'd; the cascade path-copies each ancestor it
   touches, so root→modified is always a chain of TIL-resident pages — required for
   `commit` to reach the modification.
3. A new root re-points the index-page slot; `prepareIndexPage` keeps the index page
   itself CoW'd.
4. Leaf-page splits preserve tombstones and emit new pages as full first fragments
   under DIFFERENTIAL/INCREMENTAL versioning.
5. Orphaned transaction-private leaf copies should be `close()`d to release their
   off-heap segments — but only the private copy, never a page shared with history.

---

## 5. Risk register (from `HOT_INCREMENTAL_SPLIT_VERIFICATION.md` §9)

| risk | mitigation |
|------|------------|
| split bit chosen locally, not subtree-invariant | leaf split at key-set MSDB; indirect split at `node.MSB` — never a positional or boundary-key bit |
| partials encoded from `firstKey` (F2) | encode by sparse-path from the BiNode path; reuse `HOTBulkBuilder`'s encoder |
| 1:31 split height accounting | replicate the reference's `numberEntriesInLowerPart` recomputation |
| per-node height unreliable | `BiNode.height = splitNode.height + 1` exactly; never default to 0 |
| missing trie-condition check | assert `index(parent.MSB) < index(biNode.discBit)` at every integration |
| missing pull-up ⇒ in-place mask repair | implement the pull-up; never `mask |= bit` |
| leaf page is not an `R(S)`-subtree | the merge-vs-branch decision keeps every bucket a complete `R(S)`-subtree |

---

## 6. File change list

- **New:** `index/hot/HOTIncrementalInsert.java` (with a nested transient `BiNode`
  record — §2), tests `HOTLeafPageSplitFaithfulTest`,
  `HOTIndirectPageSplitFaithfulTest`, `HOTIncrementalInsertVerificationTest`.
- **Modified:** `page/HOTIndirectPage.java` (+`split`, +`addEntry`),
  `page/HOTLeafPage.java` (+faithful split), `index/hot/AbstractHOTIndexWriter.java`
  (`doIndex`/`handleInsertFailure` rewired; interim edits removed).
- **Deleted (step 6):** the dead + anti-pattern insert machinery in
  `access/trx/page/HOTTrieWriter.java`.
- **Kept:** `HOTMalformedSubtreeDetector` (verification/repair tool, not runtime),
  `HOTBulkBuilder`, `HOTIndirectPage.computeDensePartialKey`.

---

## 7. Acceptance criteria

1. Steps 1–2 primitives validate clean against `HOTInvariantValidator` over
   adversarial inputs.
2. After **every** insert (V2), `HOTInvariantValidator` reports zero violations.
3. `HOTVersionedLeafStressTest` + `HOTFormalVerificationTest` green, I8 on.
4. Incremental build == `HOTBulkBuilder` of the same key set (V1, determinism).
5. Routing is pure highest-index subset match — the exact-match band-aid removed.
6. No `extend`/`lift`/`reconcile` mask-repair operator remains in the tree.
