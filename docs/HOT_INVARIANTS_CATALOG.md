# HOT Invariants Catalog (Stage A — Multi-Session Formal Verification)

**Purpose**: enumerate every invariant the HOT design requires, sourced from primary references, with precise predicates and Sirix-coverage status. This is the source of truth for Stage B (operations × invariants matrix), Stage C (complete validator), and beyond.

**Sources cross-referenced**:
1. **C++ reference** — `/home/johannes/IdeaProjects/hot-reference/`. The canonical implementation. Authoritative for every invariant's intent.
   - `HOTSingleThreadedNode.hpp` (~890 lines)
   - `HOTSingleThreaded.hpp` (~656 lines)
   - `HOTSingleThreadedNodeBase.hpp` (86 lines)
   - `SparsePartialKeys.hpp` (386 lines)
   - `BiNode.hpp` + `BiNodeInterface.hpp`
   - `MultiMaskPartialKeyMapping.hpp`
   - `SingleMaskPartialKeyMapping.hpp`
2. **Sirix design doc** — `docs/HOT_STRICT_BINNA_DESIGN.md`. Sirix-specific deviations + phase plan.
3. **Phase 4b diagnosis** — `docs/HOT_PHASE_4B_DIAGNOSIS.md`. Empirical findings from the campaign.
4. **Existing Sirix validator** — `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java` (732 lines).
5. **Sirix writer** — `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`. Existing inline asserts + invariant comments.

**Methodology**: every invariant is given a stable name (Sirix-side I-prefix where possible), a formal predicate, the operation(s) that must preserve it, and explicit current-coverage status. The catalog is exhaustive — any invariant in any source must appear here. Missing invariants are flagged for Stage C.

---

## §1. Reference Naming Convention

The Sirix campaign uses these names (some from the design doc, some Sirix-specific):

| Sirix Name | Binna/C++ origin | Predicate (informal) |
|---|---|---|
| **I1-leaf-key-uniqueness** | implicit (single-key leaves trivially unique) | within a leaf, every key is distinct |
| **I2-leaf-lex-sorted** | implicit | leaf entries stored in ascending lex order |
| **I3-partial-key-uniqueness** | C++ HOTSingleThreadedNode.hpp:387-394 (search-correctness assert) | within an indirect, no two children share a partial key |
| **I4-first-partial-zero** | C++ B.1 — "first mask always zero" (HOTSingleThreadedNode.hpp:179, 244, 267, 292, 320) | indirect.partialKeys[0] == 0 |
| **I5-leaf-constancy** | C++ C.6 — subtree prefix constancy | for every disc bit β captured at indirect I, every key in I.child[s].subtree has β value matching I.partialKeys[s] |
| **I6-pext-routes-to-leaf** | C++ D.5 — leaf routing soundness | for every stored key K, descend(K) lands at K's actual leaf |
| **I7-partial-keys-sorted** | C++ D.4 — sort order rule | reorder(partialKey[i]) is strictly ascending across children |
| **I8-children-sorted-by-firstkey** | implied by Binna's leaf semantics (= partial-key-order ≡ first-key-order under sparse-path) | child[i-1].firstKey < child[i].firstKey |
| **I9-height-bounded** | C++ overall design | tree height ≤ MAX_TREE_HEIGHT |
| **I10-fanout-bounded** | C++ A.2 — minimum 2 entries; max 32 (MAX_NODE_ENTRIES) | 2 ≤ indirect.numChildren ≤ MAX_NODE_ENTRIES |
| **I11-trie-condition** | C++ C.5 — `parent.MSB < child.discBit` (HOTSingleThreaded.hpp:521-523) | for every indirect with parent, parent.mostSigDiscBit < indirect.discBitsInRange.highest |
| **I12-disc-bit-monotone** | C++ asserts A.6, A.7 — insert bit ≠ parent MSB | when adding a new disc bit β, β must not equal any existing parent disc bit on the path |
| **I-Binna-sparse-path** | C++ B.4 (`(dense & sparse) === sparse`) | for every indirect I, every child c at slot s: `(I.partialKeys[s] & ~PEXT(c.firstKey, I.mask)) == 0` |
| **I-PDEP-PEXT-identity** | C++ D.7 | for every conversionMask M and value X: `_pext_u32(_pdep_u32(X, M), M) == X` |
| **I-Sparse-Path-Subtree** | extension of I-Binna for multi-entry leaves | same as I-Binna-sparse-path but ALL keys in c.subtree, not just c.firstKey (= I5 strict variant) |
| **I-CoW-pageKey-identity** | Sirix-specific (CoW model) | parent's pageKey is preserved across mutations via `withUpdatedChild` |
| **I-CoW-multi-rev-isolation** | Sirix-specific | revision N reads must see revision-N-state of every page (no cross-revision leak) |

---

## §2. Catalog: Every Invariant, Cross-Referenced

For each invariant: **(name)** — predicate, source citations, operations that MUST preserve it, current Sirix-side validator coverage, current writer-side enforcement.

### I1 — Leaf-Key-Uniqueness

**Predicate**: ∀ leaf L, ∀ keys k1, k2 ∈ L: k1 = k2 ⇒ k1 and k2 are the same entry.

**Source**: implicit in C++ (single-key leaves). Sirix-specific need due to multi-entry leaves.

**Preservation operations**:
- `HOTLeafPage.put(key, value)` — must dedupe on insert.
- `HOTLeafPage.mergeWithNodeRefs(key, value)` — must merge values for existing key, not duplicate.
- `splitToWithInsert` — must not introduce duplicates in either half.

**Current coverage**:
- Validator: ✅ checked (line 226 `addViolation("I1-leaf-key-uniqueness", ...)`).
- Writer: implicit via `findEntry` + put/merge contract.

---

### I2 — Leaf-Lex-Sorted

**Predicate**: ∀ leaf L, ∀ adjacent indices i, i+1: L.keys[i] < L.keys[i+1] (lex unsigned).

**Source**: implicit. Necessary for binary search + adjacent-pair MSDB computation.

**Preservation operations**:
- `HOTLeafPage.put` — insert at correct sorted position.
- `splitToWithInsert` — both halves sorted.
- `mergeWithNodeRefs` — preserves order.

**Current coverage**:
- Validator: ✅ checked (line 234 `addViolation("I2-leaf-lex-sorted", ...)`).
- Writer: enforced via `findEntry` lookup + `mergeWithNodeRefs` insertion at `insertPos`.

---

### I3 — Partial-Key-Uniqueness

**Predicate**: ∀ indirect I, ∀ slots i ≠ j: I.partialKeys[i] ≠ I.partialKeys[j].

**Source**: C++ HOTSingleThreadedNode.hpp:387-406 (`getInsertInformation`'s search-correctness assert). Binna §4.2.

**Preservation operations**:
- `addEntryWithPDep` (line 939) — explicit uniqueness check at line 1145 (`for i, k: if newPartialKeys[i] == newPartialKeys[k] return null`).
- `buildCompressedHalf` — uniqueness check at line 4862 (`BCH_FALLBACK_PARTIAL_COLLISION`).
- `addEntryFreshPolaritySingleMask` (line 1245+) — gate 1 check.
- `upgradeToMultiMaskWithNewBit` — partial-uniqueness verified line 3027+.
- `createNodeFromChildren` / `buildFlatNonStrict` — does NOT check (= can produce I3 violations under multi-entry-leaf pathology).

**Current coverage**:
- Validator: ✅ checked (line 270 `addViolation("I3-partial-key-uniqueness", ...)`).
- STRUCTURAL_LIMITATION_INVARIANTS: ✅ flagged as soft (= warning, not failure).
- Writer: ✅ explicit guards at most construction sites; `createNodeFromChildren-N` is the broken outlier.

---

### I4 — First-Partial-Key-Zero

**Predicate**: ∀ indirect I: I.partialKeys[0] == 0.

**Source**: C++ HOTSingleThreadedNode.hpp:179, 244, 267, 292, 320 — comment: *"This is important for the tree to have fast lookup and maintain integrity!! the first mask always is zero!!"*

**Why it matters** (per C++ author's note):
- Provides a consistent starting point for navigation.
- Ensures partial key comparisons are order-preserving.
- Enables fast lookup by having a known, distinguished entry.

**Preservation operations**:
- Every indirect-construction site (`createSpanNode`, `createMultiNode`, `createSpanNodeMultiMask`, `createMultiNodeMultiMask`, `createBiNode`).

**Current coverage**:
- Validator: ❌ **NOT CHECKED**. ★ GAP — Stage C must add.
- Writer: implicit via `sortChildrenAndPartialsByPartial` (= 0 sorts to slot 0). Not explicitly asserted.

**Verification predicate for Stage C**:
```java
if (partials != null && partials.length > 0 && partials[0] != 0) {
  addViolation("I4-first-partial-zero",
      "indirect " + I.pageKey + ".partialKeys[0]=0x" + hex(partials[0])
      + " — must be 0 per Binna invariant", I);
}
```

**Phase-4b connection**: The 1 marginal I8 violation in default mode comes from the intermediate-BiNode workaround. The BiNode wraps leftChild + rightChild. After `withUpdatedChild(splitChildIdx, biNodeRef)`, parent's partial at slot splitChildIdx is unchanged. If that partial was non-zero, slot 0's value is unchanged (still 0 if it was 0). So I4 likely holds. ★ But: does Sirix's `createBiNode` set partialKeys[0]=0? Audit needed in Stage B.

---

### I5 — Leaf-Constancy (Disc-Bit Constancy in Subtree)

**Predicate**: ∀ indirect I with disc bits Mask_I, ∀ slots s, ∀ keys K in I.child[s].subtree:
`PEXT(K, Mask_I) ⊇ I.partialKeys[s]` (the partial is a subset of every subtree key's dense PEXT).

**Equivalently**: for every bit β in I.partialKeys[s] set to 1, every key in I.child[s].subtree has β=1 at that absolute position.

**Source**: C++ HOTSingleThreadedNode.hpp:141-158 (PDEP-based partial-key conversion preserves subtree prefix). The invariant is structural — Binna's single-key leaves have it trivially; Sirix's multi-entry leaves can violate it via β-mixing.

**Preservation operations**:
- `splitToWithInsert` — must not create β-mixed leaves on ancestor disc bits.
- `addEntryWithPDep` — `bitConstantValueInSubtree` gate ensures preservation **at construction time**, but doesn't prevent later violations from subsequent inserts.
- `addEntryFreshPolaritySingleMask` — gate 3 (subtree-β-constancy).
- `splitParentAndRecurse` → `buildCompressedHalf` — repositioned partials assume subtree constancy holds.

**Current coverage**:
- Validator: ✅ added 2026-05-09 (`I5-leaf-constancy`, lines 391-432). Walks each child's entire subtree, checks (sparse & ~dense_K) == 0 for every K. Catches the multi-entry-leaf pathology that the original I-Binna-sparse-path (= against c.firstKey only) missed.
- Writer: NOT enforced at insert time. β-constancy can be silently broken by multi-entry-leaf inserts post-construction. ★ This is the structural gap requiring leaf-level redesign.

---

### I6 — PEXT-Routes-To-Leaf

**Predicate**: ∀ stored key K: `descend(rootRef, K)` lands at the leaf containing K.

**Source**: C++ D.5 — `extractAndMatchLeafValue` validates `extractKey(value) == key` after traversal.

**Preservation operations**: every operation must preserve routing soundness — adding a disc bit, inserting, splitting, merging.

**Current coverage**:
- Validator: ✅ checked (line 451-471, `verifyPextRouting`).
- STRUCTURAL_LIMITATION_INVARIANTS: ✅ flagged as soft.
- Writer: implicit. Cascades from I5 / I-Binna-sparse-path violations (= broken routing surfaces here).

---

### I7 — Partial-Keys-Sorted

**Predicate**: ∀ indirect I, ∀ adjacent slots i, i+1: `partialKeys[i] < partialKeys[i+1]` (unsigned).

**Source**: C++ D.4 — order-preservation. HOTSingleThreadedNode.hpp:788-811 (`isPartitionCorrect` sort-order verification).

**Preservation operations**:
- `addEntryWithPDep` — `sortChildrenAndPartialsByPartial` lock-step sort (line 1147).
- `buildCompressedHalf` — sort after partial computation (line 4880).
- `addEntryFreshPolaritySingleMask` / `MultiMask` — sorts.

**Current coverage**:
- Validator: ✅ checked (line 284 `addViolation("I7-partial-keys-sorted", ...)`).
- STRUCTURAL_LIMITATION_INVARIANTS: ✅ flagged as soft.
- Writer: ✅ enforced via lock-step sort at every construction.

---

### I8 — Children-Sorted-By-FirstKey

**Predicate**: ∀ indirect I, ∀ adjacent slots i, i+1: `firstKey(I.child[i]) < firstKey(I.child[i+1])`.

**Source**: implied by Binna's design (single-key leaves: partial-key order ≡ first-key order). Sirix-specific concern when multi-entry leaves break the equivalence.

**Preservation operations**:
- `sortChildrenByFirstKey` invocations.
- `intermediateBiNodePreservesSlotOrder` gate (= the c868e669c workaround's check).
- `splitParentAndRecurse` → `buildCompressedHalf` (sort step).

**Current coverage**:
- Validator: ✅ checked (line 303 `addViolation("I8-children-sorted-by-firstkey", ...)`).
- HARD violation (not in STRUCTURAL_LIMITATION_INVARIANTS).
- Writer: enforced via `intermediateBiNodePreservesSlotOrder` (= prev-side check only, currently misses next-side break — the 1 marginal violation).

**Phase-4b connection**: The 1 marginal I8 violation in default mode is at this invariant. Tightening the gate to also check next-side cascades (= 5995 violations) due to multi-entry-leaf β-mixing.

---

### I9 — Height-Bounded

**Predicate**: tree.height ≤ MAX_TREE_HEIGHT (currently 64 in `HOTTrieWriter`).

**Source**: C++ overall design.

**Preservation operations**: every height-increasing op (= split, BiNode wrap) must check.

**Current coverage**:
- Validator: ✅ checked (line 161 `addViolation("I9-height-bounded", ...)`).
- Writer: ✅ via `MAX_TREE_HEIGHT` bound + `IllegalStateException` at line 756 (`if (height >= MAX_TREE_HEIGHT) throw`).

---

### I10 — Fanout-Bounded

**Predicate**: ∀ indirect I: 2 ≤ I.numChildren ≤ MAX_NODE_ENTRIES (32 currently in Sirix).

**Source**: C++ A.2, A.14 (numberEntries >= 2; max 32 from MAXIMUM_NUMBER_NODE_ENTRIES).

**Preservation operations**: every indirect-construction site.

**Current coverage**:
- Validator: ✅ checked (lines 255-262 `addViolation("I10-fanout-bounded", ...)`).
- Writer: ✅ via `NodeUpgradeManager.MULTI_NODE_MAX_CHILDREN` cap; cascade triggers `splitParentAndRecurse`.

---

### I11 — Trie-Condition (Monotonic Disc-Bit Significance Down the Tree)

**Predicate**: ∀ indirect I with parent P: `P.mostSigDiscBit < I.discBitsRange.high`.

**Equivalently**: descending the tree, disc bits become strictly less significant (in MSB-first absolute ordering, ≥ in absolute position).

**Source**: C++ HOTSingleThreaded.hpp:521-523 — assert + comment: *"the diffing Bit index cannot be larger as the parents mostSignificantBitIndex"*.

**Why it matters**: ensures split points are ordered; no bit can be used twice on the same path → guarantees finite tree height.

**Preservation operations**:
- `addEntryWithPDep` — when adding β to parent's mask, β must be > all parent's existing disc bits (or, more precisely, β's absolute position ≥ parent.mostSigBitIndex's absolute position).
- `splitParentAndRecurse` — recursion preserves trie condition by construction.

**Current coverage**:
- Validator: ❌ **NOT CHECKED**. ★ GAP — Stage C must add.
- Writer: implicit via construction order, but not asserted explicitly.

**Verification predicate for Stage C**:
```java
// During tree walk, parent.mostSigDiscBit must be ≤ every disc bit in I.
final int parentMSB = parent.getMostSignificantBitIndex(); // smallest abs pos in parent's mask
for (int abs : ancestorDiscBitsOf(I)) {
  if (abs < parentMSB) {
    addViolation("I11-trie-condition",
        "indirect " + I.pageKey + " has disc bit " + abs
        + " which is more significant than parent.MSB " + parentMSB, I);
  }
}
```

---

### I12 — Disc-Bit-Monotone (= no double-use of disc bits on a path)

**Predicate**: ∀ root-to-leaf path P, ∀ disc bits β, β': if β is captured by some indirect on P and β' is captured by another indirect on P, then β ≠ β'.

**Source**: C++ A.6, A.7 — `assert(insertBit != parent.MSB)` ensures uniqueness when adding new disc bits.

**Preservation operations**:
- `addEntryWithPDep` — when adding β, must verify β ∉ parent.mask (and recursively β ∉ any-ancestor.mask).
- The trie condition (I11) is a stronger version that subsumes I12.

**Current coverage**:
- Validator: ❌ **NOT CHECKED** explicitly. Implicitly true if I11 holds.
- Writer: line 967 (`if ((oldMask & newBitMaskBit) != 0L)`) — handles the case-2b-iv-b-β path. If β is in mask, doesn't re-add — but this is for the SAME indirect, not ancestors.

**Verification predicate for Stage C**:
```java
final Set<Integer> seenDiscBits = new HashSet<>();
for (int depth = 0; depth < pathDepth; depth++) {
  for (int abs : appendDiscBitsOfIndirect(pathNodes[depth])) {
    if (!seenDiscBits.add(abs)) {
      addViolation("I12-disc-bit-monotone",
          "absolute bit " + abs + " captured by multiple indirects on path "
          + describePath(pathNodes, depth), pathNodes[depth]);
    }
  }
}
```

---

### I-Binna-Sparse-Path (Necessary Subset Condition)

**Predicate**: ∀ indirect I, ∀ slot s: `(I.partialKeys[s] & ~PEXT(I.child[s].firstKey, I.mask)) == 0`.

**Equivalently**: every bit set in the stored partial is also set in the dense PEXT of the child's first key.

**Source**: C++ B.4 — *Compliance: `(densePartialKey & sparsePartialKey) === sparsePartialKey`*.

**Why it's "necessary" not "sufficient"**: stored partial is a SUBSET of dense (HOT's sparse-path encoding intentionally elides off-path bits). Equality holds only when c is single-keyed (= Binna).

**Preservation operations**: every indirect-construction site that produces stored partials. Listed under each addEntry / buildCompressedHalf / etc.

**Current coverage**:
- Validator: ✅ checked (line 357 `addViolation("I-Binna-sparse-path", ...)`).
- Validator-strict variant: ✅ I5-leaf-constancy adds the SAME check applied to every key in subtree, not just firstKey.
- Writer: not explicitly asserted; implicitly preserved by correct partial encoding (= the bug surface our campaign keeps probing).

---

### I-PDEP-PEXT-Identity (Round-trip)

**Predicate**: ∀ values X, ∀ masks M:
`Long.compress(Long.expand(X, M), M) == X & ((1 << popcount(M)) - 1)`
`Long.expand(Long.compress(X, M), M) == X & M`

**Source**: C++ D.7 — bit manipulation soundness. Sirix's Phase 4b.0 round-trip test (= `MultiMaskSubLayoutTest`).

**Preservation operations**: tested by unit tests, not at runtime.

**Current coverage**:
- Validator: N/A (algebraic property, tested separately).
- Tests: ✅ `MultiMaskSubLayoutTest` (12 tests).
- Writer: relies on JDK 21+'s correct `Long.compress` / `Long.expand` implementations.

---

### I-CoW-PageKey-Identity

**Predicate**: across operations that mutate a page, the page's `pageKey` is preserved (= `withUpdatedChild` returns a new page object with the SAME pageKey).

**Source**: Sirix-specific (CoW model).

**Preservation operations**:
- `HOTIndirectPage.withUpdatedChild`.
- `HOTLeafPage.copy` + subsequent mutations.

**Current coverage**:
- Validator: ❌ NOT CHECKED. Could be added but is an internal invariant rarely violated.
- Writer: structurally enforced.

---

### I-CoW-Multi-Rev-Isolation

**Predicate**: ∀ revision N, ∀ key K stored in revision N: read at revision N returns K's revision-N value (no cross-revision leak via shared CoW pages).

**Source**: Sirix-specific (multi-revision storage model).

**Preservation operations**: every CoW path must deep-copy when updating.

**Current coverage**:
- Validator: existing multi-version regression tests verify this end-to-end.
- Writer: enforced via top-down CoW.

---

## §3. Coverage Summary — What's Currently Checked vs. Gaps

### Currently checked (Sirix validator)

| Invariant | Hard/Soft | Coverage |
|---|---|---|
| I1-leaf-key-uniqueness | hard | ✅ |
| I2-leaf-lex-sorted | hard | ✅ |
| I3-partial-key-uniqueness | **soft** (in STRUCTURAL_LIMITATION_INVARIANTS) | ✅ |
| I5-leaf-constancy | hard | ✅ (added 2026-05-09) |
| I6-pext-routes-to-leaf | **soft** | ✅ |
| I7-partial-keys-sorted | **soft** | ✅ |
| I8-children-sorted-by-firstkey | hard | ✅ |
| I9-height-bounded | hard | ✅ |
| I10-fanout-bounded | hard | ✅ |
| I-Binna-sparse-path | hard | ✅ |
| structural (page-kind, cycle, null-child, unloadable-child) | hard | ✅ |

### ★ GAPS — Invariants in C++ but NOT in Sirix validator

| Invariant | Why missing | Stage-C action |
|---|---|---|
| **I4-first-partial-zero** | Never explicitly added (implied by sorting). Could surface bugs in BiNode construction. | Add direct check on every indirect's `partialKeys[0]`. |
| **I11-trie-condition** | Implicitly true via construction; not explicitly verified. Could surface tree-shape bugs. | Add path-walking check: parent.MSB ≤ every-disc-bit-in-child. |
| **I12-disc-bit-monotone** | Subsumed by I11; redundant if I11 added. | Optional; add if I11 turns out insufficient. |
| **I-Sparse-Path-Subtree** (= I5-strict against subtree, not firstKey) | Was missing until 2026-05-09. | ✅ Added (= I5-leaf-constancy). |
| **C++ assert A.5** (insertDepth < leafDepth) | Implicit in Sirix's descend logic; could surface wrong-depth bugs. | Add as a path-construction post-condition check. |
| **C++ assert A.13** (merge entry count formula) | N/A in Sirix's CoW model (no in-place merges). | Skip. |

### ★ Construction-site gaps — Where Sirix preserves vs. relies on luck

| Construction site | Preserves I3 | Preserves I4 | Preserves I5/I-Binna | Preserves I7 | Preserves I8 | Preserves I11 |
|---|---|---|---|---|---|---|
| `addEntryWithPDep` | ✅ explicit | ❓ implicit | ✅ when bitConstantValueInSubtree==1 | ✅ explicit | ❓ implicit (sort by partial assumed = sort by firstkey) | ❓ unchecked |
| `addEntryFreshPolaritySingleMask` | ✅ gate 1 | ❓ | ✅ gate 3 | ✅ | ❓ | ❓ |
| `addEntryMultiMask` | ✅ | ❓ | ✅ when constancy holds | ✅ | ❓ | ❓ |
| `upgradeToMultiMaskWithNewBit` | ✅ | ❓ | ✅ when constancy holds | ✅ | ❓ | ❓ |
| `buildCompressedHalf` (SingleMask) | ✅ via partial-collision check | ❓ | ❌ inherited via compress→expand from STALE oldPartial | ✅ | ❓ | ❓ |
| `buildCompressedHalfMultiMask` | ✅ | ❓ | ❌ same staleness issue | ✅ | ❓ | ❓ |
| `createNodeFromChildren-N` (= `buildFlatNonStrict`) | ❌ no check | ❓ | ❌ adjacent-pair XOR scan captures non-constant bits | ✅ | ❓ | ❓ |
| `intermediate-BiNode workaround` (c868e669c) | ✅ | ❓ | ✅ | ✅ | ❌ next-side check missing | ❓ |
| `splitParentAndRecurse` | inherits from `buildCompressedHalf` | inherits | inherits | inherits | inherits | ❓ |

★ Highlighted: cells where the operation may produce violations on multi-entry-leaf trees.

### ★ The campaign's recurring failure pattern, explained by this matrix

Every fix attempt addressed ONE column (one operation × invariant pair) without checking the others. The cascades came from:
- Tightening I8 (intermediate-BiNode next-side check) → unleashes splitParentAndRecurse, which fails I5 / I-Binna under multi-entry-leaf β-mixing.
- Removing OR siblingBitValues (sparse-path encoding in addEntryWithPDep) → still produces I-Binna violations from `rightChild`'s post-construction subtree-mutation.
- Eager Phase 2 split → breaks I7 / I8 (non-MSDB splits non-contiguous).

Each was a localized patch, not a coherent fix. **Stage F's design must close all 9 columns × 11 rows simultaneously.**

---

## §4. Multi-Entry-Leaf Specific Concerns

Sirix deviates from Binna on one architectural point: multi-entry leaves (≤512 keys/leaf) vs Binna's single-key leaves. This deviation creates **invariant fragility** that doesn't exist in C++ HOT:

| C++ HOT property | Sirix implication |
|---|---|
| Single-key leaf → β-constant for every bit (trivially) | Multi-entry leaf → can hold β-mixed keys; β-constancy must be ENFORCED at insert time |
| Stored partial = dense PEXT of the (only) key | Stored partial reflects firstKey at SOME EARLIER moment; goes stale as smaller keys are inserted |
| Subtree's keys all share leaf's bit pattern | Subtree may have keys with diverging bits if multi-entry leaves below have β-mixed content |
| Adding β as parent disc bit always preserves descendant constancy | Adding β to parent doesn't propagate to existing leaves; legacy β-mixing persists |

The proper architectural responses (per design doc §3):
- **(a) Eager**: at every leaf insert, verify K's β values match leaf's existing keys at every ancestor disc bit. If not, split-on-β BEFORE inserting.
- **(b) Lazy**: when an ancestor adds β, mark all descendant leaves dirty; fix on next access.
- **(c) Single-key leaves**: revert Sirix to Binna's design; major refactor.

Each option has cost; Stage F must pick.

---

## §5. Operations Inventory (= Stage B Preview)

For Stage B, the operations × invariants matrix needs every mutating operation. Here's the inventory:

**Leaf-level operations** (HOTLeafPage):
- `put(key, value)` — direct insert.
- `mergeWithNodeRefs(key, value)` — merge or insert.
- `splitToWithInsert(target, key, ...)` — overflow split.
- `splitToWithInsertOnBit(target, key, ..., explicitBit)` — explicit-bit split (Phase 2 plumbing).

**Indirect-construction operations** (HOTTrieWriter):
- `createBiNodeTraced` — 2-child BiNode at any height.
- `createSpanNode` / `createMultiNode` — SingleMask layouts.
- `createSpanNodeMultiMask` / `createMultiNodeMultiMask` — MultiMask layouts.

**Indirect-modification operations** (HOTTrieWriter):
- `addEntryWithPDep` — adds new disc bit to SingleMask parent.
- `addEntryMultiMask` — same for MultiMask parent.
- `addEntryFreshPolaritySingleMask` / `addEntryFreshPolarityMultiMask` — Case 2b-iv-b-β.
- `upgradeToMultiMaskWithNewBit` — SingleMask → MultiMask upgrade.
- `withUpdatedChild` (HOTIndirectPage) — slot pointer swap.

**Tree-level operations** (HOTTrieWriter):
- `handleLeafSplitAndInsert` — leaf overflow → integrate.
- `updateParentForSplitWithPath` — propagate split up tree.
- `splitParentAndRecurse` — parent overflow → split + recurse.
- `buildCompressedHalf` / `buildCompressedHalfMultiMask` — half-of-split builder.
- `intermediateBiNodePreservesSlotOrder` workaround — c868e669c path.
- `tryPhase4SubtreeMerge` / `subtreeMerge` / `hoistAndReroute` — Phase 4 paths.
- `rebalanceAndIntegrate` — Phase 3.
- `rebuildParentAbsorbingSplit` — fallback rebuild.
- `createNodeFromChildren` / `buildFlatNonStrict` — broken multi-child path.

**Total**: ~25 operations × 14 invariants = 350-cell matrix for Stage B. Each cell answers: does this operation preserve this invariant, and via what mechanism?

---

## §6. Path Forward — Stages B Through H

| Stage | Goal | Output |
|---|---|---|
| **A (this doc)** | Catalog every invariant from every source. | `HOT_INVARIANTS_CATALOG.md` (this doc) ✅ |
| **B** | Operations × invariants matrix. For every mutating op, document which invariants it claims to preserve and via what mechanism. | `HOT_OPERATIONS_INVARIANTS_MATRIX.md` |
| **C** | Add the 4 missing invariants to `HOTInvariantValidator` (I4, I11, plus any others surfaced in B). Each new check must have a positive + negative unit test. | Extended validator + tests showing each fires correctly. |
| **D** | Add post-mutation invariant verification: every mutating operation calls validator on the affected sub-tree (or full tree, gated on flag). | Instrumented writer; flag `-Dhot.debug.postmutation=true`. |
| **E** | Run reproducer with C+D; capture every failure with operation × invariant × frequency. | `HOT_FAILURE_TABLE.md` — empirical data. |
| **F** | From E's data, design the comprehensive fix. Per-operation preservation logic for every invariant. | `HOT_FIX_DESIGN.md`. |
| **G** | Implement fix. 0 violations on 50K + all regression tests green. | Code + tests. |
| **H** | Phase 5 perf validation + Phase 6 cleanup. | Per design doc §4. |

**This session ends here, Stage A complete.** The next session starts Stage B.
