# HOT Option B Phase 5 — Integrated Constancy-Aware Insertion

**Goal:** Drive the 50K microbench reproducer to **0 violations** while preserving
production-workload correctness. Multi-week, multi-session task. Engineering with
formal verification at each step.

**Branch:** `fix/hot-strict-binna-conformance`.
**Baseline:** 1 marginal I8 violation at root indirect 2. 118 commits ahead of main.

---

## §1. Architectural Diagnosis (Recap)

### 1.1 The Marginal Violation

Empirically observed via `[G32-root-dump]` instrumentation:

```
Root indirect 2:
  layout=MULTI_MASK MSB=81 numChildren=5
  extractionPositions=[10, 12] extractionMasks=[0x401a000000000000]
  partials=[0, 8, 9, 10, 12]

child[0] = HOTLeafPage     firstKey=...0007 80 00 00 ...
child[1] = HOTIndirectPage firstKey=...0007 c0 00 00 ...
child[2] = HOTIndirectPage firstKey=...0007 c0 a0 02 ...  ← I8 break
child[3] = HOTIndirectPage firstKey=...0007 c0 80 08 ...  ← I8 break
child[4] = HOTIndirectPage firstKey=...0007 c0 70 10 ...
```

`partials` stored at construction time; current firstKeys produce different (and
non-unique) partials under root's mask. **Stale-firstKey artifact.**

### 1.2 Why Post-Hoc Mask Extension Fails (G.28–G.32)

Three iteration strategies were empirically proven impossible (commits
`b79794ee3`, `eaec09c89`, `0f95a669c`):

1. **Constancy-only extend** (`extendMaskWithBitI11Safe`): rejected by β-constancy
   gate — multi-entry leaves can hold any bit value at any position.
2. **Split-aware extend** (`extendIndirectMaskForClosureI11Safe`): splits maintain
   β-constancy but produce halves whose firstKeys collide on the pre-existing
   mask bits (= byte 10 bit 1 is dead-weight; doesn't discriminate).
3. **LIFT** (= absorb children's MSB into root): same partial-collision.

**Conclusion:** Post-hoc mask extension cannot simultaneously satisfy I3 (unique
partials), I4 (first partial = 0), I6 (PEXT routing correctness via β-constancy),
and I11 (parent.MSB < child.MSB). The fix must run **at insertion time**, before
the offending firstKey configuration crystallizes.

---

## §2. Phase 5 Strategy: Pre-Merge Constancy Enforcement

### 2.1 Core Invariant to Maintain

For every ancestor A with disc bit β captured in A.mask:
> Every key in every leaf reachable through A.descendSlot must have bit β
> equal to A.partial[descendSlot].β-bit-output-position.

This is **β-constancy at every ancestor**. Multi-entry leaves can naturally
violate it; Phase 5 makes the insert path responsible for preserving it.

### 2.2 The Algorithm

When inserting key K into leaf L via path P = [root → ... → leaf-parent]:

**Step 1 — Detect.** Walk all ancestor disc bits β in path P:
- Compute leaf L's β-value (= constant value if β-constant, mixed otherwise).
- Compute K's β-value.
- If they disagree, β is an **offending bit**.

**Step 2 — Split.** For each offending β in order (most-significant first):
- `splitLeafOnBit(L, β)` → produces `L_β0` (= keys with β=0) and `L_β1` (= keys
  with β=1).
- K joins `L_β=K.β` (= the half matching K's bit value at β).

**Step 3 — Reroute the wrong half.** The split produced TWO leaves. One stays
at L's slot (= the half whose β-value matches the slot's expected β). The OTHER
half (= the "wrong half") must be **rerouted** to a sibling slot at some
ancestor A_β where A_β captures β.

Reroute mechanism (already exists in `tryReRouteOffendingKey` → `bulkInsertIntoSiblingSubtree`):
- Find ancestor A_β whose mask captures β.
- Find the sibling slot at A_β whose stored partial differs from descendSlot by
  exactly the β-bit (= the slot expecting the wrong half's β-value).
- Bulk-insert wrongHalf's keys into that sibling subtree.

**Step 4 — Verify.** After all splits + reroutes, every ancestor's β-constancy
holds for L. The merge of K into L_β=K.β proceeds normally via
`leaf.mergeWithNodeRefs`.

### 2.3 Differences from Prior Phase 2 Attempts

Prior Phase 2 (Stage F, reverted) cascaded 1 → 635 violations because:
- It split the leaf but **didn't reroute the wrong half** — the wrong half stayed
  at its slot, violating β-constancy from a different angle.
- It tried multiple βs greedily, which compounded inconsistencies.

Phase 5's key insight: **split AND reroute, atomically per β**. After each
(split, reroute) pair, the tree is once again in a consistent state w.r.t. β.
Then proceed to the next offending β if any.

---

## §3. Existing Infrastructure to Leverage

| Helper | Location | Purpose |
| --- | --- | --- |
| `findAnyOffendingAncestorBit` | HOTTrieWriter:4152 | Returns ONE offending β. Needs generalization to ALL βs. |
| `findI8OffendingAncestor` | HOTTrieWriter:5747 | Different semantics: I8 at firstKey level. |
| `bitConstantValueInSubtree` | HOTTrieWriter | Checks β-constancy in a subtree. |
| `splitLeafOnBit` | HOTTrieWriter:2106 | Partitions leaf on bit β. Returns SubtreeSplit. |
| `splitSubtreeOnBit` | HOTTrieWriter:1909 | Generic split (leaf or indirect). |
| `tryReRouteOffendingKey` | HOTTrieWriter:5802 | Reroutes NEW KEY (not split halves). Needs adapter for split-half rerouting. |
| `bulkInsertIntoSiblingSubtree` | HOTTrieWriter:3129 | The reroute primitive. Takes a key+value, descends sibling subtree, inserts. |
| `collectAncestorDiscBits` | HOTTrieWriter | Returns all ancestor mask bits. |
| `multiMaskBetaOutputPos` / `singleMaskBetaOutputPos` | HOTTrieWriter | β-position-in-partial-key lookup. |

---

## §4. New Components

### 4.1 `detectAllConstancyBreaksOnInsert`

```java
/** Returns ALL ancestor β bits where inserting K into L would break β-constancy.
 *  Sorted ascending (= most-significant first). */
public int[] detectAllConstancyBreaksOnInsert(
    HOTIndirectPage[] pathNodes, int pathDepth,
    HOTLeafPage leaf, byte[] keyBuf);
```

Generalizes `findAnyOffendingAncestorBit` to return the **complete set** of
offending βs, not just the first one.

For each ancestor β:
- Iterate L's existing keys, compute β-constancy state (0-constant, 1-constant, mixed).
- For mixed: skip (already a β-mixed leaf — Phase 5 will partition it as part of
  the split chain).
- For constant: compare K's bit. If disagrees, add β to result.

### 4.2 `splitLeafAndRerouteWrongHalf`

```java
/** For one offending β:
 *  1. Split leaf into β=0 and β=1 halves.
 *  2. K's half stays at leaf's slot.
 *  3. Other half is rerouted to ancestor A_β's exact-XOR sibling subtree.
 *  Returns true on success, false if reroute is structurally infeasible.
 *
 *  Output: updates pathRefs[pathDepth-1] (= the leaf's parent ref) to point at
 *  the new K-side leaf. The wrong half's keys are bulk-inserted into the sibling
 *  subtree at A_β. */
public boolean splitLeafAndRerouteWrongHalf(
    HOTLeafPage leaf, PageReference leafRef,
    HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
    int offendingBeta, byte[] keyBuf,
    StorageEngineWriter storageEngineWriter, TransactionIntentLog log);
```

Cleanly composes:
- `splitLeafOnBit` for the partition.
- `bulkInsertIntoSiblingSubtree` for the reroute (looped over each key in the
  wrong half).

### 4.3 `applyConstancyAwareInsert`

```java
/** Top-level entry called from AbstractHOTIndexWriter.doIndex BEFORE
 *  leaf.mergeWithNodeRefs. Applies splitLeafAndRerouteWrongHalf for each
 *  offending β in MSB-first order. After all splits succeed, K is in a leaf
 *  that's β-constant for every ancestor β, and merge can proceed normally.
 *
 *  Returns true if all splits succeeded; false to fall back to the existing
 *  intermediate-BiNode path. */
public boolean applyConstancyAwareInsert(
    HOTLeafPage leaf, PageReference leafRef,
    HOTIndirectPage[] pathNodes, PageReference[] pathRefs, int[] pathChildIndices, int pathDepth,
    byte[] keyBuf, byte[] valueBuf,
    StorageEngineWriter storageEngineWriter, TransactionIntentLog log);
```

---

## §5. Integration Point: `AbstractHOTIndexWriter.doIndex`

Insertion gates (in order):

```
// Existing G.13/G.15 path (Phase 4 sibling reroute): K is rerouted to sibling.
if (findI8OffendingAncestor) { tryReRouteOffendingKey; if rerouted return; }

// NEW Phase 5 path (split-then-reroute): split L on every offending β.
if (Boolean.getBoolean("hot.strict.option-b-phase-5")) {
  if (applyConstancyAwareInsert(...)) { proceed to merge normally; }
}

// Existing merge.
boolean success = leaf.mergeWithNodeRefs(...);
```

Phase 5 fires AFTER the existing Phase 4 reroute. If Phase 4 already rerouted K
(= K's β-value matches some sibling, no split needed), we're done. Otherwise
Phase 5's split+reroute is tried.

---

## §6. Per-Step Formal Verification

To prevent cascade, every insert under Phase 5 runs the full
`HOTInvariantValidator.validateIndex` (gated `-Dhot.formal.verify=true`) and
asserts `violations.size() == 0`.

Bisection: if any insert produces a violation, capture:
- Insert index i.
- Key K, value V.
- Pre-state snapshot (writer counters).
- Post-state validator output.

This is the harness for iterative debugging.

---

## §7. Implementation Order

| Step | Task | Deliverable |
| --- | --- | --- |
| 1 | Per-insert validator harness (#52) | Gate + bisection capture |
| 2 | `detectAllConstancyBreaksOnInsert` (#49) | Helper + unit test |
| 3 | `splitLeafAndRerouteWrongHalf` (#50) | Helper + unit test |
| 4 | `applyConstancyAwareInsert` + wire into doIndex (#51) | Phase 5 gate |
| 5 | Run 50K reproducer (#53) | violations=0 target |
| 6 | Remove gate, finalize (#54) | Campaign closure |

Each step keeps:
- 100K CAS production: 0 violations.
- Default mode: 1 violation (baseline preserved).
- Unit tests for each helper.

---

## §8. Risks + Mitigations

| Risk | Mitigation |
| --- | --- |
| Reroute infeasible (no sibling at ancestor) | Fall back to intermediate-BiNode path; not a regression |
| Splits cascade (one split creates new offending β) | Per-step formal verification; iterate in MSB-first order |
| Performance: per-insert constancy walk is O(depth × maskBits × leafEntries) | Profile; if hot, cache β-constancy state on leaves |
| Multi-entry leaf already β-mixed before split | `splitLeafOnBit` handles this; partition becomes multi-way |

---

## §9. Acceptance Criteria

1. `diagnosticMicrobenchPatternReproducer` reports `violations=0`.
2. `casIndexHundredKEntryHeightBound` reports `violations=0` (unchanged).
3. Full `:sirix-core:test` suite: BUILD SUCCESSFUL.
4. Performance: insertion throughput within 20% of baseline (= acceptable cost
   for correctness; can be tuned in Phase 6).
5. The Phase 5 path is the default; no `-Dhot.strict.option-b-phase-5` gate
   needed.
