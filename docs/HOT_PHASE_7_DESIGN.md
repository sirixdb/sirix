# HOT Phase 7 — Leaf-Owned-Bits Metadata

**Goal:** Drive 50K diagnosticMicrobenchPatternReproducer to 0 violations by making
multi-entry leaves track which absolute bit positions can vary inside them. Caller
queries this metadata BEFORE inserting a key; if the key would introduce a new
non-constant bit that's captured by an ancestor's mask, the caller does a
constancy-aware split BEFORE the merge.

**Status:** Designed (not yet implemented). Multi-week scope.
**Branch:** `fix/hot-strict-binna-conformance`.

---

## §1. Diagnosis Recap (Phase 5/6 Findings)

The persistent I8 violation at root indirect 2 forms via TWO mechanisms identified
by the Explore agent investigation:

1. **Intermediate-BiNode placement (line 1289)** at idx=513: places a BiNode at
   parent.slot with leftFirstKey=c0a002. `intermediateBiNodePreservesSlotOrder`
   checked I8 against slot-1's CURRENT firstKey and passed. Future inserts into
   OTHER slots' subtrees decrease their leaves' deep-firstKeys, retroactively
   breaking I8.

2. **`leaf.mergeWithNodeRefs(K)` then CoW up via `withUpdatedChild` WITHOUT
   updating ancestor partials**: when K becomes the new leftmost in some leaf,
   the leaf's deep-firstKey changed but the parent's stored partial reflects the
   OLD firstKey.

Phase 6e (`recomputePartialsOnPath`) fires after every merge but returns null for
root when the existing mask cannot discriminate the current children's firstKeys
under PEXT (= I3 collisions or no I4 zero-partial). At end-state for our test
pattern, this is the case — root.mask = byte 10 bit 1 + byte 12 bits {3,4,6}
doesn't discriminate byte 11 differences across children.

Phase 6f (β-constant bit extension) finds NO bit that's both discriminating AND
β-constant in every child's subtree — because multi-entry leaves naturally hold
non-constant keys.

The architectural ceiling is precisely **the absence of leaf-level constancy
metadata that lets callers know whether inserting a key is safe**.

---

## §2. Data Model

Add to `HOTLeafPage`:

```java
// Sorted absolute MSB-first bit positions captured by ANY ancestor mask along
// the path that descends to this leaf. Bits NOT in this list are free to vary
// within the leaf (= multi-entry leaves can carry any value at off-path bits).
private int[] ancestorOwnedBits;

// One bit per ancestor-owned bit position: the fixed value (0 or 1) that ALL
// keys in this leaf must have at that position. Set at leaf creation/split
// time; immutable for the leaf's lifetime.
private long[] ancestorOwnedValues; // bitset, idx ≤ ancestorOwnedBits.length
```

**Persistence:** bump `HOT_LEAF_PAGE` serializer to V1. After existing payload,
append:
- `u16 ownedBitCount`
- Varint-packed `ancestorOwnedBits[]` (delta-encoded, ascending)
- Bitset bytes for `ancestorOwnedValues`

**Cost:** ~8–32 bytes per leaf (5–20 ancestor bits typical at tree height 6).

---

## §3. Maintenance Protocol

### 3.1 New leaf creation
- Empty leaf created during tree growth: `ancestorOwnedBits = []`,
  `ancestorOwnedValues = []`.
- Caller (= `splitLeafOnBit` / `handleLeafSplitAndInsert`) MUST set ancestor
  owned bits derived from path masks BEFORE installing into log.

### 3.2 `put` / `mergeWithNodeRefs`
New strict variant `mergeWithNodeRefsStrict(key, BetaBreakOut signal)`:
- Iterate `ancestorOwnedBits`. For each bit β, check `key.bit(β) ==
  ancestorOwnedValues[β]`.
- If any mismatch, set `signal.β = β` and return `false`. Caller handles split.
- If all match, proceed with normal merge.

Default `mergeWithNodeRefs` keeps current behavior for backward compat.

### 3.3 `splitLeafOnBit(absBit β)`
Both halves inherit parent leaf's `ancestorOwnedBits` ∪ {β}:
- Left half: `ancestorOwnedValues[β] = 0`.
- Right half: `ancestorOwnedValues[β] = 1`.

### 3.4 `copy()` / serialize round-trip
Preserves `ancestorOwnedBits` and `ancestorOwnedValues` as-is.

### 3.5 `compact()`
Metadata-neutral. Owned bits do not change just because keys shift in storage.

---

## §4. Integration with HOTTrieWriter

### 4.1 `applyConstancyAwareInsert` (entry point)
- Call `leaf.mergeWithNodeRefsStrict(key, signal)`.
- On `false` + signal.β: call `splitLeafAndRerouteWrongHalf` with the explicit β.
- Existing detector becomes a cross-check; the LEAF's owned bits are the
  authoritative source.

### 4.2 `addEntryWithPDep` (parent extension)
When extending parent's mask with new bit β, propagate β to ALL descendant
leaves by adding β to their `ancestorOwnedBits` AND setting their
`ancestorOwnedValues[β]` based on the leaf's actual β-value. Walk subtree once
per new bit. Phase 6d already splits β-mixed subtrees; this propagation step
follows the split.

### 4.3 `splitParentAndRecurse`
When parent splits, each new half's subtree gets the new disc bit added to its
descendant leaves' `ancestorOwnedBits`.

### 4.4 `handleLeafSplitAndInsert`
The two split products get `ancestorOwnedBits` from the leaf's parent path
masks (= via `collectAncestorDiscBits`).

---

## §5. Staged Implementation

| Stage | Deliverable | Verification |
| --- | --- | --- |
| **7a** | In-memory `ancestorOwnedBits` + `ancestorOwnedValues` fields on `HOTLeafPage`. Setter/getter. Unit tests. | Round-trip in-memory test |
| **7b** | `mergeWithNodeRefsStrict` returns `false` on β-mismatch. Unit tests for matching, mismatched, β-mixed cases. | `HOTOptionBPhase5Test` extended |
| **7c** | V1 serializer + deserializer. Round-trip test through page log. | `HOTLeafPageTest` round-trip |
| **7d** | `splitLeafOnBit` sets owned bits on halves. `addEntryWithPDep` propagates to descendants. | Per-insert formal-verify |
| **7e** | `applyConstancyAwareInsert` uses strict merge. Default mode (no flag): 0 violations on 50K reproducer. | Reproducer at 0 |
| **7f** | Performance validation: throughput regression < 20% baseline. | Microbench |

---

## §6. Acceptance Criteria

1. `diagnosticMicrobenchPatternReproducer` reports `violations=0`.
2. `casIndexHundredKEntryHeightBound` reports `violations=0` (unchanged).
3. Tree height optimal: ≤ ⌈log₃₂(N)⌉ + 1 for N keys.
4. Works for multi-value leaves throughout (= the user's explicit requirement).
5. Performance: insertion throughput within 20% of pre-7 baseline.
6. Full `:sirix-core:test` suite: BUILD SUCCESSFUL.

---

## §7. Risks

| Risk | Mitigation |
| --- | --- |
| Per-leaf metadata cost balloons | Cap at 64 owned bits/leaf; reject extension beyond |
| Persistence format change breaks existing databases | V1 format is additive; V0 leaves auto-upgrade with `ancestorOwnedBits = []` (lossy but safe) |
| Propagation cost on `addEntryWithPDep` is O(subtree size) | Bound walks to 1 level; rely on per-leaf strict-merge for deeper enforcement |
| Phase 6d's β-mixed sibling splits become redundant | OK — both checks converge on the same condition |

---

## §8. Critical Files

- `bundles/sirix-core/src/main/java/io/sirix/page/HOTLeafPage.java`
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java`
- `bundles/sirix-core/src/main/java/io/sirix/page/PageKind.java`
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java`
