# HOT Operations × Invariants Preservation Matrix (Stage B)

**Status:** Stage B deliverable for the multi-session formal-verification effort. Sourced from
`HOTTrieWriter.java`, `HOTLeafPage.java`, `HOTIndirectPage.java` as of commit `1815f9d2e`
(branch `fix/hot-strict-binna-conformance`). No code was changed for this stage — this is research.

**Inputs:** Stage A's `HOT_INVARIANTS_CATALOG.md` (16 invariants); the C++ reference at
`/home/johannes/IdeaProjects/hot-reference/`; the live writer source.

**Output of this stage:** a per-operation preservation table that names, for every mutating
operation in the writer, **which invariants it claims to preserve and via what code mechanism**,
so Stage F's fix design can be derived from data instead of intuition. The campaign's recurring
failure pattern (1 → 5993 → 9231 → 635 → 645 → 5995 violations) was each fix attempt closing one
cell while opening another. This matrix surfaces every cell at once.

---

## §1. Method

For each mutating operation:

1. **Purpose (1 line).**
2. **Code citation:** `file:line`.
3. **Invariant scoring** across the 16 invariants from Stage A:
   - ✅ **PRESERVES** — code path explicitly enforces (with reference to the line that does so).
   - ⚠ **WEAK** — preserved on happy path; no explicit verification; relies on caller / upstream invariant.
   - ❌ **VIOLATES** — known broken in some scenario (with citation to evidence).
   - N/A — invariant does not apply to this operation.
   - ? **UNKNOWN** — needs Stage E empirical verification.

The 16 invariants from `HOT_INVARIANTS_CATALOG.md`:

| ID | Short name |
| --- | --- |
| I1 | No-orphan-children |
| I2 | Mask-bits-monotone (within SingleMask window) |
| I3 | Unique-partial-keys |
| I4 | First-partial-zero **(GAP — not in validator)** |
| I5 | Leaf-constancy (β-constancy in subtrees) |
| I6 | PEXT-routes-to-leaf |
| I7 | Children-sorted-by-partial |
| I8 | Children-sorted-by-firstKey |
| I9 | Mask-non-empty |
| I10 | Window-coherent (SingleMask span ≤ 8 bytes) |
| I11 | Trie-condition (parent.MSB < child.discBit) **(GAP)** |
| I12 | Disc-bit-monotone-along-path **(GAP, subsumed by I11)** |
| I-SP | Binna-sparse-path: stored partial = `firstKey & ~prevKey` at parent's bits |
| I-PD | PDEP/PEXT identity round-trip |
| I-CoW | CoW page identity preservation |
| I-MR | Multi-revision read isolation |

---

## §2. Operations Inventory

35 mutating operations identified across 3 source files. Grouped by responsibility level.

| # | Operation | File:Line | Layer |
| --- | --- | --- | --- |
| 1 | `HOTLeafPage.put` / `putRange` | HOTLeafPage:892, 933, 1022 | leaf |
| 2 | `HOTLeafPage.mergeWithNodeRefs` | HOTLeafPage:1631 | leaf |
| 3 | `HOTLeafPage.splitToWithInsert` | HOTLeafPage:1808, 1828 | leaf |
| 4 | `HOTLeafPage.splitToWithInsertOnBit` | HOTLeafPage:1977 | leaf |
| 5 | `HOTIndirectPage.createBiNode` | HOTIndirectPage:290, 311 | factory |
| 6 | `HOTIndirectPage.createSpanNode` (SingleMask) | HOTIndirectPage:347, 1000 | factory |
| 7 | `HOTIndirectPage.createMultiNode` (SingleMask) | HOTIndirectPage:373, 1028 | factory |
| 8 | `HOTIndirectPage.createSpanNodeMultiMask` | HOTIndirectPage:1060 | factory |
| 9 | `HOTIndirectPage.createMultiNodeMultiMask` | HOTIndirectPage:1097 | factory |
| 10 | `HOTIndirectPage.withUpdatedChild` | HOTIndirectPage:974 | factory |
| 11 | `HOTTrieWriter.createNewLeaf` | HOTTrieWriter:870 | leaf-mgmt |
| 12 | `handleLeafSplitAndInsertInternal` | HOTTrieWriter:998 | overflow |
| 13 | `updateParentForSplitWithPath` | HOTTrieWriter:1127 | propagate |
| 14 | `addEntryWithPDep` (SingleMask) | HOTTrieWriter:1354 | indirect-modify |
| 15 | `addEntryFreshPolaritySingleMask` | HOTTrieWriter:1514 | indirect-modify |
| 16 | `addEntryFreshPolarityMultiMask` | HOTTrieWriter:1661 | indirect-modify |
| 17 | `addEntryMultiMask` | HOTTrieWriter:3450 | indirect-modify |
| 18 | `upgradeToMultiMaskWithNewBit` | HOTTrieWriter:3261 | layout-upgrade |
| 19 | `expandParentNode` | HOTTrieWriter:3893 | indirect-modify |
| 20 | `splitParentAndRecurse` | HOTTrieWriter:4319 | propagate |
| 21 | `splitParentHalfAndRecurse` | HOTTrieWriter:5150 | propagate-fallback |
| 22 | `buildCompressedHalf` (SingleMask path) | HOTTrieWriter:4663 | half-builder |
| 23 | `buildCompressedHalfMultiMask` | HOTTrieWriter:4926 | half-builder |
| 24 | `rebuildParentAbsorbingSplit` | HOTTrieWriter:3208 | rebuild-fallback |
| 25 | `createNodeFromChildren` / `buildFlatNonStrict` | HOTTrieWriter:5270, 5311 | rebuild-fallback |
| 26 | `createBiNodeTraced` (intermediate-BiNode workaround) | HOTTrieWriter:5329 | fallback |
| 27 | `rebalanceAndIntegrate` (Phase 3) | HOTTrieWriter:2352 | recovery |
| 28 | `buildRebalancedParentWithInheritedMask` | HOTTrieWriter:2213 | recovery |
| 29 | `splitSubtreeOnBit` / `splitLeafOnBit` / `splitIndirectOnBit` | HOTTrieWriter:1829, 2026, 2079 | recovery |
| 30 | `buildBucketWithInheritedMask` | HOTTrieWriter:1894 | recovery |
| 31 | `tryPhase4SubtreeMerge` | HOTTrieWriter:2470 | recovery-dispatch |
| 32 | `subtreeMerge` (Phase 4) | HOTTrieWriter:2641 | recovery |
| 33 | `hoistAndReroute` (Phase 4) | HOTTrieWriter:2781 | recovery |
| 34 | `bulkInsertIntoSiblingSubtree` | HOTTrieWriter:2962 | recovery |
| 35 | `splitLeafAndIntegrateInSiblingSubtree` | HOTTrieWriter:3097 | recovery |

---

## §3. Per-Operation Preservation Analysis

### Op 1 — `HOTLeafPage.put` / `putRange`

**Purpose.** Insert or update a single (key, value) pair in a leaf. Establishes / shrinks
the common prefix; appends to slot heap; binary-search-sorted by key.

**Code:** `HOTLeafPage:892, 933, 1022`. Calls `findEntry` (binary search), `insertAtWithKey`,
`handlePrefixForInsert` (slotMemory rewrite if prefix narrows).

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | N/A | leaf operation; no child references |
| I2 | N/A | no mask |
| I3 | N/A | no partials |
| I4 | N/A | no partials |
| I5 | ❌ **VIOLATES** | The operation has no notion of an "ancestor disc bit set" — it can insert any key, even one whose ancestor-disc-bit values disagree with the existing leaf's keys. **This is the root cause of the I8/I-SP cascade**: when leaf L is the descend-target of a partial-key route in some ancestor A, and a new key K is `put` into L with K's value at A's β disagreeing with the existing keys' β values, A's stored partial for L becomes stale. No check, no signal back to caller. |
| I6 | ⚠ WEAK | Caller (handleLeafSplitAndInsertInternal etc.) routed the key to this leaf via PEXT. Insertion does not re-verify that this leaf is still the right destination after prefix changes. |
| I7 | N/A | leaf |
| I8 | ⚠ WEAK | Sorted-by-key is preserved within the leaf via `findEntry`. But `firstKey` of the leaf may change when a smaller key is inserted, breaking the parent's I8 ordering — the leaf has no signal back to the parent that this happened. |
| I9 | N/A | leaf |
| I10 | N/A | leaf |
| I11 | N/A | leaf |
| I12 | N/A | leaf |
| I-SP | ❌ **VIOLATES** | Identical to I5: the parent's stored partial may go stale when this insert changes the leaf's β-bit profile. |
| I-PD | N/A | leaf |
| I-CoW | ⚠ WEAK | Caller is responsible for CoW. `put` mutates the page in place — assumes caller already CoW'd. |
| I-MR | ⚠ WEAK | Same — relies on caller. |

**Cell of concern (Stage F focus):** I5/I-SP — `put` is the operation that breaks β-constancy
silently. Stage F must intercept BEFORE this method runs, with a constancy check on every
ancestor disc bit.

---

### Op 2 — `HOTLeafPage.mergeWithNodeRefs`

**Purpose.** Insert a key/value into a leaf that already has node-ref slot bookkeeping.
Variant of `put` used during `splitToWithInsert` to populate the right half.

**Code:** `HOTLeafPage:1631`.

**Invariant scoring:** Identical to `put`. Same leaf-level invariants ⚠ WEAK; same I5/I-SP ❌ VIOLATES
because the operation has no awareness of ancestor disc bits.

---

### Op 3 — `HOTLeafPage.splitToWithInsert(target, key, ..., newSideOut)`

**Purpose.** Overflow split: choose `msdb` (most significant differing bit) over the leaf's
existing keys + the new key, partition into left/right halves on `msdb`, and report which side
the new key landed on (`newSideOut[0]` = 0 LEFT or 1 RIGHT). Phase 4b-vb.0 added the
`newSideOut` parameter.

**Code:** `HOTLeafPage:1808, 1828`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ⚠ WEAK | Splitting on `msdb` makes the two halves DIFFER at MSDB. But: MSDB is a bit chosen LOCALLY from the leaf's keys + new key — it is **not** necessarily an ancestor's disc bit. So the post-split halves may still disagree with the parent's β at some other ancestor bit. Specifically, if the new key K disagrees with existing keys at an ancestor β AND β > MSDB (so K's β value has been "absorbed" by the MSDB split), the half containing K still violates parent.β-constancy. |
| I8 | ✅ PRESERVES | Halves are partitioned in sorted order; left half holds keys with `bit-msdb=0`, right with `bit-msdb=1`, and within each half `findEntry`'s sorted-insert is preserved. |
| I-SP | ⚠ WEAK | Same as I5 — splitting on MSDB doesn't guarantee constancy at all ancestor disc bits. |
| Others | N/A | leaf-only |

**Cell of concern:** I5 / I-SP at ancestor bits ABOVE the chosen MSDB. The
`splitToWithInsertOnBit` overload was added to address this — see Op 4.

---

### Op 4 — `HOTLeafPage.splitToWithInsertOnBit(target, ..., explicitSplitBit)`

**Purpose.** Overflow split on a CALLER-supplied bit (typically an ancestor's β) instead of
local MSDB. Phase 2 of the strict-Binna design plan.

**Code:** `HOTLeafPage:1977`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ✅ PRESERVES | If `explicitSplitBit = β` for an ancestor, the resulting halves are β-constant by construction (left has β=0, right has β=1). Caller is responsible for choosing β correctly (typically by walking up the path and finding the first β where K's value disagrees with existing keys' values). |
| I8 | ⚠ WEAK | Partition on `explicitSplitBit < MSDB` produces a CONTIGUOUS partition only when explicitSplitBit is the leaf's own MSDB. For an ancestor β < MSDB, the partition is non-contiguous — the method's javadoc explicitly warns about this: "Splitting on a less-significant non-constant bit yields non-contiguous partition, which breaks the parent's children-sorted-by-firstkey invariant." Returns -1 in degenerate cases so caller falls back to MSDB. |
| I-SP | ✅ PRESERVES | Same mechanism as I5. |
| Others | N/A | leaf |

**Cell of concern:** I8 — non-contiguous partition. When the explicit β < MSDB, the right half's
firstKey may be smaller than the left half's lastKey, so the integration into parent (as two
ordered slots replacing splitChildIdx) violates parent.I8.

---

### Op 5 — `HOTIndirectPage.createBiNode(pageKey, revision, discBit, leftRef, rightRef, height)`

**Purpose.** Build a 2-child indirect with one disc bit. The simplest layout.

**Code:** `HOTIndirectPage:290, 311`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | Both refs assigned. |
| I2/I9 | ✅ | Single-bit mask is by construction. |
| I3 | ✅ | Stored partials are 0 and 1 — distinct by construction. |
| I4 | ✅ | leftChild's partial = 0 by construction (Binna's "first mask always zero"). |
| I5/I-SP | ⚠ WEAK | The disc bit `discBit` must be β-constant within left and right subtrees. Caller is responsible (e.g., `splitParentAndRecurse` line 4444 picks `msbPosition`, `updateParentForSplitWithPath` line 1145 picks `computeDifferingBit(leftMax, rightMin)`). The factory does NOT verify. |
| I6 | ⚠ WEAK | Routing correctness depends on caller's choice of discBit. |
| I7 | ✅ | 2 slots, partials [0, 1] in order. |
| I8 | ⚠ WEAK | Caller must ensure leftChild.firstKey < rightChild.firstKey. |
| I10 | N/A | BiNode has no PEXT window. |
| I11 | ⚠ WEAK | Caller must ensure parent.MSB < discBit (when this BiNode is later attached as a child). |
| I-CoW | ✅ | New page, fresh pageKey. |

**Cell of concern:** I5/I-SP at the BiNode's discBit. `createBiNodeTraced` (Op 26) wraps with
`probeConstancyOnBuild` for diagnostic purposes, but the diagnostic only LOGS — it doesn't reject.

---

### Op 6 — `HOTIndirectPage.createSpanNode` / Op 7 — `createMultiNode` (SingleMask)

**Purpose.** Build a SingleMask indirect with k≤16 (Span) or k≤32 (Multi) children, mask up
to 8 bytes wide.

**Code:** `HOTIndirectPage:347, 373, 1000, 1028`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | All refs in `children[]`. |
| I2 | ✅ | `bitMask` is a single long; bits are inherently monotone within the long's BE byte layout. |
| I3 | ⚠ WEAK | Caller passes `partialKeys[]`. Factory does NOT check uniqueness. Callers (addEntryWithPDep:1456, buildCompressedHalf:4866) self-check, but if a caller forgets, a duplicate slips through. |
| I4 | ⚠ WEAK | Not enforced. **(Stage A GAP)** |
| I5/I-SP | ⚠ WEAK | All disc bits in the mask must be β-constant in every child's subtree. Caller's responsibility. |
| I6 | ⚠ WEAK | Routing correctness = caller's partial-key encoding correctness. |
| I7 | ⚠ WEAK | Caller must pre-sort. Most call sites do (`sortChildrenAndPartialsByPartial`), but not all (e.g., `withUpdatedChild` doesn't re-sort because it preserves slot order). |
| I8 | ⚠ WEAK | Equivalent to I7 under correct partials. |
| I9 | ✅ | Mask is non-zero or factory rejects (implicit via Long.bitCount). |
| I10 | ✅ | SingleMask spans 8 bytes by definition; the `initialBytePos` field defines the window. |
| I11 | ⚠ WEAK | Caller's responsibility. |
| I-CoW | ✅ | New page. |

---

### Op 8 — `createSpanNodeMultiMask` / Op 9 — `createMultiNodeMultiMask`

**Purpose.** Build a MultiMask indirect with disc bits across multiple non-contiguous bytes
(via `extractionPositions[]` + `extractionMasks[]`).

**Code:** `HOTIndirectPage:1060, 1097`.

**Invariant scoring:** Same shape as Op 6/7, with I10 N/A (MultiMask isn't constrained to 8
bytes), and an additional weakness:

| Inv | Score | Mechanism |
| --- | --- | --- |
| I-PD | ⚠ WEAK | MultiMask uses two Long.compress calls (one per chunk for >8 bytes) — the 32-bit packed partial-key space MUST fit. No explicit overflow check; relies on caller computing extractionMasks correctly. |

---

### Op 10 — `HOTIndirectPage.withUpdatedChild(childIndex, newRef, revision)`

**Purpose.** Replace one slot's PageReference with a new ref, preserving pageKey + mask +
all other slots + all partial keys. Used pervasively by Phase 4 subtree-merge, intermediate-BiNode
fallback, etc.

**Code:** `HOTIndirectPage:974`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | newRef must be non-null (caller responsibility). |
| I2/I9/I10 | ✅ | Mask unchanged. |
| I3 | ✅ | Partials unchanged. |
| I4 | ✅ | Partials unchanged. |
| I5/I-SP | ⚠ WEAK | The new child's subtree MUST satisfy β-constancy for every parent disc bit, with bit values matching the (preserved!) stored partial at this slot. Factory does NOT verify. **This is the load-bearing case for I5.** Callers (subtreeMerge:2696, intermediate-BiNode:1257, hoistAndReroute) all rely on pre-construction guarantees. |
| I6 | ⚠ WEAK | Same: routing-correctness invariant. |
| I7 | ✅ | Slot order unchanged. |
| I8 | ⚠ WEAK | If new child's firstKey differs from old child's firstKey, parent's I8 may break. Specifically: subtreeMerge replaces splitChildIdx with `keepHalf`; if `keepHalf.firstKey > parent.children[splitChildIdx+1].firstKey`, I8 breaks. No check. |
| I-CoW | ✅ | Fresh page at SAME pageKey — preserves identity. |
| I-MR | ✅ | Revision number passed forward. |

**Cell of concern:** I8 — `withUpdatedChild` is silently I8-violating if the new child's
firstKey crosses an adjacent slot's firstKey. This is the lone observed I8 violation in the
current 1-violation reproducer baseline, attributed to the intermediate-BiNode fallback case
(updateParentForSplitWithPath:1257). The `intermediateBiNodePreservesSlotOrder` gate at line
1318 catches the prev-side case but explicitly skips the next-side check (line 1304-1313: "the
1 marginal I8 violation is the design's accepted lesser evil").

---

### Op 11 — `HOTTrieWriter.createNewLeaf`

**Purpose.** Allocate a fresh empty leaf page, register in TIL.

**Code:** `HOTTrieWriter:870`.

**Invariant scoring:** All ✅ or N/A. This is a simple allocator. The new leaf has no entries
and no parent yet.

---

### Op 12 — `handleLeafSplitAndInsertInternal`

**Purpose.** Top-level overflow handler. Allocates a right-half leaf, calls
`splitToWithInsert` (or `splitToWithInsertOnBit` if `explicitSplitBit ≥ 0`), registers both
halves in the TIL, then either creates a root BiNode (if depth=0) or calls
`updateParentForSplitWithPath` to integrate the split.

**Code:** `HOTTrieWriter:998`.

**Invariant scoring:** All preservation responsibilities are delegated. ✅ for orchestration
correctness (CoW, TIL registration); ⚠ WEAK for everything else (relies on `splitToWithInsert*`
+ `updateParentForSplitWithPath`).

**Important detail (line 1023-1043):** The `explicitSplitBit ≥ 0` path calls
`splitToWithInsertOnBit`, but if it returns -1 (degenerate), falls back to MSDB-based split. The
fallback **discards the constancy guarantee that motivated the explicit bit**, returning to the
I5-violating MSDB-only behavior. This is an accepted gap.

---

### Op 13 — `updateParentForSplitWithPath`

**Purpose.** The dispatcher. Given a leaf split (leftChild, rightChild), decides how to
integrate into parent:

1. If `parent.height > splitEntries.height` → create intermediate BiNode at the slot.
2. Else, try `addEntryWithPDep` (SingleMask) or `addEntryMultiMask`. If success, log and return.
3. Else, if `hot.strict.binna`: try `rebalanceAndIntegrate` (Phase 3). Then
   `tryPhase4SubtreeMerge`. Then `intermediateBiNodePreservesSlotOrder` → fall back to
   intermediate BiNode. Else `splitParentAndRecurse`.
4. Else, if numChildren < 20: `rebuildParentAbsorbingSplit`. Else: `splitParentAndRecurse`.

**Code:** `HOTTrieWriter:1127`.

**Invariant scoring:** This is policy. Each branch's invariant preservation is the union of the
branch target's preservation properties.

The CRITICAL DECISION POINT (line 1219-1267) is the strict-binna fallback ladder:
`Phase3 → Phase4 → intermediate-BiNode → splitParent`. Each step has different invariant
preservation properties — and the ladder doesn't fail-fast on invariant violation; it descends
to ever-weaker policies.

**Cell of concern:** I8 — the intermediate-BiNode branch (line 1248-1260) only checks the
prev-side of slot order via `intermediateBiNodePreservesSlotOrder`. The next-side gap is the
documented "1 marginal I8 violation" baseline.

---

### Op 14 — `addEntryWithPDep` (SingleMask)

**Purpose.** Extend parent's mask by ONE new disc bit β (newAbsBit), reposition all existing
partials via `Long.expand` (PDEP), set the two split halves' partials at splitChildIdx, verify
β-constancy in every non-split sibling. Mirrors Binna's
`SingleMaskPartialKeyMapping::insert + addEntry`.

**Code:** `HOTTrieWriter:1354`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | All refs preserved + new ones inserted. |
| I2 | ✅ | New mask = old mask `\|` newBitMaskBit; bit positions are inherently monotone. |
| I3 | ✅ | Self-check at line 1456-1460: pairwise compare all newPartialKeys. Returns null on collision. |
| I4 | ⚠ WEAK | Not explicitly enforced. **(Stage A GAP — partial[0] may be non-zero after the bit-injection if old partial[0] was already zero AND the new bit slot 0 ends up with new-bit=1.)** Needs Stage E empirical check. |
| I5 | ✅ | Lines 1416-1421: walks each non-split sibling, calls `bitConstantValueInSubtree(child, newAbsBit)` — returns null if any sibling is non-constant on β. |
| I-SP | ✅ | Sibling bit-values are stored at line 1446: `repositioned \| (siblingBitValues[i] << newBitOutputPos)`. |
| I6 | ✅ | Inherits PEXT routing from old layout via PDEP repositioning. **PROVIDED** I5 holds. |
| I7 | ✅ | `sortChildrenAndPartialsByPartial` at line 1469. |
| I8 | ⚠ WEAK | Partial-key sort != first-key sort in general. Under sparse-path encoding they should coincide (per Binna §4.2), but multi-entry leaves can break this. |
| I9 | ✅ | newMask non-zero by construction. |
| I10 | ✅ | Cross-window check at line 1366 → upgrade to MultiMask. |
| I11 | ⚠ WEAK | Not checked. The new bit β must be > parent.MSB if parent is itself a child of a higher node; not verified here. |
| I-PD | ✅ | Uses `Long.expand` for repositioning; bijection on the old partial-key space. |
| I-CoW | ✅ | Returns new page at SAME pageKey (caller registers via log.put). |

**Cell of concern:** I5 — the **stored sibling bit-value is captured at construction time but
goes stale on subsequent leaf inserts** that mutate sibling subtrees in I5-violating ways (Op 1,
Op 2). This is the recurring root cause documented at line 4391-4399 (post-mortem of Phase 4b-vb.3).

---

### Op 15 — `addEntryFreshPolaritySingleMask` / Op 16 — `addEntryFreshPolarityMultiMask`

**Purpose.** Case 2b-iv-b-β handler: β is ALREADY in parent's mask but no sibling encodes the
inverse polarity (¬v_L). Adds a new slot whose partial = splitChild's partial XOR β-bit.

**Code:** `HOTTrieWriter:1514, 1661`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I3 | ✅ | Pre-check at construction: rejects if any sibling already has the target partial. |
| I5 | ✅ | Re-verifies constancy on β across non-split siblings. |
| I-SP | ✅ | newPartial computed by XOR — β-bit toggled. |
| I7 | ✅ | sortChildrenAndPartialsByPartial. |
| Others | identical to addEntryWithPDep | — |

Same I5-staleness vulnerability as Op 14: stored partials reflect leaf-state at construction
time only.

---

### Op 17 — `addEntryMultiMask`

**Purpose.** Same as `addEntryWithPDep` but for MultiMask parents. Extends the
`extractionPositions[] + extractionMasks[]` layout with the new β.

**Code:** `HOTTrieWriter:3450`.

**Invariant scoring:** Same as Op 14. Adds `?` for I-PD: MultiMask's two-Long.compress chain
must not overflow 32 bits — count check exists but a rare `Long.bitCount(allBits) > 32` case
would force fallback.

---

### Op 18 — `upgradeToMultiMaskWithNewBit`

**Purpose.** Triggered when the new disc bit β is OUTSIDE the SingleMask's 8-byte window.
Promotes parent from SingleMask → MultiMask.

**Code:** `HOTTrieWriter:3261`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ✅ | Re-verifies constancy on every non-split sibling for β. |
| I-PD | ⚠ WEAK | If the resulting bit count > 32, the partial-key space overflows. The method has an implicit cap via NodeUpgradeManager but no explicit overflow rejection. |
| I10 | N/A | Result is MultiMask. |
| Others | same as addEntryWithPDep |

---

### Op 19 — `expandParentNode`

**Purpose.** Helper used by some of the legacy paths to insert a single child into parent
(extend mask, expand partials).

**Code:** `HOTTrieWriter:3893`.

**Invariant scoring:** Subset of `addEntryWithPDep`'s scoring; relies on the same primitives.

---

### Op 20 — `splitParentAndRecurse`

**Purpose.** Parent overflow path: split parent on `parent.MSB` into [smallerChildren,
largerChildren], rebuild each half via `buildCompressedHalf`, then recurse to grandparent (or
create new root BiNode if at root).

**Code:** `HOTTrieWriter:4319`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | All children preserved across two halves. |
| I8 | ⚠ WEAK | Children are partitioned by `hasBitSet(childKey, msbPosition)` — sorted-by-firstKey order is preserved IF MSB is a bit at which the children are contiguously partitioned. By Binna's invariants this should hold; under multi-entry-leaf pathology it may not. |
| I5 | ⚠ WEAK | Each half is rebuilt via `buildCompressedHalf` which itself relies on `relevant` bits. **buildCompressedHalf's documented gap** (line 4387-4399 post-mortem) is that under multi-entry-leaf pathology, β-constancy can break inside a half AFTER construction. |
| I-CoW | ✅ | Both halves get fresh pages; recursion preserves identity at the path. |
| Others | depends on buildCompressedHalf | — |

**Cell of concern:** I5/I-SP through `buildCompressedHalf` (Op 22). All Phase 4b cascades
trace to this operation.

---

### Op 21 — `splitParentHalfAndRecurse`

**Purpose.** Fallback when `splitParentAndRecurse` would produce one empty half (line 4368).
Splits at a different point.

**Code:** `HOTTrieWriter:5150`.

**Invariant scoring:** Same shape as Op 20 with the same I5/I-SP gap.

---

### Op 22 — `buildCompressedHalf` (SingleMask path)

**Purpose.** Build one half of a parent split. For each child in the half:
- Split-product children: compute partial directly from firstKey via PEXT under newMask.
- Inherited children: PEXT old partial via `relevant`, then PDEP into new layout.

**Code:** `HOTTrieWriter:4663`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I1 | ✅ | All half children preserved. |
| I2 | ✅ | newMask is a subset of oldMask (selected by `relevant`) ∪ optionally `splitBitMaskBit`. |
| I3 | ✅ | Pairwise self-check at line 4866-4873; falls back to `createNodeFromChildren` on collision. |
| I5 | ❌ **VIOLATES** | The "sparse-path encoding" comment at line 4829-4832 explicitly states: "non-split sibling's path does NOT include the split-disc-bit's BiNode, so its new-bit position stays 0 regardless of how the bit varies within its subtree. Constancy check removed." This is the documented Phase 4b gap. The check was removed because under sparse-path encoding it's ostensibly unnecessary, BUT: under multi-entry-leaf pathology, the upstream "stored partial" can have already gone stale, so when this method PEXTs+PDEPs that stored partial it propagates stale bits. |
| I-SP | ❌ **VIOLATES** | Same as I5. |
| I6 | ❌ | Routing inherited from upstream stale partials. |
| I7 | ✅ | `sortChildrenAndPartialsByPartial` at line 4878. |
| I9 | ⚠ | If `newMask == 0`, falls back to `createNodeFromChildren` (line 4755-4758). |
| I10 | ⚠ | Cross-window split bit detected at line 4726 → fallback. |
| I-PD | ✅ | Uses `Long.compress(oldPartial, relevant)` + `Long.expand(compressed, oldBitsInNewLayout)`. Bijection. |

**Cell of concern:** **I5/I-SP/I6**. This is the operation at the center of the cascade. The
documented "constancy check removed" decision is correct in theory but assumes pre-construction
data is sound; that pre-condition is exactly what multi-entry-leaf operations break. Stage F's
fix design must close this.

---

### Op 23 — `buildCompressedHalfMultiMask`

**Purpose.** MultiMask analog of Op 22. Same algorithm, MultiMask layout.

**Code:** `HOTTrieWriter:4926`.

**Invariant scoring:** Same gaps as Op 22. Adds I-PD ⚠ for the >32-bit edge.

---

### Op 24 — `rebuildParentAbsorbingSplit`

**Purpose.** Fallback used when addEntryWithPDep returns null AND fan-out < 20. Rebuilds
parent from scratch with the new (k+1) children via `createNodeFromChildren`.

**Code:** `HOTTrieWriter:3208`.

**Invariant scoring:** Inherits from Op 25 (`createNodeFromChildren`).

---

### Op 25 — `createNodeFromChildren` / `buildFlatNonStrict`

**Purpose.** Build an indirect from a sorted list of children, computing disc bits via adjacent-pair
XOR scan over first/last keys (`computeDiscBits`).

**Code:** `HOTTrieWriter:5270, 5311`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ❌ **VIOLATES** | Documented at line 5298-5304: "under multi-entry leaves with overlapping spans (e.g., warmup + main keys mixed in the same leaf), `computeDiscBits` can capture bits that are non-constant in some child's subtree — yielding I-Binna constancy violations". |
| I-SP | ❌ | Same. |
| Others | same as createSpanNode | — |

**Cell of concern:** I5/I-SP. This is the legacy "broken fallback" — Stage F may need to
hard-gate it (per pending task #21 "Phase 4b.6 — Hard-gate createNodeFromChildren-N").

---

### Op 26 — `createBiNodeTraced` (intermediate-BiNode workaround)

**Purpose.** Wrap `createBiNode` with `probeConstancyOnBuild` for diagnostic logging.
Used as the safety-net fallback in updateParentForSplitWithPath:1248-1260.

**Code:** `HOTTrieWriter:5329`.

**Invariant scoring:** Same as `createBiNode`. The intermediate-BiNode usage adds tree height
by 1 (non-faithful to Binna). Documented at line 1242-1246.

**Cell of concern:** I8 — when used as intermediate-BiNode, the slot's firstKey may decrease
relative to its predecessor (caught by `intermediateBiNodePreservesSlotOrder`) or relative to
its successor (NOT caught — the documented marginal violation).

---

### Op 27 — `rebalanceAndIntegrate` (Phase 3)

**Purpose.** When `addEntryWithPDep` rejects on β-constancy (Case 2b-ii), walk every non-split
sibling: if non-constant on β, split that sibling's leaves on β. Rebuild parent with constant
children at every slot.

**Code:** `HOTTrieWriter:2352`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ✅ | By construction: every sibling slot is either bv-constant OR was just split into two β-constant halves. |
| I-SP | ✅ | Same. |
| I3 | ⚠ WEAK | Resulting fan-out can grow beyond 32 → returns null → caller falls through. |
| Others | depends on `buildRebalancedParentWithInheritedMask` | — |

Note: Phase 3 explicitly DOES NOT handle β-already-in-mask (line 2360-2376) — that's Phase 4's
job.

---

### Op 28 — `buildRebalancedParentWithInheritedMask`

**Purpose.** Helper for Phase 3: build new parent from a `RebalancedSibling[]` plan, inheriting
mask from original parent + adding β.

**Code:** `HOTTrieWriter:2213`.

**Invariant scoring:** Algorithm is sound IF Phase 3's pre-check upholds. Returns null if integration
fails (collision, etc.).

---

### Op 29 — `splitSubtreeOnBit` / `splitLeafOnBit` / `splitIndirectOnBit`

**Purpose.** Phase 3 worker: split a subtree (recursing into leaves and indirects) on β.
Produces `(leftRef, rightRef)` where left has β=0 keys, right has β=1 keys.

**Code:** `HOTTrieWriter:1829, 2026, 2079`.

**Invariant scoring:** ✅ on β-constancy by construction. ⚠ WEAK on I8 if subtree had
non-contiguous β-partition.

---

### Op 30 — `buildBucketWithInheritedMask`

**Purpose.** Phase 3 helper that builds a sub-bucket of the rebalanced parent.

**Code:** `HOTTrieWriter:1894`.

**Invariant scoring:** Inherits from Op 28.

---

### Op 31 — `tryPhase4SubtreeMerge`

**Purpose.** Phase 4 dispatch: gate on β-already-in-mask, find the sibling slot that already
encodes ¬v_L, route to `subtreeMerge` or `hoistAndReroute`.

**Code:** `HOTTrieWriter:2470`.

**Invariant scoring:** Dispatcher; preservation = downstream branch.

---

### Op 32 — `subtreeMerge` (Phase 4)

**Purpose.** Replace splitChildIdx's slot with `keepHalf`; bulk-insert moveHalf's keys into
the existing β=¬v_L sibling subtree.

**Code:** `HOTTrieWriter:2641`.

**Invariant scoring:**

| Inv | Score | Mechanism |
| --- | --- | --- |
| I5 | ⚠ WEAK | keepHalf is β=v_L by construction (it's the half whose β-bit matches splitChild's stored partial). moveHalf is β=¬v_L. The bulk-inserts into the sibling subtree must themselves preserve I5 — recursively. |
| I-SP | ⚠ WEAK | Same. |
| I-CoW | ✅ | `withUpdatedChild` preserves parent's pageKey. |
| Others | depends on `bulkInsertIntoSiblingSubtree` recursion | — |

**Empirical activation:** Phase 4 fires rarely on the diagnostic reproducer (line 2761-2766 of
`hoistAndReroute`'s Javadoc — "the strict criterion never fires"). The path is mostly latent.

---

### Op 33 — `hoistAndReroute` (Phase 4)

**Purpose.** When subtreeMerge has no sibling slot at parent, walk UP looking for an ancestor
that does, with the EXACT target-partial match. Bulk-insert moveHalf there.

**Code:** `HOTTrieWriter:2781`.

**Invariant scoring:** ✅ on I6 by the strict criterion (line 2752-2759). Empirically does not fire.

---

### Op 34 — `bulkInsertIntoSiblingSubtree`

**Purpose.** Phase 4 worker: insert one (key, value) into a subtree, descending from
siblingRef. May trigger leaf split → Phase 4 again (bounded recursion).

**Code:** `HOTTrieWriter:2962`.

**Invariant scoring:** Recursive; preservation = sub-recursion's preservation. Same I5/I-SP
weakness via leaf insert (Op 1).

---

### Op 35 — `splitLeafAndIntegrateInSiblingSubtree`

**Purpose.** Phase 4 helper to split a leaf within the sibling subtree and integrate.

**Code:** `HOTTrieWriter:3097`.

**Invariant scoring:** Subset of `handleLeafSplitAndInsertInternal`. Same gaps.

---

## §4. Master Matrix Table

Compact view across all 35 operations × 16 invariants. Cells: ✅ PRESERVES / ⚠ WEAK / ❌ VIOLATES /
N N/A / ? UNKNOWN / dot (`.`) for "see narrative".

| # | Op | I1 | I2 | I3 | I4 | I5 | I6 | I7 | I8 | I9 | I10 | I11 | I12 | I-SP | I-PD | I-CoW | I-MR |
| --- | --- |--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|--|
| 1 | leaf.put | N | N | N | N | ❌ | ⚠ | N | ⚠ | N | N | N | N | ❌ | N | ⚠ | ⚠ |
| 2 | leaf.mergeWithNodeRefs | N | N | N | N | ❌ | ⚠ | N | ⚠ | N | N | N | N | ❌ | N | ⚠ | ⚠ |
| 3 | leaf.splitToWithInsert | N | N | N | N | ⚠ | N | N | ✅ | N | N | N | N | ⚠ | N | ⚠ | ⚠ |
| 4 | leaf.splitToWithInsertOnBit | N | N | N | N | ✅ | N | N | ⚠ | N | N | N | N | ✅ | N | ⚠ | ⚠ |
| 5 | createBiNode | ✅ | ✅ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ⚠ | ✅ | N | ⚠ | ⚠ | ⚠ | N | ✅ | ✅ |
| 6 | createSpanNode | ✅ | ✅ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | ✅ | ⚠ | ⚠ | ⚠ | N | ✅ | ✅ |
| 7 | createMultiNode | ✅ | ✅ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | ✅ | ⚠ | ⚠ | ⚠ | N | ✅ | ✅ |
| 8 | createSpanNodeMultiMask | ✅ | ✅ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | N | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | ✅ |
| 9 | createMultiNodeMultiMask | ✅ | ✅ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | N | ⚠ | ⚠ | ⚠ | ⚠ | ✅ | ✅ |
| 10 | withUpdatedChild | ✅ | ✅ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ⚠ | ✅ | ✅ | ✅ | ✅ | ⚠ | N | ✅ | ✅ |
| 11 | createNewLeaf | N | N | N | N | N | N | N | N | N | N | N | N | N | N | ✅ | ✅ |
| 12 | handleLeafSplitAndInsertInternal | . | . | . | . | . | . | . | . | . | . | . | . | . | . | ✅ | ✅ |
| 13 | updateParentForSplitWithPath | . | . | . | . | . | . | . | ⚠ | . | . | . | . | . | . | ✅ | ✅ |
| 14 | addEntryWithPDep | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ✅ | ✅ | ✅ |
| 15 | addEntryFreshPolaritySingleMask | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ✅ | ✅ | ✅ |
| 16 | addEntryFreshPolarityMultiMask | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | N | ⚠ | ⚠ | ✅ | ⚠ | ✅ | ✅ |
| 17 | addEntryMultiMask | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | N | ⚠ | ⚠ | ✅ | ⚠ | ✅ | ✅ |
| 18 | upgradeToMultiMaskWithNewBit | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | N | ⚠ | ⚠ | ✅ | ⚠ | ✅ | ✅ |
| 19 | expandParentNode | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ✅ | ✅ | ✅ |
| 20 | splitParentAndRecurse | ✅ | . | . | . | ⚠ | . | . | ⚠ | . | . | . | . | ⚠ | . | ✅ | ✅ |
| 21 | splitParentHalfAndRecurse | ✅ | . | . | . | ⚠ | . | . | ⚠ | . | . | . | . | ⚠ | . | ✅ | ✅ |
| 22 | buildCompressedHalf (SM) | ✅ | ✅ | ✅ | ⚠ | ❌ | ❌ | ✅ | ⚠ | ⚠ | ⚠ | ⚠ | ⚠ | ❌ | ✅ | ✅ | ✅ |
| 23 | buildCompressedHalfMultiMask | ✅ | ✅ | ✅ | ⚠ | ❌ | ❌ | ✅ | ⚠ | ⚠ | N | ⚠ | ⚠ | ❌ | ⚠ | ✅ | ✅ |
| 24 | rebuildParentAbsorbingSplit | . | . | . | . | ❌ | ❌ | . | . | . | . | . | . | ❌ | . | ✅ | ✅ |
| 25 | createNodeFromChildren / buildFlatNonStrict | ✅ | ✅ | ⚠ | ⚠ | ❌ | ❌ | ⚠ | ⚠ | ✅ | ⚠ | ⚠ | ⚠ | ❌ | ✅ | ✅ | ✅ |
| 26 | createBiNodeTraced (interm) | ✅ | ✅ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ❌ | ✅ | N | ⚠ | ⚠ | ⚠ | N | ✅ | ✅ |
| 27 | rebalanceAndIntegrate | . | . | ⚠ | . | ✅ | ✅ | . | ⚠ | . | . | . | . | ✅ | . | ✅ | ✅ |
| 28 | buildRebalancedParentWithInheritedMask | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ⚠ | ✅ | ⚠ | ⚠ | ⚠ | ✅ | ✅ | ✅ | ✅ |
| 29 | split*OnBit (sub/leaf/indirect) | ✅ | . | . | . | ✅ | . | . | ⚠ | . | . | . | . | ✅ | . | ✅ | ✅ |
| 30 | buildBucketWithInheritedMask | . | . | . | . | ✅ | ✅ | . | ⚠ | . | . | . | . | ✅ | . | ✅ | ✅ |
| 31 | tryPhase4SubtreeMerge | . | . | . | . | . | . | . | . | . | . | . | . | . | . | ✅ | ✅ |
| 32 | subtreeMerge | ✅ | ✅ | ✅ | ✅ | ⚠ | ⚠ | ✅ | ⚠ | ✅ | ✅ | ✅ | ✅ | ⚠ | N | ✅ | ✅ |
| 33 | hoistAndReroute | ✅ | ✅ | ✅ | ✅ | ⚠ | ✅ | ✅ | ⚠ | ✅ | ✅ | ✅ | ✅ | ⚠ | N | ✅ | ✅ |
| 34 | bulkInsertIntoSiblingSubtree | . | . | . | . | ❌ | ⚠ | . | ⚠ | . | . | . | . | ❌ | . | ✅ | ✅ |
| 35 | splitLeafAndIntegrateInSiblingSubtree | . | . | . | . | ❌ | ⚠ | . | ⚠ | . | . | . | . | ❌ | . | ✅ | ✅ |

---

## §5. Surfaced Issues

### 5.1 Hard violations (❌ cells)

These cells are **known broken** under multi-entry-leaf workloads:

1. **Op 1 (leaf.put), Op 2 (leaf.mergeWithNodeRefs)** — I5/I-SP. Leaf inserts have no awareness
   of ancestor disc bits. Every leaf insert is a potential I5/I-SP violation. **This is the
   root cause** of every cascade in the campaign.

2. **Op 22 (buildCompressedHalf SM), Op 23 (buildCompressedHalfMultiMask)** — I5/I-SP/I6.
   "Constancy check removed" (line 4829-4832) under sparse-path encoding assumes upstream
   data is sound; multi-entry-leaf invalidates this assumption.

3. **Op 24 (rebuildParentAbsorbingSplit) → Op 25 (createNodeFromChildren)** — I5/I-SP/I6.
   Documented gap at line 5298-5304; pending task #21 to hard-gate.

4. **Op 26 (createBiNodeTraced as intermediate-BiNode)** — I8 next-side. Documented design
   accepted as marginal; the lone violation in current 1-violation baseline.

5. **Op 34/35 (Phase 4 bulkInsert/splitLeafIntegrate)** — Inherit Op 1's I5 violation through
   recursion, but rare (Phase 4 doesn't fire empirically on the reproducer).

### 5.2 Weak cells worth empirical Stage E investigation

- **I4 (first-partial-zero)** — every factory + addEntry method is ⚠. Need to empirically check
  whether `partialKeys[0] == 0` holds on real reproducer trees.
- **I8 next-side at withUpdatedChild (Op 10)** — beyond the documented intermediate-BiNode case,
  does any other call to `withUpdatedChild` violate I8?
- **I11 (trie-condition)** — every indirect factory is ⚠. Need cross-level check that
  parent.MSB < child.discBit.

### 5.3 The recurring failure pattern, formally

Each fix attempt addressed ONE column of the matrix:

| Attempt | Closed cell | Opened cells (failures) |
| --- | --- | --- |
| (a) parentIndices.includes(splitChildIdx) | partial bit-derivation | I-SP across 9000+ children |
| (b) sparse-path no-OR | siblingBitValues encoding | every off-path sibling routes wrong |
| (c) eager Phase 2 (any non-constant β) | leaf I5 | parent.I8 broken by non-contiguous splits |
| (d) constrained Phase 2 (β=MSDB-with-K) | leaf I5 narrow | parent.I3 collision uptick |
| (e) intersection-encoding | β-shared bits | Op 22's sparse-path inheritance breaks 5995-fold |

Each row had a SHALLOW invariant fix (one cell green) but cascaded into other cells (multiple
cells red). The lesson encoded in Stage F's design must be: **fix all hard-violation cells
simultaneously OR reject the operation up-front**.

### 5.4 The convergence point

All cascades trace back to **Op 1 (leaf.put)** mutating a leaf in an I5-violating way, then
some downstream operation (Op 22, Op 25) propagating the staleness. The only operation that
directly closes Op 1's I5 gap is **Op 4 (splitToWithInsertOnBit)** when called via the
constancy-aware path in Op 12 (handleLeafSplitAndInsertInternal:1023-1043) — but that path
only fires on overflow, not on every insert.

**Stage F design implication:** I5 must be enforced at INSERT time, not at OVERFLOW time.
A constancy-violating insert into a non-overflow leaf is currently silent; it only manifests
when subsequent operations traverse the parent's stored partial.

---

## §6. Stage Roadmap (sourced from this matrix)

The matrix has now identified WHICH cells must be closed and BY WHAT operations. Stage C
through G follow:

1. **Stage C** — `HOTInvariantValidator` extensions:
   - Add I4 (first-partial-zero) check.
   - Add I11 (trie-condition: parent.MSB < child.discBit) check.
   - Add a **leaf-insert pre-condition** check: for every leaf insert, walk the path and verify
     the new key's β values match every ancestor's stored partial at every β. This is the
     check that surfaces ❌ Op 1 / Op 2.

2. **Stage D** — Post-mutation invariant verification gate. Run validator after EVERY mutating
   operation in the test reproducer (gated on `-Dhot.strict.validate=1`). Build the empirical
   "operation × invariant failure rate" table.

3. **Stage E** — Run reproducer with Stage D gate ON; record which operations fire which ❌
   cells, in what order, with what frequency. This tests whether the matrix in §4 matches
   reality.

4. **Stage F** — `HOT_FIX_DESIGN.md`. Per-operation preservation logic. Two design questions
   to resolve:
   - **Where do we fix Op 1's I5 gap?** Options: (a) at INSERT time via constancy-aware
     dispatch in `bulkInsertIntoSiblingSubtree` and the public insert API; (b) at OVERFLOW
     time via guaranteed-constancy split. (a) is the C++ approach; (b) is what Phase 2
     currently attempts (but only on overflow).
   - **Do we hard-gate Ops 24/25 (createNodeFromChildren-N)?** Pending task #21 says yes.
     If gated, need a fallback: split on β instead.

5. **Stage G** — Implement Stage F design. Goal: 0 violations on reproducer.

6. **Stage H** — Phase 5 perf validation + Phase 6 cleanup of intermediate-BiNode workaround.

---

## §7. Open Questions for Stage E (Empirical)

1. Does I4 actually hold today (= validator would not flag any false positive once added)?
2. Does I11 hold today?
3. What is the actual firing distribution of Phase 3 / Phase 4 / intermediate-BiNode /
   splitParent on the reproducer? (We have spot checks; need a complete table.)
4. When `splitToWithInsertOnBit` falls back to MSDB at line 1031-1037 of Op 12, does that
   fall-back ever produce I5 violations downstream that wouldn't have happened with
   `splitToWithInsert` directly?
5. For Op 10 (`withUpdatedChild`), what fraction of calls produce I8 violations beyond the
   documented intermediate-BiNode case?

These questions can ONLY be answered by running the reproducer with the Stage D gate. That is
Stage E.
