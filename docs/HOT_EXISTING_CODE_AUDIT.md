# HOT Existing-Code Audit — what is wrong, vs. Binna's thesis + reference

**Method.** Three independent audit passes (each itself a double pass) of the live
Sirix HOT writer — `HOTTrieWriter.java` (~14k lines), `AbstractHOTIndexWriter.java`,
`HOTIndirectPage.java`, `HOTLeafPage.java` — against Binna's reference
(`~/IdeaProjects/hot-reference`), the thesis, the invariants I1–I11
(`HOT_FORMAL_FOUNDATION.md` §2), and the verified incremental algorithm
(`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`).

## Root cause (all three passes converge on this)

**The Sirix HOT writer is not Binna's algorithm. It encodes the trie from single
representative keys and locally-observed bits, never from the subtree-invariant
path — then patches the damage afterward.** Every campaign "fix" (the
`lift*`/`extend*`/`reconcile*`/`recompute*` families) is either dead code or the
in-place mask-repair operator Foundation Theorem 3 proved cannot converge.

This is why the structure cannot be patched incrementally and a faithful port
(option B) is required.

## Findings

### A. Split picks the wrong discriminative bit

| # | Sev | Where | What is wrong | Breaks |
|---|-----|-------|---------------|--------|
| A1 | CRIT | `HOTLeafPage.findMsdbBit` :2567, `findMsdbWithNewKey` :2612 | Leaf split uses the MSDB of the leaf's **own** keys, not the bit that separates this `R(S)`-subtree from its siblings. Produces non-`R(S)` leaves (F3). | I5,I8,I11,I6 |
| A2 | HIGH | `splitParentAndRecurse` :12490,:12537 | Indirect split classifies each child by `firstKey`'s MSB bit, not by the child's stored partial — misclassifies any non-MSB-constant child. | I5,I1 |
| A3 | HIGH | `HOTLeafPage.splitToWithInsertOnBit` :2251 | Splits on a caller-supplied bit less significant than the MSDB → **non-contiguous** partition (its own Javadoc admits this). | I8,I7 |
| A4 | HIGH | `splitParentHalfAndRecurse` :13270, splitPoint `numChildren/2` :13276 | Fallback splits at a **positional** cut, not at any disc bit — a B⁺-tree separator, not a HOT split. | I5,I8,I11 |
| A5 | MED | `updateParentForSplitWithPath` :948 | BiNode `discBit = computeDifferingBit(leftMax,rightMin)` — derived from two boundary keys, not the split node's MSB. | I5,I11 |

### B. Partial keys / masks are firstKey-based, not path-based

| # | Sev | Where | What is wrong | Breaks |
|---|-----|-------|---------------|--------|
| B1 | CRIT | `forceRebuildIndirectFromFirstKeyMsdbs` :5366/:5407, `recomputePartialsForCurrentFirstKeys` :5212, `buildFlatNonStrict` I8-fixup :13654 | Stored partial = `densePK(firstKey)` of **one** key. A leaf holds up to 512 keys; the other 511 need not match. This is failure mode F2, and it is pervasive. | I5,I6 |
| B2 | CRIT | `computeDiscBits` :12154 | Mask = union of bits where **adjacent firstKeys** differ, with — its own comment — "no constancy filter": under-captures the MSDB-closure (stale mask, F1) and over-captures non-constant bits. Code admits it "ships known I6 violations". | I5,I6 |
| B3 | HIGH | `computeSparsePathRecursive` :12317 | The one genuine sparse-path encoder assumes each captured bit is monotone `0…0 1…1` across firstKeys — but `computeDiscBits` never enforces monotonicity. Non-monotone bit ⇒ a partial inflated with a bit its own firstKey lacks. | I5,I7 |

### C. Descent never computes the insert depth

| # | Sev | Where | What is wrong | Breaks |
|---|-----|-------|---------------|--------|
| C1 | HIGH | `AbstractHOTIndexWriter.prepareLeafOfTree` :337 | No mismatch `DiscriminativeBit` and no insert-depth computation. Descent runs to whatever leaf PEXT-routing reaches; the new key is patched in afterward. Binna inserts at the node whose mask **owns** the mismatch bit. The whole `STRICT_BINNA` block in `doIndex` is post-hoc reroute machinery compensating for this missing step. | I6,I1 |

### D. Integration / cascade is unfaithful

| # | Sev | Where | What is wrong | Breaks |
|---|-----|-------|---------------|--------|
| D1 | CRIT | `splitParentAndRecurse` :12586 | The cascade integrates a BiNode into the grandparent **with no trie-condition assert** (`parent.MSB < B.discBit`). Binna asserts it (`HOTSingleThreaded.hpp:523`) so an I11-violating level is caught, not committed. | I11→I6 |
| D2 | CRIT | `addEntryWithPDep` :1348 (`newMask = oldMask \| newBit`) | **No pull-up branch.** When a disc bit out-ranks a full node's MSB, the node's mask is extended **in place** instead of re-parenting the node under a fresh BiNode. `min(mask)` drops ⇒ I11 breaks. This is the Foundation Theorem 3 anti-pattern. | I5,I11 |
| D3 | HIGH | `extendIndirectMaskForClosure*` :10864, `phase7qExtendWithLift` :8327, `addNewRootLevelForI8` :10455 | The live `extend*`/`lift*` family — in-place mask repair, exactly Theorem 3's non-converging operator. | I5,I7,I8,I11 |
| D4 | HIGH | `getHeightFromChild` :12004 (`return 0` fall-through) | The intermediate-node decision needs per-node height; an unresolvable child yields height 0, collapsing the decision. | structure |

### Dead vs. live campaign helpers

**Dead (0 callers — delete in #9):** `commitTimeLiftAllChildMsbs`,
`liftChildMsbsForI11`, `phase7kRecursiveCommit`, `phase7jExtendWithAllClosureBits`,
`reconcileRootMaskI11Safe` (already a no-op stub).
**Live anti-pattern (must be removed, not deleted blindly):**
`extendIndirectMaskForClosure`, `phase7qExtendWithLift`, `addNewRootLevelForI8`.

### Confirmed correct

- Tombstones **are** carried through leaf splits (`splitToWithInsert`,
  `splitLeafOnBit` copy every entry incl. `0xFE`); `compactTombstones` is only
  called separately.
- The `split` helpers allocate **fresh** pages (no in-place mutation *in the split
  operator itself* — the in-place mutation is in the `extend`/`addEntry` family, D2/D3).

## Verdict

The five classes A–D are one defect: structure derived from sampled keys, not from
the path. It explains every observed failure — the `deleteAllAndReinsert` rev-1 I11
(A1+B2+D2), the 388/1000 entry loss (B1/B2 ⇒ I6 misroute ⇒ range scan misses
entries). It cannot be patched; the campaign's 126 commits are the evidence.

**The faithful port (`HOT_INCREMENTAL_SPLIT_VERIFICATION.md` §11 checklist) must
replace:** the descent (add mismatch-bit + insert-depth), the split (split at the
node MSB / true sibling-separating MSDB), the encoding (true sparse-path from the
BiNode path), the integration (faithful `integrateBiNodeIntoTree` — trie-condition
assert, pull-up, intermediate-node, terminating cascade), and **delete** the
`lift`/`extend`/`reconcile` family.
