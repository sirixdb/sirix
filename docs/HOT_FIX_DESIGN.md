# HOT Fix Design (Stage F)

**Status:** Stage F deliverable. Per-operation preservation logic for the HOT
strict-Binna conformance campaign. Sourced from Stage E's empirical data
(`HOT_EMPIRICAL_FAILURE_TABLE.md`), not intuition.

**Major reframe from Stage E**: violations come from HAPPY-PATH writer operations
(`addEntryWithPDep`, `splitParentAndRecurse`, `buildCompressedHalf` SingleMask main
path), NOT from the recovery fallbacks (Phase 3 / Phase 4 / intermediate-BiNode /
fresh-polarity). Recovery counters fired only 65 times across 50K inserts. **The fix
scope is happy-path correctness.**

---

## §1. Goal

Drive the 50K microbench-pattern reproducer (`-Dhot.strict.binna=true
-Dhot.strict.validate=true`) from the current 2-violation baseline (`{I4=1, I8=1}`)
to **0 violations**. Maintain default-mode (no flag) at 0 violations on the 100K CAS
test (current state).

Each bug gets:
- **Mechanism**: why the operation produces the violation.
- **Fix**: minimal code change.
- **Blast radius**: which other invariants the fix might affect.
- **Validation**: how Stage D gate confirms the fix.

---

## §2. Bug B1 — `addEntryWithPDep` produces non-zero smallest partial (I4)

**Location**: `HOTTrieWriter.java:1430-1448`.

**Mechanism**:

When adding new disc bit β to a SingleMask parent, `addEntryWithPDep` repositions
existing partials via `Long.expand` and OR's in each non-split sibling's β-bit value:

```java
final int repositioned = (int) Long.expand(
    Integer.toUnsignedLong(oldPartialKeys[i]), oldPartialMaskInNewLayout);
newPartialKeys[j] = repositioned | (siblingBitValues[i] << newBitOutputPos);
```

Under Binna sparse-path encoding the OLD parent had `oldPartialKeys[0] == 0` (= I4
held — leftmost slot's partial is zero because that child takes LEFT at every old
BiNode on its path).

After repositioning:
- For the leftmost ORIGINAL sibling (i=0): `Long.expand(0, mask) = 0`. New partial =
  `0 | (siblingBitValues[0] << newBitOutputPos)`.
- If `siblingBitValues[0] == 0`: new partial = 0. **I4 preserved.**
- If `siblingBitValues[0] == 1`: new partial = `1 << newBitOutputPos > 0`.

For the split products:
- LEFT half: `splitChildRepositioned = Long.expand(splitChildOldPartial, mask)`. =
  0 only when splitChildIndex was originally slot 0.
- RIGHT half: `splitChildRepositioned | (1 << newBitOutputPos)`. Always > 0 (RIGHT
  side, β-bit = 1).

**I4 breaks iff** `splitChildIndex > 0` AND `siblingBitValues[0] == 1` AND no other
slot's repositioned partial is 0. Which is exactly: the leftmost original sibling's
subtree has β=1 AND the split is not on the leftmost slot. This matches Stage E's
observation at idx 1750.

**Fix**:

Add an I4 self-check before line 1469 (`sortChildrenAndPartialsByPartial`):

```java
// I4 — first-partial-zero (Binna's "first mask always zero" rule). Under
// sparse-path encoding the leftmost slot in partial-key order must have
// partial = 0. If our construction produced no zero partial, the layout
// is structurally invalid — return null so the caller takes
// splitParentAndRecurse, which rebuilds halves from scratch.
boolean hasZeroPartial = false;
for (int i = 0; i < newNumChildren; i++) {
  if (newPartialKeys[i] == 0) { hasZeroPartial = true; break; }
}
if (!hasZeroPartial) return null;
```

**Blast radius**:
- I3 (uniqueness): unaffected — the existing check already runs.
- I7/I8 (sort order): unaffected — sort happens after.
- Caller behavior: returns null forces fall-through to `splitParentAndRecurse`. If
  `splitParentAndRecurse` produces I4-violating output too, we need to fix it as B3.

**Validation**: Stage D gate. After fix, observe whether `{I4=1, I8=1}` baseline
becomes `{I8=1}` only (or new violations from the cascade if `splitParentAndRecurse`
also breaks I4).

---

## §3. Bug B2 — `splitParentAndRecurse` post-split parent has I8 violation

**Location**: `HOTTrieWriter.java:4319-4467` + the `buildCompressedHalf` it calls.

**Mechanism (hypothesis from Stage E)**:

At idx 500 in the reproducer, the tree was height 3 with no recovery firings since
post-warmup. The I8 violation `indirect 2 child[2].firstKey ≥ child[3].firstKey` is
on indirect 2 — the root or a near-root indirect.

`splitParentAndRecurse` partitions children by `parent.MSB` into smaller (MSB=0)
and larger (MSB=1) halves. Each half is rebuilt via `buildCompressedHalf`, then
integrated upward. The integration recurses: at the grandparent, `updateParentForSplitWithPath`
gets called with `(leftNodeRef, rightNodeRef)`. If grandparent's slot ordering
breaks because `leftNodeRef.firstKey > grandparent.children[grandparentIdx-1].firstKey`,
I8 breaks.

The likely concrete bug: `buildCompressedHalf`'s `sortChildrenAndPartialsByPartial`
at line 4878 sorts by partial-key. Under sparse-path encoding partial-key order ≡
first-key order — IF the partials are correctly encoded. But Bug B1's mechanism shows
that partials can be wrong (I4-violating); when partials are wrong, partial-key order
diverges from first-key order, and I8 breaks.

**Fix**:

This bug should be FIXED IMPLICITLY by Bug B1's fix: rejecting bad partial layouts
forces a fresh `splitParentAndRecurse` path which (theoretically) produces correct
partials. If after Bug B1's fix I8 still fires, we add explicit I8 verification in
`splitParentAndRecurse` and `buildCompressedHalf` returns:

```java
// I8 self-check: verify children are sorted by first-key.
byte[] prevFirst = null;
for (final PageReference c : sortedChildren) {
  byte[] thisFirst = getFirstKeyFromChild(c);
  if (prevFirst != null && thisFirst != null
      && Arrays.compareUnsigned(prevFirst, thisFirst) >= 0) {
    return null;  // bail — caller falls back to createNodeFromChildren-N
  }
  prevFirst = thisFirst;
}
```

**Blast radius**:
- This fallback is `createNodeFromChildren-N`, which has its own known I5 issues
  (Stage B Op 25). Pending task #21 hard-gates it. May surface I5 violations if it
  fires.

**Validation**: Stage D gate. After Bug B1, see if I8 still fires at idx 500.

---

## §4. Bug B3 — `splitParentAndRecurse` may produce I4-violating output

**Location**: `HOTTrieWriter.java:4444` (`newRootDiscrimBit = msbPosition`) +
`buildCompressedHalf` recursive partial-key generation.

**Mechanism (predicted, not yet observed)**:

`buildCompressedHalf`'s SingleMask main path constructs `newPartials[]` by:
1. Sort children by first-key.
2. For split products: `computePartialKeySingleMask(firstKey, oldInitialBytePos, newMask)`.
3. For inherited children: PEXT old partial via `relevant`, PDEP into new layout.
4. Sort by partial-key.

The leftmost-by-first-key child after step 4 should have partial 0 — but the PEXT+PDEP
chain doesn't guarantee that. Specifically, if the leftmost INHERITED child had a
non-zero PEXT extract in the new layout (because of `relevant` bits), its repositioned
partial > 0.

This is a similar shape to Bug B1.

**Fix**:

Add the same I4 self-check at the end of `buildCompressedHalf` (just before the
final `createSpanNode`/`createMultiNode` factory call):

```java
boolean hasZero = false;
for (int p : newPartials) if (p == 0) { hasZero = true; break; }
if (!hasZero) {
  BCH_FALLBACK_I4_VIOLATION.incrementAndGet();
  return createNodeFromChildren(halfChildren, newPageKey, revision, parent.getHeight());
}
```

**Blast radius**: same fallback to `createNodeFromChildren-N`.

**Validation**: Stage D gate after Bugs B1+B3. If I4 still fires, dig into
`createNodeFromChildren-N` (= `buildFlatNonStrict`).

---

## §5. Bug B4 — `addEntryWithPDep` may produce I11-violating output

**Location**: `HOTTrieWriter.java:1471-1476` (the factory calls).

**Mechanism**:

I11 (trie-condition): for every parent → child indirect edge, every bit b in
child.mask must satisfy b > parent.MSB (= child's bits are less significant).

`addEntryWithPDep` adds new bit `newAbsBit` to parent's mask. The result's
`newMostSignificantBitIndex` may be `newAbsBit` if it's the smallest absolute bit
position among the resulting mask. If parent had bits at positions [10, 12] and
newAbsBit = 8, the result's MSB is 8, which is MORE significant than the original
10/12.

If this expanded indirect now sits inside a grandparent whose MSB ≥ 8, the trie
condition `parent.MSB < child.MSB` breaks at the grandparent → expanded-indirect edge.

This matches Stage E's observation: I11 fires at idx 500 with no recovery firings,
then SELF-CLEARS by main+1500 (a later happy-path operation reorders or replaces
the offending edge).

**Fix**:

In `addEntryWithPDep`, before constructing the result, verify the new MSB is still
greater than the grandparent's MSB. **But** addEntryWithPDep doesn't know the
grandparent's MSB in its current API. Two options:

(a) Pass `grandparentMSB` into `addEntryWithPDep` and reject when `newAbsBit ≤
grandparentMSB`. Caller passes `pathNodes[currentPathIdx-1].getMostSignificantBitIndex()`
or -1 at root.

(b) Add I11 verification in the caller (`updateParentForSplitWithPath`) after
`addEntryWithPDep` returns success, and re-route to `splitParentAndRecurse` if I11
breaks at the grandparent edge.

Option (b) is less invasive. Sketch:

```java
expandedParent = addEntryWithPDep(parent, ...);
if (expandedParent != null) {
  // I11 check: the new mask MSB must remain > grandparent.MSB.
  if (currentPathIdx > 0) {
    final HOTIndirectPage grandparent = pathNodes[currentPathIdx - 1];
    if (expandedParent.getMostSignificantBitIndex()
        <= grandparent.getMostSignificantBitIndex()) {
      expandedParent = null;  // would break I11 — fall back to split
    }
  }
}
```

**Blast radius**: extra `splitParentAndRecurse` calls. Should be benign — split
already runs as the fallback in many other cases.

**Validation**: Stage D gate. I11 should not appear at any checkpoint.

---

## §6. Bug B5 — `splitToWithInsert` MSDB fall-back leaves leaf-insert-precondition hits

**Location**: `HOTTrieWriter.java:1031-1037` (fall-back inside
`handleLeafSplitAndInsertInternal`).

**Mechanism**:

When `splitToWithInsertOnBit(explicitBit)` returns -1 (degenerate partition), the
code falls back to `splitToWithInsert` (MSDB-based). The fall-back loses the constancy
guarantee that motivated the explicit bit. After fall-back, the leaf may contain keys
that disagree with an ancestor's β at some position, causing the
`I-leaf-insert-precondition` to fire on subsequent inserts.

Stage E recorded **72 such pre-condition hits** out of 50K inserts (0.14 %).

**Fix**:

Two design options:
(a) **Reject the fall-back**: return false from `handleLeafSplitAndInsertInternal`,
   force the writer's caller to retry with a different keyset. This is structurally
   correct (= no I5 violation introduced) but may starve insertion in pathological
   cases.

(b) **Accept the fall-back** (current behavior): the precondition violation is
   silent — leaf-level β-mixing exists, and downstream operations may produce
   I8/I11/I4 violations. Pre-condition gate is informational.

For Stage F's first iteration, choose **(b)**: the fall-back stays. Bug B5 is
informational. Phase 2 of the original design plan (constancy-aware split with
broader fall-back logic) is a future-session item — IF Stage G's Bug-B1..B4 fixes
land 0 violations, B5 may be unnecessary.

**Validation**: After Bugs B1-B4, the Stage D run's `precondHitsTotal` may decrease
(if fewer leaves end up β-mixed) or stay the same. Re-evaluate.

---

## §7. Implementation Plan

Stage G implementation order — most localized first, validate each via Stage D gate.

| Order | Bug | Estimated effort | Validation |
| --- | --- | --- | --- |
| G.1 | B1 (addEntryWithPDep I4 self-check) | 10 lines | Stage D gate; expect `I4=1` to disappear if no cascade. |
| G.2 | B4 (addEntryWithPDep I11 reject in updateParentForSplitWithPath) | 10 lines | Stage D gate; expect `I11=1` (idx 500) to disappear. |
| G.3 | B3 (buildCompressedHalf I4 self-check) | 8 lines | Stage D gate; expect any I4 from `splitParentAndRecurse` cascades to disappear. |
| G.4 | B2 (buildCompressedHalf I8 self-check) | 12 lines | Stage D gate; expect `I8=1` to disappear or change source. |
| G.5 | (re-evaluate) | — | If violations remain, dig into `createNodeFromChildren-N` or new sources. |

Each step: implement + run Stage D gate + commit if violations decrease. If
violations INCREASE (cascade), revert and document.

---

## §8. Targeted Test Coverage

For each bug, a regression test that:
1. Constructs the offending tree state.
2. Performs the triggering operation.
3. Asserts no violation.

For B1: small synthetic SpanNode where addEntryWithPDep would naturally produce
`siblingBitValues[0] == 1 && splitChildIndex > 0`. After fix, addEntryWithPDep
returns null AND caller's fallback succeeds with valid layout.

For B4: synthetic 3-level tree where adding a new disc bit at an absolute position
< grandparent.MSB would otherwise break I11. After fix, expandedParent is rejected
and `splitParentAndRecurse` runs.

These tests live in `HOTInvariantValidatorChecksTest` (Stage C) or a new
`HOTHappyPathFixesTest` if Stage G needs more space.

---

## §9. Definition of Done

The campaign reaches 0 violations on the strict-Binna 50K reproducer when:

1. `diagnosticMicrobenchPatternReproducer` (default mode, `-Dhot.strict.binna=true`)
   prints `violations=0`.
2. `casIndexHundredKEntryHeightBound` (default mode) still prints `violations=0`.
3. Stage D gate run with `-Dhot.strict.validate=true` prints
   `first-failure: {}`.
4. `precondHitsTotal` is either 0 or marked as accepted-known-limitation per Bug B5.

After that, Stage H validates end-to-end performance and removes the
intermediate-BiNode workaround per pending task #21.
