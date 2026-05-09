# HOT Phase 4b — Concrete Diagnosis & Fix Plan

**Date**: 2026-05-09. Branch: `fix/hot-strict-binna-conformance` (worktree `agent-af0791c89c23c37fe`).
**Companion to**: `HOT_STRICT_BINNA_DESIGN.md` (campaign overview); this doc focuses Phase 4b only.

## TL;DR

The strict-Binna campaign's 50K-microbench reproducer is **one feature-fix away from 0 violations**: porting the `compressEntries{,AndAddOneEntryIntoNewNode}` algorithm from C++ to handle a **MultiMask parent** in `HOTTrieWriter.buildCompressedHalf`. The other three named fallbacks (identical keys, cross-window split bit, newMask==0) and two edge-case bailouts (unknown child, partial-key collision) **do not fire on this workload**.

Signal is unambiguous because:

1. **Default state (no gate fix)**: 1 violation (I8-children-sorted-by-firstkey from intermediate-BiNode fallback). `bch-fallbacks: multimask-parent=0 identical-keys=0 cross-window=0 new-mask-zero=0 unknown-child=0 partial-collision=0`. `splitParentAndRecurse` never runs.
2. **With next-side I8 gate-tightening**: 5993 violations (I6-pext-routes-to-leaf). `bch-fallbacks: multimask-parent=1 identical-keys=0 cross-window=0 new-mask-zero=0 unknown-child=0 partial-collision=0`. ONE fallback fires, cascades into 5993 broken stored-key descents.

Diagnostic instrumentation: `BCH_FALLBACK_*` counters in `HOTTrieWriter.java` + `[bch.multimask]` parent-state print under `-Dhot.debug.bchfallback=true`.

## The exact firing site

When the I8 gate is tightened to also check the next-side neighbour (so the marginal I8 violation is rejected and the case routes to `splitParentAndRecurse`):

```
[bch.multimask] parent.numChildren=5 parent.height=1 halfChildren.length=5 isRightHalf=true
                msbPosition=81
                parent.extractionPositions=[10, 12]
                parent.extractionMasks=[401a000000000000]
```

- Parent is MultiMask with **2 extraction positions** (bytes 10 and 12) and 4 mask bits total.
- 5 children.
- `splitParentAndRecurse` partitions them by parent's MSB at absolute bit 81 (= byte 10, bit 1 MSB-first).
- The right-half (β=1 at bit 81) is built via `buildCompressedHalf` — falls back to `createNodeFromChildren` because parent is `MULTI_MASK`.
- That `createNodeFromChildren-N` build computes disc bits via the broken adjacent-pair XOR scan over full keys, producing an indirect whose disc bits are NOT constant within each child's subtree.
- The validator subsequently walks ~5993 stored keys whose descent passes through that broken indirect, and PEXT routing returns the wrong child → leaf == null.

## Why MultiMask exists for this parent

Parent's disc bits straddle bytes 10 and 12 — both within an 8-byte SingleMask window in principle. But the parent was upgraded to MULTI_MASK earlier (likely via `upgradeToMultiMaskWithNewBit` on a cross-window addEntry that has since been re-located by other restructurings). MultiMask is also the default once the disc-bit set spans non-contiguous byte regions or the mask bits exceed what a single 64-bit packed mask can carry.

## What the C++ reference does

`HOTSingleThreadedNode::compressEntries(firstIdx, num)` and `compressEntriesAndAddOneEntryIntoNewNode(...)` ([Binna§4.2](https://db.in.tum.de/~binna/), `HOTSingleThreadedNode.hpp` lines 488–537):

1. **Compute relevant bits** via `mPartialKeys.getRelevantBitsForRange(firstIdx, num)` — exactly the bits of `parent.mask` that *differ* among the contiguous range `[firstIdx, firstIdx+num)` of children's stored partials. (For a sorted partial sequence: OR of `(p[i] & ~p[i-1])` for adjacent pairs — proof: sorted-integer monotonicity ensures every differing bit flips 0→1 at some adjacent step.)
2. **Build a sub-representation** via the C++ template `extractAndExecuteWithCorrectMaskAndDiscriminativeBitsRepresentation(parent.mDiscriminativeBitsRepresentation, relevantBits, λ)`. The template selects the right concrete class for the new mask layout (SingleMask if compactable, MultiMask otherwise) and invokes λ with that representation + a maximumMask pseudo-type for the new partial-key width. The new representation's mask bits are the SUBSET of parent's that were marked relevant.
3. **(For `compressEntriesAndAddOneEntry...`)** `intermediateRep.insert(insertInformation.mKeyInformation, ...)` adds the new disc bit β (precomputed at the leaf-split site, threaded through `InsertInformation`) — never re-derived from boundary keys.
4. **Reposition existing partials** via `Long.compress(oldPartial, parent.mask)` then `Long.expand(compressed, newMask)` — equivalent to `_pext_u64` then `_pdep_u64`. Existing children's partials are projected through the subset and rewidened in the new layout.
5. **Compute new children's partials** for the leaf-split's two halves directly from their first keys under `newMask` (or under the selected layout).

The C++ template is the abstraction that lets the algorithm work uniformly over SingleMask and MultiMask. Sirix's Java port currently has only the SingleMask body inlined into `buildCompressedHalf` (lines 4031–4172).

## What Sirix is doing today

`buildCompressedHalf` at line 4006–4170+:

```
if (parent.layoutType != SINGLE_MASK)         → fallback to createNodeFromChildren-N    [THE BUG]
relevant = OR of (p[i] & ~p[i-1])             // SingleMask-only (int over Long.bitCount(parent.mask) bits)
if (bothSplitHere) {
  splitDiscBitAbs = computeDifferingBit(splitLeft.lastKey, splitRight.firstKey)
  if (splitDiscBitAbs < 0) → fallback         // identical-keys
  byteOff = (β/8) - parent.initialBytePos
  if (byteOff < 0 || byteOff >= 8) → fallback // cross-window
  splitBitMaskBit = 1L << ((7-byteOff)*8 + (7-bitInByte))
}
newMaskFromOld = bits of parent.mask selected by relevant (PEXT-style packing)
newMask = newMaskFromOld | splitBitMaskBit
if (newMask == 0) → fallback                  // newMask-zero
for each c in halfChildren:
  if c is splitLeft/Right → newPartial = computePartialKeySingleMask(c.firstKey, oldInitialBytePos, newMask)
  else → newPartial = Long.expand(Long.compress(p[pIdx], relevant), oldBitsInNewLayout)
sort, build SingleMask indirect
```

The body assumes `parent.bitMask` is a single 64-bit packed mask anchored at `parent.initialBytePos`. For MULTI_MASK that's `parent.extractionPositions[]` + `parent.extractionMasks[]` instead — a 1-D scattered representation.

## What needs to change (concrete fix shape)

### Step 4b.0 — Helpers (1–2 days)

Add three primitive helpers on `HOTIndirectPage` (or static helpers in `HOTTrieWriter`):

- `getRelevantBitsForRangeMultiMask(parentIndices: int[], parentPartials: int[]) → int relevant` — same XOR-adjacent-pair scan, but operating on the partials sequence regardless of parent's layout (the algorithm is layout-independent; the LAYOUT is in how those partial bits are *interpreted* against parent.mask).
- `extractMultiMaskSubset(parent: HOTIndirectPage, relevant: int) → MultiMaskLayout newLayout` — return new `extractionPositions[]` and `extractionMasks[]` containing only the parent's mask bits whose output positions are in `relevant`. Drop empty extraction positions.
- `compressPartialMultiMask(oldPartial: int, relevant: int) → int compressed` and `expandPartialMultiMask(compressed: int, oldBitsInNewLayout: long) → int repositioned` — these are the same `Long.compress`/`Long.expand` already used in `addEntryWithPDep`'s MultiMask sibling (`upgradeToMultiMaskWithNewBit`). Reuse rather than re-derive.

Test gate: round-trip — for a MultiMask `parent`, every existing child's repositioned partial in the new layout `Long.expand(Long.compress(p, relevant), oldBitsInNewLayout)` must equal `computePartialKeyMultiMask(child.firstKey, newLayout)`. Same identity must hold for SingleMask (already verified via existing `buildCompressedHalf` correctness on tests).

### Step 4b.1 — Generalize the `bothSplitHere` cross-window check (0.5 day)

Currently rejected when `byteOff < 0 || byteOff >= 8`. For MultiMask, the new bit can land at *any* byte position — the new layout absorbs it via an additional extraction position. Split this into:

- For SingleMask outcome (parent was SingleMask AND new bit fits in window): existing path.
- For MultiMask outcome: emit a MultiMask child indirect with extractionPositions including the new bit's byte position. Re-uses `extractMultiMaskSubset`'s output extended with the new bit.

This also kills the `BCH_FALLBACK_CROSS_WINDOW` case as a side effect (handled correctly under the new code).

### Step 4b.2 — Implement MultiMask-parent path (3–5 days, the load-bearing change)

Branch `buildCompressedHalf` on `parent.getLayoutType()`:

```
if (parent SingleMask) { existing inlined code }
else if (parent MultiMask) {
  relevant = getRelevantBitsForRangeMultiMask(parentIndices, parent.partialKeys)
  if (relevant == 0 && !bothSplitHere) → degenerate path (single child or same-partial collision, defer to caller)
  newLayout = extractMultiMaskSubset(parent, relevant)
  if (bothSplitHere) {
    splitDiscBitAbs = insertInformationIfThreaded ?? computeDifferingBit(splitLeft.lastKey, splitRight.firstKey)
    if (splitDiscBitAbs < 0) → fallback (identical keys — unchanged for now)
    newLayout = newLayout.with(splitDiscBitAbs)  // append extraction position+mask bit
  }
  for each c in halfChildren:
    if c is splitLeft/Right → newPartial = computePartialKeyMultiMask(c.firstKey, newLayout)
    else → newPartial = expandPartialMultiMask(compressPartialMultiMask(parent.partialKeys[pIdx], relevant), oldBitsInNewLayout)
  sort, build MultiMask indirect via HOTIndirectPage.createMultiMaskNode(...)
}
```

Test gates per design doc §4 (Phase 4): the 50K reproducer with `hot.strict.binna=true` AND the next-side I8 gate-tightening from above must yield **0 violations**. Multi-version + CoW tests still green.

### Step 4b.3 — Thread `insertInformation`-style β through the leaf-split chain (2–3 days)

Currently the new disc bit is recomputed via `DiscriminativeBitComputer.computeDifferingBit(splitLeft.lastKey, splitRight.firstKey)` at every consumer (`addEntryWithPDep`, `splitParentAndRecurse`, `buildCompressedHalf`). For non-degenerate splits this is fine, but on duplicates (identical keys) it returns -1 and we bail. The C++ reference precomputes β at the leaf-overflow site (`HOTLeafPage::splitToWithInsert`) and threads it through `InsertInformation`. Sirix should do the same.

Concrete: extend `splitToWithInsert`'s return signature to provide β explicitly (or a helper that captures it from the leaf's MSDB before duplicates erase the boundary distinction). Pass it through every consumer.

This kills the `BCH_FALLBACK_IDENTICAL_KEYS` case.

### Step 4b.4 — Tighten the I8 gate (after 4b.0–4b.3 land) (0.5 day)

Re-introduce the next-side check in `intermediateBiNodePreservesSlotOrder`. With the MultiMask path correct, the gate-rejected case routes to `splitParentAndRecurse` cleanly — 0 violations.

### Step 4b.5 — Remove `BCH_FALLBACK_NEW_MASK_ZERO` defensive bailout (0.5 day, optional)

Once 4b.2 + 4b.3 land, the `newMask == 0` case can only arise from genuine upstream INV violations (children with same partial). The path can be promoted to a hard assertion (under flag) or kept as a defensive bailout. Either is fine — currently fires 0 times.

### Step 4b.6 — Hard-gate `createNodeFromChildren-N` (Phase 4d, post-4b)

Per design doc §4.4: once 4a/4b land, multi-child `createNodeFromChildren` should be unreachable from any rebuild context. Add thread-local flag + assertion in `buildFlatNonStrict` so future regressions fail fast.

## Estimated effort

| Step | Work | Estimate |
|---|---|---|
| 4b.0 | Helpers + round-trip test | 1–2 days |
| 4b.1 | Cross-window MultiMask outcome | 0.5 day |
| 4b.2 | MultiMask-parent main path | 3–5 days |
| 4b.3 | Thread insertInformation-style β | 2–3 days |
| 4b.4 | Re-introduce I8 gate next-side check | 0.5 day |
| 4b.5 | newMask==0 cleanup | 0.5 day |
| 4b.6 | Hard-gate createNodeFromChildren-N | 1 day |
| Test/regression sweep | All phases | 1–2 days |
| **Total** | | **~2 weeks of focused work** |

This matches the design doc §4's "Phase 4 ≈ 2 weeks" estimate.

## Test gates per step

- **After 4b.0**: round-trip helper test green; existing tests unchanged.
- **After 4b.2**: `diagnosticMicrobenchPatternReproducer` with strict-Binna ON: bch-fallbacks all 0 OR `multimask-parent=0` while everything else may transiently still fall back. Violations may still be >0 (waiting on 4b.3/4b.4).
- **After 4b.4**: 50K reproducer at 0 violations, height ≤ 4, fallback firings = 0. Phase 4 target met.
- **After 4b.6**: `createNodeFromChildren-N` is unreachable in rebuild contexts (assertion never trips on full test suite).

## Why this won't be the only thing

After Phase 4b lands, the campaign needs Phases 5 (perf validation) and 6 (flag flip + cleanup). Those are smaller. But this file is scoped to 4b — the load-bearing piece.

The 44 SingleMask "no-sibling-slot" cases revealed by the previous session's `[hot.phase4]` diagnostic on the *current* (pre-fix) reproducer are NOT addressed by Phase 4b directly. They're cases that today reach `intermediateBiNodePreservesSlotOrder` and pass — i.e., the BiNode fallback accepts them. Whether they need re-treatment after 4b lands depends on whether tightening the gate removes them too. Likely yes (they're *additional* 2b-iv-b-α cases that the loose gate accepts; the tightened gate would route them to `splitParentAndRecurse` → `buildCompressedHalf` → MultiMask-parent path → handled correctly).

## What to verify next session

1. Read this doc + `HOT_STRICT_BINNA_DESIGN.md` end-to-end.
2. Run reproducer with default settings; confirm 1 I8 violation, bch-fallbacks all 0.
3. Re-introduce I8-gate next-side check; confirm bch-fallbacks: `multimask-parent=1, others=0`.
4. Read C++ reference at `/home/johannes/IdeaProjects/hot-reference/.../HOTSingleThreadedNode.hpp` lines 488–537 for the exact compress-entries algorithm + DiscriminativeBitsRepresentation.hpp's extract template.
5. Start step 4b.0 (helpers + round-trip test). Land per phase, run reproducer after each.
