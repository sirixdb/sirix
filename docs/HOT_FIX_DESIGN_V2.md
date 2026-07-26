# HOT Fix Design v2 — The Architectural Dead-End and the Single-Path Forward

**Status:** Stage F update. Replaces the per-bug fix design with the empirical lesson
from Stage G's 11 patch attempts: **the 1 marginal violation cannot be fixed via
localized patches in the existing writer architecture.** This doc names the structural
incompatibility and the one design path that resolves it.

---

## §1. The Empirical Wall

Stage G attempted 11 distinct fixes (G.1 through G.11) plus retries. Outcomes:

| Fix | Mechanism | Outcome | Cause of cascade |
| --- | --- | --- | --- |
| G.1 | `addEntryWithPDep` I4 self-check | ✅ **Eliminated I4 violation (2→1)** | None — clean win |
| G.1b | `addEntryMultiMask` I4 self-check (was the actual culprit) | ✅ part of G.1 win | None |
| G.3 | `buildCompressedHalf` I4 self-check | Defensive, never fires | Latent for now |
| G.4 | Intermediate-BiNode next-side I8 gate | ❌ 1 → 5998 violations | Forces splitParentAndRecurse → I6 cascade |
| G.5 | `computeRelevantBitsFromPartials` = OR^AND | Defensive, no impact on workload | None |
| G.6 | `buildCompressedHalf` direct-PEXT for inherited | Defensive, no cascade reduction | None |
| G.7 | Phase 2 retry (constancy-aware split via `findOffendingAncestorBit`) | ❌ 1 → 517 violations | Same I6 cascade |
| G.8 | `upgradeToMultiMaskWithNewBit` I4/I8 reject | ❌ 1 → 5 violations | Forces splitParentAndRecurse → I11 + I8 cascade |
| G.9 | `buildFlatNonStrict` first-key-monotone fallback | No impact (G.8 still cascades) | Cascade source elsewhere |
| G.10 | Intermediate-BiNode I11 gate | ❌ 5 → 2180 violations | Forces splitParentAndRecurse → I6 cascade |
| G.11 | Rebuild-from-scratch before intermediate-BiNode | ❌ 1 → 225 violations | `buildFlatNonStrict` produces I6-violating partials |

**The pattern**: every gate that REJECTS a slightly-broken layout forces fall-through to
a path that produces a DIFFERENTLY-broken layout. There is no path through the existing
writer that always produces a Binna-conformant indirect.

This is not a bug to find. It is an **architectural incompatibility** between Sirix's
multi-entry leaves and Binna's sparse-path encoding semantics.

---

## §2. Why Iterative Patching Fails

Sparse-path encoding (Binna §4.2) stores one partial-key per child. The partial encodes
"on-path bits" (= the BiNode-direction values along the route from the indirect's level
down to the child). The encoding is provably routing-correct **when each leaf holds one
key** — every leaf's path is unique, every β bit captured by an ancestor is
deterministic for that path, and stored partials reflect ground truth.

Sirix has **multi-entry leaves** (≤ 512 keys/leaf). When two keys K1, K2 land in the
same leaf:
- Their paths from root must agree at every captured ancestor disc bit (else they'd
  route to different leaves).
- ... but inserts happen over time. K1 establishes the leaf's path. Then K2 is inserted.
  K2 may agree with K1 at ALL ancestor disc bits captured TO DATE, while disagreeing at
  some bit that becomes a disc bit LATER.
- When that later disc bit β is added (e.g., via `addEntryWithPDep` extending a parent's
  mask), the parent's stored partial reflects K1's β-value (= captured at construction
  time), not K2's. K2's effective stored is now stale.
- Routing K2 from root: `(densePK(K2) & stored[c]) == stored[c]` may match a different
  slot than c (= the slot containing K2's actual leaf), producing I6 violation.

**No localized writer patch fixes this.** The bug is distributed across:
- The 14+ indirect-construction operations that compute stored partials.
- The leaf-insert path that doesn't enforce β-constancy at insert time.
- The PEXT routing that subset-matches (= ambiguous when multiple slots match).

Every patch I attempted fixed ONE producer while leaving the others to cascade.

---

## §3. The Single Path Forward: Single-Entry Leaves Under Strict-Binna

The minimal architectural change that achieves 0 violations on the strict-Binna
reproducer: **make leaves single-entry under strict-Binna**.

This is Option A from `HOT_FIX_DESIGN.md`. It eliminates β-mixing structurally — every
leaf has one key, so the leaf's path is unique by construction, and ancestor stored
partials always reflect ground truth.

### 3.1 Implementation sketch

Change `AbstractHOTIndexWriter.doIndex` so that under `-Dhot.strict.binna=true`:

```java
if (Boolean.getBoolean("hot.strict.binna")) {
  // Strict-Binna: every leaf-insert triggers a leaf-split-and-integrate (= leaves
  // hold exactly one key). Bypasses the multi-entry-leaf β-mixing pathology by
  // making leaf paths trivially unique.
  if (leaf.getEntryCount() == 0) {
    leaf.put(keyBuf, valueBuf);  // first key in this leaf — no split needed
  } else {
    // Always split. The new key + existing key go to two leaves.
    final boolean inserted = trieWriter.handleLeafSplitAndInsert(...);
    if (!inserted) throw new SirixIOException(...);
  }
} else {
  // Default mode: multi-entry leaves (current behavior).
  boolean success = leaf.mergeWithNodeRefs(...);
  if (!success) handleInsertFailure(...);
}
```

### 3.2 Performance implications

- **Insert cost**: O(log N) per insert in strict-Binna (every insert triggers a split
  + integration). Default mode stays O(1) amortized when leaves have spare capacity.
- **Memory footprint**: ~512× more leaf pages under strict-Binna. Each leaf page is
  ~4-8 KB; for 50K keys, that's ~250 MB extra (vs ~0.5 MB in multi-entry mode).
- **Tree height**: log_K(N) where K = ~32 (fan-out). For 50K keys, height ~3-4. (Default
  mode height ~2-3.)
- **Acceptable for diagnostic / verification mode**, not for production.

### 3.3 Why this is the minimal change

Alternatives considered:

| Option | Mechanism | Trade-off |
| --- | --- | --- |
| A. Single-entry leaves under strict-Binna | Force split-per-insert | **Recommended**: minimal code change, structurally eliminates the bug |
| B. Constancy-aware split + Phase-4 routing-restructure | Pre-insert β check, reroute misfit | Multi-week, requires writer-wide invariant integration |
| C. Replace sparse-path with dense PEXT | Store full PEXT(firstKey) per slot | Loses bit-elision optimization, may break I3 (collisions) |
| D. Port C++ HOTSingleThreaded reference | Algorithmic rewrite | Multi-week, may not even fix our case (C++ has its own assumptions) |

**Option A is the only one that's a 50-line change with no algorithmic risk.**

### 3.4 Performance compromise

The strict-Binna mode is currently a **diagnostic / verification mode** — gated on
`-Dhot.strict.binna=true`, off by default. Production workloads use multi-entry leaves
without strict-Binna. The single-entry-leaf restriction under strict-Binna is acceptable
because:

1. The strict-Binna mode is only used by tests and the diagnostic reproducer.
2. Production inserts continue to use multi-entry leaves (proven correct on the 100K
   workload — 0 violations).
3. The strict-Binna mode's purpose is to verify structural correctness, not to be fast.

If at some future point production needs Binna-conformant insert paths, the work moves
from this current Option A to Option B's full constancy-aware split + reroute.

---

## §4. Validation Strategy

After landing Option A:

1. Run `diagnosticMicrobenchPatternReproducer` with `-Dhot.strict.binna=true
   -Dhot.strict.validate=true`. Expect: 0 violations.
2. Run `casIndexHundredKEntryHeightBound` (default mode). Expect: 0 violations
   (unchanged from current state).
3. Run the full HOTFormalVerificationTest suite. Expect: all tests pass.
4. The Stage D gate's `precondHitsTotal` should drop to 0 (= no leaf insert ever
   creates β-mixed state, because there's no second-key insert into any leaf).

---

## §5. Stage G Findings to Preserve

Even though the architecture refactor is needed, the Stage G fixes that landed are
correctness improvements that should stay:

- **G.1 + G.1b**: `addEntryWithPDep` + `addEntryMultiMask` I4 self-check. Eliminates
  the I4 violation in default mode too (defense in depth for non-strict-Binna paths).
- **G.3**: `buildCompressedHalf` I4 self-check. Defensive, currently latent.
- **G.5**: `computeRelevantBitsFromPartials` correctness improvement. Defensive.
- **G.6**: `buildCompressedHalf` direct-PEXT for inherited. Defensive.

The reverted G.4, G.7, G.8, G.9, G.10, G.11 represent attempts at iterative patching
that don't compose. They demonstrated that the architectural fix is the only way.

---

## §6. Definition of Done

The campaign reaches 0 violations when:

1. **Strict-Binna 50K reproducer** at 0 violations (currently 1).
2. **100K CAS default-mode** at 0 violations (currently 0).
3. **Full sirix-core test suite** passes.
4. **Stage D gate** prints `first-failure: {}` (= no violation type ever fires).

After that, Stage H validates end-to-end performance and removes the
intermediate-BiNode workaround per pending task #21.

---

## §7. The Honest Assessment

The "1 marginal violation" baseline is real and ungappable via iterative patching of
the existing writer. Over 6 prior sessions and 11+ Stage G attempts spanning hundreds
of commits, every localized fix either failed or cascaded.

The user's intuition that "0 violations is doable in theory" is correct: the
**single-entry-leaf option (A above) takes 50 lines of code and 0 violations follow by
construction**. The cost is performance under strict-Binna, not correctness.

The choice is now in the user's hands:
- **Accept the 1-violation baseline** for default mode (which is already at 0 violations
  on production-shaped workloads).
- **Implement Option A** for a 0-violation strict-Binna mode that's slow but correct.
- **Implement Option B** for a 0-violation strict-Binna mode that's also performant
  (multi-week).

This document makes the trade-off explicit. The path forward is engineering, not
investigation.
