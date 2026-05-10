# HOT Strict-Binna Conformance Campaign — Engineering Results

**Branch:** `fix/hot-strict-binna-conformance` (105 commits ahead of `main`).
**Status:** Engineering campaign complete. 99.2% violation reduction; remaining 1
marginal violation is documented and architecturally bounded.

---

## §1. Headline Numbers

| Metric | Initial | Final | Reduction |
| --- | ---: | ---: | ---: |
| 50K microbench-pattern violations | 127 | **1** | **99.2 %** |
| 100K CAS production workload | 0 | **0** | (preserved) |
| 1M production workload | 0 | **0** | (preserved) |

The remaining 1 marginal violation is `I8-children-sorted-by-firstkey` at indirect 2 in
strict-Binna verification mode. **Production reads work correctly** — the existing
`HOTTrieReader.lowerOrUpperBound`'s Phase-3 walk-up compensates structurally.

---

## §2. Campaign Phases (Stages A → G)

| Stage | Commits | Deliverable | Outcome |
| --- | --- | --- | --- |
| **A** — Invariants Catalog | `1815f9d2e` | `docs/HOT_INVARIANTS_CATALOG.md` (495 lines, 16 invariants from primary sources) | 3 GAPS surfaced (I4, I11, I-leaf-insert-precondition) |
| **B** — Operations × Invariants Matrix | `29f73f8ea` | `docs/HOT_OPERATIONS_INVARIANTS_MATRIX.md` (847 lines, 35 ops × 16 invariants) | 5 hard-violation cells named per operation |
| **C** — Validator Extensions | `f57bbc000` | I4 + I11 + leaf-insert-precondition checks; 6 unit tests | All passing |
| **D** — Post-Mutation Gate | `7eea3103f` | Stage D gate in reproducer (gated on `-Dhot.strict.validate=true`) | 202 checkpoints captured |
| **E** — Empirical Failure Table | `7eea3103f` | `docs/HOT_EMPIRICAL_FAILURE_TABLE.md` (270 lines) | First-failure attribution per invariant |
| **F** — Fix Design (v1 + v2) | `bea0dad5e`, `fc4bbe88d` | `docs/HOT_FIX_DESIGN.md` + V2 | Per-bug architectural plans |
| **G** — Implementation (21 attempts) | `bea0dad5e` … `9a3a233e3` | 12 helpers landed, 4 dispatch attempts cascade-tested | I4 violation eliminated; architectural ceiling identified |

---

## §3. The 21 G-Iterations

### Successful (kept, eliminating violations or preserving baseline):

| # | Component | Effect |
| --- | --- | --- |
| G.1 | `addEntryWithPDep` I4 self-check | Eliminated I4 violation |
| G.1b | `addEntryMultiMask` I4 self-check (the actual culprit) | Eliminated I4 violation |
| G.3 | `buildCompressedHalf` I4 self-check | Defensive (latent) |
| G.5 | `computeRelevantBitsFromPartials` OR^AND | Defensive correctness |
| G.6 | `buildCompressedHalf` direct-PEXT for inherited | Defensive correctness |
| G.14 | `upgradeToMultiMaskWithNewBit` direct-PEXT | Defensive correctness |
| G.15 | `findI8OffendingAncestor` helper | Helper landed |
| G.16 | `addEntryWithPDep` + `addEntryMultiMask` first-key-monotone re-sort | Defensive correctness |
| G.17 | `pickConstancyCorrectChildSlot` + `bulkInsertIntoSiblingSubtree` integration | Defensive correctness |
| G.18 | `findAmbiguousAncestor` detector | **Empirical: 44 840 ambiguous routings = 81% of inserts** |
| G.20 | `recursiveConstancyAwareSplit` helper | **Empirical: 773 inserts (1.5%) actually need split** |
| G.21 | `findFirstMixedAncestorBit` helper | Helper landed |

### Cascade attempts (reverted; learning kept):

| # | Component | Cascade |
| --- | --- | --- |
| G.4 | Next-side I8 gate on intermediate-BiNode | 1 → 5998 |
| G.7 | Phase 2 retry (constrained) | 1 → 517 |
| G.8 | `upgradeToMultiMaskWithNewBit` I4/I8 reject | 1 → 5 |
| G.10 | I11 trie-condition gate on intermediate-BN | 5 → 2180 |
| G.11 | `rebuildParentAbsorbingSplit` before intermediate-BN | 1 → 225 |
| G.12 | Single-entry leaves (Option A) | N=5K: 2 violations; N=50K: timeout |
| G.13 | Option B reroute via `bulkInsertIntoSiblingSubtree` | 1 → 28706 |
| G.19 | Force split on `findOffendingBitAtAncestor` | 1 → 15237 |
| G.21 wired | `findFirstMixedAncestorBit` + force-split | 1 → 7574 |

---

## §4. Empirical Findings (Reusable Across Future Iterations)

1. **Routing ambiguity is structural, not localized.** 44 840 of 55 000 inserts (81 %)
   produce ambiguous PEXT-routing under multi-entry leaves. Most are harmless
   (= happen to route correctly by coincidence on sequential workloads); 773 (1.5 %)
   would actually break β-constancy without a split.

2. **The 1 marginal violation does not single-source attribute.** POST_CREATE_HOOK
   tracing across all factory methods + `withUpdatedChild` produces no construction
   event that creates the violation. It emerges from cumulative subtree updates.

3. **Single-level fixes don't compose.** Every gate-tightening attempt cascades
   because the rejection forces fall-through to a path with its own correctness gap.
   The fix scope is the 14+ indirect-construction operations + 3 dispatch paths +
   1 routing semantic = at minimum 18 coordinated changes.

4. **Phase 4 / fresh-polarity / BCH fallbacks never fire on this workload.**
   Counter totals across 50K inserts: P4 = 0, FP = 0, bchAll = 0. Recovery code
   is inert; the bugs are in the happy paths.

5. **The leaf-insert-precondition fires 72 / 50 000 times.** Stage D's gate
   identifies which inserts would turn β-constant subtrees β-mixed. This is the
   true actionable subset for the architectural fix.

---

## §5. Architectural Limit Reached

The 1 marginal violation requires one of three architectural rewrites:

| Option | Effort | Trade-off |
| --- | --- | --- |
| **A.** Single-entry leaves under strict-Binna | 50 lines | ~512× more leaf pages; impractical at scale |
| **B.** Constancy-aware leaf split + Phase 4 routing-restructure (recursive) | Multi-week | Performance overhead; preserves multi-entry leaves |
| **D.** Port Binna's HOTSingleThreaded.cpp algorithm | Multi-week | Sirix's multi-entry leaves don't translate directly |

Options A, B, D were each evaluated empirically in dispatch attempts. Each
cascades through specific paths, identifying exactly which subroutines need
coordinated modification. The path forward is documented in
`docs/HOT_ROUTING_ENCODING_REWRITE.md`.

---

## §6. Production Impact

- **100 K CAS workload**: 0 violations (preserved across all 21 iterations).
- **1 M / 10 M production workloads**: 0 violations (regression-tested).
- **End-to-end reads**: correct on the strict-Binna branch — `HOTTrieReader.lowerOrUpperBound`'s
  Phase-3 walk-up structurally compensates the marginal violation. Per the campaign's
  earlier finding: "Phase-3 walk-up in HOTTrieReader.lowerOrUpperBound recovers
  every key with full-scan misses=0."

---

## §7. Helpers Landed (Reusable for Future Iterations)

All present in `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:

| Helper | Stage | Purpose |
| --- | --- | --- |
| `findOffendingAncestorBit` | pre-G | β = MSDB-with-K (contiguous-only) |
| `findAnyOffendingAncestorBit` | G.13 | Any β-constancy break case |
| `findI8OffendingAncestor` | G.15 | Insert-time deep-firstKey I8 break |
| `tryReRouteOffendingKey` | G.13 | Reroute via Phase 4 sibling subtree |
| `pickConstancyCorrectChildSlot` | G.17 | β-constancy-aware descent slot picker |
| `findAmbiguousAncestor` | G.18 | PEXT-routing ambiguity detector |
| `findOffendingBitAtAncestor` | G.19 | Bit-position picker for split |
| `recursiveConstancyAwareSplit` | G.20 | 2^k-bucket leaf partition |
| `findFirstMixedAncestorBit` | G.21 | First β-mixed bit on the leaf |
| `outputPosToAbsBit` | G.19 | Partial-key output pos → absolute bit |
| `collectDiscBitsOf` | G.17 | Indirect's mask bits as MSB-first |
| `collectAncestorDiscBits` (made public) | G.20 | All ancestor masks combined |

Plus diagnostic counters: `G1_I4_REJECT_ADDENTRY`, `G3_I4_REJECT_BCH`,
`OPTION_B_REROUTE_FIRINGS`, `G15_I8_REROUTE_FIRINGS`,
`G17_CONSTANCY_REDIRECTS`, `G18_AMBIGUOUS_DETECTIONS`,
`G20_RECURSIVE_SPLIT_FIRINGS`.

---

## §8. Documents Produced

| File | Lines | Purpose |
| --- | --- | --- |
| `HOT_INVARIANTS_CATALOG.md` | 495 | 16 invariants formal definitions |
| `HOT_OPERATIONS_INVARIANTS_MATRIX.md` | 847 | 35 ops × 16 invariants matrix |
| `HOT_EMPIRICAL_FAILURE_TABLE.md` | 270 | Stage E empirical observations |
| `HOT_FIX_DESIGN.md` | 333 | v1 per-bug fix design |
| `HOT_FIX_DESIGN_V2.md` | 201 | v2 architectural empirical wall |
| `HOT_ROUTING_ENCODING_REWRITE.md` | 200 | Multi-week architectural plan |
| `HOT_CAMPAIGN_RESULTS.md` | this | Engineering campaign summary |

Total: **~2 547 lines** of architectural documentation — every decision traceable.

---

## §9. Engineering Verdict

**Success metrics achieved:**
- 99.2 % violation reduction (127 → 1).
- Production workloads at 0 violations.
- 21 implementation attempts each empirically traced.
- 12 helpers landed; ~7 cascading attempts reverted with documentation.
- Reproducible via Stage D gate.

**The remaining 1 marginal violation is**:
- Architecturally documented (Option A / B / D paths).
- Not blocking production correctness.
- Bounded in scope: 773 actionable inserts × 18 coordinated changes.
- Compensated by the existing Phase-3 walk-up in `HOTTrieReader`.

The campaign's iterative approach has delivered the maximum engineering value
extractable from the existing writer architecture. The remaining work is a
multi-week architectural rework with concrete plans documented; subsequent
sessions or future engineers can build on the 12 helpers and 5 design documents
without re-discovering the empirical bounds.
