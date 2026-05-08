# HOT Strict-Binna Conformance — Multi-Session Engineering Project

**Status**: scoped engineering project; multi-session work plan.
**Branch**: `fix/hot-strict-binna-conformance` (off `main` after PR #973 merge).
**Audience**: implementer in any future session — read this first.

---

## TL;DR

Sirix's HOT diverges from Binna's reference in one architectural choice: **multi-entry leaves** (≤ 512 keys/leaf) vs Binna's **single-key leaves** (1 key/leaf). Every other commitment is faithful: M-ary fan-out, PEXT routing, sparse-path encoding, CoW. The single divergence creates a problem state (a leaf containing keys that span an ancestor's potential disc bits) that has *no analog in Binna's algorithm* — making this a multi-week engineering project, not a session-scale patch.

This document scopes that surgery as a phased project. **Each phase is contained, testable, and PR-reviewable.** Phases must land in order; later phases depend on infrastructure earlier phases create.

## Live status (branch `fix/hot-strict-binna-conformance`)

Commits since `main` (`6a4176da3`):

| Commit | Phase | Status | Effect on 50K reproducer |
|---|---|---|---|
| `85cafac2e` | Phase 0 — diagnostic | ✅ landed | (instrumentation only) |
| `c868e669c` | Phase 4a workaround — intermediate-BiNode fallback | ✅ landed (gated) | 127 → 1 violation, height 7, 53 fallback firings |
| `c282b1848` | Phase 1 — `collectAncestorDiscBits` helper | ✅ landed | (helper only) |
| `780453c2e` | Phase 2 measurement — fallback firing counter | ✅ landed | (measurement only) |
| `0975c8a72` | Phase 2 — leaf-side helpers (`splitToWithInsertOnBit`) | ✅ landed | (helpers only) |
| `48da0482b` | Phase 3 — sibling rebalance for Case 2b-ii | ✅ landed (gated) | 1 violation, height 6, 48 fallback firings (17 Phase 3 firings replace 5) |
| (in flight) | Phase 4 — β-already-in-mask subtree merge (SingleMask + MultiMask) | 🟡 in progress | target: 0 violations, height ≤ 4, 0 fallback firings |
| (deferred) | Phase 4b — `splitParentAndRecurse` correctness + cross-window | ⏸ pending | needed for general-workload soundness |
| (deferred) | Phase 5 — perf validation + default-on flip | ⏸ pending | |
| (deferred) | Phase 6 — flag removal | ⏸ pending | |

## Phase 2 lessons learned (after 3 failed variants)

The original plan's Phase 2 — "constancy-aware leaf split: pick split bit from ancestor disc bits" — does **not work**. Three variants tested:

- **Variant A** (most-significant ancestor bit non-constant in leaf as split bit): catastrophic — 5,652 violations, height 9. Root cause: non-MSDB split breaks contiguous-partition invariants. The leaf's keys aren't sorted by an arbitrary middle bit; only MSDB-based splits keep partitions contiguous.
- **Variant B** (only use ancestor bit when it equals MSDB): behavioral no-op. Constraint too strict.
- **Variant C** (remove `c868e669c` fallback, force `splitParentAndRecurse`): 27,954 violations from `buildCompressedHalf`'s 4 sparse-path fallback bugs (Phase 4b territory).

**Conclusion**: leaf splits MUST be on MSDB. The fix lives at the parent-integration level, not the leaf level. This shifted the work from leaf-level (Phase 2 as originally scoped) to parent-level (Phase 3 sibling rebalance + Phase 4 subtree merge).

## Updated case taxonomy of `addEntryWithPDep` rejections

When `addEntryWithPDep` (or `addEntryMultiMask`) returns null, classified by failure cause:

| Case | Cause | Status |
|---|---|---|
| 2b-i (success) | β not in mask, constant in siblings, parent not full | ✅ no fix needed |
| 2b-ii | β not in mask, sibling non-constant on β | ✅ Phase 3 (sibling rebalance) |
| 2b-iii | parent full | ⏸ Phase 4b (`splitParentAndRecurse` correctness) — bypasses addEntryWithPDep |
| 2b-iv | β already in mask (SingleMask) | 🟡 Phase 4 (subtree merge) |
| 2b-v | MultiMask parent rejection | 🟡 Phase 4 (MultiMask subtree merge) |
| 2b-vi | β cross-window | ⏸ Phase 4b (MultiMask upgrade) |

Empirical: on the 50K reproducer, the 48 remaining fallback firings are **2b-iv and 2b-v exclusively** (Phase 3 agent's `-Dhot.debug.phase3` diagnostic). 2b-iii and 2b-vi don't trigger on this workload but remain theoretically reachable on others.

---

## 1. Problem statement

`HOTInvariantValidator` flags 127 I-Binna constancy violations on the 50K microbench-pattern reproducer (`HOTFormalVerificationTest.diagnosticMicrobenchPatternReproducer`) and 159 on the 500K microbench. End-to-end reads remain correct (Phase-3 walk-up in `HOTTrieReader.lowerOrUpperBound` recovers every key with `full-scan misses=0`).

**Goal**: 0 violations on every workload, while preserving:
- multi-entry leaves (no single-key-leaf regression),
- M-ary fan-out (no binary-trie degradation),
- PEXT-based routing,
- CoW historical isolation,
- existing test suite (17 formal-verification + multi-version + CoW tests).

---

## 2. Diagnosis

100% of violations originate from `HOTTrieWriter.createNodeFromChildren` multi-child path (label `createNodeFromChildren-N`), called from `rebuildParentAbsorbingSplit` and from the 4 fallback cases inside `buildCompressedHalf`. These paths invoke `computeDiscBits(children, ...)` which performs adjacent-pair XOR scan of children's full first/last keys and captures every differing bit. **For multi-entry leaves this captures bits that are non-constant in older sibling subtrees** (because those subtrees were formed at earlier times when the bit didn't matter, and now contain keys spanning both bit values).

Binna's reference doesn't hit this because every leaf has 1 key — every bit is trivially constant. Sirix's leaves accumulate keys that span future disc bits.

Verified via stack-traced `createBiNodeTraced` instrumentation (still in the working tree; gated on `-Dhot.debug.constancy=true`).

---

## 3. The architectural fix (high-level)

**Insight**: leaves must be constant on every ancestor disc bit they will ever encounter. Two ways to achieve this:

- **(a) Eager**: when inserting a key K into leaf L, if K's value at any ancestor disc bit β differs from existing entries' bit-β values, split L on β before inserting. Maintains constancy continuously.
- **(b) Lazy**: when an ancestor adds a new disc bit β, mark all descendant leaves as potentially-non-constant on β. A background pass (or next-read fixup) splits them.

**(a) is preferred for correctness simplicity; (b) is preferred for write throughput.** The phased plan below implements (a) as the default path and (b) as a perf optimization.

Additionally, the rebuild paths (`rebuildParentAbsorbingSplit`, `buildCompressedHalf`'s 4 fallbacks) must not recompute disc bits via adjacent-pair scan — they must inherit a subset of the parent's mask, mirroring Binna's `compressEntries` / `compressEntriesAndAddOneEntryIntoNewNode`. This is independent of (a)/(b) and necessary regardless.

---

## 4. Phased work plan

Each phase ends with: clean compile, all existing tests green (in default-flag-off mode), measurable progress on the strict-Binna gate (`hot.strict.binna=true`).

### Phase 0 — Infrastructure (DONE — current branch state)

Already in place on the branch:

- `createBiNodeTraced` wrapping every `HOTIndirectPage.createBiNode` call site. Gated on `-Dhot.debug.constancy=true`.
- `probeConstancyOnBuild` — logs build-time constancy violations with stack traces.
- `discBitsAsAbsBitArray` — extracts absolute bit positions from a `DiscBitsInfo` for the probe.
- `buildFlatNonStrict` — extracted helper from `createNodeFromChildren` multi-child path. No behavior change.
- `diagnosticMicrobenchPatternReproducer` test — 0.8s reproducer at N=50K → 127 violations. Pinpoints the exact build paths.

These are diagnostic-only artifacts; default behavior unchanged. Keep them. They're the only way future sessions will know whether their changes are working.

### Phase 1 — Per-leaf ancestor-bit tracking

**Goal**: each leaf knows which absolute bit positions have been captured by some ancestor on its current path.

**Why**: Phase 2 needs this information to decide whether an insert would create a constancy violation.

**Two implementation options**:

- **(1a) On-demand**: walk the descent path and OR together each indirect's disc bits. No leaf metadata. Cheap to compute (path depth ≤ 8 levels), fits naturally into the existing descend.
- **(1b) Persisted per-leaf**: add a `long[] ancestorDiscBits` field to `HOTLeafPage`, maintained on insert and on parent restructuring. Storage cost; CoW correctness work.

(1a) is cheaper and likely sufficient. (1b) only needed if Phase 3's lazy rebalance requires it.

**Concretely (option 1a)**:

- Add a helper in `HOTTrieWriter` that takes `(HOTIndirectPage[] pathNodes, int pathDepth)` and returns the sorted union of absolute disc-bit positions across the path.
- Handle both `SINGLE_MASK` (one bit per `Long.bitCount(getBitMask())`) and `MULTI_MASK` (sum across `extractionMasks[]`) layouts.
- Verify with a same-package round-trip test: build a leaf with known keys, insert at known path, decode the helper's output, and confirm `DiscriminativeBitComputer.isBitSet(key, absBit) == leaf.getStoredPartial(...)` for every (key, absBit) pair.

**Test gates**:

- All existing tests still pass (default behavior unchanged).
- New test `phase1AncestorBitsRoundTrip` — bit-position decode matches `DiscriminativeBitComputer.isBitSet` semantics for synthetic disc-bit configurations.

**Not in this phase**: actually using this metadata to fix violations. That's Phase 2.

### Phase 2 — Constancy-aware leaf split (1-2 weeks)

**Goal**: when `splitToWithInsert` (HOTLeafPage's leaf-overflow handler) splits a leaf, choose the split bit such that both halves are constant on every ancestor disc bit.

**Why**: this ensures that after a leaf split, neither half can introduce a constancy violation at any ancestor.

**Concretely**:

- Modify `HOTLeafPage.splitToWithInsert(target, key, ...)`:
  - Read `ancestorDiscBits` (Phase 1's metadata).
  - For each bit β in `ancestorDiscBits`, check if the leaf's keys (including the new key) are non-constant on β.
  - If yes: split bit = β (the *most significant* such β, to minimize cascade depth).
  - If no: split bit = leaf's local MSDB (current behavior).
- The split bit is what determines which keys go to `target` (right half) vs stay in `this` (left half).

**Test gates**:

- All existing tests still pass.
- New test: `phase2LeafSplitConstancyPreserved` — insert N keys with adversarial bit-spans, verify every leaf is constant on every ancestor disc bit after every split.

**Not in this phase**: violations from rebuild paths. Those require Phase 4.

### Phase 3 — Lazy retroactive rebalance (1 week)

**Goal**: when an ancestor adds a new disc bit β (via `addEntryWithPDep` success), older descendant leaves may become non-constant on β. Detect and fix on next access.

**Why**: avoids cascading splits at every `addEntryWithPDep` call. Amortizes the rebalance cost.

**Concretely**:

- Add `ancestorBitsDirty: long` to `HOTLeafPage`: bits that are claimed-as-disc-bits by an ancestor but not yet verified constant.
- When `addEntryWithPDep` succeeds at adding β, mark all descendants' `ancestorBitsDirty |= (1L << bitIdx)`. (Cheap — just visit the parent's children, not full descend.)
- On read: if `ancestorBitsDirty != 0`, the read path triggers a leaf-fix that splits the leaf on the dirty bits.
- On insert: same as read.

**Test gates**:

- Multi-version + CoW tests still pass (Phase 3's lazy fix must respect snapshot isolation).
- New test: `phase3LazyRebalanceConvergence` — multi-revision workload that triggers `addEntryWithPDep` β-additions, verify all leaves eventually become constant.

### Phase 4 — Binna-faithful rebuild paths (2 weeks)

**Goal**: replace `rebuildParentAbsorbingSplit` and `buildCompressedHalf`'s 4 fallback cases with proper Binna inheritance. The disc-bit set is NEVER recomputed from full keys; it's always inherited as a subset of the parent's set.

**Why**: this is the proximate cause of the 127 violations. With Phases 1-3 establishing the leaf-level invariant, this phase ensures the rebuild paths preserve it.

**Concretely**:

- 4a — Replace `rebuildParentAbsorbingSplit`:
  - Port Binna's `compressEntriesAndAddOneEntryIntoNewNode` from `HOTSingleThreadedNode.hpp` lines 511-545.
  - Algorithm: take parent's mask, compute relevant bits via `getRelevantBitsForRange(0, numChildren)` over parent's existing partials, extract sub-mask, add the new disc bit, reposition partials via `Long.compress` + `Long.expand`.
  - Reference at `/home/johannes/IdeaProjects/hot-reference/libs/hot/single-threaded/include/hot/singlethreaded/HOTSingleThreadedNode.hpp`.

- 4b — Implement MultiMask inheritance for `buildCompressedHalf`:
  - The 4 fallback cases (MultiMask parent, identical keys, cross-window split bit, newMask==0) currently delegate to `createNodeFromChildren-N`. Replace each with a MultiMask-aware variant of the SingleMask inheritance code already in `buildCompressedHalf`.
  - The C++ template handles both layouts uniformly; port that abstraction.

- 4c — Audit `splitParentAndRecurse` sparse-path computation:
  - Attempt #5 revealed sparse-path violations (27,954 on the reproducer) when `splitParentAndRecurse` handles all the load. Trace through the partial-key recomputation in `buildCompressedHalf`'s `addHighBitToMasksInRightHalf` and verify `(stored & ~dense) == 0` holds end-to-end.

- 4d — Hard-gate `createNodeFromChildren-N`:
  - Once 4a and 4b land, the multi-child path of `createNodeFromChildren` should be unreachable from any rebuild context. Add an assertion + thread-local context flag to verify this.
  - Keep the path itself for non-rebuild contexts (e.g., root creation from scratch).

**Test gates**:

- `diagnosticMicrobenchPatternReproducer` with `hot.strict.binna=true`: 0 violations (down from 127).
- `casIndexHundredKEntryHeightBound` with strict-Binna ON: 0 violations.
- `casIndexBinnaConformanceSweep` (20 configs) with strict-Binna ON: 0 violations.
- All multi-version + CoW tests with strict-Binna ON: green.

### Phase 5 — Performance validation + flag flip (1 week)

**Goal**: verify strict-Binna doesn't regress write throughput by more than 10% on the 500K microbench. Flip the default once validated.

**Concretely**:

- Run `smallCombinedMicrobench` (500K writes + 1M reads) in both modes.
- Profile any regression with the existing `ASYNC_PROFILER` wiring (per CLAUDE.md).
- If write throughput regresses > 10%, optimize Phase 2's eager split (skip when not needed) and Phase 3's lazy rebalance (batch where possible).
- Flip `hot.strict.binna` default to `true`. Leave as escape hatch for one release.

### Phase 6 — Cleanup (0.5 week)

- Remove the `hot.strict.binna` flag (always-on).
- Remove the legacy `rebuildParentAbsorbingSplit` and the 4 fallback paths in `buildCompressedHalf`.
- Remove the diagnostic instrumentation (`createBiNodeTraced`, `probeConstancyOnBuild`) once validated stable for one release.

---

## 5. Effort estimates

I'm not going to pretend I can estimate this. The 6 in-session attempts all hit walls in unexpected places; that pattern will likely continue. The phases below are sequenced by dependency, not duration.

**One alternative ordering worth considering — Phase 4 first**:

The 127 visible violations all originate from the rebuild paths (`createNodeFromChildren-N`). Phases 1-3 establish leaf-level invariants that Phase 4 *can* exploit, but they may not be *necessary* if Phase 4's Binna-faithful inheritance suffices on its own. Concretely: try implementing Phase 4a (replace `rebuildParentAbsorbingSplit` with `compressEntriesAndAddOneEntryIntoNewNode`) first as a single-session experiment. If violations drop substantially, Phases 1-3 may be unnecessary or much smaller in scope. If they don't drop (or new violations surface), the failure tells you which leaf-level invariants are missing — better-informed Phases 1-3 follow.

The risk of Phase 4-first: it may surface the same walls the 6 in-session attempts hit (sparse-path collisions, MultiMask fallbacks, cascading constancy). The benefit: it's the shortest path to a yes/no answer about whether leaf-level changes are required.

**Recommended approach**: start with Phase 4a in isolation in the next session. If it makes meaningful progress, continue with 4b/4c/4d. If it hits the same walls, fall back to Phase 1→2→3→4 ordering with the diagnostic surfaced from the Phase 4a attempt.

---

## 6. Failed in-session attempts (lessons for future sessions)

| Attempt | Approach | Why it failed |
|---|---|---|
| 1 | Constancy filter on `computeDiscBits` | Filter rejects bits needed for I3 — partials collide everywhere → 99K violations on 100K |
| 2 | Active rebalance + wrap-indirect mid-rebuild | PageReference aliasing across old/new tree → 268 regressions in multi-version tests |
| 3 | Leaf-bit-split + walk-up via `updateParentForSplitWithPath` | α-vs-β gap: walk-up's `computeDifferingBit` returns whatever, not β; conservative gate fires for everything → 127 → 127 |
| 4 | Group-recurse + active rebalance fallback | Wall 1 (cascading splits explode child count > 32 cap) + Wall 2 (sparse-path collisions under reduced masks) |
| 5 | Disable `rebuildParentAbsorbingSplit`, force `splitParentAndRecurse` | Reveals latent sparse-path bugs in `buildCompressedHalf`'s 4 fallback paths → 27,954 violations |
| 6 | Binna inheritance for `compressEntriesAndAddOneEntryIntoNewNode` (5 sub-attempts) | All sub-attempts produced more violations than baseline. Root cause: leaves themselves contain bit-conflicting keys; no indirect-construction algorithm alone can fix that. |

**The unifying lesson**: the problem state ("a leaf containing bit-conflicting keys") has no Binna analog. No reference-based algorithm at the indirect level fixes it. The fix must be at the leaf level — Phases 1-3.

---

## 7. References

- Robert Binna et al., *HOT: A Height Optimized Trie Index for Main-Memory Database Systems* (SIGMOD 2018).
- C++ reference: `https://github.com/speedskater/hot` (locally cloned at `/home/johannes/IdeaProjects/hot-reference/`).
  - `HOTSingleThreadedNode.hpp` lines 488-547 (`compressEntries`, `compressEntriesAndAddOneEntryIntoNewNode`, `split`).
  - `HOTSingleThreaded.hpp` lines 493-547 (`integrateBiNodeIntoTree`).
  - `SparsePartialKeys.hpp` lines 175-200 (`getRelevantBitsForRange`).
  - `DiscriminativeBitsRepresentation.hpp` (`extract` template).
- Sirix HOT entry points: `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`.
- Validator: `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTInvariantValidator.java`.
- Reproducer: `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java#diagnosticMicrobenchPatternReproducer`.

---

## 8. Branch state at end of session 1 (this design phase)

What's actually delivered:

- **Diagnostic instrumentation in `HOTTrieWriter.java`** (~+130 lines): `createBiNodeTraced` wrapping every BiNode creation site, `probeConstancyOnBuild` (gated on `-Dhot.debug.constancy=true`), `discBitsAsAbsBitArray` helper, `buildFlatNonStrict` helper extracted from `createNodeFromChildren` (no behavior change). **Default behavior unchanged.**
- **Diagnostic test `diagnosticMicrobenchPatternReproducer`** (~+135 lines in `HOTFormalVerificationTest.java`): 0.8s reproducer at N=50K → 127 violations. Pinpoints the exact build paths via stack trace.
- **This design document**.
- All 17 formal verification + multi-version + CoW tests: green.
- Reproducer baseline: 127 violations (unchanged — diagnostic-only changes).
- No commits, no pushes.

**What is NOT delivered** (and was originally planned):

- No reduction in violation count. Phase 0 is diagnostic; phases 1-6 are not started.
- No Phase 1 helper code. Initially attempted but pulled — was untested and the bit-position math is exactly the kind of code that needs a round-trip test before landing.

**Honest assessment**: session 1 produced a usable diagnostic harness and a phased plan, but no functional progress on violations. The 6 in-session attempts at functional fixes all hit walls (documented in §6); the engineering pattern is that this needs a fresh start with Phase 4a in isolation, not incremental session-driven attempts on the existing branch.

**Next session should**:
1. Read this document end-to-end.
2. Verify baseline tests pass + reproducer shows 127 violations.
3. Decide between Phase 4-first (recommended, see §5) or Phase 1-first ordering.
4. Start with one focused phase. Don't chain phases in a single session — each phase deserves its own iteration cycle against the test gates.

**Branch hygiene**: before phase work begins, consider landing the diagnostic instrumentation as a small standalone PR onto `main` (so the reproducer becomes part of the regression suite). That clears the branch for actual phase work without dragging the diagnostic diff along.
