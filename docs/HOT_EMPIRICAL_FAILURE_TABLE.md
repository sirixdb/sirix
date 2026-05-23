# HOT Empirical Failure Table (Stage E)

**Status:** Stage E deliverable. Empirical observation of Stage D's post-mutation validation
gate over the 50K microbench-pattern reproducer with `hot.strict.binna=true`.

**Source:** Run of `HOTFormalVerificationTest.diagnosticMicrobenchPatternReproducer` with
`-Dhot.strict.validate=true` on commit `f57bbc000` + Stage D embedded gate.

**What this stage answers:** the 5 open questions from `HOT_OPERATIONS_INVARIANTS_MATRIX.md`
§7, plus the Stage B §5.1 hard-violation cells empirically — flipping ⚠/? cells in the
master matrix to ✅ or ❌ based on observed evidence rather than code inspection.

---

## §1. Run Parameters

| Param | Value |
| --- | --- |
| Workload | Sirix CAS index, /x/[]/v path |
| Warmup phase | 5 000 inserts at value range [N+1M .. N+1M+5K) |
| Main phase | 50 000 inserts at value range [0 .. N) |
| Validate interval | every 250 inserts during main phase |
| Total checkpoints | 202 (1 post-warmup + 200 main + 1 post-commit) |
| Strict-Binna paths | enabled (`-Dhot.strict.binna=true`) |
| Total runtime | 41 s (vs 0.4 s without gate — 100× overhead) |

---

## §2. Headline Findings

### 2.1 First-failure attribution

The insert index where each violation type first appears:

| Invariant | First-failure insert # | Tree height at failure |
| --- | ---: | --- |
| I8-children-sorted-by-firstkey | **500** | 3 |
| I11-trie-condition | **500** | 3 |
| I-leaf-insert-precondition | **515** | 3 |
| I4-first-partial-zero | **1 750** | 3 |

### 2.2 Persistence of violations

Distribution of validator-state across the 202 checkpoints:

| State | Checkpoints | Description |
| --- | ---: | --- |
| `{}` (clean) | 2 | post-warmup + main+250 |
| `{I8=1, I11=1}` | 1 | first-failure (main+500) |
| `{I8=1}` only | 4 | I11 self-cleared between main+750 and main+1500 |
| `{I4=1, I8=1}` | **195** | the campaign's de-facto baseline |

---

## §3. Counter-Delta Attribution

The Stage D gate records cumulative writer-firing counters at each checkpoint. The DELTA
between adjacent checkpoints attributes violations to the operations that fired in that
window. Counters tracked:

| Counter | Tracks |
| --- | --- |
| BN | intermediate-BiNode fallback firings |
| P3 | Phase 3 rebalanceAndIntegrate firings |
| P4 | Phase 4 subtree-merge firings |
| FP | addEntryFreshPolarity{Single,Multi}Mask firings |
| bchAll | sum of all 6 BCH (buildCompressedHalf) fallback counters |

### 3.1 Counter trajectories

| Checkpoint | BN | P3 | P4 | FP | bchAll |
| --- | ---: | ---: | ---: | ---: | ---: |
| post-warmup | 4 | 0 | 0 | 0 | 0 |
| main+250 | 4 | 0 | 0 | 0 | 0 |
| main+500 (first violation) | 4 | 0 | 0 | 0 | 0 |
| main+750 | 5 | 0 | 0 | 0 | 0 |
| main+1750 (I4 appears) | 5 | 0 | 0 | 0 | 0 |
| main+5250 | 8 | 2 | 0 | 0 | 0 |
| post-commit | 48 | 17 | 0 | 0 | 0 |

### 3.2 The critical insight: most violations are NOT caused by recovery paths

**Observation**: Between `post-warmup` (BN=4) and `main+500` (BN=4, first violation),
**zero** new firings on any of the 5 recovery counters. Yet two violations (I8, I11)
appeared in the window [main+250, main+500].

**Conclusion**: I8 and I11 were caused by **happy-path** writer operations that don't
fire any of the existing recovery counters:
- `addEntryWithPDep` success path
- `splitParentAndRecurse` success path
- `buildCompressedHalf` SingleMask path with `bch-fallbacks=0`
- `withUpdatedChild` (slot pointer swap)
- `createBiNode` / `createSpanNode` / `createMultiNode` factories

Same observation for I4 first appearing at idx=1750 with deltas (BN=0, P3=0, P4=0,
FP=0, bch=0) since the previous checkpoint at idx=1500.

This OVERTURNS the working hypothesis from Stage C that "I4 is strict-Binna-paths-
specific". I4 is caused by happy-path code regardless of recovery-path firings.

### 3.3 Counter delta totals over the full run

| Counter | Cumulative at end | Implication |
| --- | ---: | --- |
| BN (intermediate-BiNode) | 48 | rare — fires once per ~1000 inserts |
| P3 (Phase 3 rebalance) | 17 | very rare — once per ~3000 inserts |
| P4 (Phase 4 subtree-merge) | 0 | **never fires** — confirmed Stage B observation |
| FP (fresh-polarity) | 0 | **never fires** |
| bchAll | 0 | **no BCH fallbacks** — every buildCompressedHalf finishes via the SingleMask main path |

Compare to total inserts (50 000) — recovery paths fire at most 65 times total across
the entire run. **The dominant code paths are the default ones.** Stage F's fix design
must therefore be focused on default writer correctness, not on patching recovery
fallbacks.

---

## §4. Per-Insert Pre-Condition Hits

The `checkLeafInsertPreservesI5(rootRef, newKey, reader)` static helper was called
BEFORE each of the 50 000 main-phase inserts. Total hits: **72** out of 50 000 (0.14 %).

| Stat | Value |
| --- | --- |
| First hit insert # | 515 |
| Total hits | 72 |
| Hit rate | 0.14 % |
| Insert # of first I8 violation | 500 (15 inserts before first pre-condition hit) |

**Interpretation**: the leaf-insert-precondition fires very rarely. Most main-phase
inserts have a target leaf whose subtree's β-bit values either already match newKey's
or are β-mixed (= already in violation, not flagged by the precondition). The 72
flagged inserts are the ones where the validator can prove the insert WOULD turn a
β-constant subtree into a β-mixed one.

**Stage F implication**: a STRICT pre-condition gate at insert time would reject
~72 inserts on this workload. We'd need to handle them — either by routing the key
to a different subtree (= constancy-aware split before insert, Phase 2 of the
original design) or by rejecting / retrying.

---

## §5. Answers to Stage B §7 Open Questions

| # | Question | Empirical answer |
| --- | --- | --- |
| 1 | Does I4 hold today? | **No.** 1 violation, first appears at idx 1750, persists through end. |
| 2 | Does I11 hold today? | **No.** 1 violation, first appears at idx 500, self-cleared by main+1500 (a happy-path operation later restored monotonicity at the offending edge). |
| 3 | Phase 3/4/intermediate-BN firing distribution? | **48 BN + 17 P3 + 0 P4 + 0 FP** across 50K inserts. ~99.87 % of inserts use only happy-path operations. |
| 4 | Does `splitToWithInsertOnBit` fall-back-to-MSDB at line 1031-1037 produce I5 violations? | Indirect signal: 72 pre-condition hits, but no Phase 3 firings between idx=515 and the first I4 at idx=1750 → fall-back happens silently without recovery firing. **Stage F must check this explicitly with an in-writer gate.** |
| 5 | I8 violations from `withUpdatedChild` beyond intermediate-BiNode case? | **Yes.** I8 first appears at main+500 with no BN firings since main+250 — so a happy-path `splitParentAndRecurse` or `addEntryWithPDep`-via-`withUpdatedChild` is producing I8-violating slot order. Not just the intermediate-BiNode case. |

---

## §6. Empirical Update to the Op × Invariant Master Matrix (Stage B §4)

Based on the Stage D run, here's how the Stage B matrix cells flip:

### Cells confirmed ❌ (broken on real workload)

| Op | Inv | Was | Now | Evidence |
| --- | --- | --- | --- | --- |
| 14 (`addEntryWithPDep`) | I4 | ⚠ | ❌ | I4 fires with no recovery counter delta — happy-path bug |
| 14 (`addEntryWithPDep`) | I8 | ⚠ | ❌ | I8 fires at idx 500 with BN delta = 0 |
| 14 (`addEntryWithPDep`) | I11 | ⚠ | ❌ | I11 fires at idx 500 — disc-bit ordering broken on success path |
| 20 (`splitParentAndRecurse`) | I8 | ⚠ | ❌ | At idx 500 happy-path split produced an I8-violating parent |
| 22 (`buildCompressedHalf` SM) | I8 | ⚠ | ❌ | Happy-path build at idx 500 (bchAll=0 deltas confirm SingleMask main path) |
| 26 (`createBiNodeTraced` intermediate-BN) | I8 | ❌ | ⚠ DOWNGRADE | The 4 warmup BN firings did NOT plant the observed I8 — it appeared 250 inserts later |

### Cells with no new evidence

| Op | Inv | Status |
| --- | --- | --- |
| 1 (leaf.put) | I5/I-SP | Still ❌ — pre-condition fired 72 times confirming the gap, but unmaterialized in tree-walk validator (uses loose `(stored & ~dense) == 0` form which routing soundness preserves). |
| 24/25 (`createNodeFromChildren-N`) | I5/I-SP | ⚠ — never fires on this reproducer (bchAll=0 delta everywhere). Behavior on adversarial workload is unknown — Stage F may not need to gate it. |
| 32-35 (Phase 4) | various | Still ⚠ — Phase 4 never fires on this workload. |

---

## §7. Implications for Stage F (Fix Design)

**Major reframe from earlier sessions**: this campaign's recurring failure pattern was
attributed in Stage B's narrative to "Op 1 (leaf.put) breaking I5 silently". Stage E's
empirical data refines the diagnosis:

1. **The I8 + I11 first-failure is at idx 500** — height 3 tree, no recovery firings.
   That's an ordinary happy-path `splitParentAndRecurse` producing an I8-violating
   parent. The fix must address this directly.

2. **I4 first-failure at idx 1750** — also happy-path. The `addEntryWithPDep` partial-
   key repositioning leaves the smallest stored partial non-zero in some cases. Investigate
   the PDEP repositioning logic in `HOTTrieWriter.java:1430-1448`.

3. **Recovery paths are minor contributors**: 65 firings across 50K inserts. The campaign
   was patching the wrong subsystem. Phase 3 / Phase 4 / fresh-polarity are bonus correctness;
   the dominant correctness work is in `addEntryWithPDep` + `splitParentAndRecurse` happy
   paths.

4. **I-leaf-insert-precondition fires 72 times**. A strict in-writer gate at insert time
   would reject ~0.14 % of main-phase inserts. Stage F must decide: route those to a
   constancy-aware split instead, or accept and let downstream code repair.

5. **`buildCompressedHalf` SingleMask main path is producing wrong partial-key encodings.**
   The `(sparse & ~dense) == 0` predicate currently passes (Stage B's I-Binna-sparse-path
   check uses firstKey only), but the I4 / I8 / I11 violations indicate the encoding logic
   is producing structurally invalid layouts even on the happy path.

---

## §8. Stage F Targeted Investigations

Per §7, Stage F's design doc should focus on:

1. **`addEntryWithPDep` (HOTTrieWriter.java:1354)** — verify that:
   - `splitChildRepositioned` and the repositioned siblings preserve I4 (smallest stored
     partial = 0).
   - The new partials produce a valid disc-bit layout where `parent.MSB < child.MSB`
     for any indirect child (I11).
   - Slot ordering after `sortChildrenAndPartialsByPartial` preserves I8.

2. **`splitParentAndRecurse` (HOTTrieWriter.java:4319)** — verify that:
   - The half-split partition + buildCompressedHalf rebuild preserves I8 across the parent
     of the integration target.
   - Specifically: does the post-split parent always have its `firstKey` strictly increasing
     across slots? The first-failure at idx 500 says NO.

3. **`buildCompressedHalf` SingleMask main path (HOTTrieWriter.java:4663-4898)** — verify that:
   - The PEXT+PDEP repositioning produces partials where the smallest is 0 (I4).
   - The resulting node's MSB is greater than every child indirect's MSB (I11).
   - The post-`sortChildrenAndPartialsByPartial` ordering preserves first-key ordering (I8).

4. **`splitToWithInsert` fall-back branch (HOTLeafPage.java:1031-1037 / handleLeafSplitAndInsertInternal)** —
   when explicit-bit split degenerates and falls back to MSDB, the resulting halves are NOT
   β-constant at the originally-requested ancestor bit. This is a known correctness gap.
   **Stage F decides**: accept the fall-back (current behavior, leads to leaf-insert-precondition
   hits) or reject and retry with a different keyset partition.

---

## §9. Reproducing Stage E Data

```
./gradlew :sirix-core:test \
    --tests "io.sirix.index.hot.HOTFormalVerificationTest.diagnosticMicrobenchPatternReproducer" \
    --rerun -Dhot.strict.validate=true 2>&1 | grep stage-D-gate
```

Output is 202 checkpoint lines + first-failure-attribution summary. Total runtime ~41 s.

---

## §10. Snapshot of First-Failure-Attribution Output

```
[stage-D-gate] N=50000 checkInterval=250 checkpoints=202 precondHitsTotal=72
[stage-D-gate] post-warmup(idx=-1, h=3, Σ[BN=4,P3=0,P4=0,FP=0,bchALL=0], v={}, Δ[BN=4,...])
[stage-D-gate] main+250(idx=250, h=3, Σ[BN=4,P3=0,...], v={}, Δ[BN=0,...])
[stage-D-gate] main+500(idx=500, h=3, Σ[BN=4,P3=0,...], v={I11-trie-condition=1, I8-children-sorted-by-firstkey=1}, Δ[BN=0,P3=0,...])
[stage-D-gate] main+750(idx=750, h=3, Σ[BN=5,...], v={I8-children-sorted-by-firstkey=1}, Δ[BN=1,...])
...
[stage-D-gate] main+1750(idx=1750, h=3, Σ[BN=5,...], v={I4-first-partial-zero=1, I8-children-sorted-by-firstkey=1}, Δ[BN=0,...])
...
[stage-D-gate] post-commit(idx=50000, h=6, Σ[BN=48,P3=17,P4=0,FP=0,bchALL=0], v={I4=1, I8=1}, Δ[BN=0,...])
[stage-D-gate] first-failure: {I-leaf-insert-precondition=515, I11-trie-condition=500, I4-first-partial-zero=1750, I8-children-sorted-by-firstkey=500}
```
