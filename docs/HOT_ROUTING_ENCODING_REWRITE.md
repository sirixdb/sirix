# HOT Routing Encoding Rewrite Plan

**Status:** Architectural design surfaced from Stage G.17's empirical finding.
The 1 marginal I8 violation requires a fundamental change to how stored
partials encode subtree content.

> ## Phase 1 empirical finding (2026-05-20, post Stages 3c + 3b)
>
> Ran the `Direction1HitRateProbe` SLIDING_SNAPSHOT canary with the
> `-Dhot.diag.directionOneFallback=true` diagnostic + a Phase 1 probe that computes
> the candidate disc bit `β''` = `MSDB(K XOR affected.firstKey)` and checks whether
> `β''` is fresh to d*'s mask. **All 4 I8-unsafe Direction 1 fallbacks have β''
> already in d*'s mask:**
>
> | # | β''  | d*'s mask                 | β''' (vs prev) | d*'s mask contains β''' |
> |---|------|---------------------------|----------------|------------------------|
> | 1 | 133  | [60, 61, 132, 133, 134]   | 133            | yes                    |
> | 2 | 132  | [61, 62, 132, 133]        | 132            | yes                    |
> | 3 | 135  | [62, 132, 133, 134, 135, 136] | 133        | yes                    |
> | 4 | 136  | [132, 133, 135, 136]      | 133            | yes                    |
>
> **§2.2 "Option B" (proactive disc-bit extension) is REFUTED for these cases.** The
> bit is already there; it just isn't enforced as a path-bit at affected's slot. The
> structural reality: affected straddles β'' off-path (= affected's stored partial
> has β''-col = 0 even though affected.firstKey[β''] = 1), so K's densePK subset-
> matches affected regardless of K[β''] value. This is the canonical off-path-
> straddle case proven by Stage 0 (`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md`) — but it
> does cause the I8-vs-routing tension that the 4 firings represent.
>
> **The real fix is structural: ENFORCE β'' as path-bit at affected** by splitting
> affected's content on β''. Pre-split, affected straddles β''; post-split, the
> β''=0 half stays in affected's slot (β''-col=0 on-path) and the β''=1 half goes
> to a new sibling slot at affected's `partial | β''-bit`. After the split, K (with
> β''=0) routes to affected's narrowed slot (which contains only β''=0 keys, all
> with firstKey ≥ K) — and K becoming the new firstKey would propagate normally
> through the affected.partial-stays-the-same chain.

> ## Phase 2 EXPERIMENTAL RESULT — split-affected primitive is INSUFFICIENT (2026-05-20)
>
> Re-used `splitLeafPage` + `handleOffPathOverflow` (already implemented for the Issue B
> merge path) from the `tryBranchIncremental` C2 + I8-unsafe site in an experimental wire
> (uncommitted, reverted). Empirical result: **the split primitive is insufficient on its
> own.**
>
> Effect on the SLIDING_SNAPSHOT canary:
> - `rebuildSubtree calls`: 21 → 0 (all rebuilds eliminated).
> - C2 firings: 10 → 18, with 14 Direction 1 sub-inserts + 4 fallbacks.
> - Issue B firings: 2 → 6 (new firings produced by the wire).
> - But: **`interleavedInsertDeleteMultiRev` fails at rev 3 with an I8 violation**:
>
>   ```
>   [I8-children-sorted-by-firstkey] indirect 23 child[5].firstKey 8000...0400... >=
>     child[6].firstKey 8000...03e8...
>     dump=[..., c5(sparse=0x10,dense=0x12,fk=...0400...),
>                 c6(sparse=0x11,dense=0x11,fk=...03e8...), ...]
>   ```
>
> **Root cause of the failure.** The split primitive places K at affected's slot (now
> renamed L0 with `partial[β''-col]=0`). L0's new firstKey = K. But K < `prev.firstKey`
> (= the I8-unsafe condition that triggered the fallback in the first place), so
> `prev.firstKey > L0.firstKey` ⟹ I8 broken at the prev/L0 boundary.
>
> The split fixes the OFF-PATH STRADDLE inside affected, but it does NOT fix the
> ROUTING-VS-LEX MISMATCH between prev's slot and affected's slot at d* — which is the
> actual structural defect. To fix that we'd need to ALSO reorganize d*'s siblings (either
> split prev too, or shuffle slot positions) so K ends up in the lex-correct slot
> position. That's no longer a "localized split" — it's essentially what a scoped
> `rebuildSubtree(insertDepth)` does: collect all entries, sort lex-first, canonicalize.
>
> **Verdict.** The 4 residual `rebuildSubtree(insertDepth)` firings are STRUCTURALLY
> NECESSARY at this level of routing-encoding sophistication. Eliminating them would
> require either:
>
> 1. **Multi-slot reorganization at d***. A primitive that splits affected on β'' AND
>    re-positions K within d*'s slots such that K lands lex-correctly (= shifts slot
>    indices). The semantics intersect with I7 (partials ascending), so the slot indices
>    are constrained to follow partial ordering -- which is exactly what the descent
>    initially used to mis-place K. Solving this requires changing the partial encoding
>    itself (= the actual routing encoding rewrite, §2.1/§2.3 not §2.2).
>
> 2. **A different routing primitive** (e.g., Option A AND-encoding, Option C subtree-OR
>    recompute) that captures the subtree's CONTENT and not just its firstKey. The
>    existing §2.1 and §2.3 of this doc explore these. Both have their own tradeoffs
>    (Option A degenerates to ambiguity as inserts diversify; Option C is O(subtree-size)
>    per insert).
>
> Stage 4b iter-3's verdict from the HOT_REBUILD_FALLBACK_ELIMINATION_PLAN now reads:
> "**the 4 firings are bounded and bounded-cost; eliminating them is gated on the routing
> encoding rewrite (§2.1 / §2.3 Option A or C), not a localized incremental primitive**".
> The scoped rebuild IS the canonical operation for these cases under the current
> encoding.

---

## §1. The Problem

**Current encoding** (`HOTIndirectPage.partialKeys[c]` for slot c):

```
stored[c] = PEXT(child[c].firstKey, mask)
```

= the bits at mask positions, extracted from the leftmost key in c's subtree.

**Routing** (`findChildIndex`):

```
For each key K with densePK = PEXT(K, mask):
  pick the slot whose stored ⊆ densePK_K with maximum specificity (popcount).
```

**The break under multi-entry leaves**:

Consider slot c with leaf L containing keys K1 (firstKey) and K2 (multi-entry).
- `stored[c] = PEXT(K1, mask)` reflects K1's bit pattern.
- K2 has a DIFFERENT bit pattern at some mask bit β.
- When PEXT-routing for K2 from root, K2's densePK != stored[c]. Routing may pick a different slot c' whose stored ⊆ densePK(K2) more specifically. Validator I6 fires (K2 in c, but routes to c').

**Empirical evidence** (Stage G.17): the 1 marginal I8 violation at indirect 2 emerges from cumulative subtree updates that change deep-firstKey ordering at multi-entry leaves whose stored partials don't reflect their actual content.

---

## §2. The Architectural Fix

Replace the encoding with one that reflects the SUBTREE's content:

### 2.1 Option A — AND-encoding (= "bits all subtree keys agree on")

```
stored[c] = AND of densePK(K) for all K in c's subtree
```

**Routing semantics**: target.densePK ⊇ stored[c] (= target has all bits where subtree agrees).

**Pros**: deterministic, captures subtree's invariant bits.

**Cons**: as inserts diversify, stored shrinks toward 0 (= every slot matches every target → ambiguous).

### 2.2 Option B — Exact-match routing + proactive disc-bit extension

```
stored[c] remains PEXT(firstKey, mask)
At insert time: when K's densePK at A's mask doesn't equal any stored, ADD a disc bit
to A's mask such that K and existing keys are distinguished.
```

**Routing semantics**: subset-match preferred + exact-match as tie-breaker (= current).

**Pros**: routing stays correct because every distinct path gets its own disc bit.

**Cons**: parent's mask grows over time; mask saturation at ~32 bits forces splits.

### 2.3 Option C — Recompute stored from subtree at every CoW

```
Every withUpdatedChild / addEntry rewrites stored[c] = OR/AND of subtree's densePKs
```

**Pros**: stored always reflects current subtree state.

**Cons**: O(subtree-size) per insert. Prohibitively expensive at 50K scale.

### 2.4 Recommendation: Option B

Option B preserves the current encoding semantics while fixing routing
ambiguity at insert time. The disc-bit extension is exactly what
`addEntryWithPDep` already does at split time — we just need to call it
proactively when ambiguity is detected.

---

## §3. Implementation Plan (Multi-Week)

### Phase 1: detect routing ambiguity at insert time

For each ancestor A on the descent path, compute `densePK_K = PEXT(K, A.mask)`.
Compare with `stored[chosenSlot]`. If differ AND no other slot has stored ==
densePK_K, the routing is ambiguous: K's bit pattern doesn't match its descent.

Add helper: `findAmbiguousAncestor(path, K) → ancestorIdx | -1`.

### Phase 2: proactive disc-bit extension

When ambiguity detected at ancestor A: find a bit β where K differs from chosenSlot's
subtree. Add β to A.mask via addEntryWithPDep-style logic. After extension, K's
densePK has β-bit unique → routes to a distinct (possibly new) slot.

Need: `extendMaskForKey(A, K) → updated A | null`.

### Phase 3: integrate into doIndex

Before the standard merge, walk path. If `findAmbiguousAncestor` returns ≥ 0,
call `extendMaskForKey` at that ancestor. Repeat until path is unambiguous, then
proceed with standard insert.

### Phase 4: validate

Run 50K reproducer with strict-Binna gate. Expect 0 violations.

### Phase 5: performance regression test

Run 100K, 1M, 10M CAS workloads. Compare insert throughput vs baseline.
Acceptable degradation: ~10–20% (= the cost of proactive disc-bit checks).

---

## §4. Concrete Sub-Tasks

| # | Task | Effort | Helper exists? |
| --- | --- | --- | --- |
| 4.1 | `findAmbiguousAncestor` helper | 0.5d | reuses `collectAncestorDiscBits` + `densePK` |
| 4.2 | `extendMaskForKey` (= addEntry-at-runtime) | 1d | reuses `addEntryWithPDep` |
| 4.3 | Wire into `doIndex` | 0.5d | reuses `tryReRouteOffendingKey` pattern |
| 4.4 | Cascade testing on reproducer | 1d | requires Stage D gate |
| 4.5 | Performance testing | 1d | needs separate workloads |
| 4.6 | Integration with existing G.1–G.17 fixes | 1d | each fix's gate may interact |

**Total estimate**: 5 days of focused work. Multi-week if interrupted.

---

## §5. Why This Should Work

The cascade source from Option B's earlier dispatch attempts (28 706+ violations)
was: routing for newKey from root could pick descend-slot OR sibling-slot,
non-deterministically. After reroute via `bulkInsertIntoSiblingSubtree`, newKey
landed in sibling but PEXT-routing chose descend at validator time → I6.

With proactive disc-bit extension, AT THE TIME of the first ambiguous insert,
A's mask gains a bit that DISTINGUISHES the routes. After this, descend's stored
and sibling's stored have the new bit set differently → routing is deterministic.

Subsequent validator I6 checks: target K's densePK has the new bit; only one slot's
stored has that bit → unique match.

---

## §6. Validation Strategy

After each phase:

1. Run 50K reproducer with `-Dhot.strict.binna=true -Dhot.strict.validate=true`.
2. Track: violation count, mask-extension counter, addEntry rejection rate.
3. If violations decrease + mask-extension fires: progress.
4. If violations stable but extension fires: extension isn't catching the right cases.
5. If violations cascade: extension produced ambiguity elsewhere (= retry with stricter case selection).

Convergence target: 0 violations on the 50K reproducer with hot.strict.binna=true.

---

## §7. Existing Helpers (= Building Blocks)

All of these landed in Stages G.1 through G.17:

- `findAnyOffendingAncestorBit(path, leaf, key)` — detects β-constancy break.
- `tryReRouteOffendingKey(...)` — tries reroute via exact-XOR sibling.
- `findI8OffendingAncestor(...)` — detects deep-firstKey I8 break.
- `pickConstancyCorrectChildSlot(...)` — alternate slot picker.
- `collectDiscBitsOf(indirect)` — enumerate mask bits.
- `bitConstantValueInSubtree(ref, β)` — scan subtree for β-constancy.
- `addEntryWithPDep` / `addEntryMultiMask` — extend mask with new bit (call sites: split-time).
- `buildCompressedHalf` SingleMask + MultiMask — half-builder with G.6 direct-PEXT.
- `upgradeToMultiMaskWithNewBit` — cross-window upgrade with G.14 direct-PEXT.

The new work is: combining these into the proactive extension logic at insert time.

---

## §8. Why It Wasn't Done in Earlier Iterations

- Earlier iterations focused on PATCHING routing post-hoc (Option B reroute) rather
  than EXTENDING the mask proactively.
- Reroute alone fails because the destination subtree's stored partials are still
  stale (= the source's PEXT-routing remains ambiguous).
- The proactive extension fixes the ambiguity AT THE SOURCE (= the ancestor that
  routes the key) instead of trying to relocate the key after the fact.

This is the architectural insight that took 17 G-attempts to surface. The next
sessions implement it.
