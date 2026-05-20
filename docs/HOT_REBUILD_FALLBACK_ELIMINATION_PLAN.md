# HOT rebuild-fallback elimination — design plan

**Status:** Stage 4a landed (`7992b58ff`); **Stage 4b ATTEMPTED + REVERTED** (see callout
below); Stage 4c+ pending re-design. **Branch:** `fix/hot-strict-binna-conformance`.
**Follows:** `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` (Stage 0) + `HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md`
(Stages 1–2).
**Empirical trigger:** commit `c8ac969af` (Stage 3a) instrumented the structural self-heal with
`SELF_HEAL_FIRINGS`; running the HOT test suite yielded **118 firings — Stage 3b BLOCKED**.

---

> ## ⚠ Stage 4b ATTEMPTED + REVERTED — the C2 sub-insert approach breaks routing
>
> **Attempted 2026-05-20:** wire the C2 catch at the three `addChildAtCombination` sites
> (Site A: `tryBranchIncremental` not-full betaIsDiscBit; Site B: `branchFullNodeAtExistingBit`'s
> `betaCol>=0` half-fold; Site C: boundary-child not-full handler) → on
> `IllegalArgumentException` collision, call `subInsertAt(collidingChildRef, K, V)` to descend
> through the colliding child and retry the dispatch.
>
> **Result:** the **103 C2 firings dropped to 0** — sub-insert was exercised — but
> **`HOTVersionedLeafStressTest$DeleteAtScale.interleavedInsertDeleteMultiRev` failed with 11
> I6 routing violations:** stored keys present in some leaf but root-routing reaches a different
> leaf. Verbatim (one of 11):
>
> > `[I6-pext-routes-to-leaf] PEXT descent for stored key
> > 8000000000000009000580000000000003e800000000 landed in leaf 18 which does not contain it`
>
> Reverted. (Stage 4a infrastructure — `dispatchInsert`, `subInsertAt`, `findChildSlotByPartial`
> — remains; unused until Stage 4b is re-designed.)
>
> **Root cause (post-mortem).** The plan's §3.1 premise — "K structurally belongs in c''s
> subtree" — does not hold under Sirix's **exact-match-preferred / highest-index-subset-match**
> `findChildIndex`. After sub-insert places K in c''s subtree, root-routing for K still picks
> the **affected** child (or another sibling that subset-matches): both `affected.partial` and
> `c'.partial` subset-match K's `densePK`; highest-index wins; if `affected` outranks `c'` the
> routing reaches the wrong subtree → misroute. The plan §3.1 noted "empirically
> affectedChildIndex > c'", correctly diagnosed the descent imprecision, but **wrong-direction**
> remediated: sub-insert places K where routing won't reach it. The interleaved-insert-delete
> workload (tombstones + multiple revisions + complex subtree shapes) reliably triggers the
> misroute; ascending and simple random workloads coincidentally don't (routing happens to
> agree with sub-insert's placement when below-β bits are tame).
>
> **What the canonical (Theorem 2) argument needs.** For sub-insert to be routing-correct, K's
> `densePK` must NOT subset-match `affected.partial`. That requires `affected.partial` to have
> some bit set that `K's densePK` lacks. The β-bit on its own doesn't suffice (the sparse
> encoding stores `0` for the side `affected` takes when that's β's `0` side — no constraint
> on `dK`). Other distinguishing bits in `affected.partial` would need to disagree with
> `K`. On a canonical trie that *should* hold, but the actual interleaved state has
> configurations where it doesn't — the trie has drifted (likely by an *earlier* C2-tolerated
> sub-insert, or by the canonical-but-non-routing-distinguishing shape the off-path-straddle
> finding allows).
>
> **Re-design directions to evaluate** (deferred):
>
> 1. **Merge-into-affected** instead of sub-insert into c'. K joins `affected`'s leaf (off-path
>    straddle is canonical per Stage 0; I5-route is preserved as long as `affected.partial`'s
>    β-column is 0). If `affected`'s leaf overflows, `splitLeafPage` at some β; if that β ∈
>    D(d*) we hit Issue B (which has its own handler to design). This sidesteps the C2 routing
>    bug entirely — K stays where routing puts it.
> 2. **Re-organise d*** to make K's natural slot route-correct: e.g. swap `affected` and `c'`'s
>    indices (re-encode the partials' relative order). Complex; may break I7/I8 for other keys.
> 3. **Smarter `findChildIndex`** — change Sirix's exact-match-preferred routing to follow the
>    block-structure more strictly. Risks regressing all of Stage 0/1/2's verification.
>
> Direction 1 is the most promising and the natural follow-up. It also unifies with Issue B's
> design (which is itself a merge-style "L₀ stays in L's slot, L₁ goes elsewhere" handler).
> Needs its own probe + verification before a Stage 4b retry.

---

This plan covers the two issues the Stage 3a verification surfaced: the **branch-path
`comboPartial` collision (C2)** and the **merge-path `β ∈ D(N)` off-path overflow**. Both
prevent self-heal deletion. Both reduce to a single new primitive — **`subInsertAt`** —
extending the descent through a colliding/blocking child and retrying. Once landed,
Stage 3b can proceed: delete the `catch (IllegalArgumentException | IllegalStateException)`
arms, `rebuildWholeIndex`, and any unreachable helpers — completing the rebuild-fallback
elimination.

---

## §1. Problem

After Stage 0 (straddle guard removed) + Stages 1–2 (betaIsDiscBit-full and
boundary-child handled), the structural self-heal in `AbstractHOTIndexWriter` —
`catch (IllegalArgumentException | IllegalStateException) → rebuildWholeIndex` in both
`mergeIntoLeaf` (`:779`) and `branchAboveLeaf` (`:835`) — still fires on the HOT test
suite. Stage 3a (commit `c8ac969af`) measured **118 firings**:

- **103 in `branchAboveLeaf`** — `addChildAtCombination` throws
  `"sparse partial key N is already a child of the node — not a new combination"`.
  This is `HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md` §6 C2: K's `comboPartial`
  coincides with an existing child `c'` of d* (or of the boundary node) — meaning K
  **belongs inside c''s subtree**, the descent stopped one level too shallow.
- **15 in `mergeIntoLeaf`** — `addEntry` throws
  `"split bit N is already a discriminative bit of the node — not a valid integration"`.
  `splitLeafPage`'s `β = msdb(L ∪ {K})` coincides with an **off-path discriminative bit
  of L's parent N** — a NEW case not addressed by any prior plan. L is canonical (its
  stored partial has β-column = 0 — β is off L's block-path); the off-path-straddle
  finding makes this canonical, but `addEntry` rejects the fold because β is already in
  D(N).

Both are *"the existing disc bit / position is already taken"* collisions during
incremental fold. This plan handles both via a unified primitive — **`subInsertAt`** —
that extends the descent through the colliding/blocking child and retries the insert at
that depth.

**Goal of this plan.** Eliminate the 118 firings. Once `SELF_HEAL_FIRINGS` stays at 0
across all canaries, Stage 3b deletes the self-heal, `rebuildWholeIndex`, and (where
unreachable) `rebuildSubtree` + its helpers — the HOT insert path is then a pure
composition of verified incremental primitives with **no `HOTBulkBuilder` rebuild from
any code path**.

---

## §2. Invariants, definitions, primitives

Invariants and the **invariant-clean** / **height-minimal** properties are as in
`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §2 (I1–I8, I11; invariant-clean ⟹ I6 routing;
height-minimal = trie height equals `minheight(S)`). New terms specific to this plan:

- **`comboPartial`**. The sparse-path partial computed for a fresh single-entry leaf at
  the not-full betaIsDiscBit case (or the betaIsDiscBit-at-boundary case): for node N,
  `comboPartial = info.subtreePrefix() | (betaValue == 1 ? betaBitWeight : 0)` — the
  affected subtree's above-β block-prefix, β set to K's value, below-β columns zero.
- **C2 collision**. A call to `addChildAtCombination(node, comboPartial, …)` where
  `comboPartial == partials[c']` for some existing child slot `c' ≠ affectedChildIndex`.
  The new child's intended block-position is already occupied.
- **Off-path overflow** (the merge-path β-in-D case). A leaf overflow where
  `splitLeafPage`'s `β = msdb(L ∪ {K})` is in D(N) (N = L's parent) AND L's stored
  partial at N has β-column = 0 — i.e. β is off L's block-path. L's keys varied at β
  (the off-path-straddle case proven canonical in `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md`),
  so β is genuinely the leaf's key-set MSDB; but β being a disc bit of N already means
  `addEntry` rejects the fold.
- **Sub-insert depth `d_sub`**. The depth at which an insert resumes after the original
  descent stopped too shallow — `d_sub > insertDepth` for C2, `d_sub > pathDepth - 1`
  for off-path overflow's L₁-into-c' case.

The new primitive this plan adds:

```
subInsertAt(targetRef, key, value):
    descend from targetRef (= a PageReference whose page is the target subtree root)
    via findChildIndex(key) at each compound level until a leaf is reached, building a
    new path of {nodes, refs, childSlots}. Then re-run the merge-vs-branch logic at the
    extended path (analyzeDescent + the existing mergeIntoLeaf / branchAboveLeaf
    handlers). Re-points CoW pages up to targetRef. Recursive on further C2 collisions,
    bounded by tree height.
```

Conceptually: `subInsertAt` is `doIndex` starting at a specific subtree root instead of
at the index root.

---

## §3. Root-cause analysis (formal)

### §3.1 Issue A — branch-path C2 collision (103 firings)

Setting: `tryBranchIncremental`'s not-full betaIsDiscBit case at d* =
pathNodes[insertDepth]. The descent landed on `affectedChildIndex` via Sirix's
exact-match-preferred / highest-subset-match `findChildIndex`. `β = msdb(K,
residentInAffected) ∈ D(d*)` (betaIsDiscBit). `comboPartial = info.subtreePrefix() |
betaBit`.

**When does `comboPartial` collide with an existing child `c'`?**

`info.subtreePrefix() = partials[affectedChildIndex] & prefixBits` (the above-β columns
of the affected child's stored partial). `comboPartial` adds the β-column = betaValue.
By construction, `comboPartial` is the block-position with the affected child's above-β
prefix and β = K's value.

For an existing child `c'` to have `partials[c'] == comboPartial`, c' must occupy
**that exact block-position** — i.e. c' is the subtree N already routes to for keys
sharing affected's above-β prefix and having β = betaValue. K satisfies both: K agrees
with affected on above-β (by `β = msdb(K, residentInAffected)`); K[β] = betaValue. So K
**structurally belongs in c''s subtree**.

**Why didn't the descent route K to c'?** Sirix's `findChildIndex`:

```java
// exact match preferred
for (int i = 0; i < numChildren; i++)
    if (partialKeys[i] == densePartialKey) return i;
return subsetPick;     // highest-index subset match
```

K's densePK at d* = `densePK(K, M_{d*}) = comboPartial | (K's below-β bits packed at
their D(d*) positions)`. If K's below-β bits are all zero, K's densePK == comboPartial
== c'.partial → exact match → routes to c' → no C2 collision arises (the descent goes
through c').

If K's below-β bits are NON-zero (the common case for realistic keys), K's densePK >
comboPartial. K may exact-match a *different* child (one whose partial includes those
bits), or no exact match → highest-subset-match picks the higher-index match —
empirically `affectedChildIndex > c'` in the 103-firing cases, so affected wins.

**Conclusion.** C2 collision means the descent stopped one level too shallow — K
belongs in c''s subtree, not as a fresh child of d*. The fix is to **extend the
descent through c'** and retry the insert logic from depth `insertDepth + 1` onward.

### §3.2 Issue B — merge-path β ∈ D(N) off-path overflow (15 firings)

Setting: `mergeIntoLeaf` after a leaf overflow. `splitLeafPage(L, K, V, …)` returns
BiNode(β, L₀, L₁) with β = msdb(L ∪ {K}). `integrate` → `addEntry(N, biNode,
slotOfL, …)`. `addEntry` rejects if `β ∈ D(N)` (line 749).

For β ∈ D(N) AND L's slot at N has β-column = 0:

- L is a child of N. L's stored partial `p_L = partials[slotOfL]` has β-column = 0.
- By the sparse-path encoding, β-column = 0 means β is either *off-path* for L OR
  *on-path on β's 0-side*.
- If β were on-path on β's 0-side, L's keys would all have β = 0 (I5-route); but L's
  keys varied at β (else β wouldn't be msdb(L ∪ {K})). Therefore **β is off L's
  block-path** — the off-path-straddle case proven canonical in
  `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §3.1.

So β is canonical for N (off-path bit, sibling-subtree-internal). L straddles β
off-path; L ∪ {K} has β as its key-set MSDB; `splitLeafPage` partitions correctly. But
`addEntry` cannot fold β as a new disc bit (already there).

**Canonical structural fix.** Split L on β into L₀ (β=0 half) and L₁ (β=1 half):
- L's slot at N has β-column = 0. L₀'s keys all have β = 0 — **L₀ fits L's slot**
  (same partial, just narrowed keys).
- L₁'s keys all have β = 1. L₁ needs a slot with β-column = 1. The canonical position
  is `comboPartial = (L's stored partial with β-column flipped to 1)`.
  - If `comboPartial` is fresh in N: `addChildAtCombination(N, comboPartial, L₁, …)`.
  - If `comboPartial` collides with existing child c' (which has the same above-β
    prefix as L AND β-column = 1): L₁'s keys structurally belong in c''s subtree —
    **sub-insert each L₁ key into c''s subtree**.

So Issue B reduces to (L₀ slot-replace + L₁ add-or-sub-insert). The sub-insert
component is the same primitive as Issue A.

### §3.3 Unification — the `subInsertAt` primitive

Both issues reduce to: extend the descent through a target child and retry the insert
logic at that deeper depth. Concretely:

- Issue A: target = the colliding c' at d*; single key K.
- Issue B: target = the colliding c' at N (only when L₁'s combo collides); each key of
  L₁ is sub-inserted (potentially up to ≤ 256 keys per L₁).

The primitive `subInsertAt(targetRef, key, value)`:
1. Resolve `targetRef.getPage()` (CoW-load if necessary).
2. If a leaf, merge (key, value) — if overflow, recurse via `mergeIntoLeaf` at this
   depth.
3. If compound, find `findChildIndex(key)`, recurse into that child (i.e. extend the
   path, re-run analyzeDescent + the merge-vs-branch dispatch).

`subInsertAt` is essentially `doIndex` starting at a specific subtree instead of at the
index root. It can itself encounter C2 or off-path overflow — bounded by tree height
(≤ ~6 levels).

---

## §4. The fix

### §4.1 The `subInsertAt` primitive

Add to `AbstractHOTIndexWriter`:

```java
/**
 * Insert (key, value) into the subtree rooted at {@code subtreeRef}. Re-uses the
 * standard merge-vs-branch logic at a deeper-than-root starting depth. Bounded by the
 * subtree's height; recursive on further C2 / off-path-overflow at deeper levels.
 *
 * @return true iff the insert succeeded incrementally; false to defer to the caller's
 *         scoped rebuildSubtree (the bounded-recursion-exhausted defensive fallback)
 */
private boolean subInsertAt(PageReference subtreeRef, byte[] keySlice, byte[] valueSlice) {
    // Build a fresh navResult rooted at subtreeRef, descending via findChildIndex to a
    // leaf. The new navResult's "root" is subtreeRef; its pathDepth counts from
    // subtreeRef downward.
    LeafNavigationResult subNav = prepareLeafOfTreeFrom(subtreeRef, keySlice, /*keyLen*/);
    if (subNav == null) return false;        // unresolvable descent — fall back
    // Run the standard merge-vs-branch dispatch at the extended path. analyzeDescent +
    // the existing mergeIntoLeaf / branchAboveLeaf helpers handle the rest.
    HOTIncrementalInsert.DescentAnalysis analysis = HOTIncrementalInsert.analyzeDescent(
        subNav.pathNodes(), subNav.pathChildIndices(), subNav.pathDepth(),
        subNav.leaf(), keySlice);
    final int beta = analysis.mismatchBit();
    final boolean merge = beta < 0 || subNav.pathDepth() == 0
        || beta > leastSignificantDiscBit(subNav.pathNodes()[subNav.pathDepth() - 1]);
    try {
        if (merge) mergeIntoLeafAt(subNav, keySlice, /*keyLen*/, valueSlice);
        else      branchAboveLeafAt(subNav, analysis, keySlice, valueSlice);
        return true;
    } catch (StackOverflowError | IllegalStateException defensiveExhaustion) {
        return false;        // bounded recursion exhausted — caller falls back
    }
}
```

`prepareLeafOfTreeFrom(subtreeRef, …)` is a parameterized variant of the existing
`prepareLeafOfTree` that starts the descent at a given subtree root (not the index
root). Implementation: copy `prepareLeafOfTree`'s loop; initial reference = subtreeRef
instead of `rootReference`.

`mergeIntoLeafAt` / `branchAboveLeafAt` are package-internal variants of `mergeIntoLeaf`
/ `branchAboveLeaf` that operate on an *extended* navResult (one that doesn't span the
whole spine; pathRefs[0] is the subtree root reference, not the index root reference).
Their `registerFreshSubtree` calls register the touched spine UP TO the subtree root —
the caller is then responsible for chaining the CoW further up to the index root (via
the same mechanism the standard handlers use).

### §4.2 Branch-path C2 handling

Where `addChildAtCombination` is called and may throw the C2 IllegalArgumentException
(3 sites — `tryBranchIncremental` not-full betaIsDiscBit, `branchFullNodeAtExistingBit`,
the boundary-child not-full handler), wrap the call:

```java
try {
    newNode = HOTIncrementalInsert.addChildAtCombination(node, comboPartial, …);
} catch (IllegalArgumentException c2Collision) {
    // C2: comboPartial == an existing child's partial. K belongs in that child's
    // subtree — sub-insert K through it (plan §4.1).
    final int collidingSlot = findChildSlotByPartial(node, comboPartial);
    if (collidingSlot < 0) {
        comboLeaf.close();
        return false;                        // defensive: should be found
    }
    final PageReference collidingRef = node.getChildReference(collidingSlot);
    comboLeaf.close();                       // K-leaf not used; sub-insert handles K
    return subInsertAt(collidingRef, keySlice, valueSlice);
}
```

`findChildSlotByPartial(node, comboPartial)`: linear scan of `node.getPartialKeys()`
for `comboPartial` (32 children max — O(1)-bounded).

### §4.3 Merge-path β-in-D(N) handling

In `mergeIntoLeaf`, BEFORE calling `integrate`, check β:

```java
final HOTIncrementalInsert.BiNode biNode = HOTIncrementalInsert.splitLeafPage(
    leaf, keySlice, valueSlice, revision, indexType, pageKeyAllocator);
// Off-path overflow check: if β is already a disc bit of L's parent N, the standard
// addEntry rejects (β already there). Handle it: L₀ stays in L's slot, L₁ goes to the
// β=1 combo (add or sub-insert).
final int beta = biNode.discriminativeBitIndex();
final HOTIndirectPage parentN = pathDepth >= 1 ? pathNodes[pathDepth - 1] : null;
if (parentN != null
    && Arrays.binarySearch(HOTIncrementalInsert.discriminativeBits(parentN), beta) >= 0) {
    // β is in D(N): the off-path-overflow case (plan §3.2). Handle specially.
    if (handleOffPathOverflow(navResult, parentN, biNode, keySlice, valueSlice)) {
        return;
    }
    // Defensive fall-through: handler couldn't apply; let integrate try (will throw,
    // caught below by the self-heal — should be unreachable once §4 is complete).
}
// Standard integration (β fresh to N — or off-path-handler bailed).
ensurePathChildrenLoaded(navResult.pathNodes());
final HOTIncrementalInsert.IntegrationResult result;
try {
    result = HOTIncrementalInsert.integrate(…);
} catch (…) { /* self-heal */ }
registerFreshSubtree(result.touchedRef());
```

`handleOffPathOverflow(navResult, parentN, biNode, keySlice, valueSlice)`:
1. Identify L's slot in parentN = `pathChildIndices[pathDepth - 1]`.
2. Identify which BiNode half is β=0 (L₀) and which is β=1 (L₁): L₀ takes L's slot
   (β-column = 0 matches), L₁ goes to the new combo.
3. Compute `comboPartial = partials[slotOfL] | betaBitInD(N)` (L's stored partial with
   β-column flipped to 1). Note: L's β-column was 0, so this XOR/OR sets it.
4. **Slot replacement (L₀):** `pathRefs[pathDepth - 1].setPage(L₀_page)` — L₀ replaces
   L in L's slot. L's stored partial is unchanged (β-column was 0, L₀ keeps it 0).
5. **L₁ placement (add or sub-insert):**
   - Try `addChildAtCombination(parentN, comboPartial, L₁_ref, parentN.getHeight(), …)`:
     - Success: re-point parentN's slot in its own parent. Done.
     - C2 collision (`comboPartial == c'.partial`): for each key K_i ∈ L₁'s entries,
       `subInsertAt(c'-ref, K_i, V_i)`. (≤ 256 sub-inserts; each O(height).)
6. `registerFreshSubtree(pathRefs[topModifiedDepth])`.

### §4.4 Stage 3b — self-heal deletion (after §4.1–§4.3 land)

Re-run the canaries with `SELF_HEAL_FIRINGS` instrumented. If 0 firings:
1. Delete `catch (IllegalArgumentException | IllegalStateException)` in
   `mergeIntoLeaf` (`:779`) and `branchAboveLeaf` (`:835`).
2. Delete `rebuildWholeIndex` (no remaining callers).
3. Verify `rebuildSubtree` is still called only from the explicit `tryBranchIncremental
   → return false` fallbacks (plan §10) and its own height-escalation. If `subInsertAt`
   never returns false on the canaries → delete `rebuildSubtree`,
   `collectSubtreeEntries`, `collectSubtreeLeafRefs` too. (Otherwise keep them for the
   defensive fallback.)
4. Delete `SELF_HEAL_FIRINGS` (now unreferenced).

---

## §5. Formal verification

Inductive hypothesis (carried from prior plans): the trie is **canonical-enough** —
invariant-clean and height-minimal — before each insert. The Stage 0/1/2 fixes proved
the merge fast-path, leaf-overflow `addEntry`, betaIsDiscBit-full, and
betaIsDiscBit-boundary cases all preserve this. This section discharges the C2 and
off-path-overflow cases via the new `subInsertAt` primitive.

### §5.1 Theorem 1 — `subInsertAt` preserves invariants

> Sub-inserting (key, value) into a canonical subtree rooted at `targetRef` yields an
> invariant-clean subtree.

*Proof.* `subInsertAt` re-runs the standard merge-vs-branch dispatch (`analyzeDescent`
+ `mergeIntoLeafAt` / `branchAboveLeafAt`) on the extended path. Each dispatched
handler — by Stage 0/1/2 + this plan's §4.2/§4.3 — preserves invariants on its
subtree. Composition of invariant-preserving operations on disjoint subtrees preserves
the whole (Binna Lemma 3 — recursive structure: a subtree is canonical in isolation;
splicing a canonical subtree under canonical ancestors yields canonical whole). ∎

### §5.2 Theorem 2 — C2 collision is correctly resolved

> If `addChildAtCombination(N, comboPartial, K-leaf, …)` throws C2 (collision with
> child `c'`), `subInsertAt(c'-ref, K, V)` yields a tree where K is correctly placed
> in c''s subtree, and the resulting tree is invariant-clean + routes K correctly.

*Proof.* By §3.1, K structurally belongs in c''s subtree: K shares c''s above-β
prefix (msdb argument) and K[β] = c''s β-bit value (= betaValue). So `c'` is the
correct subtree for K. `subInsertAt(c'-ref, K, V)` extends the descent through c' and
runs the standard handlers — by Theorem 1, the result is invariant-clean for c''s
subtree (now including K). Routing K from the root: descent reaches c' (via the
existing routing — c' is the highest-subset-match for K's densePK at N, or exact match
if K's below-β bits are zero), then into c''s subtree, then to K's leaf (the standard
descent argument, `HOT_FORMAL_FOUNDATION.md` Theorem 2). ∎

### §5.3 Theorem 3 — off-path overflow is correctly resolved

> If `splitLeafPage` returns β ∈ D(N) where N is L's parent, `handleOffPathOverflow`
> yields an invariant-clean tree with L's keys + K correctly placed and routable.

*Proof.* L is canonical with β-column = 0 (off-path at N). `splitLeafPage` partitions
L ∪ {K} at β into L₀ (β=0) and L₁ (β=1). Two parts to verify:

- **L₀ in L's slot.** L's slot at N has stored partial p with β-column = 0. L₀'s keys
  all β=0. By I5-route at slot for bit β: bit-set in p is 0 (β-column = 0), so I5-route
  trivially satisfied for β regardless of L₀'s keys. Other D(N) columns: L₀ ⊂ L, so
  L₀ inherits L's I5-route for those columns. L₀ is a valid replacement.
- **L₁ at comboPartial.**
  - If `addChildAtCombination` succeeds: L₁ is a fresh child of N at `comboPartial`.
    L₁'s keys all β=1; β-column of comboPartial = 1 → I5-route for β holds (every L₁
    key has β=1, comboPartial requires β=1 set). Other columns: L₁ ⊂ L, L₁ inherits L's
    above-β agreement. Invariant-clean (Theorem 1 of the not-full
    `addChildAtCombination` correctness — Q1 from `BetaIsDiscBitRoutingProbe`).
  - If C2 collision (L₁'s combo equals existing c'): by argument analogous to §3.1,
    L₁'s keys structurally belong in c''s subtree (same above-β prefix, β=1 matching
    c''s β-bit). Per-key `subInsertAt(c'-ref, K_i, V_i)` for each K_i ∈ L₁ — by
    Theorem 1, each preserves invariants on c''s subtree; the cumulative result is
    invariant-clean.

Routing: every L₀ key routes to L's slot (unchanged); every L₁ key routes to
comboPartial-slot (if fresh) or descends through c' (if collision). All keys reach
their leaves. ∎

### §5.4 Theorem 4 — height-minimal preserved

> §4.2 and §4.3 do not inflate trie height beyond the minimum.

*Proof.* The fixes use:
- `addChildAtCombination` — adds one child to a not-full node; height unchanged
  (`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §5.2 Theorem 3 corollary).
- `subInsertAt` — recursively invokes the merge-vs-branch handlers; each handler is
  proven height-preserving (Stage 0/1/2 §5.2-3). The descent only extends DOWN through
  existing nodes — no new height level.
- Slot replacement (L → L₀) — leaf-only, no height change.

For Issue B's L₁ branch: if L₁ is added as a new child, N gains one child (no height
change for N since N was not made full by this operation). If L₁'s keys are
sub-inserted into c''s subtree, each sub-insert is height-preserving by induction.

Hence height = `minheight(S ∪ {K})` throughout (the same minimum the Stage 0–2 plans
established for their cases). ∎

### §5.5 Theorem 5 — termination

> Both fixes terminate in O(height) operations per insert.

*Proof.* `subInsertAt`'s recursion strictly extends the descent each call — bounded by
tree height (≤ ~6 for realistic Sirix HOT trees, hard cap at MAX_PATH_DEPTH = 64).
Each call does O(node) work (findChildIndex + the dispatched handler).

For Issue B's L₁ per-key sub-insert: L₁ has ≤ 256 keys; each sub-insert is O(height);
total O(256 × height) ≤ O(1600). Bounded.

Cumulative per-`doIndex` worst case: O(height × 256) for the worst sub-insert chain —
acceptable for the rare cases (15 firings per HOT-suite run currently). ∎

### §5.6 Theorem 6 — self-heal becomes unreachable

> After §4.2 + §4.3 land, the
> `catch (IllegalArgumentException | IllegalStateException)` arms are unreachable.

*Proof.* The exceptions caught are thrown only by precondition guards:
- `addEntry`'s "β already a disc bit" (line 749 of `HOTIncrementalInsert`) — fired by
  Issue B's off-path overflow; §4.3 handles it BEFORE `integrate` is called.
- `addEntry`'s "node full" (line 741) — `tryBranchIncremental` checks node-full and
  dispatches to `branchSplitFullNode` / `branchFullNodeAtExistingBit`; never reaches
  `addEntry` with a full node.
- `addEntryWithInsertInfo`'s "β already a disc bit" — only called when β was DROPPED
  from a half by `compressHalf` (Stage 1's branchFullNodeAtExistingBit); β being
  dropped means β ∉ D(half), so the precondition holds.
- `addChildAtCombination`'s "sparse partial already a child" — Issue A's C2; §4.2
  catches and sub-inserts.
- `integrate`'s trie-condition assertion (line 1327) — fires only if the BiNode's bit
  ≥ parent's MSB. On a canonical tree this doesn't happen (induction). [Has not been
  observed in Stage 3a — counter showed only the two exceptions above.]
- `splitIndirect`'s preconditions (≥ 2 children, MSB discriminates) — fire only on a
  malformed input; never on a canonical input.

So with §4.2 + §4.3, none of these throws → the self-heal `catch` is unreachable. The
Stage 3b gate is the empirical `SELF_HEAL_FIRINGS == 0` check on a re-run of the
canaries; once green, the catches can be deleted with confidence. ∎

---

## §6. Corner cases (exhaustive)

| # | Case | Handling |
|---|---|---|
| C1 | **Recursive C2** — `subInsertAt`'s descent into c' hits another C2 at a deeper node. | `subInsertAt` recurses; each level strictly extends the path. Bounded by tree height; ≤ MAX_PATH_DEPTH defensive cap. If exhausted, returns false → caller's scoped rebuildSubtree (a defensive fallback that should never fire on canonical trees). |
| C2 | **`comboPartial` collides with the affected child itself** (`c' == affectedChildIndex`). | Cannot happen: comboPartial's β-column = betaValue, and the affected child's β-column = 1 - betaValue (β = msdb separates them). Different β-columns → different partials. Defensive: if observed, throw an `IllegalStateException` (genuine bug). |
| C3 | **L₁ is empty after split** (all of L ∪ {K} has β = 0). | Cannot happen: β = msdb(L ∪ {K}) means at least one key has β = 1 (else β wouldn't be a *differing* bit). `splitLeafPage`'s postcondition guarantees both halves non-empty. |
| C4 | **L₀ is empty** (all β = 1). | Same as C3 — both halves are non-empty by msdb definition. |
| C5 | **`splitLeafPage` returns a deeper-than-leaf BiNode** (an oversized half recursed through `HOTBulkBuilder` into a compound subtree, `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §6 C5). | The off-path-overflow handler accepts arbitrary BiNode children. The slot replacement (L → L₀) handles a compound L₀; the L₁ placement handles a compound L₁. Stage 0's existing tests cover this. |
| C6 | **L₁'s combo collides AND the colliding c' is full** | `subInsertAt(c'-ref, K_i, V_i)` for each key — each call descends through c'; if c' itself is full and a deeper structural overflow occurs, the standard `branchSplitFullNode` / `branchFullNodeAtExistingBit` (Stage 1) handles it. Standard recursion. |
| C7 | **β is in D(N) but L's slot has β-column = 1** (L is on-path-1 for β at N). | By I5-route, L's keys all β = 1. `msdb(L ∪ {K})` = β only if K[β] = 0 (so L ∪ {K} differs at β). But the merge-vs-branch bound (`β > leastSignificantDiscBit`) puts every D(N) bit *above* β when K is being merged — contradiction. So this case cannot arise under the merge bound. Defensive: if observed, falls back to self-heal (or throws IllegalStateException as a bug). |
| C8 | **N (L's parent) is itself a compound node whose D(N) doesn't include β at all** | Then β ∉ D(N) — standard `integrate` path. Not Issue B. |
| C9 | **MultiMask layout (disc bits span > 8 bytes)** | `discriminativeBits`, `getInsertInformation`, `addChildAtCombination`, `addEntryWithInsertInfo` are all layout-agnostic (absolute MSB-first bit indices). MultiMask cases handled the same way. Test coverage: `BetaIsDiscBitRoutingProbe` already covers WIDE_SPAN keys. |
| C10 | **Tombstones in L** | `splitLeafPage` carries tombstones through both halves; `subInsertAt` preserves them; `addChildAtCombination` doesn't care about value semantics. Tombstone preservation invariant (`hot-tombstone-preservation`) unchanged. |
| C11 | **Multi-revision / DIFFERENTIAL · INCREMENTAL versioning** | All ops emit fresh CoW pages registered via `registerFreshSubtree`; the same TIL discipline as Stage 0/1/2. `subInsertAt` extends the CoW chain to the deeper depth — `registerFreshSubtree` covers it. |
| C12 | **L₀'s page key equals L's page key** (slot replacement: do we CoW L₀ to a fresh key?) | `splitLeafPage` allocates fresh page keys for both halves (via the allocator). L₀ has a NEW page key, not L's. Slot replacement via `setPage(L₀_page)` re-points the reference. The old L page is orphaned and released via `releaseOrphanedHOTLeaves` (the existing batch mechanism). |
| C13 | **`subInsertAt` reaches a leaf that's full and overflows** | Standard `mergeIntoLeaf` recursion at the deeper depth. The recursion uses Stage 0's straddle-free leaf-overflow → addEntry path. May further sub-insert if it hits Issue A at the deeper level. |
| C14 | **The colliding c' for L₁'s combo IS a compound node** (not a leaf) | `subInsertAt(c'-ref, …)` descends through c''s `findChildIndex` for each L₁ key — standard descent. The compound c' handles the routing internally. |
| C15 | **subInsertAt's findChildIndex returns NOT_FOUND** at some level | Defensive: returns false → caller's scoped rebuildSubtree. Shouldn't happen on a canonical tree where K has a valid descent; the existing `findChildIndex` always returns a valid slot for any key under MAX_NODE_ENTRIES children with valid partials. |
| C16 | **The L₀ slot-replacement breaks I8 (firstKey ordering)** | L₀'s firstKey ≥ L's firstKey (L₀ is L's β=0 subset, in sorted order). N's siblings are unchanged in firstKey. So I8 — children sorted by firstKey of their subtree's leftmost leaf — is preserved (L₀'s firstKey is ≥ L's, but L's slot's `min subtree` was L's firstKey; the only concern is if some L key < L₀'s firstKey now goes to L₁, lowering the OLD slot's min-subtree-firstKey. But L₀ replaces L; L₀'s leftmost = L₀.firstKey ≥ L's leftmost. So min subtree(slotOfL) ≥ before. Adjacent siblings: slotOfL-1 must have firstKey < slotOfL's. slotOfL-1's firstKey unchanged; slotOfL's firstKey ≥ before; ordering preserved. Similarly slotOfL+1.) Detailed argument in implementation review. |
| C17 | **`registerFreshSubtree` invocation depth** | Standard handlers call `registerFreshSubtree(result.touchedRef())` for the topmost re-pointed reference. `subInsertAt` extends the re-pointing chain DOWN; the topmost re-pointed reference is still the deepest spine node that changed. `registerFreshSubtree` walks down from there — the post-order TIL register walks the new subtree exactly once. Unchanged semantics. |
| C18 | **Concurrent reads during the operation** | Sirix's HOT writer holds the exclusive write lock; readers see either pre- or post-operation state via the page-cache guards (`HOT_EVICTION_ASYMMETRY` fix). `subInsertAt`'s CoW chain doesn't change this discipline. |
| C19 | **Memory: L₁'s leaf page is allocated by splitLeafPage, but if its combo collides, we sub-insert and L₁'s page is orphaned** | Release L₁'s page via `close()` after sub-insert succeeds. (L₁'s page was only used to materialize splitLeafPage's BiNode; the L₁ leaf data is sub-inserted into c''s subtree instead.) |
| C20 | **What if subInsertAt is called with an empty subtree?** | Cannot happen: subtrees in HOT are always non-empty (every leaf has ≥ 1 entry; every compound has ≥ 2 children). Defensive: if observed, throw IllegalStateException. |

---

## §7. Implementation steps

1. **`prepareLeafOfTreeFrom(subtreeRef, key, keyLen)`** — new method, parameterized
   variant of `prepareLeafOfTree`. Copy the descent loop; initial reference = the
   argument, not `rootReference`. Returns `LeafNavigationResult` (or null on
   unresolvable).
2. **`mergeIntoLeafAt(subNav, …)` and `branchAboveLeafAt(subNav, …)`** — variants of
   the existing `mergeIntoLeaf` / `branchAboveLeaf` that operate on a subNav. The CoW
   chain stops at `subNav.pathRefs()[0]` (the subtree root reference); the caller is
   responsible for chaining further up. (Could be implemented as wrappers that pass an
   extra "stop depth" parameter to the existing helpers.)
3. **`subInsertAt(subtreeRef, key, value)`** — composes 1+2. §4.1.
4. **`findChildSlotByPartial(node, partial)`** — linear-scan helper. O(32).
5. **Wrap `addChildAtCombination` at 3 sites** with the C2 try/catch + `subInsertAt`
   call (plan §4.2): `tryBranchIncremental` not-full betaIsDiscBit (existing handler at
   line ~864), `branchFullNodeAtExistingBit` (Stage 1's method), and the
   boundary-child not-full handler (Stage 2's addition).
6. **`handleOffPathOverflow(navResult, parentN, biNode, keySlice, valueSlice)`** —
   plan §4.3. New method in `AbstractHOTIndexWriter`.
7. **`mergeIntoLeaf`** — call `handleOffPathOverflow` BEFORE `integrate`. If it
   returns true, return. Else proceed with standard integrate.
8. **Run all HOT tests** — assert `SELF_HEAL_FIRINGS == 0` (a new test that asserts
   the counter is 0 after the canary run).
9. **Stage 3b — delete the self-heal** once step 8 confirms 0 firings (plan §4.4).
10. **Update `docs/HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md`** with a "superseded
    by" / "completed via this plan" note.

---

## §8. Test plan

1. **`BetaIsDiscBitRoutingProbe` — extend with C2 cases** — construct cases where
   `comboPartial` collides with an existing child; verify `subInsertAt` correctly
   places K in c''s subtree (strict routing + detector clean).
2. **New probe — `OffPathOverflowProbe`** — construct cases where `splitLeafPage`'s β
   coincides with an off-path D(N) bit; verify `handleOffPathOverflow` produces a
   canonical tree with all keys routable.
3. **`HOTFormalVerificationTest`** — re-run all canaries; assert `SELF_HEAL_FIRINGS ==
   0` at the end.
4. **`HOTVersionedLeafStressTest`** — full suite green; multi-revision invariance.
5. **`StraddleCanonicityProbe`** — unchanged, must still pass (Stage 0 regression
   test).
6. **`HOTIndirectPageSplitFaithfulTest`** — unchanged, must still pass.
7. **`HOTIntegrateTest`** — unchanged, must still pass.
8. **Per-insert validator probe** (re-add `-Dhot.diag.perInsertValidate` from
   `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §8) — run with the canaries; assert no drift
   (I5-route + I6) after any insert.
9. **Performance** — `HOTMicrobenchmark` (ascending) unchanged; add a random-workload
   500K microbench and confirm no `rebuildSubtree` / `rebuildWholeIndex` calls in the
   profile.

A fix is accepted only when 1–9 are green and 3 confirms `SELF_HEAL_FIRINGS == 0`.

---

## §9. Review passes

**Pass 1 (the unification).** — *Confirmed:* both Issue A (branch C2) and Issue B
(merge off-path overflow) reduce to "extend the descent through a target child and
retry." The `subInsertAt` primitive is the right abstraction.

**Pass 2 (recursive `subInsertAt`).** — *Caught:* `subInsertAt`'s descent can itself
hit C2 at a deeper level. *Resolved:* recursive call, bounded by tree height; defensive
fallback to scoped rebuildSubtree if exhausted (§6 C1).

**Pass 3 (off-path overflow vs the straddle finding).** — *Confirmed:* β ∈ D(N) with
L's β-column = 0 is exactly the off-path-straddle case proven canonical in Stage 0.
This plan handles its specific overflow manifestation (the splitLeafPage → addEntry
rejection) without contradicting the straddle finding.

**Pass 4 (CoW + TIL discipline).** — *Confirmed:* `subInsertAt` extends the existing
CoW chain DOWN; `registerFreshSubtree` is called at the topmost re-pointed reference
and walks the new subtree; no new TIL discipline.

**Pass 5 (height + termination).** — *Confirmed:* §5.4 height-minimal proof — neither
fix introduces a new height level (existing primitives are height-preserving;
`subInsertAt` only descends). §5.5 termination — bounded by tree height + L₁'s
key count.

**Pass 6 (self-heal unreachability).** — *Confirmed:* §5.6 enumerates every catchable
exception source; each is either handled by §4.2/§4.3 or precondition-holds-by-induction.
The Stage 3b gate is empirical (`SELF_HEAL_FIRINGS == 0`), not just proven.

**Pass 7 (the L₀ slot replacement and I8).** — *Caught:* slot replacement could break
I8 if L₀'s firstKey changes the slot's min-subtree. *Resolved:* §6 C16 argues L₀'s
firstKey ≥ L's firstKey, so the slot's min-subtree-firstKey is non-decreasing; adjacent
sibling ordering preserved.

**Pass 8 (L₁'s combo collision: many-key merge).** — *Caught:* L₁ has up to 256 keys;
naive per-key sub-insert is O(256 × height). *Considered:* would a bulk merge into c''s
subtree be faster? — yes but more complex; for 15 firings per test suite, the O(256 ×
6) ≈ 1500 sub-inserts is negligible. Keep simple per-key.

**Pass 9 (corner-case exhaustiveness).** — *Reviewed:* 20 corner cases (§6) covering
recursive C2, edge orderings, tombstones, MultiMask, versioning, page lifecycle,
defensive failures. Coverage feels exhaustive — additional cases caught in
implementation will surface via the per-insert probe (§8.8).

**Pass 10 (Stage 3b safety gate).** — *Confirmed:* deletion of the self-heal is GATED
on empirical zero-firings (§4.4 step 1; §8 step 3). The plan does NOT delete the
self-heal on faith — even with Theorems 5/6, the deletion is empirically validated
first.

*Ten passes. The fix path is verified-by-composition (re-uses Stage 0/1/2 primitives
+ the new `subInsertAt`), corner cases exhaustively enumerated, Stage 3b deletion
empirically gated. Ready for implementation.*

---

## §10. Scope boundary

**In scope.** The two `SELF_HEAL_FIRINGS` sources identified by Stage 3a — the
branch-path `comboPartial` C2 collision (103 firings) and the merge-path off-path
overflow (15 firings) — handled via the new `subInsertAt` primitive. Stage 3b (delete
the self-heal + `rebuildWholeIndex` + possibly `rebuildSubtree` and its helpers) is in
scope once the canaries confirm zero firings.

**Out of scope.** The reader-side lower-bound / lex fallbacks — they're a separate
read-path concern. The performance microbench characterization vs. the prior
straddle-rebuild baseline — measure but don't gate on absolute numbers.

**Prerequisite.** `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` (Stage 0) and
`HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md` (Stages 1–2) must be landed (they are
— commits `0d37f8e7c`, `5b180e328`, `723ac1e1d`). This plan is Stage 3b's prerequisite.

**Result when complete.** A HOT insert is a composition of verified incremental
primitives — `splitLeafPage`, `splitIndirect`, `addEntry`, `addChildAtCombination`,
`addEntryWithInsertInfo`, `splitIndirectWithEntry`, `integrate`, `subInsertAt`,
`handleOffPathOverflow` — with **zero `HOTBulkBuilder` subtree or whole-trie rebuilds
from any code path**, incremental and minimum-trie-height-preserving after every insert.

---

## §11. Stage 4b re-design — merge-into-affected (Direction 1)

**Status:** designed 2026-05-20 (this session, post-revert of `873065f82`).
**True Direction 1 BLOCKED on I8 — scoped-rebuild fallback LANDED in this iteration**
(see §11.X below for the empirical finding).
**Replaces:** the §3.1 / §4.2 sub-insert-into-c' approach.
**Scope:** Issue A (the 103 branch-path C2 firings). Issue B (the 15 merge-path
firings) keeps the §4.3 design — its slot-replacement + `comboPartial` placement is
already routing-correct under Sirix's sort-by-partial invariant (the L₁ slot lands at a
strictly higher partial than L's old slot, so highest-index-subset-match picks it for
L₁'s keys; L₀ inherits L's old slot, so β=0 keys still route there). Re-validate §4.3
when implementing, but the routing argument holds.

> ### §11.X EMPIRICAL FINDING — true Direction 1 breaks I8
>
> **Attempted 2026-05-20 (this session):** wire Site 1's C2 catch to
> `subInsertAt(affectedRef, K, V)` — sub-insert K into the slot the descent already
> chose, expecting the routing tautology (§11.1) to make it both correct AND
> rebuild-free.
>
> **Result:** routing IS correct (no I6 violations — Lemma 2 holds empirically), but
> `HOTVersionedLeafStressTest$DeleteAtScale.interleavedInsertDeleteMultiRev` failed with
> an **I8 violation** (range-scan ordering). Verbatim diagnostic:
>
> > `[I8-children-sorted-by-firstkey] indirect 19 child[5].firstKey 80...040000... >=
> > child[6].firstKey 80...03e8...`
>
> **Mechanism (post-mortem).** When K becomes the new `firstKey` of `affected` (because
> K < `affected.firstKey` in lex order) AND the trie has an **MSDB-closure gap** at d*'s
> mask — i.e. d*'s mask is missing the most-significant bit that distinguishes
> `affected`'s lex range from a sibling — sub-inserting K shifts `affected`'s lex range
> down to K, and the slot ordering (by sparse partial) no longer matches the lex order
> (by firstKey). I7 (partials ascending) still holds; I8 (children-by-firstKey) breaks.
> Per `HOTInvariantValidator.java:377-380`, **"I8 is NOT merely cosmetic: HOTRangeCursor
> does in-order trie traversal with a parent stack ... so a range scan is correct only
> when child-index order equals key order"** — so this is a real correctness regression.
>
> **The Stage 0 off-path-straddle finding (proven canonical, no I6 misroute) lives
> WITHIN a child.** True Direction 1 *lifts* the straddle up to the sibling boundary at
> d*, where I8 requires strict lex ordering — the canonical envelope ends there.
>
> **Mitigation landed (Site 1 only, this iteration):** on C2, **`return false`** from
> `tryBranchIncremental` — caller (`branchAboveLeaf`) falls through to
> `rebuildSubtree(navResult, analysis.insertDepth(), …)` — a *scoped* rebuild at d*
> instead of `rebuildWholeIndex` (= scoped at depth 0). Strictly better than the
> baseline self-heal (smaller scope, bounded by d*'s subtree size, not the index),
> and the C2 firings drop out of `SELF_HEAL_FIRINGS` entirely (no exception bubbles up,
> so the `mergeIntoLeaf` / `branchAboveLeaf` catches don't fire on this path).
>
> **Net change vs baseline (2026-05-20).**
> - `SELF_HEAL_FIRINGS`: 118 → 15 (Issue B unchanged; this plan's §4.3 retry is the
>   follow-up to drop the remaining 15).
> - Branch-path C2: 103 self-heal whole-index-rebuilds → 103 scoped rebuilds at d*.
> - `interleavedInsertDeleteMultiRev` and all 79 canary tests green.
>
> **Iteration 2 — true Direction 1 with I8-safety pre-check (LANDED 2026-05-20).**
> Added `isDirectionOneI8Safe(navResult, insertDepth, affectedIdx, keySlice)`:
> - O(1) short-circuit: if K ≥ affected.firstKey, K cannot become the new leftmost
>   key of `affected` — no firstKey change propagates → safe.
> - Else K becomes affected's new firstKey. Check I8 at d* around `affectedIdx`:
>   `prev.firstKey < K < next.firstKey` must hold (each comparison is one leftmost-walk
>   per sibling, bounded by tree height).
> - K's firstKey-change propagates upward as long as the current slot is 0 (leftmost).
>   Walk the spine from d* upward; at each level where the current slot is 0, repeat
>   the I8 check around the parent's slot. Stop when a non-zero slot is encountered
>   (further ancestors unaffected).
>
> On safe → `subInsertAt(affectedRef, K, V)` and `DIRECTION_ONE_SUBINSERT++`. On unsafe
> → `return false` (the iteration 1 scoped-rebuild fallback) and `DIRECTION_ONE_FALLBACK++`.
> Total cost per firing: O(height × siblings_checked) ≤ O(MAX_PATH_DEPTH²) — tiny vs a
> rebuild.
>
> **Empirical hit rate** (`Direction1HitRateProbe.measureC2HitRateOnInterleavedWorkload`,
> the same workload pattern as `interleavedInsertDeleteMultiRev` — 10 revs × 1000 entries
> with overlapping ranges):
> - C2 firings total: **16**.
> - Resolved via D1 sub-insert: **11 (68.8%)** — no rebuild.
> - Fallback to scoped rebuild: **5 (31.3%)** — I8-unsafe (MSDB-closure gap).
> - `SELF_HEAL_FIRINGS`: 3 (Issue B unchanged — follow-up §4.3).
>
> So roughly 2/3 of the C2 firings are now resolved purely incrementally, and the
> remaining 1/3 use the strictly-smaller scoped rebuild (still better than baseline's
> whole-index self-heal).
>
> **Still deferred.** Sites 2 (`branchFullNodeAtExistingBit`) and 3 (boundary-child
> not-full handler) keep their existing `return false` catches — applying iteration 2's
> sub-insert at those sites is a follow-up. Issue B (§4.3) is the bigger remaining win
> (drops `SELF_HEAL_FIRINGS` to 0 → enables Stage 3b deletion of the self-heal arms).
>
> **Iteration 3 — scope Issue B's rebuild to pathDepth-1 — ATTEMPTED + REVERTED
> (2026-05-20).** Tried changing `mergeIntoLeaf`'s catch from `rebuildWholeIndex` to
> `rebuildSubtree(navResult, pathDepth - 1, …)` (= scoped at the deepest indirect, N,
> = L's parent). Result: `HOTVersionedLeafStressTest$MultiRevisionIsolation.oracleVerifiedMultiRevRangeQueries`
> failed (38 of 90 expected range-query entries returned at the latest revision).
>
> **Root cause:** `rebuildSubtree` replaces N's page at N's parent's child reference,
> but leaves the parent's *stored partial* for that slot stale. If the rebuild changes
> N's `firstKey` (which it does when K becomes the new minimum in N's subtree),
> `parent.partials[slotOfN] = PEXT(oldN.firstKey, parent.mask)` no longer matches
> the new N's `firstKey` — and routing at the parent level becomes inconsistent with
> the routing N expects. Whole-index rebuild (`rebuildWholeIndex`) avoids this because
> it rebuilds the root too, re-canonicalizing every partial top-down. **Lesson:**
> scoping a rebuild below the root requires either no-firstKey-change OR a partials-update
> cascade up the spine. The latter is essentially the I8-safety pre-check applied in
> the rebuild direction — a non-trivial extension. Iteration 1+2's branch-path
> `rebuildSubtree(insertDepth)` works because branch inserts are constrained: K's PEXT
> at every ancestor above insertDepth matches the existing partial structure (the
> descent's subset-match condition). Merge-path rebuilds have no analogous constraint —
> K can become the new firstKey at any level.
>
> Issue B keeps the whole-index rebuild self-heal for now. The follow-up is the true
> §4.3 incremental handler (slot-replacement L → L₀, L₁ placement at comboPartial via
> `addChildAtCombination` with the I8-safety check from iteration 2 applied per-L₁-key).
>
> **Iteration 5 — conditional Issue B scope guard — ATTEMPTED + REVERTED (2026-05-20).**
> Hypothesis: iter-3's failure was N.firstKey changing → stale parent partial. Add a
> pre-check: if `keySlice >= N.firstKey`, K can't become N's new firstKey, so a scoped
> rebuild at pathDepth-1 should be safe. Otherwise, escalate to `rebuildWholeIndex`.
>
> **Result:** same `oracleVerifiedMultiRevRangeQueries` failure as iter-3 (38 of 90
> expected entries). The escalation guard was insufficient — even when
> `K >= N.firstKey` (so N.firstKey is provably unchanged after the rebuild), the
> scoped rebuild still breaks range scans on this workload. The stale-parent-partial
> hypothesis from iter-3 doesn't fully explain it. Open question: what *else* does a
> scoped rebuild at pathDepth-1 mutate that the whole-index rebuild doesn't? Candidates
> to investigate next: (a) `releaseOrphanedHOTLeaves` closes leaves still referenced
> by a CoW chain higher up; (b) the rebuilt N's children's sparse partials shadow some
> structure that ancestors of N rely on; (c) `registerFreshSubtree`'s post-order walk
> at pathDepth-1 leaves an in-flight reference in an inconsistent state vs the
> ancestor chain. The root cause needs targeted instrumentation — a follow-up.
>
> **Iteration 8 — pinpointed: scoped rebuild OVER-PARTITIONS N relative to whole-index
> (2026-05-20).** A/B diagnostic on the failing test captured exact tree state:
>
> | Firing | Pre | Post(whole) | Post(scoped) |
> |---|---|---|---|
> | #1 (pathDepth=1) | 4 leaves / 1416 entries | 32 leaves / 1417 | (same — depth-0 == whole) |
> | #2 (pathDepth=2) | 36 leaves / 3234 entries | 32 leaves / 3235 | **64 leaves / 3235** |
>
> For firing #2 the SCOPED rebuild produces 64 leaves (32 untouched in slot 0 + **32
> brand-new leaves under N**, replacing the prior 4). HOTBulkBuilder, given N's ~400 entries
> in isolation, picks 32 disc bits (`N.partials=[0,1,2,3,...,19,32,34,35,36,38,40,42,43,44,46,48,50]`)
> and ~12 entries/leaf — vastly more aggressive than the whole-index build, which spreads
> the same 3235 entries over only 32 leaves (~100 entries/leaf). The new N is at the
> 32-child cap.
>
> All children have valid logKeys and NULL persistent keys (state is normal for fresh
> TIL-resident pages). The test still fails -- 38 of 90 entries returned by the
> range query at the latest revision -- but the failure happens AFTER commit + read.
> So the root cause is either:
>
> 1. **Commit-time persistence.** The scoped rebuild's CoW chain (CoW'd root + new N
>    in TIL) doesn't propagate persistent keys correctly when serializing. New N's slot
>    in root might be written as a NULL reference rather than chained-with-child-first
>    persistence.
> 2. **Read-time routing.** The over-partitioned N's structure (32 disc bits, many of
>    which are "off-path" relative to the canonical whole-index structure) makes the
>    PEXT/lex-fallback search miss leaves.
> 3. **Cursor sibling traversal.** With 32 children on N + cursor's parent-stack
>    advancement, some edge of the traversal logic mismatches.
>
> True incremental Issue B (plan §4.3) avoids this entirely by NOT calling
> HOTBulkBuilder on N's subtree — it surgically places L₀ in L's slot + L₁ at
> comboPartial, preserving N's existing structure. This sidesteps the over-partitioning
> root cause. **Reaching Stage 3b (delete the self-heal) now depends on §4.3, not on
> any further scoped-rebuild tuning.**
>
> **Iteration 7 — hypothesis (a) experiment: skip releaseOrphanedHOTLeaves — DOESN'T HELP
> (2026-05-20).** Implemented a `rebuildSubtreeNoRelease` variant identical to
> `rebuildSubtree` but with the `releaseOrphanedHOTLeaves` call removed, then routed
> Issue B's scoped path through it. Test still failed with the same 38/90 range-query
> result. So **hypothesis (a) is eliminated** — orphaned-leaf close is not the cause.
> Hypotheses (b) "rebuilt children's sparse-partial shadowing" and (c)
> "`registerFreshSubtree` post-order ref-state" remain candidates; investigating either
> needs a targeted state diff (TIL snapshot + root-side partial comparison before/after).
> The experimental variant was removed; code state remains `rebuildWholeIndex` for
> Issue B.
>
> **Iteration 6 — diagnostic instrumentation on `oracleVerifiedMultiRevRangeQueries`
> (2026-05-20).** Captured the two Issue B firings during the test verbatim:
> ```
> firing #1: pathDepth=1 N.height=1 N.children=4 K=8000…0006 N.fk=8000…0003 kGteN=true
>            ex=IllegalArgumentException:"split bit 131 is already a discriminative bit"
> firing #2: pathDepth=2 N.height=1 N.children=4 K=8000…000c N.fk=8000…0009 kGteN=true
>            ex=IllegalArgumentException:"split bit 131 is already a discriminative bit"
> ```
> Both `kGteN=true` (K ≥ N.firstKey → no firstKey change after rebuild). Firing #1 at
> pathDepth=1 → scoped depth=0 = whole-index, fine. **Firing #2 at pathDepth=2 → scoped
> depth=1 (= deeper than root) — this is the one that breaks `oracleVerifiedMultiRevRangeQueries`
> with firstKey provably preserved**, eliminating hypothesis (a)+(b)+(c)'s firstKey-based
> framing. The bug is something stricter than "stale parent partial" — possibly tied to
> `releaseOrphanedHOTLeaves` closing in-memory pages still referenced via the multi-revision
> fragment chain. Targeted scoped-rebuild instrumentation (TIL state before/after, fragment
> chain integrity check) is the next investigation step. Code state remains
> `rebuildWholeIndex` for Issue B.
>
> Sites 2 and 3 already do "return false → scoped rebuild" via their existing catches
> (Stage 1 + Stage 2 conservative implementations). The §11.5 / §11.6 designs for those
> sites are equivalent to the Site 1 change landed now: they keep the same `return false`
> fallback. No code change at Sites 2 and 3 in this iteration.

### §11.1 The routing tautology

`findChildIndex` is deterministic. The descent for K runs `findChildIndex` at each
level and ends in some leaf via `affectedChildIndex` at d*. After the insert, the
partials at d* are unchanged (sub-insert never touches d*'s `partialKeys`), so
**re-running `findChildIndex(K)` at d* returns the same `affectedChildIndex`**.
Therefore, if K is placed anywhere inside `affected`'s subtree, root-routing for K
reaches `affected`, then descends inside `affected` to K's leaf — and K is found.
Conversely, if K is placed in *any other* subtree under d* (including `c'`), root-routing
still reaches `affected` and K is unreachable. This is the I6 failure observed in
Stage 4b's 11 misroutes.

Direction 1 is therefore: **on C2, sub-insert K into `affected`, not into `c'`**.

### §11.2 Why Stage 1's not-full betaIsDiscBit handler does not have this problem

Stage 1's `addChildAtCombination` adds K's leaf as a *new* sibling at slot `comboPartial`.
By the I7 sort-ascending-partials invariant and `comboPartial > affected.partial` (the
β-column is the only difference and `comboPartial`'s β-bit equals `betaValue`, while
`affected.partial`'s β-column is 0 by the off-path-straddle reasoning in §11.3),
the new sibling's index is strictly greater than `affectedChildIndex`. Highest-index
subset-match picks the new sibling for K → root-routing reaches K's freshly-folded
leaf. `BetaIsDiscBitRoutingProbe` Q1 verified 134/134 cases of this.

C2 fires only when `comboPartial` is already occupied. Then `addChildAtCombination`
cannot create the new sibling — and Direction 1 redirects K downward into `affected`
instead.

### §11.3 Formal verification

**Convention.** A node's *partial* at d* is the sparse-path encoding: each disc-bit
column holds `firstKey(child)[bit]` (the first key's bit-value at that disc-bit
position). I5-route: for every key K' in the child's subtree and every disc-bit column
`b` of d*, if `partial[b] = 1` then `K'[b] = 1` (the route only constrains when the
column is *set*; a `0` column allows both values — the off-path-straddle case).

#### Lemma 1 — affected.partial has β-column = 0

> In any C2-firing configuration, `affected.partial[β] = 0`.

*Proof.* C2 fires inside the not-full `betaIsDiscBit` branch of `tryBranchIncremental`.
Therefore β ∈ D(d*), and `β = mismatchBit(K, residentInAffected)` per
`analyzeDescent`. By the msdb definition K and residentInAffected agree on bits more
significant than β and differ at β. K[β] = `betaValue`; residentInAffected[β] =
`1 − betaValue`.

Assume `affected.partial[β] = 1`. Then by I5-route every key in `affected` has β = 1.
But residentInAffected ∈ `affected` has β = `1 − betaValue`. So `betaValue = 0` and
residentInAffected[β] = 1 — consistent only if `affected.partial[β] = 1`.

Conversely if `betaValue = 1`, residentInAffected[β] = 0 — and `affected.partial[β] = 1`
contradicts I5-route (a key with β=0 cannot live in a slot whose partial requires β=1).

So one of two cases:
- `betaValue = 0`, `affected.partial[β] = 1` — every `affected` key has β=1 except
  residentInAffected has β=0. Contradiction. Impossible.
- `betaValue = 1`, `affected.partial[β] = 1` — every `affected` key has β=1.
  residentInAffected has β=0. Contradiction. Impossible.

Therefore `affected.partial[β] = 0` in every C2 configuration. ∎

This is the off-path-straddle case proven canonical in
`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §3.1: β ∈ D(d*), `affected.partial[β] = 0`,
keys in `affected` straddle β.

#### Lemma 2 — root-routing for K reaches `affected`

> Before AND after sub-inserting K into `affected`, `findChildIndex(K)` at d* returns
> `affectedChildIndex`.

*Proof.* `findChildIndex` reads only d*'s `partialKeys`, `bitMask`, and K's bytes. The
sub-insert mutates only pages strictly below d*'s child-reference array — never d*'s
partials or mask. So the inputs to `findChildIndex` at d* are bit-identical before and
after. Determinism gives the same result: the slot the descent already chose,
i.e. `affectedChildIndex`. ∎

#### Lemma 3 — sub-inserting K into `affected` preserves I5-route at d*

> Adding K to the key-set of `affected`'s subtree leaves d*'s `partials[affectedChildIndex]`
> a valid I5-route witness.

*Proof.* I5-route requires that for each column `b` set in `affected.partial`, every key
in `affected`'s subtree (including the new K) has bit `b` = 1. The descent picked
`affected` because K's densePK subset-matches or exact-matches `affected.partial`:
`(affected.partial & ~K_densePK) == 0`. Equivalently every set bit of
`affected.partial` is set in K_densePK, i.e. K has those disc bits = 1. So K satisfies
I5-route at every set column of `affected.partial`; the unchanged keys in `affected`
satisfy it by hypothesis. ∎

#### Theorem 1 — Direction 1 is invariant-clean and routing-correct

> Let `affectedRef = d*.getChildReference(affectedChildIndex)`. If
> `subInsertAt(affectedRef, K, V)` returns true on a canonical-enough trie, the
> resulting trie is invariant-clean and root-routes K correctly.

*Proof.* By induction on tree height. Base: `affectedRef` resolves to a leaf — `subInsertAt`'s
`dispatchInsert` reduces to `mergeIntoLeaf` at depth 0 of the extended path, which is
the standard merge-vs-branch dispatch — by Stage 0 + this plan's §4.3, invariant-clean.
Step: `affectedRef` resolves to a compound. `subInsertAt` re-descends through it via
`findChildIndex(K)` and re-runs `dispatchInsert` at the deeper path. Each handler
preserves invariants on its subtree by Stage 0/1/2 + §11.4 (the recursive Direction 1
catch). Splicing the modified subtree under the unchanged d* keeps d*'s invariants
intact (Lemmas 2, 3).

Routing: root → ... → d* → `affected` (Lemma 2) → ... → K's leaf (the extended-path
descent that placed K). ∎

#### Theorem 2 — height-minimal preserved

> Direction 1 does not inflate trie height beyond `minheight(S ∪ {K})`.

*Proof.* `subInsertAt` only descends and dispatches the standard handlers, each proven
height-preserving (Stage 0/1/2 §5.2-3, plan §5.4). It never adds a level above d*.
Since the standard handlers never raise height beyond the minimum for the post-insert
key set, height stays at the minimum. ∎

#### Theorem 3 — termination

> Direction 1 terminates in O(height) operations per C2 firing.

*Proof.* `subInsertAt`'s descent strictly extends the path each call; bounded by
`MAX_PATH_DEPTH` (= 64). Each call does O(node) findChildIndex + O(handler-work). A C2
at depth d* re-descends to ≤ MAX_PATH_DEPTH − insertDepth deeper levels. Worst case is
linear in tree height. ∎

### §11.4 Recursive C2

`subInsertAt`'s descent into `affected` may itself fire C2 at a deeper level. The catch
at each `addChildAtCombination` site (§11.5) handles this — `subInsertAt` recurses into
the *new* `affectedChildIndex` at the new depth. Bounded by `MAX_PATH_DEPTH`; the
defensive `return false` in `subInsertAt` (descent-failure) falls back to the caller's
scoped rebuild, which is unreachable on canonical-enough trees but stays as a defensive
gate.

### §11.5 The fix — wire the catch at three sites

Each `addChildAtCombination` call inside `tryBranchIncremental` is wrapped:

```java
try {
  final HOTIndirectPage newNode = HOTIncrementalInsert.addChildAtCombination(
      node, comboPartial, swizzle(comboLeaf), node.getHeight(), revision,
      pageKeyAllocator);
  pathRefs[insertDepth].setPage(newNode);
  registerFreshSubtree(pathRefs[insertDepth]);
  return true;
} catch (IllegalArgumentException c2Collision) {
  // Direction 1: comboPartial coincides with an existing child c' of d*. Sub-insert
  // K into AFFECTED (the slot routing already picked) — see §11.1-3.
  comboLeaf.close();
  final PageReference affectedRef = node.getChildReference(analysis.affectedChildIndex());
  return subInsertAt(affectedRef, keyBuf, keyLen, valueBuf, valueLen);
}
```

Three sites:
1. `tryBranchIncremental` not-full betaIsDiscBit (current uncaught — bubbles to
   `branchAboveLeaf`'s self-heal): `affected = node.getChildReference(analysis.affectedChildIndex())`
   where `node = pathNodes[insertDepth]`.
2. `branchFullNodeAtExistingBit`'s half-fold (Stage 1 — its catch currently returns
   false to the caller's scoped rebuild): `affected` is the affected half's affected
   child slot (the half-local index translates via §11.6).
3. The boundary-child not-full handler (Stage 2 — its catch at line 1078 returns
   false): `affected = child.getChildReference(childSlots[insertDepth + 1])` where
   `child = pathNodes[insertDepth + 1]`.

Site 1 needs a try/catch added; sites 2 and 3 already have try/catch arms that just
need to switch from "return false → rebuild" to "subInsertAt(affectedRef, ...)".

### §11.6 Site-2 specifics — the half-fold affected reference

`branchFullNodeAtExistingBit` splits d* at d*.MSB into two halves, then folds K into
the half that K's β-bit selects. The fold uses `addChildAtCombination` on the half at a
half-local `comboPartial`. On C2 inside the half, the colliding child is in the
half — and the routing tautology applies at the half: `findChildIndex(K)` on the
half (post-fold) reaches the half's `affected` slot. So:

```java
final int affectedSlotInHalf = /* the slot the fold computed as the half's affected */;
final PageReference affectedRefInHalf = half.getChildReference(affectedSlotInHalf);
return subInsertAt(affectedRefInHalf, keyBuf, keyLen, valueBuf, valueLen);
```

Concretely the half's affected slot is derived from `info.firstAffected()` mapped to
the half's coordinates (the fold-time half-local index). The half is a fresh
`HOTIndirectPage` produced by `compressHalf` — its `partialKeys` re-encode the original
node's partials, and the half-local affected slot corresponds to the original
`info.firstAffected()` offset inside the affected run on the post-split half.

### §11.7 Out of scope (this plan iteration)

The merge-path Issue B (§4.3) is independent and is **not modified by this plan
iteration**. Its slot-replacement + comboPartial placement remains correct under
Sirix's sort-by-partial invariant: L₁'s `comboPartial` is `L.partial | betaBit` > L's
partial, so L₁ lands at a strictly higher slot index than L₀; highest-index-subset-match
routes β=1 keys to L₁ correctly. The L₁'s-combo-collides-with-c' sub-case still has the
same routing question as Issue A — if it ever fires in practice, the *same* Direction
1 redirection applies (sub-insert each L₁ key into the affected slot at N, not c'). Stage
3a's measurement shows Issue B at 15 firings on the canaries; a follow-up probe will
classify whether the L₁-combo-collides-with-c' sub-case actually arises among them.

### §11.8 Test plan (Stage 4b retry)

1. Wire the catch at site 1 of `tryBranchIncremental` (the not-full betaIsDiscBit case
   — currently uncaught). Sites 2 and 3 keep their existing catch arms but swap "fall
   back to scoped rebuild" for "subInsertAt(affectedRef, …)".
2. Run `HOTVersionedLeafStressTest$DeleteAtScale.interleavedInsertDeleteMultiRev` —
   the test that produced the 11 I6 violations in Stage 4b. Expected: green, 0
   I6 violations.
3. Run `HOTFormalVerificationTest` (all canaries) — expect 0 misroutes, 0 missed
   values; `SELF_HEAL_FIRINGS` drops from 118 to ≤ 15 (only Issue B remains).
4. Run `BetaIsDiscBitRoutingProbe`, `StraddleCanonicityProbe`,
   `HOTIndirectPageSplitFaithfulTest`, `HOTIntegrateTest` — all must stay green.
5. **New probe — `MergeIntoAffectedProbe`** — construct cases where `comboPartial`
   collides with an existing c'; verify Direction 1 yields a strict-routing-clean,
   detector-clean tree (mirror of `BetaIsDiscBitRoutingProbe`'s Q1, but for the C2
   sub-case).
6. Stage 3b — if `SELF_HEAL_FIRINGS` drops to 0 after the Issue B handler also lands
   (separate follow-up), delete the self-heal arms and `rebuildWholeIndex`. If it
   stays at ~15 (Issue B not yet handled), defer Stage 3b until then.

### §11.9 Review passes

**Pass 1 (the tautology).** *Confirmed.* Routing is deterministic; the descent chose
`affected`; sub-inserting K into `affected` makes routing find K. Stage 4b broke this
by retargeting to `c'`.

**Pass 2 (Lemma 1 — `affected.partial[β] = 0`).** *Confirmed.* The off-path-straddle
finding shows this is canonical (`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §3.1).

**Pass 3 (I5-route after sub-insert).** *Confirmed.* Lemma 3 — K satisfies
`affected.partial`'s set bits because the descent's subset-match condition holds.

**Pass 4 (recursive C2).** *Confirmed.* §11.4 — the catch is at every `addChildAtCombination`
site, so a deeper C2 dispatches the same redirection. Bounded by `MAX_PATH_DEPTH`.

**Pass 5 (CoW + TIL).** *Confirmed.* `subInsertAt` already CoWs the sub-path
(local `prepareIndirectPage` chain) and calls `registerFreshSubtree` via `dispatchInsert`
→ handlers. d*'s parents are unaffected (their child references to d* are unchanged;
d* itself is in the TIL from the original descent).

**Pass 6 (height-minimal).** *Confirmed.* Theorem 2 — sub-insert never adds a level
above d*; the recursive handlers are height-preserving.

**Pass 7 (Stage 4b's misroutes will pass).** *Verifying.* The 11 misroutes happened
because K was placed in c' but routing went to affected → K missed. Direction 1 puts K
in affected → routing finds K. Expected: 0 I6 violations on the same workload.

**Pass 8 (Site 2 — half-fold).** *Caught.* The half's `affected` slot is the
half-local mapping of `info.firstAffected()`. §11.6 sketches it; implementation must
verify the half-local index lookup is correct.

**Pass 9 (no Issue B regression).** *Confirmed.* §11.7 — Issue B's 15 firings still
self-heal via the existing catch arm; no change to its handling. A separate plan
iteration handles Issue B (§4.3 retry or follow-up design).

**Pass 10 (orphaned `comboLeaf`).** *Caught.* On C2, the freshly-allocated `comboLeaf`
was for the abandoned new-sibling approach — must be `.close()`'d to release its
off-heap slot. The Stage 4b code did this; preserve it.

*Ten passes. Direction 1 is a one-keystroke retarget (`c'-ref → affected-ref`) of the
existing Stage 4a infrastructure, formally clean by the routing tautology, and
empirically falsifiable via the Stage 4b regression test. Ready for implementation.*
