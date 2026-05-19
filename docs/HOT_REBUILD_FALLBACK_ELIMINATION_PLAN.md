# HOT rebuild-fallback elimination — design plan

**Status:** design, pending review. **Branch:** `fix/hot-strict-binna-conformance`.
**Follows:** `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` (Stage 0) + `HOT_BETAISDISCBIT_REBUILD_ELIMINATION_PLAN.md`
(Stages 1–2).
**Empirical trigger:** commit `c8ac969af` (Stage 3a) instrumented the structural self-heal with
`SELF_HEAL_FIRINGS`; running the HOT test suite yielded **118 firings — Stage 3b BLOCKED**.

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
