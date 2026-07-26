# HOT betaIsDiscBit rebuild elimination — design plan

**Status:** design, pending review. **Branch:** `fix/hot-strict-binna-conformance`.
**Follows:** `docs/HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` (removes the *leaf-overflow* rebuild).
This plan removes every *remaining* `rebuildSubtree` / `rebuildWholeIndex` call from the HOT
insert path, so a HOT insert is — after both plans land — a composition of verified
incremental primitives with **no `HOTBulkBuilder` rebuild of any subtree or the whole trie**.
**Empirically grounded:** `BetaIsDiscBitRoutingProbe` (3 tests, all green).

---

## §1. Problem

After the straddle-guard removal, the HOT insert path still falls back to a rebuild in
exactly three places — the **complete branch-path inventory**, read from
`AbstractHOTIndexWriter`:

- **(A)** `tryBranchIncremental:877-878` — `info.betaIsDiscBit() && node` **full** →
  `return false` → `branchAboveLeaf` → `rebuildSubtree(insertDepth)`. The branch key's
  mismatch bit β collides with an existing discriminative bit of a *full* compound node d*.
- **(B)** `tryBranchIncremental:932-933` — `singleEntry && !leafEntry`, the boundary child
  `pathNodes[insertDepth+1]` is **not full**, and **β is already a discriminative bit of
  that boundary child** → `return false` → `rebuildSubtree(insertDepth)`.
- **(C)** `branchAboveLeaf:840` and the twin in `mergeIntoLeaf` — `catch
  (IllegalArgumentException | IllegalStateException)` → `rebuildWholeIndex`. The
  structural self-heal.

`branchSplitFullNode` (`:1010`, the `!betaIsDiscBit` full-node case →
`splitIndirectWithEntry` + `integrate`) was read in full: it **always returns `true`**,
never falls back. The straddle-guard removal eliminates the two
`catch (HOTStraddleException)` arms. So (A), (B), (C) are *all* that remain.

> **(A) and (B) are one problem** — β colliding with an *existing* discriminative bit
> (`betaIsDiscBit`). The *not-full* form is already handled incrementally
> (`addChildAtCombination`). **(C) is downstream**: it catches a precondition violation;
> once (A) and (B) make every branch op total and invariant-clean-preserving, nothing
> throws and the `catch` is dead code.

---

## §2. Invariants, definitions, primitives

Invariants and the **invariant-clean** / **height-minimal** properties are as in
`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §2 (I1–I8, I11; invariant-clean ⟹ I6 routing —
`HOT_FORMAL_FOUNDATION.md` Theorem 2; height-minimal = trie height equals `minheight(S)`).

**betaIsDiscBit.** A branch insert routes key `K` to a leaf, diffs it against the
resident, gets mismatch bit `β = msdb(K, resident)`, and stops at compound node d* (=
`pathNodes[insertDepth]`). `getInsertInformation(d*, affectedChildIndex, β)` reports
`betaIsDiscBit = true` iff `β` is already one of d*'s discriminative bits — the
approximate descent crossed β rather than branching at it.

**The verified not-full primitives** (`HOTIncrementalInsert`, already ported, used on the
live path, exercised by `HOTIndirectPageSplitFaithfulTest`):

- `addChildAtCombination(node, comboPartial, newChildRef, height, …)` — adds one child at
  an *existing*-disc-bit combination. Disc bits unchanged. `comboPartial =
  subtreePrefix | (betaValue ? betaColumnWeight : 0)` — the affected subtree's above-β
  prefix, β set to K's value, below-β columns zero.
- `addEntryWithInsertInfo(node, β, betaValue, firstAffected, affectedCount,
  subtreePrefix, newChildRef, height, …)` — folds β as a **genuinely new** disc bit plus
  the new child. Precondition: `β ∉ D(node)`.
- `splitIndirect(node, …)` — splits a node at `node.MSB` into `BiNode(node.MSB, L, R)`;
  each half `compressHalf`'d (every disc bit *constant across the half* is dropped, the
  rest re-packed). Verified: `HOT_INCREMENTAL_SPLIT_VERIFICATION.md` Theorem II,
  `HOTIndirectPageSplitFaithfulTest`.
- `integrate(…)` — folds a `BiNode` into the spine, cascading a full parent. Verified.

---

## §3. The mechanism (root cause + the verified decomposition)

### §3.1 The not-full betaIsDiscBit case is correct (and was already wired)

`addChildAtCombination` places K's fresh single-entry leaf at `comboPartial`. The
campaign long suspected this was only "soft" — `comboPartial` is a *sparse* partial
(below-β columns zero), and K's real `densePK` carries the below-β bits, so
`comboPartial` is often a **strict subset** of `densePK(K)`. **`BetaIsDiscBitRoutingProbe`
settled it:** of 134 not-full betaIsDiscBit folds, 78 have `comboPartial ⊊ densePK(K)` —
and **all 134 route correctly** under strict, no-fallback root-to-leaf descent
(`misroutes=0`). Highest-index subset-match is exactly the right rule: a strict-subset
stored partial still subset-matches, and no higher-index sibling shadows K's combo-child
(`HOT_FORMAL_FOUNDATION.md` Theorem 2 — the LCA block-bit argument). End-to-end: 500 K
CAS keys, **both** reader fallbacks disabled (`-Dhot.cas.leftmostfallback.disable=true
-Dhot.strict.phase7u.lexfallback.disable=true`) → `hits=500000/500000`. So
`addChildAtCombination` does **not** rely on a reader fallback. It is genuinely correct.

### §3.2 The full-node decomposition — and the β-survival dispatch

When d* is **full**, no child can be added — d* must split. The decomposition (verified
by `BetaIsDiscBitRoutingProbe.fullNodeBetaIsDiscBitDecomposition`, **74/74 cases,
0 misroutes**, incl. 40-byte MultiMask `widespan` keys):

1. `BiNode split = splitIndirect(d*)` — `d*` splits at `d*.MSB` into two **not-full**
   halves (32 children → two halves, each ≤ 31).
2. K routes into one half by `d*.MSB`: `half = bitAt(K, d*.MSB) ? split.right :
   split.left`.
3. **Dispatch on whether β survived the split into that half.** `compressHalf` drops
   every disc bit *constant across the half*; β is a disc bit of d* but may or may not
   still branch children *within K's half*:
   - **β survived** (`β ∈ discriminativeBits(half)`) → still `betaIsDiscBit` for the
     half → `addChildAtCombination(half, comboPartial, K-leaf, …)`.
   - **β dropped** (`β ∉ discriminativeBits(half)`) → β is *constant* across the half, so
     β is now a **genuinely new** discriminative bit distinguishing {K} from the half →
     `addEntryWithInsertInfo(half, β, …)`.
4. Re-point the half's reference to the folded half; the `BiNode(d*.MSB, …)` now carries
   it.
5. `integrate` the `BiNode` at `insertDepth` — the standard capacity cascade.

> **This is why the prior 7 attempts failed.** A decomposition (`splitIndirect` then
> `addChildAtCombination`) *was* tried (`hot-phase3-checkpoint`) and failed
> `assertRoutesAll`. It used `addChildAtCombination` **unconditionally**.
> `addChildAtCombination`'s `comboPartial` is computed against the half's disc bits
> *assuming β is among them*; when `compressHalf` has dropped β, that index is wrong and
> K misroutes. The probe shows the split sends K to a β-survived half in 32 cases and a
> β-dropped half in 42 — **the dispatch is mandatory, not optional.** With it, 74/74
> route correctly.

### §3.3 The prior "blocked" diagnosis is void

`hot-betaisdiscbit-fullnode-blocked` declared betaIsDiscBit-full "blocked — fires only on
an already-non-canonical (I5-violated) subtree," proved via "`betaValue=0` ⟹
non-canonical." That proof used the **raw-constancy I5** debunked in
`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §3. Under the real one-sided I5-route, `betaValue=0`
is *normal* on a canonical tree. The probe confirms: betaIsDiscBit-full arises on
freshly-`HOTBulkBuilder`-built (canonical) trees and is fully solvable. "Blocked" is void.

---

## §4. The fix

### §4.1 (A) — betaIsDiscBit + full d*

Replace `tryBranchIncremental`'s `return false` (`:878`) with a new method
`branchFullNodeAtExistingBit(navResult, node, insertDepth, beta, betaValue, keySlice,
valueSlice)` implementing §3.2:

```
split      = splitIndirect(d*)
kHalf      = bitAt(K, d*.MSB) ? split.right : split.left
halfPage   = kHalf.getPage()
if halfPage is a leaf            → §6 C1 (1:31 lone-child half)
half       = (HOTIndirectPage) halfPage
halfInfo   = getInsertInformation(half, half.findChildIndex(K), beta)
keyLeaf    = fresh single-entry HOTLeafPage(K, V)
folded     = halfInfo.betaIsDiscBit()                       // == (beta ∈ D(half))
               ? addChildAtCombination(half, comboPartial(half, halfInfo, beta, betaValue),
                                       swizzle(keyLeaf), half.height, …)
               : addEntryWithInsertInfo(half, beta, betaValue, halfInfo.firstAffected(),
                                       halfInfo.affectedCount(), halfInfo.subtreePrefix(),
                                       swizzle(keyLeaf), half.height, …)
kHalf.setPage(folded)
result     = integrate(pathNodes, spineRefs, childSlots, insertDepth, split, …)
registerFreshSubtree(result.touchedRef())
```

`comboPartial(half, halfInfo, beta, betaValue)` = `halfInfo.subtreePrefix() | (betaValue
== 1 ? 1 << (discriminativeBits(half).length - 1 - betaColumnInHalf) : 0)` — identical to
the not-full d* form, recomputed in the *half's* coordinate space (this re-derivation is
the §3.2 fix).

### §4.2 (B) — betaIsDiscBit at the boundary child

`tryBranchIncremental`'s `singleEntry && !leafEntry` case ("Binna's false positive — the
insert depth was one level too shallow") at `:932-933` returns `false` when β is already
a disc bit of the boundary child `c = pathNodes[insertDepth+1]`. This is *exactly*
betaIsDiscBit, one level down. Replace the `return false` with the §3.1/§3.2 handling
applied to `c`:

- `c` **not full** → `addChildAtCombination(c, comboPartial(c, …), K-leaf, …)`; re-point
  `pathRefs[insertDepth+1]`.
- `c` **full** → the §4.1 decomposition on `c`, integrated at `insertDepth+1`.

(The `c`-full sub-case currently flows to `tryBranchIncremental:976`, which wraps `c`
under `BiNode(β)` — but β is *inside* `c`, so that BiNode would put β twice on one path,
an I11 breach. §6 C5 — this plan routes a full `c` with β ∈ D(c) through the §4.1
decomposition instead.)

### §4.3 (C) — the structural self-heal becomes dead

Once (A) and (B) land, every `tryBranchIncremental` path is a composition of total,
invariant-clean-preserving primitives (§5). The `IllegalArgumentException |
IllegalStateException` the self-heal catches are thrown only by *precondition guards*
(`integrate`'s trie-condition check; `addEntry*` "β already a disc bit"; `splitIndirect`
"≥ 2 children"; `addChildAtCombination` "partial already a child"). On a tree kept
canonical by the §5 induction those preconditions hold — nothing throws. The plan:

1. Land (A) + (B). Re-add the per-insert validator probe (`-Dhot.diag.perInsertValidate`,
   I5-route + I6 — `HOTMalformedSubtreeDetector`). Run every canary.
2. With the probe confirming **zero** structural exceptions across all canaries, the
   `catch` arm is unreachable: **delete** the `catch (IllegalArgumentException |
   IllegalStateException)` arms and — once also unused by the straddle-guard removal —
   `rebuildWholeIndex`, `rebuildSubtree`, `collectSubtreeEntries`, `collectSubtreeLeafRefs`.
3. Defensive residue (optional): keep a `-Dhot.assert.canonical` debug check
   (`HOTMalformedSubtreeDetector` on the touched subtree) — off in production, never a
   rebuild.

**`consolidateSubtree` is *not* a rebuild** and stays: it is the periodic incremental
leaf-consolidation sweep (merges adjacent under-full *leaf* pages), not a `HOTBulkBuilder`
reconstruction.

---

## §5. Formal verification

Inductive hypothesis: the trie is **canonical-enough** — invariant-clean and
height-minimal — before insert `i` (base: the empty index). The merge path and the
straddle-free `addEntry` path preserve this (`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §5).
This section discharges the branch path.

### §5.1 Theorem 1 — the (A) decomposition preserves invariant-cleanness

> betaIsDiscBit + full d*: the §4.1 decomposition yields an invariant-clean trie.

*Proof.* `splitIndirect(d*)` yields `BiNode(d*.MSB, L, R)` with `L`, `R` invariant-clean
and `d*.MSB` more significant than every bit of `D(L)`, `D(R)`
(`HOT_INCREMENTAL_SPLIT_VERIFICATION.md` Theorem II). K's half `H ∈ {L, R}` is not full
(d* had `MAX_NODE_ENTRIES` children; a split at `d*.MSB` is a non-degenerate prefix/suffix
cut — `splitIndirect:228` rejects `s∈{0,n}` — so each half has `1 ≤ k ≤ 31` children).
Two cases by §3.2's dispatch:

- **β ∈ D(H).** `addChildAtCombination(H, comboPartial, K-leaf)` adds one child, disc
  bits unchanged. I3 — `comboPartial` distinct (the op throws on collision — §6 C2);
  I4/I7 — inserted at its ascending position, `comboPartial > 0` so it never displaces
  `p₀ = 0`; I5-route — K's leaf has the single key K and `comboPartial ⊆ densePK(K)`
  (probe Q2: `comboPartial`'s set bits are H's affected-subtree on-path bits above β plus
  β=K's value; K shares the above-β prefix and has K[β], so all are set in `densePK(K)`);
  I8 — for a canonical node I7 ⟺ I8 (`HOT_FORMAL_FOUNDATION.md` Theorem 1); I11 — disc
  bits unchanged, K's leaf is height 0. `H'` invariant-clean. ∎
- **β ∉ D(H).** `compressHalf` dropped β because H's children are all β-constant; K
  differs from them at β (`β = msdb(K, resident)`), so β genuinely partitions {K} vs H —
  a valid new disc bit. `addEntryWithInsertInfo(H, β, …)` folds it (precondition `β ∉
  D(H)` holds). This is the *same* verified op the live not-full multi-affected branch
  case uses (`tryBranchIncremental:993`); it preserves I3–I8/I11 (its existing
  correctness). β's significance: β was a non-MSB disc bit of d*, `D(H) ⊆ D(d*)`, so β
  slots consistently among `D(H)`; β below `d*.MSB` keeps I11 vs the `BiNode`. ∎

The folded `H'` replaces `H` under `BiNode(d*.MSB, …)`; `d*.MSB` above `D(H') ⊇ D(H) ∪
{β?}` (β was below `d*.MSB`). `integrate` folds the `BiNode` — its standard cascade,
verified. ∴ invariant-clean. ∎

### §5.2 Theorem 2 — routing (I6)

Invariant-clean ⟹ I6 (`HOT_FORMAL_FOUNDATION.md` Theorem 2). Directly corroborated:
`BetaIsDiscBitRoutingProbe` — not-full 134/134, full-node decomposition 74/74, canonical
cross-check 100%, **0 misroutes** under strict no-fallback descent; end-to-end 500 K CAS
keys 100% readable with both reader fallbacks disabled. ∎

### §5.3 Theorem 3 — height-minimal

d* is **full**. A full node accepting a new child must split — this is the forced
capacity case. `splitIndirect` + `integrate` is the *same* cascade the leaf-overflow
capacity case and `branchSplitFullNode` already use; by `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md`
§5.2 Theorem 3 (cascade clause) it raises trie height by one level only when the cascade
reaches the root — i.e. only when every node on the insert path is full, exactly the
condition under which `minheight` itself rises. So the decomposition is height-minimal.
For (B) not-full, `addChildAtCombination` adds a child to a non-full node — height
unchanged, minimal by the same argument as the straddle plan's §5.2 `addEntry` clause. ∎

### §5.4 Theorem 4 — termination, and no rebuild

`splitIndirect`, the dispatch, the fold, `integrate`'s cascade (strictly decreasing
depth) — all terminating, all O(children)/O(height). No path enters `rebuildSubtree`.
Per-insert cost is O(node) + O(height), never O(subtree). ∎

### §5.5 Theorem 5 — the self-heal (C) becomes unreachable

With (A) and (B) handled, `tryBranchIncremental` returns `true` on every input — every
case is a composition of total primitives whose preconditions hold on a canonical tree
(Theorem 1 + the straddle plan's induction keep the tree canonical; `integrate`'s
trie-condition holds because every `BiNode` bit is correctly below the parent's;
`addChildAtCombination`'s "partial already a child" is pre-checked and re-routed — §6 C2).
So no `IllegalArgumentException | IllegalStateException` is thrown, the `catch` is dead,
and `rebuildWholeIndex` is unreferenced. **Discharge is empirical** (§4.3 step 2): the
per-insert probe must show zero structural exceptions across all canaries before the
`catch` is deleted. ∎

> **Confidence gradient (stated honestly).** (A) is **probe-verified and concrete**
> (`BetaIsDiscBitRoutingProbe` Q1/Q3/Q4). (B) is the *same mechanism* at depth+1 but is
> **not yet probe-verified at the boundary-child level** — §8 makes a (B)-specific probe
> a prerequisite to implementing it. (C) is **gated** on the empirical zero-exception
> result. The plan does not claim (B)/(C) are verified — it claims a verified path to them.

---

## §6. Corner cases (exhaustive)

| # | Case | Handling |
|---|---|---|
| C1 | **1:31 split** — K's half is a lone child (`splitIndirect`'s `compressHalf` returns the bare child reference, `:258-259`) | The probe `continue`d here (uncovered). Handling: a lone-child half is a single subtree `c`; K branches off it at β → pair them under a fresh `BiNode(β, …)` (K-leaf and `c`), which replaces the half. β is fresh *for this 2-entry node* (it has no disc bits yet). Then `integrate`. §8 adds a 1:31 probe case. |
| C2 | `addChildAtCombination`'s `comboPartial` **already a child** of the (not-full) node | K's combo coincides with an existing child `c` — K actually belongs *inside* `c`'s subtree (the descent stopped one level too shallow). Re-descend into `c` and re-run the branch analysis at that depth (the betaIsDiscBit handling is depth-recursive). Must be handled or it throws → (C). §8 probes it. |
| C3 | β **survives** the split into K's half | `addChildAtCombination` on the half — §4.1, Theorem 1 case 1. Probe: 32/74. |
| C4 | β **dropped** from K's half by `compressHalf` | `addEntryWithInsertInfo` re-adds β as a genuinely new bit — §4.1, Theorem 1 case 2. Probe: 42/74. **The dispatch that the prior attempts missed.** |
| C5 | (B) boundary child `c` is **full** and β ∈ D(c) | Currently `tryBranchIncremental:976` would wrap `c` under `BiNode(β)` — β twice on a path, an I11 breach. This plan routes it through the §4.1 decomposition on `c` (integrated at `insertDepth+1`). |
| C6 | MultiMask layout (disc bits span > 8 bytes) | `discriminativeBits` / `getInsertInformation` / `compressHalf` are absolute-bit-index based, layout-agnostic. Probe Q4 covers 40-byte MultiMask `widespan` keys (the prior "WIDE_SPAN beta=242" failure does **not** reproduce). |
| C7 | The decomposed `BiNode` cascades when `integrate`d (parent full) | `integrate`'s capacity cascade — verified, unchanged. The `BiNode`'s height is `d*.height + 1` (a split raises height one); `integrate` handles it exactly as the leaf-overflow cascade. |
| C8 | Folded half's height vs the `BiNode`'s recorded height | `addChildAtCombination` / `addEntryWithInsertInfo` keep the half's height (a leaf child never raises it), so `split.height()` stays valid after the fold — no recompute needed. |
| C9 | Tombstones in d*'s subtree | `splitIndirect` re-points child *references* only; `addChildAtCombination`/`addEntryWithInsertInfo` add K's leaf; no entry is dropped. Tombstones preserved (`hot-tombstone-preservation`). |
| C10 | Multi-revision / DIFFERENTIAL·INCREMENTAL versioning | All ops emit fresh CoW pages registered via `registerFreshSubtree` (fresh leaves `setCompleteDump(true)`). Removing the rebuilds *reduces* orphaned-leaf churn. §8 gates on `HOTVersionedLeafStressTest`. |
| C11 | d* is the index **root** and full | `splitIndirect` + `integrate` at `insertDepth = 0` → `integrate`'s new-root case raises height by one — forced (the root was full). Height-minimal (Theorem 3). |
| C12 | `comboPartial == densePK(K)` exactly (56/134 in the probe) vs strict subset (78/134) | Both route correctly — highest-index subset-match; exact-match preference picks K's combo-child when equal. Probe Q2. |
| C13 | β-survival check on a half that is itself a 1:31 lone *indirect* child | The lone child is an indirect node; `discriminativeBits(half)` is well-defined; the dispatch proceeds normally. Only a lone *leaf* child is C1. |
| C14 | The key is **already present** | `analyzeDescent` reports `keyAlreadyPresent`; `doIndex` merges the value (existing path) — betaIsDiscBit handling is not reached. |
| C15 | `getInsertInformation` on the half returns `affectedCount == half.numChildren` (β above the half's MSB) | β-dropped sub-case where the whole half is affected → the pull-up shape; `addEntryWithInsertInfo` with a whole-node affected run is the existing full-affected handling — verify in the (A) probe extension (§8). |
| C16 | After (A)+(B), a residual `IllegalArgumentException` still fires | Then the tree drifted — a real bug, not something to paper over. The per-insert probe (§4.3) surfaces it; do **not** ship the `catch` deletion until the probe is clean. |

---

## §7. Implementation steps

1. **`branchFullNodeAtExistingBit`** — new private method in `AbstractHOTIndexWriter`
   implementing §4.1 (parallel to `branchSplitFullNode`). Includes the C1 (1:31) and
   C2 (`comboPartial` collision → re-descend) handling.
2. **`tryBranchIncremental:877-878`** — replace `return false` with
   `return branchFullNodeAtExistingBit(…)`.
3. **`tryBranchIncremental:929-935`** — replace the `singleEntry && !leafEntry` +
   `return false` with the §4.2 boundary-child betaIsDiscBit handling (not-full →
   `addChildAtCombination` on `c`; full → `branchFullNodeAtExistingBit` on `c` at
   `insertDepth+1`). Route the C5 full-`c`-with-β∈D(c) case here too.
4. **Per-insert probe** — (re-)add the `-Dhot.diag.perInsertValidate` hook running
   `HOTMalformedSubtreeDetector` (I5-route + I6) after every `doIndex`.
5. **Verify zero structural exceptions** — run every canary (§8). Only when the per-insert
   probe is clean *and* no `IllegalArgumentException | IllegalStateException` fires:
6. **Delete the self-heal** — remove the `catch (IllegalArgumentException |
   IllegalStateException)` arms in `mergeIntoLeaf` and `branchAboveLeaf`; delete
   `rebuildWholeIndex`, and (once the straddle-guard removal has also dropped its caller)
   `rebuildSubtree`, `collectSubtreeEntries`, `collectSubtreeLeafRefs`.
7. **Optional** `-Dhot.assert.canonical` debug check (§4.3) — off by default.

Steps 1–3 are additive (new incremental paths); step 6 is the deletion, gated on step 5.

---

## §8. Test plan

A fix is accepted only when 1–6 are green.

1. **`BetaIsDiscBitRoutingProbe`** — kept as a permanent regression test (already green:
   Q1 not-full 134/0, Q3 canonical 100%, Q4 full-node decomposition 74/0).
2. **Extend the probe** — a (B)-specific case: betaIsDiscBit at a *boundary child* (the
   `singleEntry && !leafEntry` shape), not-full and full; the **C1** 1:31 lone-child
   half; the **C2** `comboPartial`-collision re-descend; **C15** whole-half-affected.
   Each: strict no-fallback routing + `HOTMalformedSubtreeDetector` clean. **This is the
   prerequisite for implementing (B) — §5.5 confidence gradient.**
3. **End-to-end, writer-driven** — `HOTFormalVerificationTest` canaries (ascending,
   descending, random-shuffle, bimodal, fully-random, random-with-replacement,
   conformance sweep, multi-rev) + `HOTVersionedLeafStressTest`: `violations=0`,
   `missedValues=0`, with both reader fallbacks disabled.
4. **Per-insert probe** — `-Dhot.diag.perInsertValidate` (I5-route + I6) reports **no
   drift** on every canary, and **zero** `tryBranchIncremental` rebuild-fallbacks and
   **zero** structural `IllegalArgumentException | IllegalStateException` (a
   `rebuildSubtree`/`rebuildWholeIndex`-call counter == 0).
5. **`HOTMicrobenchmark` + a random microbench** — `full-scan misses=0`; write CPU shows
   no `rebuildSubtree` frames; per-op latency improved vs the rebuild baseline.
6. **Counters** — assert betaIsDiscBit-full and betaIsDiscBit-boundary paths *fire* on
   the random canaries (the new code is exercised) and the rebuild counter is 0.

---

## §9. Review passes

**Pass 1 (inventory).** — *Confirmed* against the source: the only branch-path rebuilds
are (A) `:878`, (B) `:933`, (C) `:840`+twin; `branchSplitFullNode` always returns `true`.

**Pass 2 (the dispatch).** — *Caught the prior-attempt bug:* a `splitIndirect` +
`addChildAtCombination` decomposition fails when `compressHalf` drops β from K's half.
*Fix:* dispatch on `β ∈ discriminativeBits(half)` — survived → `addChildAtCombination`,
dropped → `addEntryWithInsertInfo`. Probe Q4: 32 survive / 42 drop, 74/74 route.

**Pass 3 (is the not-full case really sound?).** — *Confirmed:* probe Q1/Q2 — 134 folds,
78 with `comboPartial ⊊ densePK`, 0 misroutes, 500 K end-to-end with fallbacks disabled.
`addChildAtCombination` does not lean on a reader fallback.

**Pass 4 (the prior "blocked" verdict).** — *Voided:* it rested on raw-constancy I5
(`HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` §3); under I5-route `betaValue=0` is normal.

**Pass 5 ((B) confidence).** — *Caught an overreach:* (B) is the same mechanism but the
investigation only probed the d*-level. *Fix:* §5.5 states the confidence gradient
explicitly; §8.2 makes a (B)-specific probe a prerequisite — (B) is not implemented on
faith.

**Pass 6 (the (B) full-`c` I11 hazard).** — *Caught:* a full boundary child with β ∈ D(c)
currently flows to a `BiNode(β)` wrap (`:976`) putting β twice on a path. *Fix:* C5 —
route it through the §4.1 decomposition on `c`.

**Pass 7 (1:31 + collision).** — *Caught two uncovered cases:* the probe skipped the
lone-child half (C1) and the `comboPartial`-collision (C2). *Fix:* §4.1 and §6 handle
both; §8.2 probes them. C2's re-descend makes the betaIsDiscBit handling depth-recursive
— bounded by tree height.

**Pass 8 ((C) is not free).** — *Caught:* deleting the self-heal is sound *only* if
nothing throws. *Fix:* §4.3 / §8.4 gate the deletion on the per-insert probe showing zero
structural exceptions; C16 — a residual throw is a real bug, not to be re-buried.

**Pass 9 (height + termination).** — *Confirmed:* the decomposition is the existing
verified capacity cascade; height-minimal (Theorem 3), terminating, O(node)+O(height),
no rebuild.

**Pass 10 (scope).** — *Confirmed:* `consolidateSubtree` is incremental leaf merging, not
a rebuild — out of scope, retained. After this plan + the straddle-guard removal the HOT
insert path has **zero `HOTBulkBuilder` subtree/whole-trie rebuilds**.

*Ten passes. (A) is verified and concrete; (B) and (C) have a verified path gated by
§8.2's probe and §8.4's zero-exception result — stated honestly, not claimed done.*

---

## §10. Scope boundary

**In scope.** Eliminating the three remaining branch-path rebuilds: (A) betaIsDiscBit +
full d* — the `splitIndirect` + β-survival-dispatch decomposition; (B) betaIsDiscBit at
the boundary child — the same mechanism at depth+1; (C) the `IllegalArgumentException |
IllegalStateException` self-heal — deleted once (A)+(B) make the branch path total and
the per-insert probe confirms zero structural exceptions.

**Prerequisite.** `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md` must land first (it removes the
leaf-overflow rebuild and the `HOTStraddleException` catch arms; this plan then removes
what remains).

**Out of scope.** `consolidateSubtree` (incremental leaf consolidation, not a rebuild —
retained). The reader-side lower-bound / lex fallbacks — once every insert is verified
routing-correct they become dead, but their removal is a separate read-path cleanup.

**Result.** With both plans landed, a HOT insert is a composition of verified incremental
primitives — `splitLeafPage`, `splitIndirect`, `addEntry`, `addChildAtCombination`,
`addEntryWithInsertInfo`, `splitIndirectWithEntry`, `integrate` — with **no
`HOTBulkBuilder` rebuild of any subtree or of the whole trie**, incremental and
minimum-trie-height-preserving after every insert.
