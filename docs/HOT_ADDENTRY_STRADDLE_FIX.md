# HOT `addEntry` straddle fix — design plan

**Status:** design, pending review. **Branch:** `fix/hot-strict-binna-conformance`.
**Diagnosed:** 2026-05-19, per-insert deep-I5 probe (`hot-betaisdiscbit-fullnode-blocked`).

---

## §1. Problem

When a multi-value leaf page `L` (≤ 512 keys) overflows, `HOTIncrementalInsert.splitLeafPage`
splits it — together with the new key `K` that did not fit — at the resulting key-set MSDB
`λ` (`= msdb` of `L`'s keys ∪ `{K}`; the split bit carried by the returned `BiNode`) into two
halves `L₀` (`λ=0`) and `L₁` (`λ=1`). `integrate` then folds the `BiNode(λ, L₀, L₁)` into
`L`'s parent compound node `N` via `addEntry`, which **adds `λ` as a new discriminative bit
of `N`** — but only when `N` is height 1 (a height-≥2 parent takes `integrate`'s
intermediate-node path instead, which adds no discriminative bit and so cannot straddle —
see §4.1).

`addEntry` re-encodes every *non-affected* sibling of `L` with the new `λ`-column set to `0`
— it **assumes every sibling subtree is `λ`-constant**. That assumption is free for Binna's
reference HOT (single-entry leaves: a one-key leaf is trivially constant on every bit). It is
**false for Sirix's multi-value leaves**: a sibling leaf holds up to 512 keys spanning a key
range and can *straddle* `λ` (have keys with `λ=0` and `λ=1`).

When it does, the post-`addEntry` node `N` has a child whose subtree is not constant on `N`'s
discriminative bit `λ` — **invariant I5 is violated**.

### Evidence (per-insert deep-I5 probe)

A deep I5 check (every interior leaf key — not just `firstKey`) run after every insert of a
random-with-replacement workload pinned the **first** violation exactly:

```
FIRST I5 violation after insert #1031, path=merge
  indirect#6 child[2] (505 keys) NOT constant at disc bit 129 — discBits=[128,129]
```

`child[2]` is a *pre-existing* 505-key sibling leaf; a split product would be `λ`-constant by
construction, so the non-constant child must be a shifted pre-existing sibling. Insert #1031
overflowed a leaf, split it at `λ=129`, and `addEntry` folded `λ=129` into `indirect#6` — but
`child[2]` straddles bit 129.

### Why this is the root cause of the `betaIsDiscBit`-full rebuilds

A `betaIsDiscBit` branch insert at a full node `d*` can only have `betaValue=0` when `d*`'s
subtree is **already non-canonical** (proof in §3.4). Every observed
`splitIndirectAtCombination` failure had `betaValue=0` (57/58). So `betaIsDiscBit`-full is a
*downstream symptom*: it observes a subtree this `addEntry` step already drifted.
`rebuildSubtree` "works" there only because it *recanonicalizes* — it is not a branch insert.
**Fixing this `addEntry` step removes the drift and is the actual correctness fix.**

---

## §2. Invariants (the contract a HOT compound node must satisfy)

Let `N` be a compound node with discriminative bits `D(N) = {b₁ < b₂ < … < bₖ}` (absolute
MSB-first bit indices; `b₁` most significant = smallest index) and children `c₀ … cₙ₋₁`.

- **I2 — leaf order.** A leaf page's keys are sorted strictly ascending unsigned.
- **I3 — distinct partials.** The children's stored partial keys are pairwise distinct.
- **I4 — first partial zero.** `partial(c₀) = 0`.
- **I5 — child β-constancy.** For every child `cᵢ` and every `b ∈ D(N)`, all keys in `cᵢ`'s
  subtree have the same value at bit `b`. (This is what `addEntry` breaks.)
- **I6 — routing.** For every stored key `K`, the highest-subset-match descent
  (`findChildIndex(K)`) from the root reaches the leaf that actually contains `K`. I6 is the
  *data-findability* property; for a structurally-canonical node it is a **consequence** of
  the others — the sparse-partial-key routing theorem (`HOT_FORMAL_FOUNDATION.md` §5.3.1) —
  but it is tracked explicitly because a straddle breaks it too (a `λ`-mixed sibling can be
  shadowed by a `λ=1` child of higher index).
- **I7 — partials ascending.** `partial(c₀) < partial(c₁) < … < partial(cₙ₋₁)`.
- **I8 — children sorted by firstKey.** `firstKey(c₀) < … < firstKey(cₙ₋₁)`.
- **I11 — trie condition.** Every disc bit of `N` is strictly *more significant* than every
  disc bit of every child compound node of `N`.
- **R(S) — subtree contiguity.** Every subtree holds a *contiguous* range of the sorted key
  space (HOT is order-preserving). A leaf page holds one such range.

`HOTBulkBuilder.build` produces a structure satisfying all of the above *by construction*
(`HOT_FORMAL_FOUNDATION.md` Theorem 1; property-test-verified). Call such a structure
**canonical**.

---

## §3. Root-cause analysis (formal)

### §3.1 Lemma 1 — prefix agreement of a sorted contiguous range

> Let `k₁ ≤ k₂ ≤ … ≤ kₙ` be a sorted (unsigned) key range and `λ = msdb(k₁, kₙ)`. Then
> **every `kᵢ` agrees with `k₁` on all bits strictly more significant than `λ`.**

*Proof.* `k₁` and `kₙ` share a common prefix `P` on the bits above `λ` (definition of `msdb`
— the first differing bit is `λ`). Take any `kᵢ`; let `P'` be its prefix on those same bits.
The bits above `λ` are the most significant, so prefix order = key order on that slice. If
`P' < P` then `kᵢ < k₁`; if `P' > P` then `kᵢ > kₙ`. Both contradict `k₁ ≤ kᵢ ≤ kₙ`. Hence
`P' = P`. ∎

### §3.2 Corollary — when a subtree cannot straddle a bit

By **R(S)**, a child subtree `c` holds a sorted contiguous range; let
`msdb(c) = msdb(firstKey(c), lastKey(c))`. By Lemma 1, **every key in `c` agrees on every bit
strictly more significant than `msdb(c)`.** Therefore:

> **`c` does not straddle bit `β` whenever `β` is strictly more significant than `msdb(c)`**
> (i.e. `index(β) < index(msdb(c))`).

This is the **soundness** basis of the fix's guard (§4): it has *no false negatives* — if the
guard says "safe", `c` provably does not straddle `β`. (It may have false positives — `β` at
or below `msdb(c)` does not *force* a straddle — which only costs an unnecessary, still-correct
recanonicalization.)

### §3.3 The bug condition, formally

`addEntry` folding `BiNode(λ, L₀, L₁)` into `N` (affected child = `L`) preserves I5 **iff**

> for every non-affected sibling `c`, `c` does not straddle `λ`.

A *sufficient and soundly-checkable* condition (Corollary §3.2):

> **`λ` is strictly more significant than `msdb(c)` for every non-affected sibling `c`.**

Since `λ = msdb(L)`, this is: **`L`'s key-set MSDB is strictly more significant than every
sibling's key-set MSDB.** When a sibling has an MSDB at least as significant as `λ`, `addEntry`
is unsafe.

### §3.4 Proposition — `betaValue=0` betaIsDiscBit ⟹ non-canonical (the downstream link)

> On a canonical tree, a `betaIsDiscBit` branch insert at `d*` always has `betaValue=1`.

*Proof.* The descent picks `d*`'s child `e` by highest-subset-match, so `partial(e) ⊆ dK`
(`dK` = `K`'s dense partial). `β` a disc bit of `d*`. If `partial(e)` had `β=1`, then
`dK` has `β=1`, so `K[β]=1`; the resident `r ∈ e`'s subtree has `r[β]=1` by I5; then `K` and
`r` agree at `β`, contradicting `β = msdb(K,r)`. So `partial(e)` has `β=0`; by I5 the whole
subtree of `e` has `β=0`; `r[β]=0`; `K` differs from `r` at `β` ⇒ `K[β]=1` ⇒ `betaValue=1`. ∎

Contrapositive: an observed `betaValue=0` proves I5 is violated at `d*`. Every observed
`splitIndirectAtCombination` failure was `betaValue=0` ⇒ the drift fixed here is *the* cause.

---

## §4. The fix

**Principle.** A discriminative bit may be added to a compound node only when it is provably
straddle-free. Detect the unsafe case with a sound O(children) guard; on the safe path keep
the existing (now provably-correct) `addEntry`; on the unsafe path fall back to a **scoped
recanonicalization** of the affected node's subtree.

### §4.1 Why recanonicalize rather than "deepen" (intermediate node)

An alternative for the unsafe case is to *not* widen `N` at all: materialize `BiNode(λ,L₀,L₁)`
as a standalone 2-entry node `M` in `L`'s slot (the existing `integrate` intermediate-node
shape). `N` gains no bit, so no straddle. It is correct, but **rejected**:

1. It produces a **non-canonical (deeper) shape** — it fights HOT's height optimization; `N`
   accumulates fanout-2 children instead of widening.
2. `N`'s height field goes stale (1 → 2), which forces a **height cascade** rebuild of every
   spine ancestor — more moving parts, more corner cases to verify.
3. Recanonicalization reuses one already-formally-verified primitive (`HOTBulkBuilder`),
   minimizing new proof burden — decisive for "get it correct finally".

Recanonicalization is **rare** (≈ 1 insert in 1000, per the probe), **bounded** (a height-1
node's subtree ≤ 32 × 512 ≈ 16 K keys), and produces the **canonical height-optimal** shape.

### §4.2 The guard

`msdb`-based, O(children), sound (§3.2). The affected child run `[affectedFirst,
affectedFirst+affectedCount)` is exempt (it is *meant* to be split on `β`).

```java
/**
 * The key-set MSDB of a child subtree — by Lemma 1, every key in it agrees on every
 * strictly-more-significant bit. O(1), and crucially **no descent**:
 *  - a compound child: its stored MSB, {@code getMostSignificantBitIndex()}. For a
 *    canonical node the most significant discriminative bit IS the most significant bit
 *    its key set differs on (the top of its flattened trie) — induction (Thm 4) keeps
 *    every existing child canonical, so this equals the subtree key-set MSDB.
 *  - a leaf child: {@code msdb(firstKey, lastKey)} read from its own page (2 keys).
 *  - a one-key leaf: constant on every bit → sentinel {@code Integer.MAX_VALUE}
 *    ("never straddles" — Binna's single-entry-leaf case; also avoids msdb-of-equal-keys).
 * `addEntry` is a pure static method with no storage access, so a *leaf* child's page must
 * already be resolved — see §7: the driver pre-resolves every path node's children before
 * `integrate`. A compound child needs no page load — only its (already-in-memory) MSB.
 */
private static int subtreeMsdb(final Page subtree) {
  if (subtree instanceof HOTIndirectPage indirect) {
    return indirect.getMostSignificantBitIndex();
  }
  final HOTLeafPage leaf = (HOTLeafPage) subtree;
  final int n = leaf.getEntryCount();
  return n < 2 ? Integer.MAX_VALUE
               : HOTBulkBuilder.msdb(leaf.getKey(0), leaf.getKey(n - 1));
}

/**
 * True iff discriminative bit {@code beta} can be folded into {@code node} without violating
 * I5 — no non-affected child subtree straddles {@code beta}. SOUND (Corollary §3.2): a `true`
 * result is a proof of straddle-freedom; a `false` result is conservative (recanonicalize).
 */
static boolean splitBitIsSafe(final HOTIndirectPage node, final int beta,
    final int affectedFirst, final int affectedCount) {
  for (int c = 0; c < node.getNumChildren(); c++) {
    if (c >= affectedFirst && c < affectedFirst + affectedCount) {
      continue;                       // affected run — exempt; it is split ON beta
    }
    // beta must be strictly MORE significant (smaller absolute index) than child c's MSDB.
    if (beta >= subtreeMsdb(node.getChildReference(c).getPage())) {
      return false;                   // child c may straddle beta
    }
  }
  return true;
}
```

### §4.3 Wiring — a precise signal + a scoped fallback

The bit-adding primitive **`addEntry`** gains the guard (the scope boundary — see §4.4 and
§10 for why `addEntryWithInsertInfo` / `compressRangeWithEntry` are *not* in scope). On a
detected straddle `addEntry` throws a **new, specific** exception so the catch is precise (it
must not also swallow unrelated `IllegalStateException`s):

```java
/** Thrown when a discriminative bit cannot be folded into a node because a multi-value
 *  sibling subtree straddles it. Carries the spine depth of the unsafe node so the caller
 *  scopes the rebuild minimally. */
public final class HOTStraddleException extends RuntimeException {
  public final int nodeDepthHint;          // spine depth of the addEntry'd node; -1 = untagged
  HOTStraddleException(String message, int nodeDepthHint) { … }
}
```

`addEntry` is a pure static method: it cannot recanonicalize, and — crucially — it does not
know its own spine depth. So `addEntry` throws with `nodeDepthHint = -1`, and **`integrate`
tags it**: every `integrate` frame wraps its `addEntry` / `foldIntoHalf` call in
`catch (HOTStraddleException e) { throw e.nodeDepthHint >= 0 ? e : new HOTStraddleException(…,
parentDepth); }`. The *first* (deepest) frame past which the exception travels stamps the
real depth; outer frames see it already tagged and propagate unchanged. So the driver
**always** receives a concrete depth — there is no blind `rebuildSubtree(0)` (this is what
keeps the fallback minimally scoped — see C9/C13 for why that matters to versioning):

```java
// AbstractHOTIndexWriter.mergeIntoLeaf / branchAboveLeaf — a new catch, distinct from the
// existing broad IllegalArgumentException|IllegalStateException catch:
} catch (HOTStraddleException straddle) {
  // The split bit straddles a multi-value sibling; an incremental fold is impossible.
  // Recanonicalize the unsafe node's subtree — HOTBulkBuilder is canonical by construction
  // (Theorem 2), so it re-partitions straddle-free. The depth is the unsafe node's own
  // spine depth (integrate tagged it); for the diagnosed leaf-overflow case that is the
  // deepest path node, pathDepth-1.
  rebuildSubtree(navResult, straddle.nodeDepthHint, keySlice, valueSlice);
  return;
}
```

`rebuildSubtree` already escalates one level shallower when the rebuilt subtree's height
changes — so a recanonicalization that grows the node's height re-derives its ancestors too,
terminating at the root. A genuine *root*-level straddle thus rebuilds the whole index — the
true minimal scope in that (rare) case; §8 step 3 gates it on the versioned suite.

### §4.4 Where the guard fires (call sites) — and the scope boundary

`addEntry` is invoked only from inside `integrate` — directly (the not-full case) and via
`foldIntoHalf` (the capacity cascade). `integrate` itself runs on **both** the merge path
(folding a leaf-overflow `BiNode`) and the branch path (folding a branch-insert `BiNode` —
e.g. a leaf paired with the new key's leaf, or a pulled-up subtree). The guard lives **in
`addEntry`**, so it fires for *every* invocation and covers all of these uniformly:

| `addEntry` invoked from | Exempt (affected) slot | Depth hint (stamped by `integrate`) |
|---|---|---|
| `integrate`, not-full parent (leaf-overflow merge — the **diagnosed** case — or a branch integration) | `{affectedChildIndex}` | `parentDepth` (= `pathDepth-1` for the leaf-overflow case) |
| `foldIntoHalf`, capacity cascade (fold into a split half) | `{indexInHalf}` | `parentDepth` of the cascade frame |

**Why only `addEntry`.** `addEntry` *replaces* its affected child with the BiNode's two
children, which `addEntry`'s standing input precondition (per BiNode kind,
`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`) guarantees are each `β`-constant — split halves
`L₀`/`L₁` for a leaf overflow, an existing leaf/subtree + the new key's leaf for a branch.
So the affected slot is correctly exempt (it is gone; its replacements are sound), and the
only new risk is a *non-affected* sibling — exactly what the guard checks.

`addEntryWithInsertInfo` and `compressRangeWithEntry` are **different**: they *keep* their
affected-run children and merely re-stamp each with `β = affectedBetaValue`. Correctness
there needs every run child to be `β`-constant **and equal to** `affectedBetaValue` (the
affected run must be genuinely one-sided on `β`) — a *strictly stronger* condition than
`addEntry`'s straddle-freedom, and the `addEntry` guard's "exempt the affected run" is wrong
for them. Their straddle-safety is a **separate analysis** (§10) and is gated by the
empirical probe (§8): if fixing `addEntry` does not bring the per-insert deep-I5 probe to
zero drift, the next-reported drift identifies which of those two needs its own fix.

---

## §5. Formal verification

### §5.1 Theorem 1 — the safe path preserves every invariant

> If `splitBitIsSafe(N, λ, affectedFirst, affectedCount)` returns `true`, then `addEntry`
> folding `BiNode(λ, L₀, L₁)` into `N` yields a **canonical** node `N'` — it satisfies the
> structural invariants I3, I4, I5, I7, I8, I11, and hence also I6 (routing).

*Proof.*
- **I5.** Children of `N'` fall in two groups.
  - *(a) the affected child's two replacements* `biNode.left`, `biNode.right`. That each is
    constant on `λ` *and* on `D(N)` is `addEntry`'s **standing input precondition** — proven
    per BiNode kind in `HOT_INCREMENTAL_SPLIT_VERIFICATION.md`, **unchanged** by this fix.
    Concretely: for the diagnosed leaf overflow they are `splitLeafPage`'s halves `L₀`/`L₁`
    — `λ`-constant by the split, and `D(N)`-constant because `L`'s keys together with the
    new key `K` all agree on `D(N)` (the merge-vs-branch test placed every bit of `D(N)`
    *above* the mismatch bit, so `K` agrees with `L` there); for a branch `BiNode` they are
    an existing leaf/subtree (`λ`-constant — `λ` is above its key-set MSB) and `K`'s
    single-entry leaf (constant on everything). The guard does not touch group (a).
  - *(b) every non-affected sibling* `c` — **the new obligation.** The guard returned
    `true`, so `λ index < subtreeMsdb(c) index`; by Corollary §3.2 `c` is `λ`-constant; `c`
    is structurally unchanged so it is still `D(N)`-constant.

  Every child of `N'` is thus constant on `D(N') = D(N) ∪ {λ}`. ∎(I5)
- **I11.** `D(N')` adds `λ`; `λ` must be strictly more significant than every child *compound
  node*'s disc bits. Two groups again:
  - *non-affected compound siblings* `c` — the guard gave `λ index < subtreeMsdb(c) index`,
    and for a canonical node `subtreeMsdb(c) = index(c.MSB)` (its most significant disc bit
    *is* its key-set MSDB — §4.2). So `λ` is more significant than `c.MSB`, hence than all of
    `c`'s disc bits.
  - *the affected child's replacements* `biNode.left`/`right`, when compound — `λ` more
    significant than their disc bits is `addEntry`'s **standing precondition** that the
    `BiNode` is well-formed (`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`): a split half's keys
    all share `λ`'s value and vary only *below* `λ`, so its MSB is below `λ`.

  `D(N)`'s bits were already more significant than every child's (N canonical); `λ` now is
  too; I11 holds. ∎(I11)
- **I3, I4, I7, I8.** Unchanged from the existing `addEntry` correctness proof
  (`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`): `addEntry` re-encodes partials MSB-first and
  inserts the two products at the affected slot in ascending order; the guard does not touch
  this. ∎
- **I6 (routing).** `N'` satisfies the structural invariants I3/I4/I5/I7/I8/I11 (above), so
  it is canonical, and a canonical node routes every stored key to its leaf by the
  sparse-partial-key theorem (`HOT_FORMAL_FOUNDATION.md` §5.3.1). The straddle-specific
  reason: with the guard, every non-affected sibling is `λ`-constant, so `λ` is *off-path*
  for it; the sparse encoding stores `0` for an off-path bit, and subset-match treats stored
  `0` as "don't care" — so no sibling is shadowed by, nor shadows, the new `λ`-children
  (§5.1 Remark). ∎(I6)

**Remark (a `λ`-constant-*1* sibling is fine).** `addEntry` re-stamps every non-affected
sibling's new `λ`-column to `0`. A reviewer may worry about a sibling `c` that is
`λ`-constant with value **1** (keys all `λ=1`, stored `λ`-column `0`). This is correct: with
no sibling straddling `λ`, `λ` branches *only* inside `L`'s subtree, so `λ` is **off-path**
for every sibling; the sparse-path encoding stores `0` for an off-path bit *regardless* of
the keys' actual value, and subset-match routing treats a stored `0` as "don't care"
(`storedPartial ⊆ densePK` holds because `0 ⊆ anything`). So `c` still routes correctly. I5
requires only *constancy* — which a `λ`-constant-1 sibling satisfies — not a particular
value. (This is exactly why `addEntry` is straddle-*freedom*-bounded, not value-bounded —
and why `addEntryWithInsertInfo`, which puts `λ` *on*-path for its run, needs the stronger
value condition of §4.4.)

### §5.2 Theorem 2 — the unsafe path is correct

> On a detected straddle, `rebuildSubtree(depth)` yields a canonical subtree.

*Proof.* `rebuildSubtree` collects every `(key,value)` of the depth-`depth` subtree, sorts and
de-duplicates them, and calls `HOTBulkBuilder.build`, which is canonical by construction
(`HOT_FORMAL_FOUNDATION.md` Theorem 1 — every invariant of §2 holds, property-test-verified).
Height escalation re-derives ancestors if the rebuilt height changed. ∎

### §5.3 Theorem 3 — the guard is sound and complete *enough*

> **Sound:** `splitBitIsSafe` ⇒ no sibling straddles `λ` ⇒ the safe path is correct (Thm 1).
> **No false negatives:** if some sibling *does* straddle `λ`, the guard returns `false`.

*Proof of no-false-negatives.* If sibling `c` straddles `λ`, its keys differ at `λ`, so `λ` is
at or below `msdb(c)` in significance (`λ index ≥ msdb(c) index`) — Lemma 1: above `msdb(c)`
all keys agree. The guard's test `beta >= subtreeMsdb(c)` is exactly `λ index ≥ msdb(c) index`,
which is then true ⇒ guard returns `false`. ∎

False *positives* are admissible: `λ` at/below `msdb(c)` does not force an actual straddle, so
the guard may recanonicalize when `addEntry` would have been safe. This only costs an extra
(correct) rebuild and is bounded by §6-C8.

### §5.4 Theorem 4 — termination, and global canonicity by induction

*Termination.* The unsafe path calls `rebuildSubtree`, which does **not** call back into the
incremental insert; its only recursion is height escalation, strictly decreasing `depth`,
terminating at `0`. The safe path is straight-line. ∎

*Global canonicity (conditional — see the caveat).* By induction over inserts. Base: the
empty index is canonical. Step: assume canonical before insert `i`. `doIndex` does one of:

- **fast-path merge** — `K` joins a leaf, no overflow. Preserves I5: the merge-vs-branch
  bound `β > leastSignificantDiscBit(deepest path node)` (with `β = msdb(K, resident)` the
  *global-maximum* msdb over the leaf's keys) puts every path disc bit *above* `β`, so `K`
  agrees with the resident — hence with the leaf's I5-constant value — on every path disc
  bit (`HOT_INCREMENTAL_PORT`).
- **overflow merge / `addEntry`-leaf-split** — routed through the guarded `addEntry`: safe
  path preserves all invariants (Thm 1), unsafe path recanonicalizes (Thm 2).
- **recanonicalization** — canonical by construction (Thm 2).

So insert `i` leaves the index canonical. ∎

**Caveat — the branch ops.** A branch insert may instead go through `addEntryWithInsertInfo`
or `compressRangeWithEntry`, which this plan does **not** prove straddle-safe (§4.4, Review
Pass 5). The induction is therefore *complete only if those ops also preserve canonicity*.
§8's acceptance gate — the per-insert deep-I5 probe reporting **zero drift** across the
adversarial workloads — is exactly the empirical discharge of that premise. If the probe is
not zero after this fix, the next-reported drift is a second bug (a follow-up with its own,
run-one-sidedness analysis); global canonicity is then not yet established.

**Corollary.** Once global canonicity holds (this fix + a green zero-drift probe),
Proposition §3.4 gives: a `betaIsDiscBit` insert always has `betaValue=1` — the
`betaValue=0` failure class is *eliminated*. `betaIsDiscBit`-full on a canonical tree is a
separate, now-tractable follow-up (not in this plan's scope).

## §6. Corner cases (exhaustive)

| # | Case | Handling |
|---|---|---|
| C1 | `node` has only the affected child (degenerate) | Guard loop is vacuous → `true` → safe `addEntry`. A real compound node has ≥ 2 children, so the live case always checks ≥ 1 sibling. |
| C2 | The **affected** child straddles `λ` | Expected — it is *meant* to be split on `λ`. Exempt via the `affectedFirst/Count` range. |
| C3 | `λ` is already a disc bit of `node` | Pre-existing `addEntry` precondition (`IllegalArgumentException` "split bit already a discriminative bit"). Distinct from the straddle guard; both can run, the existing one first. |
| C4 | A non-affected sibling is a **compound node**, not a leaf | `subtreeMsdb` reads the subtree's leftmost/rightmost leaf keys; Lemma 1 holds for any contiguous subtree. Theorem 1 §5.1 also covers I11 for compound siblings (`λ` more significant than the child's MSB). |
| C5 | A split half is still > 512 keys → `buildHalf` recurses to `HOTBulkBuilder` | The BiNode child becomes a compound node; `addEntry` accepts BiNode children of any height; the guard is about *siblings* and is unaffected. |
| C6 | The unsafe `addEntry` is at the **index root** | `rebuildSubtree(0)` = whole-index recanonicalize. Safe because the guard throws from a **pure pre-mutation check** (no spine re-pointing has happened) — unlike the reverted experiment, where the splice threw mid-mutation. See C9. |
| C7 | MultiMask-layout node (disc bits span > 8 bytes) | `msdb` / `subtreeMsdb` use absolute bit indices — layout-agnostic. |
| C8 | Guard false positive (`λ` ≤ `msdb(c)` but `c` does not actually straddle) | Correct but a needless rebuild. Aggregate worst case ≈ `(N/512)` rebuilds × ≤ 16 K keys = `O(N)`. If measured costly: add a precise check (deep β-scan of the *flagged* sibling only). Deferred refinement. |
| C9 | Fallback rebuild vs multi-revision isolation | The reverted experiment regressed 2 versioned tests; the **exact cause was not pinned** — the leading candidate is that it changed `betaIsDiscBit`-full's fallback from a *scoped* `rebuildSubtree(insertDepth)` (baseline) to `rebuildWholeIndex` (= `rebuildSubtree(0)`), whose wider leaf release may free pages a historical revision still needs. This plan **avoids the risk by construction**: `integrate` tags `HOTStraddleException` with the unsafe node's real depth (§4.3), so the fallback is `rebuildSubtree(thatDepth)` — for the diagnosed leaf-overflow case `rebuildSubtree(pathDepth-1)`, *exactly* the scoped form baseline `betaIsDiscBit`-full already uses and the versioned suite already passes. A blind `rebuildSubtree(0)` never occurs. `addEntry` is also pure and throws before any `setPage`, so the spine is pristine; `splitLeafPage`/`splitIndirect` orphan pages are harmless garbage (release the speculative `L₀/L₁` — §7 step 5). §8 step 3 is the gate. |
| C10 | A split half is tiny (under-full) | Orthogonal — the periodic `consolidateSubtree` sweep handles underflow. |
| C11 | Duplicate keys / tombstones in the overflowed leaf | `splitLeafPage` OR-merges duplicate values; `HOTBulkBuilder` treats a tombstone as an ordinary entry. Unchanged. |
| C12 | `node` is **full** (32 children) | A full node never `addEntry`s — `integrate` cascades (`splitIndirect` + fold into a half). The half's `addEntry` is guarded (row 2 of §4.4). |
| C13 | Straddle detected **inside the capacity cascade** | `integrate` cannot recanonicalize, but it *does* know the cascade frame's `parentDepth` and tags `HOTStraddleException` with it (§4.3). The driver then scopes `rebuildSubtree(thatDepth)` — the cascade node's own subtree, not the whole index. Rare (a cascade needs a full node). |
| C14 | A sibling **leaf** page is not swizzled (`getPage() == null` — TIL-only or on-disk) | `subtreeMsdb` of a leaf reads its `getKey(...)`, so the leaf page must be in memory. `addEntry` is a pure static method and cannot resolve pages. **Fix:** the driver pre-resolves every path compound node's children (a height-1-inclusive extension of `ensurePathChildrenLoaded`) before `integrate` — §7 step 3. Compound siblings need no load (`subtreeMsdb` uses their stored MSB). The cascade's split halves share the original (now-resolved) child references, so they are covered too. Cost: ≤ 32 leaf resolutions per leaf overflow, almost all TIL/cache hits within the transaction. |
| C15 | A sibling subtree has exactly **one key** | `msdb` of equal keys is undefined. A 1-key subtree is constant on *every* bit (it is Binna's single-entry-leaf case). `subtreeMsdb` returns `Integer.MAX_VALUE` (sentinel "never straddles") so `beta >= MAX_VALUE` is `false` → that sibling is safe. **Must be special-cased** or `msdb` throws. |
| C16 | Empty subtree | Cannot occur — every child of a compound node is non-empty (I-structure). |

---

## §7. Implementation steps

1. **`HOTStraddleException`** — new `public final class` in `io.sirix.index.hot`, extends
   `RuntimeException`, carries `int nodeDepthHint`.
2. **`HOTIncrementalInsert`** — add `subtreeMsdb(Page)` (compound → stored MSB; leaf →
   `msdb(firstKey,lastKey)`; one-key leaf → sentinel) and `splitBitIsSafe(node, beta,
   affectedFirst, affectedCount)`. No descent helpers are needed — see §4.2.
3. **`AbstractHOTIndexWriter` — pre-resolve guard inputs.** Before every `integrate` call on
   the structural-overflow path, ensure every path compound node's *leaf* children are
   swizzled (a height-1-inclusive extension of `ensurePathChildrenLoaded`, via
   `resolveHOTPageForTraversal`). Required because `addEntry`'s guard is a pure static method
   that reads `HOTLeafPage.getKey(...)` on sibling leaves (C14). Compound children need no
   load.
4. **`HOTIncrementalInsert.addEntry`** — after the existing "β already a disc bit" check, call
   `splitBitIsSafe(node, beta, affectedChildIndex, 1)`; on `false` throw `HOTStraddleException`.
   This is the **only** bit-adding primitive changed (§4.4 / §10).
5. **`AbstractHOTIndexWriter.mergeIntoLeaf` and `branchAboveLeaf`** — add a `catch
   (HOTStraddleException)` that calls `rebuildSubtree` at `nodeDepthHint` (≥ 0) or the driver's
   natural depth (`pathDepth-1` for merge, `insertDepth` for branch) or `0` if the hint is `-1`.
   Keep these distinct from the existing broad `IllegalArgumentException | IllegalStateException`
   catch (which stays for genuine structural inconsistencies). Optionally release the orphaned
   speculative `L₀/L₁` pages here (C9).
6. **No production change to the safe path** — `addEntry`'s body is untouched apart from the
   guard call; the guard only gates entry. The common hot path is identical, zero-cost on
   success besides the O(children) check.

The change is **purely additive** on the safe path and a **scoped fallback** on the unsafe
path; it removes no existing behavior.

---

## §8. Test plan

**Verification gate — the per-insert probe must report ZERO drift.** Re-add the diagnostic
hook (`-Dhot.diag.perInsertValidate`, after every `doIndex`) and broaden it from the
diagnosis-time I5-only check to a **deep I5 *and* I6 check**: I5 = every child subtree
constant on each parent disc bit (every interior leaf key); I6 = every stored key
`findChildIndex`-routes from the root to its containing leaf. The I6 check is needed because
Theorem 4's "global canonicity" premise is all-invariants — a residual branch-op drift
(`addEntryWithInsertInfo` / `addChildAtCombination`) would surface as I6, not I5. Run:

1. **Drift probe (the reproduction):** random-with-replacement N = 20 000 (the workload that
   failed at insert #1031) → expect **no `[hot.diag]` line**. Then descending, ascending,
   random-shuffle, bimodal, clustered — all → no drift.
2. **`HOTFormalVerificationTest`** — all 17 canaries (descending / bimodal / fully-random /
   random-shuffle / conformance sweep / multi-rev) → `violations=0`, `missedValues=0`.
3. **`HOTVersionedLeafStressTest`** — full suite green (the 2 tests the reverted experiment
   regressed must pass — confirms C9).
4. **`HOTMicrobenchmark.smallCombinedMicrobench`** — `full-scan misses=0/500000`,
   `validator violations=0`; write throughput within ~10 % of the 2692 ns/op baseline (the
   guard is O(children); recanonicalizations are rare).
5. **Unit tests** (new, `HOTIndirectPageSplitFaithfulTest` style, on swizzled in-memory pages):
   - `splitBitIsSafe` returns `false` for a constructed straddling sibling, `true` otherwise.
   - `subtreeMsdb` of a one-key subtree returns the sentinel.
   - An overflow whose split bit straddles a sibling → driver recanonicalizes → `assertClean`
     + `assertRoutesAll` on the result.
6. **Counter:** instrument the recanonicalize-on-straddle path with a counter; assert it is
   `> 0` on the random-with-replacement workload (the fix path is genuinely exercised) and
   small relative to N (rare, as predicted) — this also measures the false-positive cost (C8).
7. **Regression audit — existing `addEntry` direct-call tests.** `HOTIndirectPageSplitFaithfulTest`
   (`addEntryIntegratesLeafSplit`, `addEntryAcceptsFreshKey`, `mergeUndoesLeafSplit`) call
   `HOTIncrementalInsert.addEntry` directly; a constructed case whose split bit straddles a
   sibling will now throw `HOTStraddleException` instead of returning. Audit each: either the
   case is straddle-free (passes unchanged) or it must be updated to `assertThrows` /
   reconstructed straddle-free. The whole HOT test package (`./gradlew :sirix-core:test
   --tests 'io.sirix.index.hot.*'`) must stay green.

A fix is accepted only when **1–4 and 7 are all green and 6 confirms the path fired.**

---

## §9. Review passes

**Pass 1 (initial).** Guard = "`λ` more significant than every sibling leaf's MSDB"; fix =
recanonicalize on failure. — *Refined:* stated the guard's soundness as a theorem (§5.3)
rather than an assertion; added Lemma 1 as the foundation.

**Pass 2 (siblings are not all leaves).** A non-affected sibling can be a **compound node**.
— *Caught:* Lemma 1 still applies (R(S) contiguity); I11 must be re-checked for compound
siblings — folded into Theorem 1 §5.1. Added C4. (`subtreeMsdb` for compound siblings is
finalized in Pass 6.)

**Pass 3 (the cascade, the fallback scope).** — *Caught:* (a) the capacity-cascade `addEntry`
can straddle with no clean depth to scope to → `nodeDepthHint=-1` → `rebuildSubtree(0)` (C13);
(b) `rebuildWholeIndex` regressed versioning **only** because the reverted experiment threw
mid-mutation — a pure pre-mutation guard makes the scoped rebuild safe (C9); (c) the
one-key-subtree `msdb` edge (C15); (d) TIL-only sibling pages (C14).

**Pass 4 (does the fix actually reach the goal?).** — *Confirmed:* with the tree kept
canonical, `betaValue=0` betaIsDiscBit is eliminated (Corollary §5.4). The remaining
`betaIsDiscBit`-full `betaValue=1` case on a canonical tree is rare and is explicitly a
**follow-up**, tractable precisely because the tree is now canonical. This plan's deliverable
is the **correctness fix**.

**Pass 5 (the "check again" pass — the overclaim).** — *Caught a real error:* an earlier
draft claimed `addEntryWithInsertInfo` and `compressRangeWithEntry` "share the identical
guard" as `addEntry`. **They do not.** `addEntry` *replaces* its affected child with two
`β`-constant split halves, so it is bounded by straddle-*freedom* of the non-affected
siblings (and a `β`-constant-1 sibling is harmless — §5.1 Remark). The other two *keep* their
affected-run children and re-stamp them `β = affectedBetaValue`, which puts `β` *on*-path for
the run — so they need the **stronger** condition that the run is genuinely one-sided on `β`
(`β`-constant *equal to* `affectedBetaValue`), not just straddle-free. Exempting the affected
run, as the `addEntry` guard does, is *wrong* for them. *Fix:* the verified scope is narrowed
to `addEntry` only (§4.4); `addEntryWithInsertInfo` / `compressRangeWithEntry` straddle-safety
is a separate analysis, **empirically gated** by §8's zero-drift probe — if fixing `addEntry`
does not bring the probe to zero, the next drift names the culprit. Theorem 4 §5.4 is
correspondingly conditioned: global canonicity is *proven* for merge / safe-`addEntry` /
recanonicalize and *empirically established* for the branch ops by the zero-drift gate.

**Pass 6 (the "check again several times" pass — the page-access hole).** — *Caught two real
problems.* (a) **Page access.** `addEntry` is a *pure static* method with no storage engine;
its guard cannot resolve an unloaded sibling page. Earlier `subtreeMsdb` drafts "descended to
the leftmost/rightmost leaf" — impossible from a static method when those pages are TIL-only.
*Fix:* `subtreeMsdb` of a *compound* sibling now uses its **stored** `getMostSignificantBitIndex()`
(for a canonical node — induction Thm 4 — that equals the subtree key-set MSDB), so it needs
no descent and no load; only a *leaf* sibling needs its page, and §7 step 3 makes the driver
pre-resolve every path node's leaf children before `integrate`. (b) **Existing tests.**
`HOTIndirectPageSplitFaithfulTest` calls `addEntry` directly; the new throw can break a
constructed straddling case — added §8 step 7 (regression audit). Also tightened: §1's `λ`
formula (it is `msdb` of `L`'s keys *plus the new key*, the `BiNode`'s actual split bit, not
`msdb(L)` alone); the §5.1 Remark (why a `λ`-constant-*1* sibling is harmless under the
sparse off-path encoding) — pre-empting the most likely reviewer objection.

**Pass 7 (consistency sweep).** — *Verified:* every cross-reference resolves
(§3.2↔§4.2↔§5.3 soundness chain; §4.4↔§10 scope); the guard's exemption
(`affectedFirst/Count`) is used only by `addEntry` with `(affectedChildIndex, 1)`;
`HOTStraddleException extends RuntimeException` is disjoint from the existing
`IllegalArgumentException | IllegalStateException` catch, so the two catches do not interfere;
the safe path (`addEntry` body) is byte-for-byte unchanged.

**Pass 8 (the "check again several times" pass — fallback scope + probe coverage).** —
*Caught two more.* (a) **`rebuildSubtree(0)` versioning risk.** An earlier draft, on a cascade
straddle, fell back to a blind `rebuildSubtree(0)` (whole index) because the cascade depth was
not threaded out. The reverted experiment's versioning regression — cause never pinned — most
plausibly came from exactly such a wide rebuild. *Fix:* `integrate` now tags
`HOTStraddleException` with the unsafe node's real spine depth (§4.3), so the fallback is
*always* minimally scoped — `rebuildSubtree(pathDepth-1)` for the diagnosed case, which is the
*same scoped form* baseline `betaIsDiscBit`-full already uses under the green versioned suite.
A blind whole-index rebuild now happens only for a genuine root straddle (its true minimal
scope). C9/C13 rewritten honestly (the experiment's cause is *unpinned*; the plan avoids the
risk structurally rather than claiming it understood it). (b) **Probe coverage.** The
diagnosis-time probe checked I5 only; Theorem 4's premise is all-invariants. A residual
branch-op drift (`addEntryWithInsertInfo`, or `addChildAtCombination` whose sparse
`comboPartial` is an I6 — *routing* — hazard, not I5) would slip a pure-I5 probe. §8's gate is
widened to a per-insert **I5 + I6** check.

**Pass 9 (the "even more passes" round — the proof gap).** — *Caught:* Theorem 1's I5 proof
covered only the leaf-overflow BiNode (`L₀`/`L₁`) and said its halves are "subsets of `L`'s
keys" — but `splitLeafPage` splits `L`'s keys **∪ {K}**, and `addEntry` is *also* reached on
the branch path with a leaf-pair / pull-up BiNode whose affected child is *kept*, not split.
*Fix:* the I5 proof is restructured into (a) the affected child's two replacements — covered
by `addEntry`'s **standing precondition** (each `λ`-constant; proven per BiNode kind in
`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`, unchanged here; `K`-agrees-with-`L`-on-`D(N)` is the
merge-vs-branch bound) — and (b) the non-affected siblings — the new obligation discharged by
the guard. §4.4 reframed: `addEntry` fires for merge *and* branch integrations *and*
cascades, all uniformly guarded.

**Pass 10 (invariant completeness).** — *Caught:* the doc relied on **I6 (routing)**
everywhere (the probe, the §5.1 Remark, the betaIsDiscBit link) but §2 never defined it, and
Theorem 1 proved only the structural invariants. *Fix:* I6 added to §2; Theorem 1 restated to
yield a **canonical** `N'` and an explicit I6 clause (canonical ⇒ routing-correct by the
sparse-partial-key theorem; the straddle-specific argument that off-path `λ`-columns keep
subset-match exact).

**Pass 11–14 (re-verification, no new holes).** — *Confirmed by re-derivation:* the
recanonicalize fixes the straddle and leaves ancestors unaffected (their disc bits and the
node's key *range* are unchanged; `K` lies within that range); the new key `K` is counted
exactly once (the un-mutated overflowed leaf is collected *without* `K`; `rebuildSubtree`
adds `K`); a straddling sibling breaks I6 as well as I5 (a `λ=1` higher-index child can
shadow it), so the guard protects both; `subtreeMsdb` cannot throw (distinct keys ⇒ `msdb`
defined; one-key ⇒ sentinel); termination holds (`rebuildSubtree` never re-enters the
incremental insert; no infinite re-recanonicalization — each insert makes progress); base
cases hold (the first leaf overflow has `pathDepth=0` ⇒ `integrate`'s new-root case, no
`addEntry`, guard not reached); the guard runs only on a not-full node (`integrate` cascades
a full one); the §8 I5+I6 probe catches a guard false-negative empirically.

**Pass 15 (the I11 proof gap).** — *Caught:* Theorem 1's I11 proof covered compound
*siblings* (via the guard) but not the affected child's **compound** replacements (the C5
case — a split half too large for one page). *Fix:* the I11 proof is split like the I5 proof
— siblings via the guard, the affected child's replacements via `addEntry`'s standing
well-formed-`BiNode` precondition (a `λ`-split half varies only below `λ`, so its MSB is
below `λ`).

**Pass 16–17 (expert-reviewer simulation, no new holes).** — *Confirmed:* the recanonicalized
subtree correctly replaces the old node — `rebuildSubtree` re-points the spine slot and its
height escalation re-derives ancestors if the rebuilt height changed (existing, tested);
`getMostSignificantBitIndex()` is reliably maintained (set by every `HOTIndirectPage` factory
from the disc-bit mask, SingleMask *and* MultiMask), so the guard's compound-sibling shortcut
is reliable; a leaf's parent may itself be height ≥ 2 (mixed leaf/compound children) — then
`integrate` takes its **natural** intermediate-node case (`parent.height > biNode.height`),
which adds no discriminative bit, needs no height fix-up, and cannot straddle — so the guard
correctly fires only for a height-1 parent; `addEntry`'s pre-existing `IllegalArgumentException`
throws ("β already a disc bit", "node full") never fire on a canonical-tree leaf overflow
(β = `L`'s MSDB ∉ `D(N)` by I5; the node is not full or `integrate` would cascade), and are
in any case disjoint from the new `HOTStraddleException` catch.

*Seventeen passes total. No outstanding holes; the plan is internally consistent, every
theorem's proof is complete for the in-scope operation (`addEntry`), and the plan is ready
for review.*

---

## §10. Scope boundary

**In scope:** the **`addEntry`** straddle guard (`splitBitIsSafe`) + the `HOTStraddleException`
signal + the scoped `rebuildSubtree` fallback. This is the diagnosed first drift source and
the only operation this plan formally verifies.

**Out of scope — straddle-safety of `addEntryWithInsertInfo` / `compressRangeWithEntry`.**
These branch-insert bit-adders keep and re-stamp their affected run, so they need a *stronger*
property — the affected run must be one-sided on `β` — with a different guard and a separate
correctness proof (Review Pass 5). They are *not* changed here. §8's zero-drift probe is the
gate: if it stays green after the `addEntry` fix, they are empirically straddle-free on the
tested workloads; if it reports drift, that becomes a focused follow-up.

**Out of scope — incremental `betaIsDiscBit`-full splice.** Once global canonicity holds
(this fix + green probe), that case is always `betaValue=1` with a one-sided affected run —
the precondition under which a faithful `compressRangeWithEntry`-style splice can be made
correct. A separate plan.

