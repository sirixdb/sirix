# HOT Incremental Split — Formal Verification Pass

**Status:** Second formal verification pass for the HOT structural-correctness work.
The first pass — `HOT_FORMAL_FOUNDATION.md` — derived a *detect-and-rebuild* fix and
proved `bulkBuild` correct. This pass verifies the **incremental insert + split +
cascade** algorithm of Binna's HOT, so it can be ported faithfully from the
reference implementation instead.

**Why this pass exists.** The campaign concluded (Foundation Theorem 3) that
"incremental repair cascades 1 → thousands and cannot converge", and on that basis
set the incremental approach aside. That conclusion is *correct for the operator it
was about* — in-place mask repair — but it was over-generalized to Binna's
incremental split, which is a different operator. This document separates the two,
verifies Binna's split + cascade (termination, invariant preservation, the
cascade-to-root), and reconciles the result with Foundation Theorem 3.

**Primary sources** (extracted directly, not paraphrased): the HOT C++ reference
`~/IdeaProjects/hot-reference` — `HOTSingleThreaded.hpp`, `HOTSingleThreadedNode.hpp`,
`HOTSingleThreadedNodeBase.hpp`, `HOTSingleThreadedInsertStackEntry.hpp`,
`commons/{DiscriminativeBit,BiNode,InsertInformation,PartialKeyMappingBase}.hpp`; and
R. Binna, *Fast and Space-Efficient Indexing …*, PhD thesis, Univ. Innsbruck 2020 —
§3.2–3.3 (structure, insertion), §4.2–4.3 (SMHP and its lemmas).

---

## 1. Conventions and model

### 1.1 Bit significance

An **absolute bit index** numbers key bits MSB-first from byte 0. **Lower index =
more significant.** `node.MSB` is `mMostSignificantDiscriminativeBitIndex`, defined
as the **minimum** (most significant) discriminative-bit index of the node
(`PartialKeyMappingBase.hpp:28-34`). "Bit `a` is more significant than bit `b`"
means `index(a) < index(b)`.

### 1.2 Structure

- **Compound node** (= Sirix `HOTIndirectPage`): a fanout-`≤k` node, `k = 32`
  (`MAXIMUM_NUMBER_NODE_ENTRIES`). It flattens a binary Patricia trie of `≤ k−1`
  **BiNodes** and carries `≤ k` child pointers, a discriminative-bit mask, and
  per-child sparse partial keys. It has an integer `mHeight`.
- **BiNode**: one binary split `{discriminativeBitIndex, height, left, right}` —
  the unit produced by a split and consumed by integration.
- **Leaf.** Binna's leaf holds one entry. *Sirix's leaf page holds `≤ C = 512`
  entries* — see §8. A leaf has no discriminative bit; its insert-stack MSB is the
  sentinel `UINT16_MAX` (`HOTSingleThreadedInsertStackEntry.hpp:14-18`).

### 1.3 Invariants

The structural invariants are those of `HOT_FORMAL_FOUNDATION.md` §2:
**I1** (leaf key-disjointness), **I3** (distinct partials per node), **I4**
(`p_0 = 0`), **I5** (every captured disc bit is subtree-constant), **I6** (routing
reaches the holding leaf), **I7** (partials strictly ascending), **I8** (children
ordered by first-key), **I11** (the *trie condition* — disc bits become strictly
less significant down the tree). I6 is the goal; I5 + the encoding entail it
(Foundation Theorem 2). This pass shows the incremental algorithm preserves all of
them; the load-bearing one for the cascade is **I11**.

> **Trie condition (I11), node form.** For every parent compound node `P` and child
> compound node `C`: `index(P.MSB) < index(C.MSB)`. Equivalently, for a BiNode `B`
> being attached under `P`: `index(P.MSB) < index(B.discriminativeBitIndex)`. The
> reference asserts exactly this — `HOTSingleThreaded.hpp:523`.

---

## 2. The algorithm (as extracted from the reference)

`insert(K, tid)` — `HOTSingleThreaded.hpp:227-274`:

1. **Descent.** `searchForInsert` walks root→leaf, pushing onto an explicit parent
   stack `insertStack[0..leafDepth]` an entry `(childPointerSlot,
   {entryIndex, MSB})` per level (`HOTSingleThreaded.hpp:96-109`). The leaf's MSB is
   the sentinel.
2. **Mismatch bit.** `executeForDiffingKeys` finds the first byte where `K` differs
   from the resident key and the bit within it (`__builtin_clz`) — the
   `DiscriminativeBit β` (`DiscriminativeBit.hpp:52-55`).
3. **Insert depth.** Walk *down* the stack to the node that owns `β`'s level:
   `while (index(β) > insertStack[d+1].MSB) ++d;` (`HOTSingleThreaded.hpp:265-269`).
   The chosen depth `d*` is the shallowest node whose child's MSB is *not* more
   significant than `β`.
4. **Local op at `node[d*]`** (`insertNewValueIntoNode`, `:417-489`):
   - **not full** → `addEntry(β, value)` in place (`HOTSingleThreadedNode.hpp:415`).
   - **full, `β` less significant than `node.MSB`** → `B := split(node)` then
     `integrateBiNodeIntoTree(d*, B)` (`HOTSingleThreaded.hpp:480-483`).
   - **full, `β` more significant than `node.MSB`** → **pull-up**:
     `B := BiNode::createFromExistingAndNewEntry(β, node, value)` — the *entire*
     node becomes one child of a fresh BiNode on `β` — then
     `integrateBiNodeIntoTree(d*, B)` (`:484-487`).
5. **`split(node)`** — `HOTSingleThreadedNode.hpp:549-565`: partition the node's
   entries at *its own* root BiNode (i.e. at `node.MSB`) into a smaller half and a
   larger half; `compressEntries` recodes each half into a fresh compound node
   (dropping the now-consumed `node.MSB`, re-encoding partial keys); the new value
   joins the affected half. Returns `BiNode{discBit = node.MSB,
   height = node.height+1, left, right}`.
6. **`integrateBiNodeIntoTree(stack, d, B)`** — `HOTSingleThreaded.hpp:493-547`:
   - `d == 0` → wrap `B` in a 2-entry compound node; it becomes the **new root**
     (`createTwoEntriesNode`, `:494-495`). *The only height-increasing step.*
   - `parent.height > B.height` → **intermediate node**: wrap `B` in a fresh
     2-entry node slotted where the old node was; the parent is untouched
     (`:501-503`).
   - else (`parent.height == B.height`):
     - `parent` not full → `addEntry` the new side into the parent, patch the old
       slot (`:504-519`).
     - `parent` full → assert the trie condition (`:523`), `B' := split(parent)`,
       patch, **`integrateBiNodeIntoTree(stack, d−1, B')`** — the cascade
       (`:520-536`).

---

## 3. The two cascade modes

A single insert can propagate to the root in exactly two ways.

- **Capacity cascade.** Step 6's last branch: a *full* parent cannot absorb `B`, so
  the parent is itself split and `integrateBiNodeIntoTree` recurses one level
  shallower. If every node on the path is full, the recursion reaches `d = 0` and a
  new root is created.

- **Pull-up cascade.** Step 4's third branch: `β` is more significant than
  `node[d*].MSB`, so `β` cannot live *inside* `node[d*]`'s block — the whole node is
  re-parented under a fresh BiNode on `β`. That BiNode is integrated at the *same*
  depth; if its `β` is in turn more significant than the parent's MSB, or the parent
  is full, a capacity cascade follows — again, possibly to a new root.

Both modes are bounded structural recursions, not fixpoint iterations (§7). Both
terminate (§4) and preserve every invariant (§5).

---

## 4. Theorem I — Termination

> **Theorem I.** `insert` — descent, local op, and either cascade — performs
> `O(H)` work and terminates, where `H` is the tree height.

**Proof.** The descent visits each stack level once: `O(H)`. The local op is `O(k)`.
The cascade is the recursion `integrateBiNodeIntoTree(stack, d, ·)`. Its four
branches: three (`d == 0`; `parent.height > B.height`; `parent` not full) **return
without recursing**; the fourth recurses as `integrateBiNodeIntoTree(stack, d−1,
·)`. The recursion argument `d` is a non-negative integer that **strictly
decreases** on the only recursive branch, and the `d == 0` branch is non-recursive.
Hence at most `d* + 1 ≤ H + 1` calls, each `O(k)`. Total `O(H·k) = O(H)`. ∎

**Bound on `H`.** The reference's stack is `std::array<…, 64>`. With `k = 32`,
`H ≤ ⌈W / log₂ k⌉` for key width `W`; for `W = 64`, `H ≤ 13`. A faithful Java port
must size its descent stack `≥ 64` (Sirix's `MAX_PATH_DEPTH = 64` already does).
Height grows by at most one per insert (§6), so `H` cannot outrun the bound.

---

## 5. Theorem II — The trie condition (I11) is preserved through the cascade

> **Theorem II.** If I11 holds before an insert, it holds after — at every node
> touched by the descent op and by every level of either cascade.

**Proof.** Induct on the structural operations. Hypothesis: I11 holds pre-insert.

**(a) `addEntry` at node `N` (not full).** Step 3 chose `d*` so that `index(β) ≥
index(N.MSB)` — `β` is *not* more significant than `N`'s own MSB (otherwise the
pull-up branch, not `addEntry`, fires). So `N.MSB = min(N.MSB, β)` is unchanged, and
I11 between `N` and its parent is untouched. `β` is folded into the affected child's
sub-block only (`getInsertInformation` isolates the affected subtree,
`HOTSingleThreadedNode.hpp:383-406`); `index(β) < index(C.MSB)` for that child `C`
(again by the `d*` choice), so I11 between `N` and `C` holds. Other children are
not touched. ∎(a)

**(b) `split(N)` ⇒ `B`.** `B.discriminativeBitIndex = N.MSB`. The two halves
`Nₗ, Nᵣ` are `compressEntries` of `N`'s block *minus its root bit* `N.MSB`;
therefore every discriminative bit of `Nₗ` and of `Nᵣ` is strictly less significant
than `N.MSB`, i.e. `index(Nₗ.MSB) > index(N.MSB)` and `index(Nᵣ.MSB) >
index(N.MSB)`. Since `B.discBit = N.MSB`, I11 holds for `B`'s two children. ∎(b)

**(c) Pull-up `B = createFromExistingAndNewEntry(β, N, leaf)`.** This branch fires
only when `index(β) < index(N.MSB)`. `B.discBit = β`, one child is `N` ⇒
`index(B.discBit) < index(N.MSB)` ⇒ I11(`B`, `N`); the other child is a leaf (no
MSB) ⇒ vacuous. ∎(c)

**(d) Integration of `B` at depth `d`.** Parent `P = node[d−1]`.
- *Initial integration* (`B` from the descent op): the reference asserts
  `index(P.MSB) < index(B.discBit)` (`HOTSingleThreaded.hpp:523`); Binna's thesis
  §3.3 proves the descent's `d*` choice (step 3) establishes it. We take this as a
  cited result.
- *Cascade step* (`B'` from splitting a full parent `P`): by (b),
  `B'.discBit = P.MSB`. `B'` is integrated at `P`'s parent `GP = node[d−2]`. By the
  **pre-insert** I11, `index(GP.MSB) < index(P.MSB) = index(B'.discBit)`. So the
  trie-condition assert holds at `GP` *without relying on the descent* — it reduces
  to I11 of the untouched upper trie. By induction up the path, every cascade
  level's integration satisfies I11.
- *New root* (`d = 0`): no parent — vacuous.

Combining (a)–(d): every node created or modified satisfies I11, and every node
left untouched retains it. ∎

**Reading.** The cascade is *self-justifying*: each split emits a BiNode whose
discriminative bit equals the split node's MSB, and the parent already satisfied
`index(parent.MSB) < index(child.MSB)` — so the very assert the next integration
needs is the pre-insert I11. No bit is ever "extended" or "repaired"; the trie
condition is *carried upward unchanged*.

---

## 6. Theorem III — Split preserves I1/I5/I7/I8; Theorem IV — height

> **Theorem III.** `split(N)` and its integration preserve I1, I5, I7, I8.

**Proof (sketch — this is Binna's split correctness, thesis §3.3).** `split`
partitions `N`'s entries at `N.MSB`. By Fact R1 (`HOT_FORMAL_FOUNDATION.md` §3),
`N.MSB` is constant within each half — `0` on the left, `1` on the right — so each
half is a complete `R(S)`-subtree ⇒ **I5**. `compressEntries`
(`HOTSingleThreadedNode.hpp:487-508`) drops `N.MSB` and re-encodes the remaining
disc bits MSB-first ⇒ partials strictly ascending ⇒ **I7**, and `p_0 = 0` ⇒ I4. The
halves are disjoint contiguous key ranges (Fact R3) ⇒ **I1**, emitted
left-before-right ⇒ **I8**. `addEntry` adds one disc bit to one affected subtree;
the same Facts give I5/I7/I8 locally. ∎

> **Theorem IV.** Tree height increases by at most one per insert, only via the
> `d = 0` new-root step; and the result is the canonical SMHP HOT for the key set.

**Proof.** The only height-increasing operation is `createTwoEntriesNode` at
`integrateBiNodeIntoTree`'s `d == 0` branch, reached at most once per insert (the
cascade recurses monotonically toward `d = 0` and stops there). The
`parent.height > B.height` test routes a BiNode either into the existing parent or
into a fresh *intermediate* compound node; this realizes the SMHP level function
`l(v)` (thesis §4.2). By **Binna Lemma 2** the SMHP of a key set is deterministic
and insertion-order-independent, so the incremental algorithm and `bulkBuild`
converge to the *same* canonical HOT. ∎

> **Corollary.** The incremental algorithm and `HOTBulkBuilder` produce
> structurally identical tries for the same key set. The first verification pass
> (`HOTFormalModelTest`, 4800 adversarial key sets, 0 violations) therefore *also*
> validates the incremental algorithm's *target* — what remains to verify is the
> *path*, which Theorems I–III do.

---

## 7. Reconciliation with Foundation Theorem 3 — the crux

Foundation Theorem 3 states: *the operator "extend one indirect's mask to repair a
local I5 violation" is not a contraction — one application can create new
I5/I7/I8/I11 violations at the node, its parent, and its children; the repair
relation has cycles and is not well-founded.* That is correct, and it is why the
campaign's `extendIndirectMaskForClosure` family produced 1 → thousands cascades.

**Binna's cascade is a different operator, and Theorem 3 does not apply to it:**

| | Foundation Theorem 3's operator | Binna's split cascade |
|---|---|---|
| Acts on | a *malformed* trie, fixed shape | a *valid* trie |
| Per step | mutates a mask **in place** | creates **fresh** nodes (`compressEntries`), changes shape |
| Recursion | fixpoint search over malformed tries — may not terminate | primitive recursion on `depth`, strictly decreasing — terminates (Thm I) |
| Post-condition | none guaranteed | a valid HOT, height ≤ H+1 (Thms II–IV) |
| I11 | perturbed at node, parent, children | carried upward unchanged (Thm II) |

The campaign never extended a mask the way Theorem 3 describes *because Binna
doesn't* — Binna **splits** (fresh masks) and **re-parents** (pull-up). The
campaign's `HOTTrieWriter` accreted mask-repair patches *instead of* faithfully
porting the split, hit Theorem 3's wall, and the wall was then mistaken for a
property of incremental insertion in general. It is not.

> **Conclusion of §7.** Foundation Theorem 3 forbids in-place mask repair. It does
> **not** forbid Binna's incremental split. A faithful port of `HOTSingleThreaded`'s
> insert is sound — Theorems I–IV — and is the recommended implementation.

---

## 8. Cascade-to-root — concrete walkthrough

**Capacity cascade.** Insert `K`; suppose every node on the descent path is full.
At `d*`, `split(node[d*]) ⇒ B`. `integrateBiNodeIntoTree(d*, B)`: parent full ⇒
`split(parent) ⇒ B'` ⇒ recurse at `d*−1`. … At `d = 0`: `createTwoEntriesNode(B_top)`
is the **new root** — a 2-entry compound node, *not full*. Height `+1`. The next
insert cannot immediately re-cascade through the new root. Every level's
trie-condition assert held (Thm II(d)).

**Pull-up cascade.** Insert `K` that branches off at `β` more significant than the
descent's landing node — `node[d*]` is full, pull-up fires: `node[d*]` becomes one
child of `B{β}`. `integrateBiNodeIntoTree(d*, B)`: if `index(β) < index(parent.MSB)`
the parent cannot host `B` either, so `B` re-parents the parent in turn — the
pull-up climbs until it reaches a node whose MSB is more significant than `β`, or
the root. **This is precisely the case the rebuild approach was introduced to
handle** (`HOT_FORMAL_FOUNDATION.md` §6, F1 "stale mask"): a key branching above an
ancestor's mask. Binna does *not* leave a stale mask — it re-parents the subtree
*below* a fresh, more-significant BiNode. Drift is structurally impossible because
the algorithm never lets a key sit in a subtree whose ancestor fails to discriminate
its branch bit.

---

## 9. Adversarial scrutiny — port risk areas

Genuine subtleties that a faithful port must reproduce; each is a place the campaign
could have gone wrong.

1. **The 1:31-split caveat** (`HOTSingleThreaded.hpp:524-528`). When a full parent
   splits and one side ends up with a single *non-leaf* entry, that entry is itself
   pulled up, so `numberEntriesInLowerPart` is `1`, not the side's entry count. The
   port must recompute the affected index accordingly or it patches the wrong slot.
2. **Leaf MSB sentinel.** The descent's `while (index(β) > insertStack[d+1].MSB)`
   relies on the leaf entry carrying `MSB = UINT16_MAX`. Omitting it makes the
   descent stop one level too shallow.
3. **`compressEntries` recoding.** Dropping the consumed root bit and re-encoding
   partial keys MSB-first, including the `uint8→uint16→uint32` partial-key
   widening. Sirix's `HOTIndirectPage` has *two* layouts (SingleMask, MultiMask);
   the recoding must pick the correct one and round-trip (`computeDensePartialKey`
   must agree with the stored partials — already centralized this pass).
4. **Per-node `mHeight`.** The intermediate-node decision is `parent.height >
   B.height`. The port must track height on every `HOTIndirectPage` and set
   `B.height = splitNode.height + 1` exactly.
5. **Sirix copy-on-write / TIL.** Binna mutates nodes in place. Sirix is a
   persistent structure: every `addEntry`, `split`, and integration must produce a
   *new* page copy-on-written into the transaction-intent log, and the cascade's
   "walk up the parent stack" becomes a **path-copy to the index root**. The
   descent already CoWs the path (`prepareLeafOfTree`); the cascade must continue
   that discipline upward and re-point the index-page root slot when a new root is
   created.
6. **Leaf *pages* vs. Binna leaves (§1.2).** Binna's leaf holds one entry; Sirix's
   `HOTLeafPage` holds `≤ 512`. The port therefore has **two** overflow events,
   both feeding `integrateBiNodeIntoTree`:
   - *leaf-page overflow* → split the leaf page at the MSDB of its keys into two
     leaf pages → a `BiNode{MSDB, leftPage, rightPage}` → integrate.
   - *indirect-page overflow* (32 children, during integration) → Binna's
     compound-node split + capacity cascade.
   The leaf-page split's `discBit` is the leaf page's keys' MSDB; integrating it
   satisfies `index(parent.MSB) < index(MSDB)` **because the leaf page is a
   complete `R(S)`-subtree** (Foundation §3.2) — its keys agree on every ancestor
   bit, so their MSDB lies below the parent's mask. The port must therefore keep
   the leaf-page-is-an-`R(S)`-subtree invariant, which is exactly what a clean
   integration maintains inductively.
7. **Versioned leaf fragments.** A split/rebuilt leaf page is a new page; under
   DIFFERENTIAL/INCREMENTAL versioning it is emitted as a full first fragment, and
   tombstones must be carried through the split (see `hot-tombstone-preservation`).
8. **`getInsertInformation` / affected subtree.** Computing the maximal subtree
   that agrees with `K` above `β` from Sirix's mask layout — the port's equivalent
   of `HOTSingleThreadedNode.hpp:383-406`, including its round-trip self-check.

---

## 10. Verification plan

The theorems above are pen-and-paper. Back them with an executable clean-room model
mirroring `HOTFormalModelTest` (which found a real error in the first pass):

- **V1 — Incremental == canonical.** For adversarial key sets in adversarial
  insertion orders, assert the incrementally built trie is structurally identical
  to `bulkBuild` of the same set (Theorem IV corollary; exercises Binna Lemma 2).
- **V2 — Invariants after every insert.** After each individual insert (not just at
  the end), `HOTInvariantValidator` reports zero violations — catches a cascade
  step that transiently breaks I11/I5.
- **V3 — Cascade coverage.** Generators that force (a) full-path capacity cascades
  and (b) pull-up-to-root; assert height grows by exactly one and the new root has
  2 children.
- **V4 — Termination bound.** Assert every insert touches `≤ H+1` nodes.
- **V5 — End-to-end.** `HOTVersionedLeafStressTest` + `HOTFormalVerificationTest`
  with I8 enforcement on — 0 violations across all revisions and versioning types.

A change is accepted only when V1–V5 are green.

---

## 11. Conclusion and port checklist

The faithful incremental split is **sound**: it terminates in `O(H)` (Thm I),
preserves I11 through both cascade modes (Thm II), preserves I1/I5/I7/I8 (Thm III),
grows height by ≤1 per insert toward the canonical SMHP (Thm IV), and is *not* the
divergent operator Foundation Theorem 3 forbids (§7). The cascade-to-root is
correct and bounded — by design, like a B⁺-tree split, ending in a fresh 2-entry
root.

**Port checklist** (each item maps to a §9 risk):
1. Descent stack with leaf MSB sentinel; depth-finding `while` loop.
2. `addEntry` / `split` / `createFromExistingAndNewEntry` on `HOTIndirectPage`.
3. `integrateBiNodeIntoTree`: root / intermediate-node / merge / capacity-cascade —
   recursion strictly decreasing `depth`.
4. `compressEntries` with correct SingleMask/MultiMask recoding.
5. Every node op as copy-on-write into the TIL; path-copy to root; new-root
   re-points the index-page slot.
6. Two overflow entry points (leaf-page split, indirect-page split) both feeding
   `integrateBiNodeIntoTree`.
7. The 1:31-split index recomputation.
8. Tombstone-preserving leaf-page split; versioned-fragment emit.

The verification clears the approach; the executable model (V1–V5) should be built
alongside the port so each checklist item is mechanically guarded.
