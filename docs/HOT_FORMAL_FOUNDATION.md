# HOT Routing — Formal Foundation and Verification Plan

**Status:** Theoretical foundation for the HOT structural-correctness work.
Supersedes the empirical option-picking in `HOT_ROUTING_ENCODING_REWRITE.md`:
that doc enumerated three encodings and *recommended* one; this doc *derives*
the correct fix from a model and proves it.

**Why this exists.** The strict-Binna campaign (126+ commits, Stages A–H) never
had a formal routing-correctness theorem — every fix was patch-and-measure, and
the survey of all 13 `docs/HOT_*.md` confirms "no doc states a fully formal
MSDB-closure theorem." Consequently every incremental fix attempt cascaded
1 → thousands of violations. This document closes that gap.

**Primary sources** (re-read for this document, not paraphrased from memory):
R. Binna, *Fast and Space-Efficient Indexing for Main-Memory Database Systems
on Modern Hardware*, PhD thesis, Univ. Innsbruck, 2020 — Ch. 3 (HOT structure),
Ch. 4 (sHOT/SMHP and the proofs), §5.3.1 (linearized patricia tries); and the
HOT C++ reference (`~/IdeaProjects/hot-reference`, GitHub `speedskater/hot`) —
`SparsePartialKeys.hpp`, `HOTSingleThreadedNode{,Base}.hpp`, `DiscriminativeBit.hpp`.
Where this document cites a "Binna Lemma N" it is that thesis's Lemma N.

---

## 1. Formal model

### 1.1 Keys

A **key** is a fixed-width bit string `K ∈ {0,1}^W` (variable-length byte keys
are right-padded to the index's maximum width; comparison is unsigned
big-endian, which is lexicographic on the bit string). `K[b]` is the bit at
position `b`, with `b = 0` the most significant. The key order `<` is
lexicographic: `K < K'` iff at the least `b` with `K[b] ≠ K'[b]`, `K[b] = 0`.

For a non-empty key set `X`, write `min X` / `max X` for the lex extremes.

### 1.2 Trie

A **HOT trie** `T` over a key set `S` is a rooted tree of two node kinds.

- **Leaf** `L`: a set `entries(L) ⊆ S × Values`, with `|entries(L)| ≤ C`
  (`C = 512`). `keys(L)` is the key projection. Tombstones are entries with a
  sentinel value; they are keys and participate in everything below.
- **Indirect** `I`: a **mask** `M_I ⊆ ℕ` (a finite set of bit positions, the
  *discriminative bits*), and an ordered list of `n` children, `2 ≤ n ≤ 32`.
  Child `i` carries a **stored partial** `p_i ∈ ℕ` and a child node.

`subtree(x)` is the set of keys in all leaves at or below `x`.
`anc(I)` is the set of indirects on the path from the root to `I` inclusive.

### 1.3 PEXT and dense partial keys

For a key `K` and mask `M = {b_0 < b_1 < … < b_{m-1}}`, the **dense partial
key** is

```
densePK(K, M) = PEXT(K, M) = Σ_{j=0}^{m-1} K[b_{m-1-j}] · 2^j
```

i.e. the `m` key-bits at the mask positions, packed MSB-first into an integer.
This is `Long.compress` on the masked word(s) — see
`HOTIndirectPage.findChildSpanNode` (`HOTIndirectPage.java:473-510`).

### 1.4 Routing and descent

At an indirect `I`, `findChildIndex(I, K)` is (`HOTIndirectPage.java:485-509`):

```
d := densePK(K, M_I)
Subsets := { i : (d & p_i) == p_i }          # p_i ⊆ d  (subset match)
if ∃ i with p_i == d:  return i              # exact match preferred
else:                  return max(Subsets)   # highest-index subset match
```

**Reference deviation.** Binna's HOT routes by *pure* highest-index subset
match — `SparsePartialKeys::search` yields the subset-match bitmask and
`toResultIndex` returns `getMostSignificantBitIndex` of it
(`HOTSingleThreadedNodeBase.hpp:44-48`); there is no exact-match branch. On a
*well-formed* trie the two agree — the exact match, where it exists, already is
the highest-index subset match. Sirix's exact-match preference is an
undocumented band-aid for *malformed* tries; it should be unnecessary, not
load-bearing, once the trie is canonical (§8).

`descend(root, K)`: from the root, repeatedly replace the current indirect by
`child[findChildIndex(·, K)]` until a leaf is reached; return that leaf.

A read is *correct* iff `descend(root, K)` returns the leaf that actually holds
`K`. The whole structural problem is making `descend` agree with key location.

---

## 2. Invariants

The executable specification is `HOTInvariantValidator`; the predicates are:

| Inv | Predicate |
|-----|-----------|
| **I1**  | leaves are pairwise key-disjoint: `L ≠ L' ⟹ keys(L) ∩ keys(L') = ∅` |
| **I3**  | partials within an indirect are distinct: `i ≠ j ⟹ p_i ≠ p_j` |
| **I4**  | `p_0 = 0` for every indirect |
| **I5**  | `∀I ∀i ∀K ∈ subtree(child_i): (densePK(K,M_I) & p_i) == p_i`  — every bit of `p_i` is set in **every** subtree key's dense PK |
| **I6**  | `∀K ∈ S: descend(root,K)` is the leaf holding `K` |
| **I7**  | `∀I ∀i: p_i < p_{i+1}` (unsigned) |
| **I8**  | `∀I ∀i: min subtree(child_i) < min subtree(child_{i+1})` |
| **I11** | `∀I, ∀ parent P: min(M_P) < min(M_I)` — each node's *most significant* disc bit is strictly less significant (larger bit position) than its parent's. `bulkBuild` satisfies the stronger per-path trie condition (§4). |

These are not independent (§5). I6 is the *goal*; the rest are structural
conditions that, jointly, entail it.

---

## 3. The binary Patricia trie — the reference structure

The reference structure is **Binna's binary Patricia trie** (R. Binna, PhD
thesis *Fast and Space-Efficient Indexing for Main-Memory Database Systems on
Modern Hardware*, Univ. Innsbruck, 2020 — §3.1–3.2). For a key set `S`,
`|S| ≥ 2`, the trie `R(S)` — Binna's BiNode tree — is defined by the standard
MSDB recursion:

```
R(X):
  if |X| ≤ 1: return Leaf(X)
  β := the most significant bit position where two keys of X differ
  X0 := { K ∈ X : K[β] = 0 } ;  X1 := { K ∈ X : K[β] = 1 }
  return Branch(β, R(X0), R(X1))
```

Both `X0`, `X1` are non-empty (β is a *differing* bit), so the recursion is
well-founded. Two structural facts, immediate from the definition:

> **Fact R1 (partition).** Every branch bit `β` is *constant* within each of its
> two child subtrees: all keys under the 0-child have `K[β]=0`, all under the
> 1-child have `K[β]=1`.

> **Fact R2 (descending significance).** Along any root-to-leaf path the branch
> bits are *strictly increasing* (each recursive `β` is a differing bit of a
> strict subset, hence less significant than — numerically greater than — the
> parent `β`, which the subset already agrees on).

> **Fact R3 (in-order = sorted).** Listing `R(S)`'s leaves left-child-first
> yields `S` in lexicographic order; for any branch, all 0-child keys `<` all
> 1-child keys.

A **HOT trie is a compression of `R(S)`**: a HOT indirect `I` corresponds to a
connected block of `R(S)` branch nodes (`M_I` = the set of their bit
positions, `|M_I| ≤ 32`); a HOT leaf corresponds to an `R(S)` subtree whose key
group is *cut early* (kept whole rather than recursed) because it already fits
in `C` entries. The stored partial `p_i` is the **sparse-path encoding** of the
block-path to child `i`: bit `j` of `p_i` is 1 iff branch `b_j` lies on that
path *and* the path takes the 1-side there.

### 3.1 The canonical HOT — Binna's SMHP

Binna proves the compression is not arbitrary. **Static Minimum Height
Partitioning** (SMHP, thesis §4.2, after Kovács & Kis) partitions `R(S)` into
compound nodes — each holding ≤ `k−1` BiNodes, hence ≤ `k` children — so the
induced tree has minimum height. Two of his lemmas are the backbone of this
document:

> **Binna Lemma 2 — determinism (thesis §4.3.3).** For a given key set, the
> SMHP of its binary Patricia trie is *deterministic and independent of
> insertion order*. There is a unique **canonical HOT** for `S`.

> **Binna Lemma 3 — recursive structure (thesis §4.3.4).** Every subtree of an
> SMHP partitioning is itself the SMHP over its own underlying keys — a
> vertex's level and intrapartition weight depend only on its descendants.

Lemma 2 makes "the correct trie for `S`" a well-defined object; Lemma 3 makes
it **compositional** — a subtree is correct in isolation, independent of its
surroundings. The §8 fix rests on exactly these two facts.

### 3.2 Sirix is disk-based — leaf pages, not leaf entries

Binna's sHOT stores one entry per leaf. That is a *proof device* — it keeps the
SMHP weight function clean — **not a design requirement**, and it is wrong for
Sirix: a HOT leaf here is a 64 KiB disk page, and one entry per page would be
absurd. A Sirix `HOTLeafPage` holds up to `C = 512` entries.

Binna Lemma 3 is what makes multi-entry pages sound. A leaf page stores a
*complete `R(S)` subtree* `subtree(v)`; by Lemma 3 that subtree is itself a
canonical, self-contained HOT over its own keys. The page simply stores that
sub-HOT *flat* — a sorted entry array with an intra-page PEXT search — instead
of as nested indirect pages. **Multi-entry leaf pages are fully correct; they
are not the bug.**

A disk-based engine must, however, accept two consequences:

1. **A leaf page must be a *complete* `R(S)` subtree — never an arbitrary pack
   of ~512 contiguous keys.** A contiguous lexicographic range is, in general,
   *not* an `R(S)` subtree — e.g. for `S = {000,001,010,011,100}` the range
   `[001,100]` spans the root branch and equals no subtree. The indirect
   structure routes by PEXT on disc bits, and PEXT can only address complete
   subtrees. Packing arbitrary ranges into pages yields a *B⁺-tree*
   (separator-key comparison routing) — a fundamentally different index, not a
   HOT.

2. **Leaf-page fill is data-determined.** `bulkBuild` cuts a page at the highest
   `R(S)` node whose subtree fits a page (≤ `C` entries and ≤ page bytes). On
   realistic key sets sibling subtrees are roughly balanced, so pages land
   ~50–100 % full — comparable to a B⁺-tree's half-full invariant. On
   pathologically skewed key sets a page can be underfull; that is the price of
   trie-routing correctness and the same height-vs-fill trade HOT already makes
   at the indirect level (HOT optimizes height, not fill — thesis §3.1).

Hence `bulkBuild` for Sirix is two deterministic phases: (a) cut leaf pages at
the highest `R(S)` subtrees that fit a page; (b) SMHP-partition the remaining
upper `R(S)` into indirect pages (≤ `k` children), each leaf page counting as a
weight-0 leaf. Both phases are deterministic, so `bulkBuild(S)` is a
deterministic total function and Lemma 2's uniqueness extends to it.

> **The defect, stated precisely.** A Sirix leaf page that is *not* a complete
> `R(S)` subtree — one holding keys from two or more Patricia subtrees, i.e.
> keys disagreeing on a bit used as an ancestor disc bit — breaks Fact R1 for
> that bit, hence breaks I5, hence (Theorem 2) breaks routing. Every failure
> mode in §6 is an instance of this one statement.

---

## 4. Theorem 1 — Bulk-build correctness

> **Theorem 1.** Let `bulkBuild(S)` compute the SMHP compression of `R(S)` into
> HOT indirects (`≤ k` children each) and HOT leaves (each a *complete* `R(S)`
> subtree with `≤ C` keys), with sparse-path-encoded partials. Then
> `bulkBuild(S)` satisfies **I1, I3, I4, I5, I6, I7, I8, I11** simultaneously;
> and by Binna Lemma 2 its output is the unique canonical HOT for `S`,
> independent of the order in which `S`'s keys were supplied.

**Proof.**

- **I1.** `R(S)`'s leaf key-groups are disjoint (recursion partitions `X`). ∎
- **I5.** Take indirect `I`, child `i`, key `K ∈ subtree(child_i)`. Every bit
  `β ∈ M_I` set in `p_i` is, by the sparse-path encoding, a branch bit on the
  block-path to child `i` taken on the 1-side. `K` lies under that branch's
  1-child, so by **Fact R1** `K[β] = 1`, hence that bit of `densePK(K,M_I)` is
  1. Thus `p_i ⊆ densePK(K,M_I)`. ∎
- **I11.** `min(M_P)` is `P`'s block-root bit; it lies on the `R(S)` path to
  every key under `P`, hence on the path to child indirect `I`. Every bit of
  `M_I` labels an `R(S)` BiNode strictly below `I`'s attachment point — itself
  below that path — so by **Fact R2** all of `M_I` exceeds it, giving
  `min(M_P) < min(M_I)`. The stronger *per-path* trie condition holds by the
  same argument: `max{ M_P bits on P's internal path to I } < min(M_I)`. This
  is **not** `max(M_P) < min(M_I)`: `M_P` bits on *other* children's paths may
  be numerically larger than `min(M_I)` — Fact R2 orders bits only *along a
  path*. ∎
- **I4, I8.** By **Fact R3** the leftmost child is the all-0 branch path,
  encoded `p_0 = 0` (I4); children are emitted in `R(S)` left-to-right order,
  so `min subtree(child_i)` is strictly increasing (I8). ∎
- **I7.** By **Fact R2** the bits strictly increase along any path inside `I`'s
  block, so every block BiNode's bit is *more significant* than every bit in
  its own block-subtree; and `densePK` packs `M_I` MSB-first, so that bit
  out-weighs everything below it. At the block root, every left-subtree child
  encodes 0 there and every right-subtree child 1; the root bit dominates, so
  every left partial `<` every right partial. Inducting down the block tree,
  the left-to-right child enumeration is strictly increasing in partial value
  — I7. (This needs `densePK` to pack by absolute bit significance; Sirix's
  big-endian extraction does, so for Sirix I7 ⟺ I8. Binna's general multi-byte
  masks need not — see `SparsePartialKeys.hpp` ~line 157.) ∎
- **I6.** Follows from **Theorem 2** below, whose hypothesis (I5 + valid
  sparse-path encoding) is met by the two bullets above. ∎

**Crucially, no clause of the proof depends on leaf cardinality.** A leaf may
hold up to `C` keys; they are an `R(S)` subtree, so every *ancestor* disc bit is
constant across them (Fact R1). The keys differ only on bits that label no
ancestor branch — bits invisible to routing. **Multi-entry leaves are not the
problem; leaves that are *not* `R(S)` subtrees are.**

---

## 5. Theorem 2 — Routing soundness

> **Theorem 2.** If a trie satisfies **I5** at every indirect and its partials
> are valid sparse-path encodings (I3 + I4 + the encoding discipline of §3),
> then `descend(root,K)` returns the leaf holding `K`, i.e. **I6** holds.

**Proof sketch (reduction to Binna).** Fix an indirect `I` on `K`'s true path
and let `i*` be the child whose subtree contains `K`; let `d = densePK(K,M_I)`.

1. By **I5**, `p_{i*} ⊆ d`, so `i* ∈ Subsets` — the true child is always a
   subset candidate.
2. No child `j > i*` is a subset match. Let `β*` be the block BiNode that is
   the lowest common ancestor of `i*` and `j`. Since `j > i*` and children are
   enumerated left-to-right, `i*` lies on `β*`'s 0-side and `j` on its 1-side;
   so `j`'s block-path takes the 1-side at `β*`, and the sparse-path encoding
   sets that bit in `p_j`. But `K ∈ subtree(child_{i*})` descends `β*`'s
   0-side, so `K[β*] = 0` and the `β*` bit of `d` is 0 — hence `p_j ⊄ d`. So
   every subset match has index `≤ i*`; with step 1 (`i*` itself is a match),
   `max(Subsets) = i*`. This is exactly Binna's `SparsePartialKeys::search` +
   `toResultIndex` (HOT thesis §4.2, `HOTSingleThreadedNodeBase.hpp:44`); its
   sole precondition is that every disc bit genuinely partitions — which is I5.
3. The exact-match branch only fires when some `p_j = d`; by I3 that `j` is
   unique and step 2 already identifies it as `i*`.

So `findChildIndex(I,K) = i*` at every indirect on the path; by induction on
height `descend` reaches `K`'s leaf. ∎

> **Corollary (necessity).** If some indirect violates I5 — a disc bit `β ∈ M_I`
> is *not* constant across some `subtree(child_i)` — then a key `K ∈
> subtree(child_i)` with the minority `β` value has `densePK(K,M_I)` missing (or
> carrying) the `β`-bit, so `max(Subsets)` may select a sibling. This is exactly
> the I6 violation and the 2288-entry cascade observed in
> `HOTVersionedLeafStressTest.deleteAllAndReinsert`.

**Therefore I5-global is the linchpin: it is necessary and (with the encoding
discipline) sufficient for routing correctness.**

---

## 6. The multi-entry-leaf failure modes

A leaf becomes a non-`R(S)`-subtree — straddling an ancestor disc bit, breaking
I5 — by exactly three mechanisms, empirically confirmed by the `indirect 2` mask
dumps (`HOTVersionedLeafStressTest`, FULL/Oracle revision 2):

- **F1 — stale mask.** `indirect 2` had `mask = 0xfff6…`, `initialBytePos=11`:
  it discriminates key bytes 11–12 (the node-key region) while its children's
  firstKeys differ at byte 7 (the value: `06,06` vs `03,03`). The mask was
  MSDB-closed for the *rev-1* population; rev-2's delete-and-reinsert changed
  the subtrees' key ranges, and the mask was never recomputed. Disc bits no
  longer partition.
- **F2 — `firstKey`-only encoding.** `stored p_i = densePK(firstKey(child_i),
  M_I)` reflects one key. When `subtree(child_i)` later holds keys with other
  bit patterns at `M_I` positions, `p_i` no longer lower-bounds them — I5
  breaks even with a "correct" mask (`HOT_ROUTING_ENCODING_REWRITE.md §1`).
- **F3 — non-MSDB leaf split.** An overflowing leaf is split on a bit `β` that
  is *not* the MSDB separating it from its siblings; the resulting leaves are
  not `R`-subtrees of the parent's block, and `β` added to the parent mask is
  not constant across siblings.

All three reduce to one statement: **the live trie has drifted away from being
a compression of `R(S)` for its current key set `S`.**

`buildFlatNonStrict` *does* MSDB-close at construction (`HOTTrieWriter.java`
~13647, the Phase-7w augment loop) — so a freshly built indirect is sound. The
defect is purely **drift after construction**, driven by incremental
insert/split into multi-entry leaves.

---

## 7. Theorem 3 — The cascade theorem

> **Theorem 3.** The operator "extend one indirect's mask to repair a local I5
> violation" is not a contraction on the space of tries: a single application
> can create new I5/I7/I8/I11 violations at the indirect, its parent, and its
> children. Hence incremental repair has no convergence guarantee.

**Proof (by exhibiting the coupled perturbations).** Let `I` gain a bit `β`.

- `densePK(·,M_I)` changes for **every** key under `I` ⟹ every `p_i` must be
  recomputed; recomputation can reorder them ⟹ **I7** perturbed.
- If `β` is not constant across some sibling `subtree(child_k)` (Fact R1 fails
  for the *new* bit), that sibling now violates **I5** — a new violation, not
  the one being fixed.
- `min(M_I)` moves ⟹ the per-path trie condition (I11) between `P` and `I`,
  and between `I` and each of its children, can break ⟹ **I11** perturbed at
  the parent and at every child.
- Reordered children change `min subtree(child_i)` order ⟹ **I8** perturbed.

Each perturbation is itself an indirect-local violation that invites another
extension. The dependency graph over `{I, parent(I), children(I)}` has cycles,
so the repair relation is not well-founded. ∎

This is the theoretical content of the campaign's empirical record: G.21
"1 → 7574", G.19 "1 → 15237", Option-B dispatch "1 → 28706/37257". The
`HOT_FIX_DESIGN_V2.md` observation — "every gate that rejects a slightly-broken
layout forces a differently-broken fallback; no localized writer patch
composes" — is Theorem 3.

---

## 8. The derived fix — detect-and-rebuild

Theorems 1–3 *derive* the fix; they do not leave it to taste.

- Theorem 3 ⟹ **incremental mask repair has no convergence guarantee** — and
  with the campaign's 126-commit empirical record (1 → thousands, every
  attempt) it cannot be relied on as the fix. This sets aside
  `HOT_ROUTING_ENCODING_REWRITE.md`'s recommended Option B (proactive disc-bit
  extension) and the `extendIndirectMaskForClosure` family.
- Theorem 1 ⟹ **`bulkBuild` is correct and, being a pure total function
  `S ↦ trie`, has no fixpoint iteration and therefore cannot cascade.**

**Fix:** *detect* drift and *rebuild* the drifted subtree with `bulkBuild`.

1. **`bulkBuild(entries)`** — materialize `R(keys)`, compress to HOT, sparse-path
   encode. Pure; includes tombstone keys (they are keys). Output satisfies
   I1/I3/I4/I5/I6/I7/I8/I11 by Theorem 1.
2. **Detection** — `HOTInvariantValidator` already decides I5/I7/I8 per indirect;
   a malformed indirect is one with a violation in its own block.
3. **Rebuild** — replace the malformed subtree by `bulkBuild` of its key set,
   spliced in by copy-on-write with full path-copy to the root (Sirix's
   persistent-page discipline; tombstones preserved — `hot-tombstone-preservation`).
   **Binna Lemma 3 licenses this:** a subtree's canonical form is the SMHP over
   its own keys *only* — it does not depend on the rest of the tree — so a
   rebuilt subtree is correct in isolation and splices in without perturbing
   any sibling. This is the formal reason subtree-local rebuild is sound where
   incremental mask repair (Theorem 3) is not.
4. **Trigger** — at **commit**: one post-order walk per HOT index per
   transaction; rebuild only the *highest* malformed indirect of each disjoint
   malformed region (rebuilding an ancestor subsumes its descendants).
   Cost `O(|S|)` per commit, bounded, cascade-free; and a correct trie removes
   the `O(N)` `HOTIndexReader.collectViaLeafWalk` fallback, so steady-state
   reads get *faster*.

**Why not eager (per-split) rebuild:** correct but it rebuilds far more often
and re-pays `O(subtree)` on the hot write path. Commit-time amortizes to one
`O(|S|)` pass. (Open question Q3.)

**Bound on rebuild scope.** A rebuild is confined to the malformed subtree;
clean siblings are untouched (their CoW identity is preserved). In the common
case only small subtrees drift, so per-commit cost is far below `O(|S|)`.

---

## 9. Formal verification plan

The theorems above are pen-and-paper. They are *backed* by executable checks so
regressions are caught mechanically. The validator is the executable
specification of §2; verification is layered:

**V1 — Construction soundness (property-based).**
For random key sets `S` drawn from adversarial generators — ascending,
descending, bimodal, value/node-key-uncorrelated, delete-then-reinsert,
duplicate-heavy, chunk-boundary-straddling — assert
`HOTInvariantValidator.validate(bulkBuild(S)).isEmpty()`. Thousands of seeds.
This is the executable form of **Theorem 1**.

**V2 — Routing refinement against an oracle.**
For random `S` and random probe keys, assert `descend(bulkBuild(S), K)` returns
the leaf a `TreeMap`-oracle assigns `K`. Executable form of **Theorem 2 / I6**.

**V3 — Idempotence / no-cascade.**
`validate(bulkBuild(S))` empty ⟹ `bulkBuild(keys(bulkBuild(S)))` is
invariant-equivalent and re-validates clean. A rebuild of an already-correct
trie is a no-op up to CoW identity. Executable counterpart of **Theorem 3**'s
converse: rebuild *does* converge (in one step).

**V4 — Key-set preservation.**
`keys(rebuild(subtree)) == keys(subtree)` including tombstones — no insert is
lost, no delete resurrected. Cross-checked against the multi-revision oracle in
`HOTVersionedLeafStressTest`.

**V5 — End-to-end regression.**
The full `HOTVersionedLeafStressTest` + `HOTFormalVerificationTest` suites with
**I8 enforcement on** must reach 0 violations across all revisions and all
versioning types.

**V6 — Machine-checked theorems (stretch).**
`R(S)` Facts R1–R3 and Theorem 1 are small enough to encode in a proof
assistant over a bounded key width; deferred unless V1–V5 prove insufficient.

A change is accepted only when V1–V5 are green; V1–V4 run on every build.

---

## 10. Open questions

- **Q1.** Leaf-cut policy: when `R(S)` recursion reaches a group of `≤ C` keys,
  cut. But a group `> C` whose remaining differing bits are sparse may need a
  forced split — does any pathological key set force a leaf below the natural
  `R` boundary, reintroducing straddle? Conjecture: no, because any cut at an
  `R` branch node is pure; needs proof.
- **Q2.** Detection granularity: is per-indirect I5/I7/I8 checking at commit
  sufficient to catch every drift, or are there violations visible only
  across levels (I11)? I11 is cross-level — detection must include it.
- **Q3.** Trigger: commit-time vs. eager. Commit-time is the §8 recommendation;
  measure rebuild frequency and per-commit cost on the 100M umbra workload
  before finalizing.
- **Q4.** Incremental fast path: can a leaf split that *is* on the true MSDB and
  *does* preserve `R`-subtree-ness skip the rebuild? If so, most splits stay
  cheap and rebuild is rare. This is the performance escape hatch.

---

## Appendix — relation to existing docs

- `HOT_INVARIANTS_CATALOG.md` — the 16-invariant catalog; §2 here is its
  routing-relevant core, made into predicates with proven dependencies.
- `HOT_ROUTING_ENCODING_REWRITE.md` — proposed Options A/B/C and recommended B;
  **Theorem 3 refutes B** (it is incremental extension) and Theorem 1 justifies
  a fourth option (rebuild) the doc did not consider.
- `HOT_OPERATIONS_INVARIANTS_MATRIX.md` — the 35-ops × 16-invariants matrix; its
  "every fix closes one cell and opens others" is Theorem 3.
- `HOT_STRICT_BINNA_DESIGN.md` — "only MSDB-based splits keep partitions
  contiguous" is Fact R1 stated informally.
