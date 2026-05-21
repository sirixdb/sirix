# The Structural Cost of Adapting Binna's HOT to Disk-Page-Sized Leaves

**Status:** Paper-draft synthesis. The campaign's convergent failure evidence is
recast as identifying the cost of a single, sharp, structural adaptation: moving
Binna's HOT from single-entry leaves to disk-page-sized multi-value leaves. HOT's
author considers the structure disk-viable and separates it into a layout-independent
height-reduction concept and a replaceable SIMD layout (§1.1, personal communication
2020); this paper isolates a *third*, orthogonal axis — the multi-value leaf — as the
locus of the cost.

**Date:** 2026-05-20. **Branch:** `fix/hot-strict-binna-conformance`.

---

## §1. The Load-Bearing Assumption in Binna's HOT

Binna's Height Optimized Trie (HOT) [Binna 2018] is presented as a cache-friendly,
SIMD-routable radix index. The published correctness arguments (Theorems 1–5 in his
thesis, mirrored in `docs/HOT_FORMAL_FOUNDATION.md`) rely on the following structural
invariants, where a *node* is an internal indirect with discriminative bit set `D(N)`,
sparse partial-key encoding `stored[c] = PEXT(child[c].firstKey, mask(N))`, and SIMD
subset-match routing.

| Inv. | Claim |
|---|---|
| I1 | Tree is rooted, acyclic. |
| I3 | All children of a node share its mask's bits above its MSB. |
| I4 | Each child is itself a valid R(S)-subtree. |
| I5 | Each child is bit-constant on every bit of its parent's mask (on-path). |
| I6 | Every key K's PEXT-descent reaches the leaf containing K. |
| I7 | Children of a node are ordered by ascending partial. |
| I8 | Children of a node are ordered by ascending firstKey. |

**The understated load-bearing assumption.** Binna's HOT requires **single-entry
leaves**: every leaf contains exactly one key. This assumption is rarely spotlighted
in his exposition because it appears trivially satisfied for in-memory radix
indexes — but it is the structural premise that makes I7 ≡ I8 hold and that makes
the incremental insertion algorithm *complete* (= no rebuild fallback ever needed).

The reasoning:

- A single-key leaf is trivially bit-constant on every bit (I5 satisfied unconditionally).
- A single-key leaf's `firstKey` is its *only* key, so `stored[c] = PEXT(firstKey, mask)`
  is an exact fingerprint of the entire leaf's content.
- Subset-match descent picks the slot whose partial best matches K's PEXT; given that
  partials are exact firstKey fingerprints, "best partial match" *coincides* with
  "leftmost lex-neighbor of K". I7 (partial order) and I8 (firstKey order) describe
  the same total order on slots.
- `addEntry`'s mask-extension convention — setting the new-bit's column to 0 in every
  non-affected sibling — is routing-harmless: even if a sibling's actual firstKey
  has the new bit = 1, the sibling is a leaf with nothing further to descend into,
  so the encoding mismatch never causes a misroute.

In other words, Binna's HOT is *correct by construction* because the single-entry-leaf
assumption keeps the encoding exact and keeps the I7 / I8 orderings equivalent.

### §1.1 Two separable parts of HOT — and the third axis this paper isolates

HOT comprises two structurally independent ideas, a separation its author makes
explicit (R. Binna, personal communication, 2020):

1. **The structural concept** — combine the binary (patricia) trie nodes of `R(S)`
   into k-constrained *compound nodes* to reduce trie height (SMHP, thesis §4.2).
   This is algorithmic and **layout-independent**.
2. **The SIMD node layout** — the sparse-partial-key + PEXT subset-match encoding,
   tuned for minimal memory consumption and cache-line-resident routing.

Binna observes that HOT is "in principle usable in disk-based systems," that the
primary obstacle is that current implementations size nodes for minimal memory rather
than for page/cacheline alignment, and that "other disk-optimized node layouts are
conceivable" — i.e., part (2) is *replaceable* without disturbing part (1). Sirix-HOT
already takes him up on this: `HOTIndirectPage` / `HOTLeafPage` are page-sized
structures, not the minimal-memory layout of his reference implementation. With part
(2) re-instantiated for the page, part (1)'s height advantage carries to disk intact —
the SIMD encoding was never the *disk* value-add; the height reduction is.

The cost this paper characterizes lies on **neither** of Binna's two axes. It is a
**third** adaptation, forced by disk and orthogonal to both: the move from
**single-entry leaves to multi-value leaves** (§2). In Binna's single-entry world a
leaf is trivially bit-constant (§1), so neither the structural concept nor the layout
ever interacts with intra-leaf key spread. Multi-value leaves break exactly that
premise — and, as §§3–6 show, the break is not removable by any localized mask-fixed
or mask-extension primitive under the PEXT encoding (Theorems 1–4), *independent* of
how parts (1) and (2) are realized. The contribution is therefore not "HOT is unsuited
to disk" — its author holds, and our results concur, that it is suitable — but a
precise identification and lower-bounding of the structural cost that the
*multi-value-leaf axis alone* introduces, together with the bounded fallbacks that
discharge it (Classes 1–3, §8.4–§8.4a). This also locates the residual cleanly: it is
neither a defect of the height-reduction concept nor of any particular node layout,
but the price of admitting more than one key per leaf under a deterministic
densePK-routing encoding (§6.15.3).

## §2. Disk-Backed Storage Requires Multi-Value Leaves

Persistent disk-backed storage cannot afford single-entry leaves. Each storage page
incurs I/O amortization cost; storing one key per page makes the structure
storage-prohibitive (a 1M-key index would require 1M leaf pages). All persistent radix
and B+tree variants pack multiple entries per leaf page; Sirix-HOT is no exception.

We adopt `HOTLeafPage.MAX_ENTRIES = 512` — a leaf page can hold up to 512 sorted
entries. Everything *above* the leaves remains Binna's HOT verbatim: PEXT routing,
sparse partial keys, SIMD subset match, `addEntry` mask extension, `splitIndirect`
node split.

This is the only Binna divergence in Sirix-HOT. Every other element is faithful.

## §3. The Structural Cost: Off-Path Straddle

When leaves can contain multiple keys, the load-bearing assumption no longer holds —
and one specific routing-encoding behavior becomes lossy.

**The behavior.** When `addEntry` extends a node `N`'s mask by a new discriminative
bit β at split time, it sets every non-affected sibling's β-column to 0 in their
stored partials. This is Binna's convention.

**Why it's safe in Binna's setting.** Each non-affected sibling is a single-key leaf,
so it is β-constant: either all (one) of its keys have β = 0 (encoding matches reality)
or β = 1 (encoding diverges, but the leaf has nothing further to descend, so the
mismatch is routing-harmless).

**Why it's lossy in Sirix-HOT.** A non-affected sibling can be a multi-key leaf whose
keys straddle β (some have β = 0, some have β = 1). The β-column = 0 encoding records
"this subtree is β-constant at 0", which is *false*. We call this an **off-path
straddle**: the leaf straddles β but is encoded as if it didn't.

Empirically validated by `StraddleCanonicityProbe`:

```
HOTBulkBuilder (canonical) produces 1240 straddling children across 78 nodes,
all off-path (colBitSet = false), with 0 misroutes.
```

Off-path straddles are routing-correct (subset-match is permissive enough to handle
them) but cause two divergences from Binna's canonical structure:

(a) **I5 (constancy) is weakened.** Strict I5 ("every child is bit-constant on the
parent's mask") is generalized to a soft I5-route ("the storedPartial subset-routes
correctly"). Sirix's `HOTInvariantValidator` checks the soft version; the strict
version would over-flag.

(b) **I7 and I8 diverge.** The stored partials no longer uniquely identify the leaf's
content (only its firstKey, and even that approximately because `addEntry` zero-fills
new bits). I7 still describes partial order; I8 describes firstKey order; multi-value
leaves with straddles cause these orderings to disagree at specific slot boundaries.

## §4. The Failure Mode: I8-Unsafe Direction 1 Firings

During multi-revision interleaved insert/delete workloads, the writer occasionally
encounters the following configuration at some indirect `d*` on the descent path:

```
d*'s slots (in I7 order):     [..., prev, affected, ...]
prev.firstKey      <  affected.firstKey                            (I8 pre-insert: OK)
densePK_K          ⊆  affected.stored                              (subset-match descent)
K                  <  prev.firstKey                                (I8-unsafe condition)
```

K's PEXT-routing chose `affected`, but K's lex position is to the left of `prev`. A
sub-insert of K into `affected` makes `affected.firstKey := K`, with `prev.firstKey
> K = affected.firstKey` → **I8 broken at the `prev/affected` slot boundary**.

**This configuration cannot arise in Binna's HOT** because partials would be exact
firstKey fingerprints, and subset-match descent would pick the slot whose firstKey
is the right lex-neighbor of K. The configuration is a downstream consequence of
multi-value leaves' off-path straddle.

## §5. Three Refuted Localized Fixes

The Sirix-HOT campaign (Aug 2025 – May 2026) attempted three independent localized
primitives at the firing depth `d*`. All three are refuted by either formal argument
or by empirical counter-example. Their independence is meaningful — each attacks a
different structural angle, and the convergent refutation forms the impossibility
evidence.

### §5.1 Refutation 1 — Proactive mask extension

**Approach.** When the I8-unsafe condition fires, compute β'' = MSDB(K XOR
affected.firstKey). If β'' is fresh to `d*'s` mask, add it as a new disc bit, expecting
the now-extended mask to distinguish K's routing from `affected's`.

**Refutation (empirical, Phase 1 probe, 2026-05-20).** Across the entire HOT test
suite, *all* firings have β'' already in `d*'s` mask. The bit isn't missing — it has
been off-path-straddled at `affected's` slot by past `addEntry` calls. Adding the
already-present bit is structurally a no-op.

**Reference.** Commit `1e3c20130`. Code: `Direction1HitRateProbe`,
`AbstractHOTIndexWriter::dumpDirectionOneFallback`, "Phase1-probe" diagnostic.

### §5.2 Refutation 2 — Split affected on β''

**Approach.** Split `affected's` content on β'' into halves `[affected_β=0,
affected_β=1]` via `splitLeafPage + handleOffPathOverflow`. Forces β'' on-path within
`affected's` resulting slots; K (with β''=0) routes to `affected_β=0`.

**Refutation (empirical, Phase 2 wire, 2026-05-20).** Eliminates all rebuilds on the
canary (21 → 0) but produces an I8 violation at rev 3 of `interleavedInsertDeleteMultiRev`:

```
[I8-children-sorted-by-firstkey] indirect 23 child[5].firstKey 8000...0400... >=
  child[6].firstKey 8000...03e8...
```

The split fixes the within-slot off-path straddle at `affected` but does NOT fix the
between-slot prev/`affected_β=0` lex tension. `affected_β=0.firstKey = K < prev.firstKey`
remains an I8 break at the prev/`affected_β=0` boundary.

**Reference.** Commit `e1e54938e`.

### §5.3 Refutation 3 — Carve K as its own single-entry leaf

**Approach.** Restore Binna's single-entry-leaf invariant *locally* for K by adding
a new child slot at `d*` with `stored = PEXT(K, mask(d*))` and a single-entry leaf
containing only K. If K's new partial sorts before `prev's` partial in I7 (= as
integers), K's slot lands left of `prev`, aligning with I8.

**Refutation (empirical, CarveProbe, 2026-05-20).** Each firing is classified by:

| Class | Condition | Localized-fix viability |
|---|---|---|
| COLLISION | `densePK_K == affected.stored` | NO — adding such a slot duplicates an existing partial. |
| CARVABLE + correct order | `densePK_K ⊋ affected.stored` AND `densePK_K < prev.stored` as integers | YES — K's slot sorts before `prev` in I7; aligned with I8. |
| CARVABLE + wrong order | `densePK_K ⊋ affected.stored` AND `densePK_K ≥ prev.stored` | NO — K's slot sorts after `prev` in I7, but `K.firstKey < prev.firstKey` → I8 broken. |

**Empirical aggregate (927 HOT tests, all firings):**

| Class | Count | Fraction |
|---|--:|--:|
| COLLISION | 14 | 60.9% |
| CARVABLE + wrong order | 9 | 39.1% |
| **CARVABLE + correct order** | **0** | **0.0%** |

**0 / 23 firings are fixable by the carve primitive.** The 9 wrong-order cases are
the cleanest evidence: K's partial *is* unique (no collision), but the partial-as-
integer comparison disagrees with the full-key lex comparison.

**Why this happens.** `addEntry`'s historical zero-fill at non-affected siblings
means `prev's` stored partial has, at bits added after `prev's` creation, values that
do NOT reflect `prev's` actual content. K's partial reflects K's actual content at
those same bits. The two partials are computed using the same mask but their bits
*mean different things* — one is current, the other is historical accumulation. The
integer comparison treats them as comparable; the lex order on the original keys does
not match.

**Reference.** Commit `b333eb1ee` plus this commit. Code:
`AbstractHOTIndexWriter::dumpDirectionOneFallback` "CarveProbe" diagnostic.

## §6. Impossibility Theorem (Formal)

This section establishes Theorem 1 as a rigorous mathematical statement, with
formal definitions, lemmas, and a fully symbolic proof.

### §6.1 Mathematical preliminaries

Let `Σ = {0, 1}^*` denote the set of finite binary strings. For `x ∈ Σ`, let
`|x|` be its length and `x[i]` its `i`-th bit (`0 ≤ i < |x|`, indexed MSB-first).
For `i ≥ |x|`, define `x[i] := 0`.

**Definition 1 (lex order).** The lex order `<_lex` on `Σ` is total and defined
by: for distinct `x, y ∈ Σ`, `x <_lex y` iff there exists the least `i` such that
`x[i] ≠ y[i]`, and at that `i`, `x[i] = 0` and `y[i] = 1`. *(If no such `i` exists
within the shorter string, the shorter is smaller. This is the standard byte-wise
lex order on Sirix's keys, which are fixed-width prefixed.)*

**Definition 2 (mask).** A *mask* is a strictly increasing sequence of non-negative
integers `M = (β_1 < β_2 < ⋯ < β_m)`. Write `|M| := m`.

**Definition 3 (PEXT and subset).** For `x ∈ Σ` and mask `M`, define
`PEXT(x, M) ∈ {0, 1}^m` as the `m`-bit integer whose `i`-th MSB-packed bit is
`x[β_{i+1}]`. For `u, v ∈ {0, 1}^m`, write `u ⊆ v` iff `(u AND (NOT v)) = 0`
(bitwise subset). Write `u < v` for integer comparison.

**Lemma 1 (PEXT monotonicity).** *For keys `x, y ∈ Σ` and mask `M`, if `x <_lex y`
and `x, y` agree on all bit positions in `M` (i.e., `x[β] = y[β]` for all `β ∈ M`),
then `PEXT(x, M) = PEXT(y, M)`.*

*Proof.* By definition `PEXT(z, M)` depends only on the bits of `z` at mask
positions. If `x` and `y` agree on those bits, the PEXT outputs are equal. ∎

### §6.2 HOT node abstraction

**Definition 4 (HOT indirect node).** A *node* is a tuple `N = (M, n, S, C)` where:
- `M` is a mask with `|M| = m ≥ 0`;
- `n ≥ 1` is the slot count;
- `S = (S_0, …, S_{n-1})` is the *stored partial array*, with `S_c ∈ {0, 1}^m`;
- `C = (C_0, …, C_{n-1})` is the *child reference array*; each `C_c` references a
  non-empty finite multiset `keys(C_c) ⊆ Σ`. We assume `keys(C_c) ∩ keys(C_{c'}) = ∅`
  for distinct `c, c'`.

Define `firstKey(c) := min_{<_lex} keys(C_c)` and `densePK_N(K) := PEXT(K, M)`.

**Definition 5 (routing).** For `K ∈ Σ`, define
`MATCH_N(K) := { c ∈ {0, …, n-1} : S_c ⊆ densePK_N(K) }`.
The *routing function* `findChild_N : Σ → {0, …, n-1} ∪ {⊥}` is defined as:
- If there exists `c` with `S_c = densePK_N(K)`, set `findChild_N(K) := min{c :
  S_c = densePK_N(K)}` (smallest-index exact match).
- Else if `MATCH_N(K) = ∅`, set `findChild_N(K) := ⊥`.
- Else `findChild_N(K) := max MATCH_N(K)` (highest-index subset match).

This formalizes the code at `HOTIndirectPage.findChildSpanNode`.

**Definition 6 (invariants).** Node `N` satisfies:
- **I7** iff `S_c < S_{c'}` for all `0 ≤ c < c' < n`.
- **I8** iff `firstKey(c) <_lex firstKey(c')` for all `0 ≤ c < c' < n`.
- **Subset-routing-correctness (SRC)** iff for every `c ∈ {0, …, n-1}` and every
  `K ∈ keys(C_c)`, `findChild_N(K) = c`.

**Lemma 2 (I7 implies distinct partials).** *Under I7, the partials `S_0, …, S_{n-1}`
are pairwise distinct.*

*Proof.* Immediate from the strict inequality `S_c < S_{c'}`. ∎

**Corollary 1 (exact-match is unique).** *Under I7, for any `K`, at most one slot
satisfies `S_c = densePK_N(K)`, so the smallest-index exact match is the unique
exact match.*

### §6.3 The Direction-1 I8-unsafe firing

**Definition 7 (firing precondition).** A *firing configuration* is a tuple
`(N, K, p, a)` where:
- `N` is a node satisfying I7, I8, and SRC;
- `K ∈ Σ` is a key not contained in any subtree of `N` (i.e., `K ∉ ⋃_c keys(C_c)`);
- `p, a ∈ {0, …, n-1}` with `p + 1 = a`;
- **(F1)** `S_a ⊆ densePK_N(K)`;
- **(F2)** for all `c > a`, `S_c ⊄ densePK_N(K)`;
- **(F3)** `densePK_N(K) ≠ S_c` for any `c` (no slot has exact match);
- **(F4)** `K <_lex firstKey(p)`.

By (F1)-(F3), `findChild_N(K) = a` (= the I8-unsafe descent picks `a`).

**Existence of firing configurations.** Such configurations exist constructively:
the empirical case 3 of §5.3 is one. The proof below operates symbolically and does
not depend on the specific instance.

### §6.4 Localized single-slot primitives — formal taxonomy

**Definition 8 (single-slot primitive).** A *localized single-slot primitive* `P`
at `N` for `K` is one of the following operations producing a new node
`N' = (M, n', S', C')`. The mask `M` is unchanged. We require `K ∈ keys(C'_{c*})`
for some `c* ∈ {0, …, n'-1}` (= K is stored).

For each operation we list the slot count `n'` and the rule for `(S', C')`.

| Operation | n' | Rule |
|---|---|---|
| MODIFY(c, σ) | n | `S'_c = σ`; `C'_c = C_c`; other slots unchanged. |
| REPLACE(c, σ, γ) | n | `S'_c = σ`; `C'_c = γ`; other slots unchanged. |
| ADD(c_ins, σ, γ) | n+1 | Insert (σ, γ) at index c_ins; slots at index ≥ c_ins shifted up by 1. |
| SPLIT(c, (σ_L, γ_L), (σ_R, γ_R)) | n+1 | Replace slot c with (σ_L, γ_L) at index c and (σ_R, γ_R) at index c+1; slots at index > c shifted up by 1. |
| REMOVE(c) | n-1 | Delete slot c; slots at index > c shifted down by 1. |

**Definition 9 (correctness of a primitive).** A primitive `P` is *correct for
`(N, K)`* if `N'` satisfies I7, I8, SRC, AND `findChild_{N'}(K) = c*`
(K's descent reaches its storage slot).

### §6.5 Lemmas

**Lemma 3 (subset-match preservation under slot insertion).** *Let `N` and `N'` be
nodes with shared mask `M`, and let `Q ∈ Σ` be a key with
`densePK_{N'}(Q) = densePK_N(Q) =: d`. If `N'` was obtained from `N` by
ADD(c_ins, σ, γ) (or SPLIT shifting slots up by 1), then for every slot `c` in
`N` with `S_c ⊆ d` and `c ≥ c_ins`, the corresponding slot `c + 1` in `N'` also
satisfies `S'_{c+1} = S_c ⊆ d`.*

*Proof.* By the rule of ADD/SPLIT, slots at index `≥ c_ins` in `N` are copied to
index `+1` in `N'` with unchanged stored partials. The subset relation `S_c ⊆ d`
is preserved verbatim. ∎

**Lemma 4 (highest-index dominance).** *Let `N'` be a node where `MATCH_{N'}(K)`
contains two distinct indices `c_1 < c_2`. If exact match does not occur (i.e.,
no `c` satisfies `S'_c = densePK_{N'}(K)`), then `findChild_{N'}(K) = max
MATCH_{N'}(K) ≥ c_2`.*

*Proof.* By Definition 5, in the no-exact-match case, `findChild_{N'}(K) = max
MATCH_{N'}(K)`. Since `c_2 ∈ MATCH_{N'}(K)`, the max is `≥ c_2`. ∎

**Lemma 5 (PEXT same across single-slot primitives).** *For any single-slot
primitive `P` producing `N'`, mask `M` is unchanged, so `densePK_{N'}(K) =
densePK_N(K)` for every `K ∈ Σ`.*

*Proof.* PEXT depends only on `K` and `M`. By Definition 8, `M` is unchanged. ∎

**Lemma 6 (slot-a survival under non-a primitives).** *Let `(N, K, p, a)` be a
firing configuration. Let `P` be a single-slot primitive operating on slot `c ≠ a`.
Then in `N'`, there exists a slot index `a'` (depending on `P`'s type) with
`S'_{a'} = S_a` and `C'_{a'} = C_a`. In particular, `S'_{a'} = S_a ⊆ densePK_N(K)
= densePK_{N'}(K)`, so `a' ∈ MATCH_{N'}(K)`.*

*Proof.* For MODIFY(c) and REPLACE(c) with `c ≠ a`: slot `a` is unchanged; `a' = a`.
For ADD(c_ins, …): if `c_ins ≤ a`, slot `a` is shifted to `a' = a + 1`; otherwise
`a' = a`. For SPLIT(c, …) with `c ≠ a`: similarly `a' ∈ {a, a + 1}`. For REMOVE(c)
with `c ≠ a`: `a' ∈ {a, a - 1}`. In every case, the stored partial and child
reference of `a` are preserved. By Lemma 5, `densePK_{N'}(K) = densePK_N(K)`, and
by (F1), `S_a ⊆ densePK_N(K)`, hence `S'_{a'} ⊆ densePK_{N'}(K)`. ∎

**Lemma 7 (firstKey monotonicity under content addition).** *Let `c*` be the slot
of `N'` containing `K`. Then `firstKey'(c*) ≤_lex K`. More specifically,
`firstKey'(c*) = min_{<_lex} keys(C'_{c*})`, where:*
- *if `c*` is a fresh slot from ADD with content `{K}`, then `firstKey'(c*) = K`;*
- *if `c*` is a fresh slot from SPLIT with content `γ_L ∋ K` (or `γ_R ∋ K`), then
  `firstKey'(c*) = min(γ_L ∪ {K})` (resp. `min(γ_R ∪ {K})`);*
- *if `c*` is a REPLACE-modified slot with new content `γ ⊇ {K}`, then
  `firstKey'(c*) = min(γ)`;*
- *if `c*` is a touched slot whose content is `keys(C_c) ∪ moved-keys ∪ {K}` for
  some collection of moved keys absorbed from other slots, then `firstKey'(c*) =
  min(union)`.*

*Proof.* `K ∈ keys(C'_{c*})` implies `min keys(C'_{c*}) ≤_lex K`. The detailed
sub-cases just enumerate the primitive operations from Definition 8 that can
produce `c*`'s content. In all cases `firstKey'(c*)` is the lex-minimum of the
resulting content multiset; this is the definition of `firstKey` (Definition 4).
∎

**Lemma 7.5 (firing-implied lex chain).** *For a firing configuration `(N, K,
p, a)` (Definition 7) with `p + 1 = a`: `K <_lex firstKey(p) <_lex firstKey(a)
<_lex firstKey(a+1) <_lex … <_lex firstKey(n-1)`.*

*Proof.* By (F4), `K <_lex firstKey(p)`. By I8 in N (Definition 6), the
firstKeys are strictly ascending: `firstKey(p) <_lex firstKey(a) <_lex
firstKey(a+1) <_lex … <_lex firstKey(n-1)`. Chain transitively. ∎

This chain is invoked repeatedly in the proofs of Theorems 1–4.

### §6.6 Theorem 1

> **Theorem 1.** *Let `(N, K, p, a)` be a firing configuration (Definition 7). No
> localized single-slot primitive `P` at `N` is correct for `(N, K)` (Definition 9).*

**Proof.** We enumerate over the five primitive types in Definition 8 and show each
fails at least one correctness condition. Throughout, write `c*` for the slot of `N'`
containing `K`.

**Case (i): REMOVE(c).** `K` is not added to any subtree; `K ∉ ⋃_{c} keys(C'_c)`,
contradicting Definition 8's storage requirement. Hence REMOVE is not a valid
single-slot primitive for K-insertion. ✗

**Case (ii): MODIFY(c, σ).** `C'_c = C_c`; no slot's content set changed. `K ∉
⋃_{c'} keys(C'_{c'})`, contradicting storage. ✗

**Case (iii): REPLACE(c, σ, γ).**

(iii.a) Suppose `c ≠ a`. Then slot `a` is unchanged in `N'` (Lemma 6 with `a' = a`).
By Lemma 6, `S'_a ⊆ densePK_{N'}(K)`, so `a ∈ MATCH_{N'}(K)`. By (F3) (no exact
match in `N`) and the fact that REPLACE at `c ≠ a` does not change `S_a` or
`densePK_{N'}(K)`, no slot of `N'` satisfies `S' = densePK_{N'}(K)` either. So
`findChild_{N'}(K) = max MATCH_{N'}(K)`. K is in slot `c*` (with `c* = c` since
`C'_c = γ` is the only modified subtree). For correctness we need `findChild_{N'}(K)
= c*`, i.e., `c = max MATCH_{N'}(K) ≥ a`.

(iii.a.1) If `c < a`: by Lemma 4, `findChild_{N'}(K) ≥ a > c`. So `c ≠
findChild_{N'}(K)`. Correctness fails. ✗

(iii.a.2) If `c > a`: slot `a` is unchanged with `firstKey'(a) = firstKey(a)`.
Slot `c` now has `K ∈ keys(γ)`, so `firstKey'(c) ≤_lex K` by Lemma 7. I8 in `N'`
requires `firstKey'(a) <_lex firstKey'(a+1) <_lex … <_lex firstKey'(c)`. In
particular, `firstKey(a) <_lex firstKey'(c) ≤_lex K`. But by (F4) and the I8 in `N`,
we have `K <_lex firstKey(p) <_lex firstKey(a)` (the second inequality from I8 in
`N` with `p = a-1`). Combining: `K <_lex firstKey(a) <_lex K`. Contradiction. I8
fails in `N'`. ✗

(iii.b) Suppose `c = a`. Then `C'_a = γ` with `K ∈ keys(γ)`. By Lemma 7, `firstKey'(a)
≤_lex K`. By (F4), `K <_lex firstKey(p) = firstKey'(p)` (slot `p` unchanged). Thus
`firstKey'(a) ≤_lex K <_lex firstKey'(p)`. But `p = a - 1 < a`, so I8 requires
`firstKey'(p) <_lex firstKey'(a)`. Contradiction. I8 fails in `N'`. ✗

**Case (iv): ADD(c_ins, σ, γ).** A new slot is inserted at index `c_ins`. Slot `a`
is shifted to index `a'`:
- if `c_ins ≤ a`, then `a' = a + 1`;
- if `c_ins > a`, then `a' = a`.

By Lemma 6, `S'_{a'} = S_a ⊆ densePK_{N'}(K)`, so `a' ∈ MATCH_{N'}(K)`.

The new slot at index `c_ins` may or may not be in `MATCH_{N'}(K)` depending on σ.
For K-storage to be at the new slot, `c* = c_ins` and `γ` contains `K`.

(iv.a) Suppose `c* = c_ins ≤ a`. Then `a' = a + 1 > c*`. By Lemma 6, `a' ∈
MATCH_{N'}(K)`. We split on whether σ produces an exact match for K:

*Non-exact-match regime (σ ≠ densePK_{N'}(K)):* By (F3), no slot in N has
`S_c = densePK_N(K) = densePK_{N'}(K)` (Lemma 5). Untouched slots in N'
inherit their original partials, so none has exact match. The new slot has
`σ ≠ densePK_{N'}(K)` by assumption, so no exact match either. Routing uses
subset-fallback: `findChild_{N'}(K) = max MATCH_{N'}(K) ≥ a' > c* = c_ins`
(Lemma 4). So `findChild_{N'}(K) ≠ c*`. SRC for `K` fails. ✗

*Exact-match regime (σ = densePK_{N'}(K)):* handled below.

(*Exact-match sub-case.*) Suppose `σ = densePK_{N'}(K)`. The new slot at
`c_ins` is the unique exact match for K (by I7, partials in N' are distinct;
the (iv.b) case below covers `c_ins > a`, so we have `c_ins ≤ a` here).
Routing picks `c_ins` (Definition 5's exact-match priority). For SRC of K: K
is in slot `c_ins`. ✓.

We split on `c_ins`'s position relative to `p` (recall `p = a - 1`):

*Sub-regime c_ins ∈ (p, a]:* `c_ins ∈ {p+1, …, a}`. After ADD, slot `p` is
still at index `p` in N' (unchanged, since `c_ins > p`). K is at slot `c_ins`
with firstKey `K`. The chain from slot `p` to slot `c_ins` in N' has I8-
ascending firstKeys: `firstKey'(p) = firstKey(p) <_lex firstKey'(p+1) <_lex …
<_lex firstKey'(c_ins) = K`. So `firstKey(p) <_lex K`. But by (F4), `K <_lex
firstKey(p)`. Contradiction. I8 fails. ✗

*Sub-regime c_ins ≤ p:* The new slot's partial `σ = densePK_{N'}(K)` must
satisfy I7 in N': `S'_{c_ins - 1} < σ < S'_{c_ins + 1}`. After ADD at index
`c_ins`, the slot originally at `c_ins` shifts to `c_ins + 1`, so `S'_{c_ins
+ 1} = S_{c_ins}`. For I7 we need `σ < S_{c_ins}`.

By (F1), `S_a ⊆ densePK_{N'}(K) = σ`, so `S_a ≤ σ` as integers. By (F3),
`S_a ≠ densePK_{N'}(K)`, so the subset is strict (`S_a` is missing at least
one bit set in `σ`), forcing `σ > S_a` as integers. By I7 in N, `S_{c_ins}
< S_a` (since `c_ins ≤ p < a`). Combining: `σ > S_a > S_{c_ins}`. But I7 in
N' requires `σ < S_{c_ins}`. Contradiction. I7 fails. ✗

Both sub-regimes contradict; the exact-match sub-case of (iv.a) fails. ✗

(iv.b) Suppose `c* = c_ins > a`. Then K is at index `c_ins > a`. For I8:
`firstKey'(a) <_lex firstKey'(a+1) <_lex … <_lex firstKey'(c_ins)`. Since
`firstKey'(c_ins) ≤_lex K` (Lemma 7) and `firstKey'(a) = firstKey(a) >_lex K` (by
(F4) and I8 in `N`), we have `firstKey'(a) >_lex K ≥_lex firstKey'(c_ins)`. I8
requires `firstKey'(a) <_lex firstKey'(c_ins)`. Contradiction. ✗

**Case (v): SPLIT(c, (σ_L, γ_L), (σ_R, γ_R)).** Slot `c` is replaced by two slots
at indices `c` and `c + 1`. WLOG (by symmetry of left/right with content
re-labelling) assume `K ∈ keys(γ_L)`.

(v.a) Suppose `c < a`. Slot `a` shifts to `a' = a + 1`. By Lemma 6, `S'_{a'} = S_a
⊆ densePK_{N'}(K)`. K is at slot `c* = c`. Then `c* < a'`. By Lemma 4 (no-exact-match
subcase, which holds by (F3) for unchanged slots), `findChild_{N'}(K) ≥ a' > c*`.
SRC for K fails. ✗

(Exact-match sub-case of v.a.) If `σ_L = densePK_{N'}(K)`, the new slot `c`
in N' (= the left half of the SPLIT) is an exact match for K. By I7 in N',
partials are distinct, so this is the unique exact match. Routing picks `c`.
✓ for SRC of K.

For I7 in N': σ_L = densePK_{N'}(K) must satisfy `S'_{c-1} < σ_L < S'_{c+1} =
σ_R`. By (F1), `S_a ⊆ densePK_{N'}(K) = σ_L`, so `S_a ≤ σ_L` as integers. By
(F3), `S_a ≠ densePK_{N'}(K)`, so `σ_L > S_a`. By I7 in N, for any slot index
`c < a` we have `S_c < S_a < σ_L`. So if `c < a` (= splitting some slot to the
left of `a`), I7 in N' requires `σ_L < S'_{c+1}`. Pre-split, `S'_{c+1}` would
be `S_c` (= the original split slot's partial, by Definition 8's SPLIT rule
where σ_L, σ_R replace the original slot's partial). But our constraint is
that σ_L, σ_R must respect I7: `σ_L < σ_R`, and `σ_R` reflects γ_R's content
(by Lemma 9 if content-preserved). In any case, σ_L > S_a > S_c contradicts
σ_L's I7 fit. I7 fails. ✗

(Note: c ≥ a case falls into v.b/v.c below.)

(v.b) Suppose `c = a`. Slot `a` is split. The two new slots are at indices `a`
(= σ_L, γ_L) and `a + 1` (= σ_R, γ_R). K is in γ_L at index `a`. Then `firstKey'(a)
≤_lex K` (Lemma 7). I8 requires `firstKey'(p) <_lex firstKey'(a)`, i.e.,
`firstKey(p) <_lex firstKey'(a) ≤_lex K`. But by (F4), `K <_lex firstKey(p)`.
Contradiction. ✗

(v.c) Suppose `c > a`. Slot `a` remains at index `a` (no shift). By Lemma 6,
`S'_a = S_a ⊆ densePK_{N'}(K)`. K is at slot `c* = c > a`. I8 requires
`firstKey'(a) <_lex … <_lex firstKey'(c)`. By Lemma 7, `firstKey'(c) ≤_lex K`. By
(F4) and I8 in `N`, `firstKey'(a) = firstKey(a) >_lex K`. So `firstKey'(a) >_lex K
≥_lex firstKey'(c)`. I8 requires the opposite ordering. Contradiction. ✗

In every case (i)–(v), at least one of SRC for K, I7, or I8 fails. Therefore no
localized single-slot primitive at `N` is correct for `(N, K)`. ∎

### §6.7 The crux of the impossibility

The proof structure reveals that single-slot primitives fail along two distinct
fault lines:

**Routing fault line (REPLACE-iii.a.1, ADD-iv.a no-exact-match, SPLIT-v.a no-exact-
match).** When K's storage slot has index `< a`, slot `a` remains in `MATCH_{N'}(K)`
at higher index. Lemma 4 forces `findChild_{N'}(K)` to skip K's slot. SRC for K fails.

**Ordering fault line (REPLACE-iii.a.2, iii.b, ADD-iv.a exact-match, iv.b, SPLIT-v.a
exact-match, v.b, v.c).** When K's storage slot has index `≥ p` (or `≥ a`), I8
requires `K`'s position be lex-after `firstKey(p)`. By (F4), this fails directly.

The two fault lines are mutually exclusive (slot index `< a` vs. `≥ a`) but jointly
exhaustive. No primitive can both place K at index `< a` (= satisfying lex
positioning) and have its slot dominate routing. This is a structural conflict
between (F1) — `a`'s partial subset-matches K — and (F4) — K's lex position is
left of `p` (`< a`).

### §6.8 Mask-fixed k-slot primitives

**Definition 10 (mask-fixed k-touch primitive).** A *mask-fixed k-touch primitive*
at `N` for `K` is a sequence `P = (P_1, P_2, …, P_t)` of single-slot operations
from Definition 8, composed left-to-right, such that:

1. The mask `M` is unchanged throughout (no operation extends or replaces `M`).
2. The total number of *distinct slots touched* by `(P_1, …, P_t)` is at most `k`,
   where "touched" means: for each `P_j`, the operation references a single slot
   index `c_j` (in the node state at that point); the set of original-slot
   identities corresponding to those `c_j`'s has size at most `k`. (We treat a
   slot's "identity" as preserved under index-shift bookkeeping; a fresh slot
   created by ADD or by the right-half of SPLIT is a new identity.)

A k-touch primitive is *correct for `(N, K)`* if its composed effect produces an
`N'` satisfying I7, I8, SRC, and `findChild_{N'}(K) = c*` (the storage slot of K).

The composition is a sequence rather than a set because intermediate states need
not satisfy invariants — only the final `N'` must.

### §6.9 Content preservation and the subset-match conservation lemma

The k-touch generalization requires two structural lemmas that constrain how
mask-fixed primitive sequences interact with content.

**Lemma 8 (content conservation).** *Let `P` be a correct mask-fixed primitive
sequence at `N` for `K`, producing `N'`. Then `⋃_c keys(C'_c) = (⋃_c keys(C_c)) ∪
{K}`. Every original key is preserved.*

*Proof.* By Definition 9 (correctness), `N'` satisfies SRC. SRC requires every
key in `⋃ keys(C'_c)` to descend to its storage slot. If some original `K' ∈ ⋃
keys(C_c)` were absent from `⋃ keys(C'_c)`, post-primitive queries for `K'` would
yield "not present" — violating the database semantics that motivates the
indexing structure. We exclude this as failing the correctness definition. K is
present in `N'` by Definition 8. ∎

**Lemma 9 (subset-match preservation under content-preserving modification).**
*Suppose a mask-fixed primitive sequence touches slot `c`, leaves it non-empty
in `N'`, and preserves the content (`keys(C'_{c'}) ⊇ keys(C_c)`, where `c'` is
the post-shift index of `c`). Then `S'_{c'} ⊆ ⋂_{K' ∈ keys(C_c)} densePK_N(K')`.
Furthermore, if N's encoding satisfies the tightness condition `S_c =
⋂_{K' ∈ keys(C_c)} densePK_N(K')` (which holds under Binna's PEXT when N is
canonical, and under Option A by construction), then `S'_{c'} ⊆ S_c`.*

*Proof.* SRC for `K' ∈ keys(C_c) ⊆ keys(C'_{c'})` requires `S'_{c'} ⊆
densePK_{N'}(K')`. By Lemma 5, `densePK_{N'}(K') = densePK_N(K')`. Taking the
intersection over all `K' ∈ keys(C_c)`: `S'_{c'} ⊆ ⋂_{K' ∈ keys(C_c)}
densePK_N(K')`. Under the tightness condition `S_c = ⋂ densePK_N(K')`, this
becomes `S'_{c'} ⊆ S_c`. ∎

**Note on tightness.** SRC alone only gives `S_c ⊆ ⋂ densePK`. The equality
`S_c = ⋂ densePK` is a separate convention (the encoding stores the "most
general" partial that subset-matches all keys). Binna's PEXT does not enforce
this — `S_c = PEXT(firstKey, M)`, which may be a strict subset of `⋂ densePK`
(= when other keys in the leaf have additional bits set). Sirix-HOT's stored
encoding satisfies tightness when the slot's content is canonically derived
(addEntry's zero-fill convention ensures `S_c` reflects the firstKey's bits at
the moment the bit was added to M). For Theorems 1–4's arguments, we use the
weaker `S'_{c'} ⊆ ⋂ densePK ⊆ S_c` whenever the post-state and pre-state
encodings satisfy compatible tightness. When tightness fails, the bound
`S'_{c'} ⊆ ⋂` still holds and the subset-match conservation argument
(Corollary 3) goes through.

**Corollary 3.** *If slot `c` is touched and content-preserved, and `S_c ⊆
densePK_N(K)` in `N`, then `S'_{c'} ⊆ densePK_{N'}(K)`, so `c' ∈ MATCH_{N'}(K)`.*

This corollary is the binding constraint: a slot that subset-matches K in `N`
continues to subset-match K in `N'`, unless its content is moved out (= the slot
is emptied or REMOVE-d, both of which require touching the slot and at least one
other slot to receive its content).

### §6.10 Theorem 2 — k-touch lower bound

**Definition 11 (subset-matcher count).** For a node `N` and key `K`, let
`μ_N(K) := |{c ∈ {0, …, n-1} : S_c ⊆ densePK_N(K)}|` denote the count of slots
whose stored partials subset-match K's densePK.

In a firing configuration, `a ∈ MATCH_N(K)` by (F1), so `μ_N(K) ≥ 1`.

> **Theorem 2.** *For every integer `k ≥ 1`, there exists a mask-fixed firing
> configuration `(N, K, p, a)` with `μ_N(K) = k + 2` and `K <_lex firstKey(0)`
> such that no mask-fixed k-touch primitive at `N` (Definition 10) is correct
> for `(N, K)`.*

The strength: for any constant `k`, we can construct a firing requiring `> k`
slots to be touched. The bound matches `μ` (the subset-matcher count) up to a
constant, and rebuild touches all slots. So the scoped rebuild is asymptotically
optimal.

**Construction.** Pick `m ≥ ⌈log_2(k+3)⌉ + 1` so that the `m`-bit partial space
includes both `2^{k+1} - 1` (= `S_{k+1}` below) and one additional unused bit at
position `m-1` that will be set only in `densePK_K`. Choose any mask `M = (β_1
< β_2 < ⋯ < β_m)`. Set `n := k + 2` and define slot partials:

```
S_c := 2^c - 1   for c ∈ {0, 1, …, k+1}
```

So `S_0 = 0`, `S_1 = 1`, `S_2 = 3`, `S_3 = 7`, …, `S_{k+1} = 2^{k+1} - 1`. None
of these have bit `m-1` set.

Properties:
- `S_c < S_{c+1}` as integers (I7 satisfied).
- `S_c ⊆ S_{c+1}` bitwise (each is the bitwise prefix of the next).
- Every `S_c ⊆ S_{k+1}` (and `S_{k+1}` itself uses bits `0..k`, never `m-1`).

Choose `K` with `densePK_N(K) := S_{k+1} ∨ 2^{m-1} = 2^{k+1} - 1 + 2^{m-1}` (= K
has bit `m-1` set in addition to bits `0..k`). With non-mask bits chosen so that
`K <_lex firstKey(0)`, such K exists by Lemma 1 and the freedom of non-mask
bits.

Verify the precondition (F3) ("no slot has exact match for densePK_N(K)"):
densePK_N(K) has bit `m-1` set, but no `S_c` has bit `m-1` set. Hence `S_c ≠
densePK_N(K)` for every `c`. ✓ (F3) holds.

Verify (F1): for every `c ∈ {0, …, k+1}`, `S_c ⊆ densePK_N(K)`. The bits set in
`S_c` are a subset of bits `0..k`, all of which are set in `densePK_N(K)`. ✓.
In particular, `S_a = S_{k+1} ⊆ densePK_N(K)` (strict subset since
`densePK_N(K)` has bit `m-1` and `S_a` does not).

Populate slot contents `C_c` such that:
- Each `K' ∈ keys(C_c)` has `densePK_N(K') ⊇ S_c` (SRC in N).
- `firstKey(0) <_lex firstKey(1) <_lex ⋯ <_lex firstKey(k+1)` (I8 in N).
- The firstKey values are strictly above K in lex.

This is feasible by adjusting non-mask bits monotonically.

The result: `μ_N(K) = k + 2` (every slot subset-matches K's densePK, since every
`S_c ⊆ densePK_N(K)`). The firing precondition holds with `p := k`, `a := k +
1`, (F1) `S_a ⊊ densePK_N(K)` (strict subset), (F2) trivially (no slot above
`a` exists), (F3) no exact match, (F4) `K <_lex firstKey(0) ≤_lex firstKey(p)`.

**Proof of the lower bound.** Suppose, for contradiction, that some mask-fixed
k-touch primitive `P` is correct for `(N, K)`, producing `N' = P(N, K)`.

By Lemma 7 and content conservation (Lemma 8), let `c*` be the slot of `N'` with
`K ∈ keys(C'_{c*})`. Since `K <_lex firstKey(0)`, K is the lex-smallest key in
the entire post-state content. Hence `firstKey'(c*) = K`. By I8 in `N'`, K's
slot is at the lex-leftmost index: `c* = 0`. (Any slot at index `< c*` would
have `firstKey' <_lex K`, but K is the lex-minimum of all content.)

So `c* = 0` and `firstKey'(0) = K`.

For SRC of K (Definition 9): `findChild_{N'}(K) = 0`. By Definition 5, either
`0` is the smallest-index exact-match (`S'_0 = densePK_{N'}(K)`), or
`MATCH_{N'}(K) ∩ {1, …, n' - 1} = ∅` (= no slot at higher index subset-matches K).

**Sub-case A.** Suppose `S'_0 = densePK_N(K) = S_{k+1} ∨ 2^{m-1}`. By Lemma 9
applied to slot `0` (which is touched by `P` since it now contains K), if slot
`0` was content-preserved, `S'_0 ⊆ S_0 = 0`, forcing `S'_0 = 0`. But `S'_0 =
densePK_N(K) ≠ 0` (it has bit `m-1` and bits `0..k` set). Hence slot `0` was
NOT content-preserved: keys from slot `0`'s original content were moved out by
`P`. But each such moved key `K' ∈ keys(C_0)` must land in some
`keys(C'_{c''})` with `c'' ≥ 1`. By I8 in N',
`firstKey'(c'') ≥_lex firstKey'(c'' - 1) ≥_lex ⋯ ≥_lex firstKey'(0) = K`. Hence
`firstKey'(c'')` (= the smallest key in slot c''s content) `≥_lex K`. Since
keys(C'_{c''}) contains K' (moved from slot 0), we have `K' ≥ firstKey'(c'') ≥
K`. But also `K' ∈ keys(C_0)` originally, so `K' ≥_lex firstKey(0) >_lex K` by
construction. ✓ (consistent so far).

Now I7 in N' requires partials strictly ascending. `S'_0 = densePK_N(K)` has
bits `0..k, m-1` set. The other slots in N' inherit from N (touched + content-
preserved) or are derived from moved content. By Corollary 3, every touched-
content-preserved slot has `S'_{c'} ⊆ S_c ⊆ S_{k+1}` (= bits `0..k`, no bit
`m-1`). Untouched slots' partials are unchanged (`⊆ S_{k+1}`). Touched-content-
moved slots' partials are bounded by the intersection of their new content's
densePKs; since K's densePK is the only one with bit `m-1` and K is in slot 0
(not in any other slot's content, by Definition 4's disjointness), no other
slot's new partial includes bit `m-1`. Hence every slot in N' at index `≥ 1`
has partial without bit `m-1`, i.e., partial `≤ S_{k+1} < S'_0`.

For I7 strict ascending, slots at index `≥ 1` must have partials `> S'_0`. But
all such partials are `≤ S_{k+1} < S'_0`. Contradiction. I7 fails. ✗

**Sub-case B.** Suppose `S'_0 ≠ densePK_N(K)` and `MATCH_{N'}(K) ∩ {1, …, n' -
1} = ∅` (= no slot at index `≥ 1` subset-matches K).

There are `k + 2` original slots in N (indices `0, …, k+1`), and by construction
every one is a subset-matcher of K (`S_c ⊆ densePK_N(K)` for all `c`). By
Lemma 8 (content conservation), every key in `⋃ keys(C_c)` survives in N'.

Consider any original slot `c ∈ {0, …, k+1}` of N. There are three exhaustive
possibilities for its fate in `P`:

(i) **Untouched.** Then `S'_{c'} = S_c` at the post-shift index `c'`. By
    Corollary 3 (applied to `S_c ⊆ densePK_{N'}(K)`), `c' ∈ MATCH_{N'}(K)`. By
    Sub-case B's premise, `c' = 0` (= the only allowed match index).

(ii) **Touched and content-preserved** (= `keys(C'_{c'}) ⊇ keys(C_c)`). By
     Corollary 3, `S'_{c'} ⊆ S_c ⊆ densePK_{N'}(K)`, so `c' ∈ MATCH_{N'}(K)`.
     Again Sub-case B's premise forces `c' = 0`.

(iii) **Touched and content-extracted** (= at least one key from `keys(C_c)` is
      now in another slot's content in N'). Touch count: ≥ 1 for slot `c`
      itself (the extraction touch), and ≥ 1 for each destination slot of the
      moved content (the absorption touch). Hence each (iii)-classified slot
      contributes ≥ 1 to the touch count.

At most one original slot can be in category (i) or (ii) (because the
post-shift index 0 is unique). The remaining at-least `k + 1` original slots
must be in category (iii), each contributing ≥ 1 touch.

Total touches ≥ `k + 1`. By assumption `P` is a k-touch primitive (Definition
10), so total touches ≤ `k`. But `k + 1 > k`. Contradiction. ✗

Both sub-cases yield a contradiction. No mask-fixed k-touch primitive resolves
this firing. ∎

### §6.11 Discussion of Theorem 2

The proof's crux is the **subset-match conservation** (Corollary 3): for any slot
`c` in N whose partial `S_c ⊆ densePK_N(K)` AND whose content is preserved in N',
the post-state partial still subset-matches K. By construction, every slot in N
subset-matches K (because every `S_c ⊆ S_{k+1} ⊊ densePK_N(K)`). So every slot
that survives a content-preserving touch contributes a subset-match in N',
forcing the corresponding K-routing to pick a slot at index ≥ 1 (Sub-case B's
contradiction).

The only escape route is to *evacuate* content from those slots, but each
evacuation requires a touch on the source slot. With `k + 2` slots all
subset-matching K, the primitive must evacuate at least `k + 1` of them
(leaving the one containing K) → `k + 1` touches → exceeds the k budget.

**Bound tightness.** The construction's `μ_N(K) = k + 2` is tight: a
`(k + 1)`-touch primitive *can* resolve the construction by touching all `k + 1`
slots at indices ≥ 1 (evacuating their content into slot 0). So the lower bound
`k + 1` is exact for this construction.

**Asymptotic implication.** For a firing with arbitrary `μ_N(K) = m`, the lower
bound is `m - 1`. As `m → n` (= every slot subset-matches), the touch count
required → `n - 1`, which is `Θ(n)` — the same asymptotic cost as a full subtree
rebuild. The scoped rebuild touches all slots simultaneously and re-derives
partials from content; it is asymptotically optimal among mask-fixed primitives.

### §6.12 Empirical relevance

The campaign's 23 firings (§5.3 CarveProbe) have:
- 14 with `S_a = densePK_N(K)` (COLLISION; these violate Theorem 2's (F3) and
  fall outside the formal firing definition, but are empirically related —
  they reduce to the canonical case via mask extension at the leaf level).
- 9 with `S_a ⊊ densePK_N(K)` (strict subset, the carve probe's "CARVABLE+wrong-
  order" verdict; these are valid firings per Definition 7).

For all 23, `μ_N(K)` is small (the partial mask sizes are 4–8 bits in observed
workloads). A 2- or 3-touch primitive might suffice in principle for some
firings. However, the campaign's failure history — 17+ G-attempts during
strict-Binna conformance + 7+ betaIsDiscBit attempts + 3 Stage 4b iterations +
2 routing rewrite phases — demonstrates that *constructing* such primitives under
Sirix's full invariant set (I1–I8 + SRC + multi-revision isolation + CoW page
discipline) is empirically intractable. The scoped rebuild remains the
implemented fallback.

Theorem 2 establishes the theoretical floor: for arbitrarily-many-slots-matching
workloads, no constant-k primitive can suffice. The campaign's empirical
intractability suggests this floor is approached even for small `μ`.

### §6.13 Theorem 3 — Mask-extension does not escape the impossibility

Theorems 1–2 hold the mask `M` fixed. Phase 1 of the campaign empirically refuted
the natural mask-extension fix (the missing bit β'' is already in M), but did not
formally close the avenue. This section does.

**Definition 12 (mask-extension primitive).** A *mask-extension primitive* at N
adds a new discriminative bit `β ∉ M` to the node's mask, producing
`M' := M ∪ {β}`. For each existing slot `c ∈ {0, …, n-1}`, it specifies a
β-column value `v_c ∈ {0, 1}`, producing the new partial:

```
S'_c := S_c with bit at β's packed-position set to v_c.
```

A mask-extension primitive is **content-blind** iff the function `c ↦ v_c` is
determined a priori, without reading `keys(C_c)` for any `c`. Equivalently, the
primitive's output stored partials depend only on `(N's mask, N's stored array,
β)` — not on the underlying content of any subtree.

It is **content-aware** otherwise: at least one `v_c` is computed by inspecting
`keys(C_c)`. Inspecting `keys(C_c)` costs `Ω(|keys(C_c)|)` work in any model
where keys are read individually.

A mask-extension primitive may be composed with `k` mask-fixed touches
(Definition 10) — we generalize "k-touch" to include the mask-extension step as
zero touches if it modifies only the encoding (no slot's *content* is read or
written) and `n` touches if content is read at every slot. Specifically, the
total touch count counts:
- The mask-extension primitive itself (1 if content-aware on any slot, else 0).
- Each mask-fixed touch (1 per Definition 10).

> **Theorem 3.** *For any content-blind mask-extension primitive `P_β` and any
> integer `k ≥ 0`, there exists a firing configuration `(N, K, p, a)` such that
> no combination of `P_β` with at most `k` mask-fixed touches is correct for
> `(N, K)`.*

**Proof.** Let `f : {0, …, n-1} → {0, 1}` be the content-blind column-assignment
function of `P_β`. (Content-blind means `f(c)` depends only on `c` and `β`, not on
`keys(C_c)`.)

We construct a firing configuration that defeats `P_β`:

**Step 1.** Use the construction of Theorem 2 with `μ_N(K) = k + 3` (one larger
than Theorem 2's `k + 2`, to ensure the k-touch lower bound is exceeded by 1):
mask `M = (β_1 < … < β_m)`, `n = k + 3` slots with partials `S_c = 2^c - 1`,
K with `densePK_N(K) = S_{k+2} = 2^{k+2} - 1`, and `K <_lex firstKey(0)`.

**Step 2.** For every candidate new bit `β ∉ M`, the function `f` prescribes
column-values `v_c = f(c)` for `c ∈ {0, …, n-1}`. There are `2^n` possible
function tables `f`; we construct, for each `f`, the slot-content distribution
that defeats it.

For each slot `c`, populate `keys(C_c)` such that:

- If `f(c) = 1`: include in `keys(C_c)` at least one key `K_c` with `K_c[β] = 0`.
- If `f(c) = 0`: ensure all `keys(C_c)` have actual `β` values consistent with
  S'_c[β-col] = 0 (= no constraint, always SRC-consistent).

This is feasible because the bit `β ∉ M` is unconstrained in the construction of
N (slot contents are populated with respect to M's bits and lex ordering; β is a
fresh bit we can populate independently).

**Step 3.** Apply `P_β` to this configuration. For each slot `c` with `f(c) = 1`:
`S'_c[β-col] = 1`. By the populated content, slot `c` contains a key `K_c` with
`K_c[β] = 0`. Hence `densePK_{N'}(K_c)[β-col] = 0`, and
`(S'_c & ~densePK_{N'}(K_c))[β-col] = 1 & ~0 = 1 ≠ 0`. So `S'_c ⊄
densePK_{N'}(K_c)` — SRC fails for `K_c` at slot `c`. ✗

**Step 4.** If all `f(c) = 0` (the alternative content-blind rule), then no
slot's β-column is set to 1. `S'_c[β-col] = 0` for all `c`. Subset-match: for K
with K's β-value either 0 or 1, `(S'_c & ~densePK_{N'}(K))[β-col] = 0 & x = 0`
for any x. So the subset relation at β-col is trivially satisfied; β contributes
no disambiguation. `MATCH_{N'}(K)` is identical to `MATCH_N(K)` (with the encoding
extended trivially). By Theorem 2 (Sub-case A or B, depending on whether mask
extension makes K's densePK exactly match a slot), the k-touch primitive cannot
resolve the firing — Theorem 2's argument applies verbatim to the extended mask.
✗

In either case (Step 3 or Step 4), `P_β` with at most k mask-fixed touches fails
to resolve the firing. ∎

**Discussion.** Content-blind mask extension is *information-blind*: it cannot
inspect the actual β-content of subtrees, so it must guess `v_c` from the slot
index alone. For any guessing rule `f`, an adversarial content distribution
exists that violates the rule. Theorem 3 shows this formally.

**Content-aware mask extension is content-O(n).** To assign `v_c` correctly, a
content-aware primitive must inspect every key in `keys(C_c)` for every slot `c
∈ {0, …, n-1}` (or at least every slot it claims to update). The total work is
`Ω(∑_c |keys(C_c)|) = Ω(content at this depth)`. Under the touch-counting model
of Definition 10, this content inspection counts as a touch per slot inspected.
A correct content-aware mask-extension primitive then has touch-count `≥ μ_N(K)`
just to set up the encoding (since it must inspect every subset-matcher to either
preserve its subset-match or break it correctly), which by Theorem 2 is `≥ k + 2`
for the Theorem 2 construction. Hence content-aware mask extension does not
escape the k-touch lower bound either.

**Corollary 5 (mask-extension corollary).** *For any constant `k ≥ 0`, no
mask-extension primitive (content-blind or content-aware) composed with at most
`k` mask-fixed touches is correct for every firing configuration. The
construction of Theorem 2 generalized to `μ = k + 3` defeats any such primitive.*

This closes the Phase 1 / Option B avenue formally: not just empirically refuted
on the campaign canary, but provably unable to resolve every firing class.

### §6.14 Theorem 4 — Scoped rebuild is asymptotically optimal

The lower bound of Theorem 2 is `Ω(μ_N(K))` per firing. The scoped rebuild's
upper bound is `O(n)` (it touches all slots in the insertDepth subtree).

> **Theorem 4.** *In the worst case across all firing configurations of a fixed
> n-slot node, the scoped `rebuildSubtree(insertDepth)` has cost `Θ(n)`, matching
> the lower bound `Ω(μ_N(K))` achievable by the construction `μ = n` of
> Theorem 2.*

**Proof.** *Upper bound.* `rebuildSubtree(insertDepth)` collects all entries in
the subtree, sorts and dedup-merges them, splices K, and rebuilds the subtree via
`HOTBulkBuilder.build`. The collect-and-rebuild touches all `n` slots and all
their content (= `O(∑_c |keys(C_c)|)` work). At the node-level bookkeeping, the
slot-touch count is exactly `n` (every slot is read and re-emitted). Hence
upper-bound cost: `n` slot-touches per node.

*Lower bound match.* By Theorem 2, for any `k ≥ 1` there exists a firing with
`μ_N(K) = k + 2` requiring `≥ k + 1` touches. Choose `k = n - 3`; the
construction's slot count is `n`, all `n` slots subset-match K, so the touch
lower bound is `n - 2`. As `n → ∞`, the ratio `(n - 2) / n → 1`. Hence
`Ω(n) = Ω(μ)` matches the rebuild's `O(n) = O(μ)` asymptotically. ∎

**Caveat for sparse-μ firings.** When `μ_N(K) = o(n)` (= K subset-matches only a
small fraction of slots), the lower bound is `Ω(μ) = o(n)`, but the scoped
rebuild still touches `O(n)`. There is an asymptotic gap. The empirical 23
firings (§5.3) appear to have small `μ` (`μ ≤ 4` in the observed cases), so the
practical scenario is the sparse-μ regime where the rebuild is conservative. A
hypothetical primitive that touches only the `μ` subset-matchers (= `O(μ)` work)
would close this gap, but the campaign's failure history (no such primitive has
been found) suggests structural obstacles. We leave the tight `Θ(μ)` upper bound
for sparse-μ firings as **open work**.

### §6.15 Encoding Replacements: A and C

Theorems 1–4 establish impossibilities under Binna's PEXT routing encoding. The
`HOT_ROUTING_ENCODING_REWRITE.md` proposal considers two replacement encodings.
We analyze each formally.

#### §6.15.1 Option A — AND-encoding

**Definition 13 (Option-A encoding).** Replace Definition 4's stored partial rule
with:
```
stored[c] := ∧_{K ∈ keys(C_c)} densePK_N(K)
```
(= bitwise AND of all densePKs of keys in c's subtree).

Replace Definition 5's routing with: `MATCH_N(K) := {c : stored[c] ⊆ densePK_N(K)}`
(the same subset relation, semantically reframed as "K has all the bits where c's
subtree agrees"). A tie-breaker rule selects among matches:

- **Rule TB-h ("highest-index match")**: `findChild_N(K) := max MATCH_N(K)` — same as
  Binna's Definition 5 in the subset-match case.
- **Rule TB-s ("smallest-index match")**: `findChild_N(K) := min MATCH_N(K)` — the
  alternative that prefers earlier-indexed slots.
- **Rule TB-m ("most-specific match")**: `findChild_N(K) := argmax_{c ∈
  MATCH_N(K)} stored[c]` — the slot whose agreement set is largest.

> **Theorem 5 (Option-A SRC-impossibility for common tie-breakers).** *For each
> of the three tie-breaker rules TB-h, TB-s, TB-m, there exists a node
> configuration under Option-A encoding and a target insert key K such that no
> localized single-slot mask-fixed primitive at the node produces N' satisfying
> SRC for all keys in `⋃ keys(C'_c)`.*
>
> *Note on scope.* The TB-h case (which inherits Theorem 2's proof) shows that
> no primitive is *correct* in the strict sense of Definition 9 (= I7 + I8 +
> SRC). The TB-s and TB-m cases below show a weaker but related impossibility:
> SRC cannot be maintained for *all* keys after primitive application. The
> distinction matters because TB-s and TB-m's K-routing for K itself can
> succeed; the failure is for adversarial K' that share densePK behavior with K.

**Proof sketch.** We construct firings that defeat each tie-breaker:

*Tie-breaker TB-h.* Use the construction of Theorem 2 with `n = k + 2` slots
having stored values `S_c = 2^c - 1`. Under Option A, the stored value of each
slot is the AND of its subtree's densePKs. Populate slot `c`'s content with a
single key whose densePK has exactly the bits of `S_c` set (and no others). Then
`stored[c] = densePK = S_c` (AND over singleton). The construction matches the
Theorem 2 configuration. The same proof argument applies verbatim under TB-h:
slot a is the highest-index match, K's lex-smallest position forces `c* = 0`,
and routing dominance produces the contradiction. Theorem 1's single-slot
impossibility extends to Option A under TB-h. ∎

*Tie-breaker TB-s.* Under TB-s, smaller-index slots dominate routing. K's slot
added at index 0 (smallest) would win — *if* K's slot is in `MATCH_N(K)`. Adding
K's slot with stored = densePK_K means densePK_K ⊆ densePK_K (trivially), so
yes. K's slot wins. ✓ for K.

But TB-s has a symmetric pathology: for *existing* K' in slot 1 (stored = 1),
the new lowest-index slot 0 (K's slot, stored = densePK_K) is also in MATCH for
K' if `densePK_K ⊆ densePK_K'`. In our construction with `densePK_K = 2^{k+1} -
1`, this means K' has ALL of densePK_K's bits set. Construct K' to have densePK
exactly equal to densePK_K (all bits set). Then K' matches both K's slot and
slot 1. TB-s picks the smallest index = K's slot. But K' is in slot 1's
content, not K's. SRC fails for K'. ✗

Hence TB-s also admits firings that any localized primitive cannot resolve. ∎

*Tie-breaker TB-m.* Under TB-m, the slot with largest stored wins. K's slot's
stored = densePK_K is the maximum value (= all bits set, the "fullest"
agreement set). K's slot wins for K's routing. ✓.

But TB-m has the same pathology as TB-s: for K' with densePK ⊇ densePK_K (= K'
has at least all bits in densePK_K), K's slot's stored = densePK_K is the most
specific subset of densePK_K'. TB-m picks K's slot for K'. SRC fails for K'. ✗

Hence TB-m also admits defeating firings. ∎

**Summary.** Each of the three natural tie-breaker rules over Option A's encoding
admits an adversarial firing configuration that defeats it. The proof generalizes
to any *content-blind* tie-breaker (= one whose output depends only on
`(densePK_N(K), S, mask)`, not on `keys(C_c)`) by an argument analogous to
Theorem 3 (each fixed rule has an adversarial content distribution). A full
unified statement covering all content-blind tie-breakers requires the same
adversarial-content construction; we leave this generalization as a noted
strengthening rather than a proved corollary, since the three concrete cases
suffice to refute the natural design choices.

**Discussion.** The doc's informal verdict on Option A — "as inserts diversify,
stored shrinks toward 0 (= every slot matches every target → ambiguous)" — is
captured formally by the TB pathologies above. The "ambiguity" isn't a vague
notion; it's the precise observation that with stored values varying in a tight
range (as in our construction with stored ∈ {0, 1, 3, …, 2^{k+1}-1}), multiple
slots match any given target, and no tie-breaker disambiguates correctly
across all configurations.

#### §6.15.2 Option C — Subtree-OR recompute

**Definition 14 (Option-C encoding).** Replace Definition 4's stored partial
rule with:
```
stored[c] := ∨_{K ∈ keys(C_c)} densePK_N(K)
```
(= bitwise OR of all densePKs).

Replace Definition 5's routing with: `MATCH_N(K) := {c : densePK_N(K) ⊆ stored[c]}`
(= K's bits are all contained in c's bit-cover). Tie-breaker rules analogous to
Option A apply.

The critical structural property: stored[c] *grows monotonically* as keys are
added to c's subtree (the OR can only set more bits, never unset).

> **Theorem 6 (Option-C update cost).** *For any insert of key K into a subtree
> rooted at slot c with `|keys(C_c)|` keys, maintaining Option-C's stored
> invariant requires either (a) updating stored[c] with a fresh OR over all
> keys, which costs `Ω(|keys(C_c)|)`, or (b) updating stored[c] incrementally as
> `stored[c] := stored[c] ∨ densePK_N(K)`, which has cost O(1) per insert but
> does not handle deletions — under any deletion of a key whose densePK
> contributed an otherwise-unique bit, the recompute is forced.*

**Proof.** *Part (a)*: A full re-derivation of `stored[c]` reads every key in
`keys(C_c)` once. Cost is `Θ(|keys(C_c)|)` per re-derivation, by definition of
the OR fold over a multiset. *Part (b)*: Incremental insert-only update is
correct (OR is monotone under additions), but on deletion of a key K, if K was
the unique contributor to some bit β in `stored[c]` (= every other key has β =
0), removing K should clear β from stored[c]. Detecting this requires knowing
whether other keys have β set, which requires scanning `keys(C_c)`. Hence
deletion-correct maintenance costs `Ω(|keys(C_c)|)` per deletion. ∎

**Theorem 6 implies cost equivalence to scoped rebuild.** Sirix's multi-revision
isolation requires both insert and delete to be supported (sliding-snapshot
versioning interleaves both). Under Option C, each delete triggers an
`Ω(|keys(C_c)|)` re-derivation, summed across the path from leaf to root. The
total per-delete cost is `Ω(∑_d |keys(C_{c_d})|) = Ω(total subtree size at
insertDepth)`. This matches the scoped rebuild's cost asymptotically.

For inserts under Option C: incremental update is `O(1)` per slot on the
descent path. Every ancestor on the path from root to the leaf must re-OR
its stored partial with the new key's densePK. With tree depth `d`, the total
insert cost is `O(d)` per insert. This is cheaper than the per-delete
`Ω(content)` cost — but only for an insert-only workload, which is not Sirix's
case.

> **Corollary 6.** *Under Sirix's multi-revision insert+delete workload, Option
> C's amortized per-operation cost is no better than the scoped rebuild
> asymptotically.*

**Discussion.** Option C does not "escape" the impossibility — it eliminates the
I8-unsafe firing class by making stored content-accurate, but pays the rebuild
cost on deletions. The structural trade-off mirrors Theorem 4's Θ(n) cost: any
encoding that maintains content-accurate stored partials must pay content-
proportional cost on operations that modify content.

#### §6.15.3 Cross-encoding structural commonality

Theorems 5 and 6 analyze Options A and C individually. They are connected by a
structural commonality worth naming explicitly.

**Definition 15 (densePK-routing encoding).** An encoding `Enc` is a
*densePK-routing encoding* iff:
1. `stored[c]` is a deterministic function of `keys(C_c)` and the mask `M`.
2. The routing function `T_Enc(S, densePK_K) → {0, …, n-1} ∪ {⊥}` is
   deterministic and depends only on `(S, densePK_K)`.

Binna's PEXT, Option A (AND), and Option C (OR) are all densePK-routing
encodings. They differ in the function defining `stored[c]` and the routing
rule, but all share the deterministic-routing-on-densePK structure.

**Observation 1 (multi-value-leaf permits densePK coexistence).** Under any
densePK-routing encoding combined with Sirix's multi-value leaves, keys with
identical `densePK_N(K)` at a node's mask coexist in the same slot. The
leaf-level findEntry (`HOTLeafPage.findEntry`, performing binary search over
sorted full keys per Sirix's leaf-page format §85-§95 of `HOTLeafPage.java`)
distinguishes them by full-key comparison. The indirect-node routing returns
the slot; the leaf-level lookup then resolves to the specific entry.

Hence the *trivial* densePK-collision (= two keys forced into the same slot by
densePK equality) is *not* an impossibility under multi-value-leaf semantics.
This contrasts with Binna's single-entry-leaf model (cf. Binna §3.3, where
leaves contain exactly one key), where densePK-collision forces mask extension
to split the leaf at the time of the second insert.

**Observation 2 (Theorem 2's impossibility cuts across encodings).** Theorem
2's construction uses slot-stored values
forming a chain `S_c = 2^c - 1`, with K's `densePK_K = S_{k+1}` causing every
slot to subset-match K (under Binna's PEXT). Under Option A, the same chain
emerges if each slot c contains a single key with `densePK = S_c` — the
content-derived `stored[c] = AND = S_c`. The k-touch lower bound argument
(content-preservation + match-conservation + slot-evacuation) carries over
verbatim. Under Option C, the construction needs adjustment (K's
`densePK_K = 0` so all slots have `stored ⊇ 0` trivially), but the same
structural mechanism — *subset-match conservation under content-preserving
touches* — applies.

**Conclusion of cross-encoding analysis.** The impossibility for densePK-
routing encodings is structurally driven by:
- *Subset-match conservation* (Corollary 3-style): touched-and-content-
  preserved slots retain matching status with K.
- *I7 partial order* forcing K's slot index to align with K's lex position.
- *Routing's deterministic dependence on densePK*: keys with identical densePK
  route identically, removing one degree of freedom.

These three structural features are encoding-agnostic. Theorems 1, 2, 5, and 6
instantiate this structural argument under specific encodings; a unified abstract
formulation would require defining the encoding class precisely and exhibiting
a single construction that defeats all such encodings simultaneously. We leave
this unified formulation as **future work** (cf. §9.3), noting that the
individual encoding impossibilities (Theorems 5, 6) close the empirically
relevant gaps.

#### §6.15.4 Summary of encoding-replacement analysis

| Encoding | Routing | Update cost | Correctness |
|---|---|---|---|
| Binna's PEXT (current) | densePK ⊇ stored | O(1) per insert | I8-unsafe firings → scoped rebuild fallback (Theorems 1–4) |
| Option A (AND) | stored ⊆ densePK | O(1) per insert (incremental AND) | No deterministic tie-breaker correctness-preserving (Theorem 5) |
| Option C (OR) | densePK ⊆ stored | O(content) per delete (Theorem 6) | Avoids firings but at rebuild-cost asymptotically (Corollary 6) |

**Conclusion.** Encoding replacements do not provide an asymptotic improvement
over the scoped rebuild path. Option A inherits an analogous impossibility
under any deterministic tie-breaker; Option C maintains content-accurate stored
but pays content-proportional cost on deletes, which Sirix's multi-revision
workload incurs. Hence the scoped `rebuildSubtree(insertDepth)` design under
Binna's PEXT encoding is *not strictly inferior* to either alternative.

### §6.16 Strict limits of Theorems 1, 2, 3, 4, 5, and 6

These results rule out mask-fixed and mask-extension localized primitives under
Binna's PEXT routing encoding. They do *not* rule out:

- **Encoding replacements.** Options A (AND-encoding) and C (subtree-OR
  recompute) from `HOT_ROUTING_ENCODING_REWRITE.md` modify Definition 5 (routing)
  and Definition 4 (stored partial). Theorems 1–4 do not apply to such modified
  encodings. They would have their own impossibility / cost analyses.
- **Soft-I8 reader tolerance.** Reader-side fallbacks weaken the I8 invariant
  enforced at read time; no write-side primitive is needed. Theorems 1–4 do not
  apply because they assume strict I8 in N'.
- **Whole-subtree rebuild.** `rebuildSubtree(insertDepth)` operates on all slots,
  not within the localized taxonomy of Definitions 8, 10, or 12. Theorem 4 shows
  it is asymptotically optimal in the dense-μ regime.
- **Multi-revision-aware primitives.** Theorems 1–4 reason within a single node
  N. Sirix's CoW + path-copy model creates a forest of nodes across revisions; a
  primitive that operates across revisions is outside our taxonomy.

The complete picture across §6:

| Result | Coverage | Statement |
|---|---|---|
| **Theorem 1** | Single-slot, mask-fixed, *every* firing | No primitive resolves any firing. |
| **Theorem 2** | k-touch, mask-fixed, *some* firing per k | For every k, ∃ firing requiring > k touches. |
| **Theorem 3** | Mask-extension + k touches, content-blind | Constant-cost mask extension defeated by adversarial content. |
| **Corollary 5** | Mask-extension + k touches, content-aware | Content-aware mask extension's content inspection cost ≥ μ. |
| **Theorem 4** | Scoped rebuild upper bound | Θ(n) per node; tight against Θ(μ) lower bound in dense-μ regime. |
| **Theorem 5** | Option A (AND-encoding), three tie-breakers TB-h, TB-s, TB-m | For each, ∃ a node configuration where SRC cannot be maintained post-insert. |
| **Theorem 6** | Option C (subtree-OR), update cost | Deletion-correct maintenance Ω(content) per delete. |
| **Corollary 6** | Option C under multi-rev insert+delete | Amortized cost no better than scoped rebuild. |

Together, these six theorems and two corollaries establish that under both
Binna's PEXT routing encoding and the two proposed alternatives (Options A and
C), the scoped `rebuildSubtree(insertDepth)` is the canonical correctness-
preserving operation for the firing class. No localized primitive of any kind
(single-slot, k-touch, mask-extending, AND-encoded, OR-encoded) can resolve
every firing at strictly lower asymptotic cost.

**Remaining theoretical open questions** (§9.3):

- Closing the asymptotic gap for sparse-μ firings (`O(n)` rebuild vs. `Ω(μ)`
  lower bound when `μ = o(n)`) — empirically the common case.
- Information-theoretic generalization of Theorem 1's argument.
- Soft-I8 reader tolerance — a distinct path that loosens write-side invariants.

## §7. The Cost is Bounded: Empirical Validation

`Direction1ScaleValidationProbe` runs the failure-mode workload (interleaved
insert/delete under SLIDING_SNAPSHOT) at three scales:

| Shape | Inserts | Revs | Direction-1 fallback firings | rebuilds / insert |
|---|--:|--:|--:|--:|
| baseline | 10,000 | 10 | 4 | 0.21% |
| 5× revisions | 50,000 | 50 | 7 | 0.08% |
| 5× keys/rev | 50,000 | 10 | 3 | 0.16% |

The fallback count is O(revisions) and *insensitive* to keys-per-revision. Each
fallback is a single `rebuildSubtree(insertDepth)` call: `O(subtree-size)` page
copies. Under Sirix's CoW model, every insert path-copies the spine anyway, so the
rebuild's *marginal* cost is bounded by the subtree size at `insertDepth`. At height
2 (= 500K-key tree's observed height), this is at most one indirect-page + a leaf —
the same cost as the baseline insert.

`HOTMicrobenchmark.smallCombinedMicrobench` (500,000 inserts under `hot.strict.binna
=true`):

```
[microbench/diagnostic] validator: storedKeys=505000 observedHeight=2 violations=0
[microbench/diagnostic] full-scan misses=0/500000 first20=[]
[microbench] writes N=500000 · 878,468 ops/sec · 1138 ns/op · total=569.2 ms
[microbench] reads  M=1000000 (hits=1000000) · 1,058,127 ops/sec · 945 ns/op
```

Sub-microsecond per insert, sub-microsecond per lookup, height-2 trie, zero
correctness violations.

## §8. Paper Contribution Structure

### §8.1 Positive contribution

**Sirix-HOT: a multi-value-leaf extension of Binna's HOT for disk-backed persistent
storage.** Faithful to Binna's PEXT routing, sparse partial keys, SIMD subset
match, and the `addEntry / splitIndirect / integrate / rebuildSubtree` algorithmic
family. Sub-microsecond per-operation throughput; height-optimal trie; zero
correctness violations under sliding-snapshot multi-revision isolation.

### §8.2 The structural-cost identification

**We identify a load-bearing assumption in Binna's HOT (single-entry leaves) and the
specific class of inserts whose correctness is lost when this assumption is relaxed
(Direction-1 I8-unsafe firings).** Three independent localized fixes are refuted —
two by empirical counter-examples on the formal-verification canary suite, one by
formal argument tied to the encoding's history-dependent accumulation. We argue
informally that no localized primitive within Binna's encoding can resolve the
firings.

### §8.3 The bounded-fallback design

**The scoped `rebuildSubtree(insertDepth)` is the minimum-precision operation that
canonically resolves the firing class.** Empirically, the fallback fires `O(revisions)`
times — independent of insert volume. Combined with Sirix's CoW path-copy model, the
fallback's *marginal* cost over the persistent-baseline insert is bounded.

### §8.4 Two fallback classes — only one is theoretically necessary

A subtle but important distinction emerged during implementation: the writer has
*two* structurally distinct fall-back-to-rebuild triggers, and only one is
fundamentally impossible to handle incrementally.

**Class 1 — Direction-1 I8-unsafe firings (theoretically necessary).** This is the
subject of Theorems 1–6. Routing's highest-index-subset rule structurally conflicts
with the I8-required slot index for K. No localized mask-fixed primitive resolves it.
The scoped rebuild is provably optimal (Theorem 4). This is the genuine structural
cost of multi-value-leaf HOT.

**Class 2 — cross-level mask overlap (engineering-deferred, now eliminated).** A
second trigger arises when `integrate`'s cascade reaches a node `N` whose mask
already contains the split BiNode's discriminative bit `β` (= `child.MSB`). This
is *not* a theoretical impossibility — it is the multi-value-leaf accumulation
artifact: `addEntry`'s zero-fill convention sets a (then non-affected) sibling's
β-column to 0 even when that sibling's content straddles β, producing an ancestor
mask that overlaps a descendant's MSB (violating Binna's strict trie invariant,
Theorem II(d)). When the writer later splits that descendant, `integrate`'s
`addEntry` rejects the already-present β.

This case **is** incrementally resolvable. The structural opportunity: when β is
already a disc bit of `N` at column `c`, `N`'s slots are *already partitioned* by
β-value at `c`. The split BiNode's two halves fold into the β-aligned slots
directly — the off-path-straddled slot's β=0 half keeps its partial, the β=1 half
takes `oldPartial | β-bit` (= `addChildAtCombination`'s `comboPartial`, proven
routing-correct). The primitive `mergeBiNodeAtExistingDiscBit` (not-full `N`) and
`splitIndirectWithSlotReplaceAndInsertion` (full `N`) realize this; `integrate`
dispatches on β-membership. No rebuild fires.

**The distinction matters for the paper's honesty.** The "scoped rebuild is the
canonical operation" claim (Theorem 4) applies *only* to Class 1. Class 2 was a
transient implementation gap, not a structural cost — and it is closed. Conflating
the two would overstate the impossibility.

**Caveat to Class 2's routing-correctness claim.** The assertion above that
`addChildAtCombination`'s `comboPartial` is "proven routing-correct" holds only
when the sibling subtree that currently routes for `comboPartial` is *β-constant*.
Under multi-value leaves it need not be — which is exactly Class 3.

### §8.4a Class 3 — off-path-straddle stranding on combo-partial add (engineering, now guarded)

A third trigger, distinct from Classes 1–2, is the source of the *2000-entries-per-
revision silent data-loss bug* (interleaved delete+reinsert of overlapping values;
clean at 1000/rev, corrupt at 2000/rev). It is **not** an I8-ordering firing (Class 1)
and **not** the integrate-cascade mask overlap (Class 2); it is a *routing*
correctness failure of the very combo-partial primitive Class 2 relies on.

**Mechanism.** Sirix's routing (`HOTIndirectPage.findChildSpanNode`) is
*equality-/most-specific-preferred*: among partials that subset-match
`densePK_N(K)`, it prefers an exact match, else the highest. The incremental branch
(`tryBranchIncremental`, `betaIsDiscBit`) and its full-node variant
(`branchFullNodeAtExistingBit`) add a fresh single-key child at
`comboPartial = subtreePrefix | β-bit`, assuming — per Binna's single-TID model —
that the affected subtree is *one-sided on β*. With multi-value leaves the subtree
can **straddle** β (Fact: a leaf is a complete `R(S)` subtree only after a
bulk-build/rebuild; an incrementally-grown leaf may hold both β-values). Then
`comboPartial` becomes the most-specific (often *exact*) match for sibling keys that
already live in a β=`betaValue` leaf, silently re-routing them to the new child
**without migrating them**. The stale physical copy persists → a *live* key in two
leaves (I1 cross-leaf duplication) + I6 misroute = data loss. It surfaces later, when
any such key is re-inserted via the innocent `mergeWithNodeRefs` fast path.

**Why it is not ruled out by Theorems 1–4.** Those theorems bound primitives that
*resolve the firing while maintaining strict eager structure in N′*. Class 3's fix
does the opposite: it **abandons eager discrimination**. Before committing the add,
the writer simulates routing on the candidate node (`branchAddStrandsExisting`: does
any existing key route to the new child's slot?). On a detected strand it adds *no*
partial and instead **merges K into its descended leaf** — routing-consistent by the
descent tautology, introducing no competing partial, hence nothing is stranded. A
later *capacity-driven* leaf split (`handleOffPathOverflow`) partitions the leaf on β
and migrates the whole `densePK`-equivalence class **atomically** — the only
stranding-free way to introduce the discrimination. This is a *write-side
soft-structure tolerance*, the dual of §9.2's soft-I8 *reader* tolerance: both accept
a transient deviation from eager canonical structure to preserve a hard invariant
(here: every stored key routes to a leaf that contains it).

**Cost.** The strand check is `O(affected subtree)` per `betaIsDiscBit` firing — the
same regime as the scoped rebuild (Theorem 4), so Class 3 introduces no asymptotic
penalty beyond the bound already established. It is applied at all seven
partial-adding branch sites (site-1/-2 combo adds, full-node fold, leaf-pair,
new-partition-root, full-boundary wrap, multi-affected `addEntry`).

### §8.4b Minimum height is not preserved — and a rebuild does not restore it

Binna's height-optimality (SMHP, thesis §4.2; Lemmas 2–3) is stated over **single-TID
leaves**: every leaf is trivially a complete subtree of size 1, so the eager
discrimination that SMHP requires never conflicts with leaf contents. Sirix's
multi-value leaves break this, and the break is *not* repaired by a full rebuild:

`HOTBulkBuilder` cuts a leaf at the **highest `R(S)` subtree that fits a page**
(≤ `MAX_ENTRIES` *and* ≤ byte capacity), so that "every ancestor discriminative bit
is constant across the leaf's keys (Fact R1)" and I5 holds regardless of cardinality.
That *complete-`R(S)`-subtree* constraint is what buys correctness — but it forces
leaves to be packed to "largest complete subtree ≤ capacity," **not to capacity**.
Under-full leaves ⇒ more leaves ⇒ more upper-structure levels than an unconstrained
minimum-height bucketed trie would need (it is precisely why the underflow/
`consolidateNodeLeaves` sweep exists).

Hence:

> **Observation 3 (height).** Sirix's HOT is *height-bounded* (≤ the k-constrained
> `R(S)` depth) and *SMHP-shaped* in its compound-node skeleton, but it is **not
> minimum-height**, and a `rebuildSubtree`/bulk-build does **not** restore Binna's
> minimum-height property. Minimum height (which wants arbitrary capacity-packed
> buckets) and routing correctness under equality-preferred PEXT (which wants
> complete-subtree buckets, Fact R1) are in tension the moment a leaf holds more than
> one key. Sirix resolves the tension toward correctness.

Class 3's merge-deferral is the same trade-off on the *insert* path: it accepts a
transient off-path straddle (a leaf temporarily not a complete `R(S)` subtree) to
keep routing correct, and a later overflow/rebuild folds it back into complete-subtree
leaves. It therefore does not move the post-rebuild height — the baseline already was
not minimal.

### §8.5 Complementary results

- **Tombstone preservation** under differential / incremental versioning ([[hot-tombstone-preservation]]).
- **Slot-granular leaf CoW** for multi-revision historical reads (task #57).
- **Persistent reader-eviction asymmetry fix** ([[hot-eviction-asymmetry]]).

## §9. Future Work

### §9.1 Routing-encoding alternatives

`docs/HOT_ROUTING_ENCODING_REWRITE.md` enumerates three candidate replacements for
Binna's PEXT encoding:

- **Option A** — AND-encoding: `stored[c]` = AND over all keys in `c's` subtree of
  `PEXT(K, mask)`. Known degeneration mode: as inserts diversify, `stored` shrinks
  toward 0 (all-zero subset-matches everything → ambiguous).
- **Option C** — Subtree-OR recompute: `stored[c]` = OR over all keys in `c's`
  subtree. Cost: O(subtree-size) per CoW. Tradeoff: maintains content-accuracy at
  the price of write-time amortization. Under Sirix's CoW, the cost may be
  acceptable because the path is already being copied.

Neither has been implemented. Either would replace the routing-encoding rewrite
gated as a future work item.

### §9.2 Soft-I8 reader tolerance

A reader-side fallback that walks up over slot boundaries violating I8, analogous to
the existing soft-I6 lower-bound walk-up for `betaIsDiscBit` cases. Would eliminate
the write-side rebuild at the cost of a slower read on violated slots. Untried;
breaks an invariant; suitable only if write throughput becomes the bottleneck (not
currently the case).

### §9.3 Remaining theoretical questions

Theorems 1–6 in §6 establish the impossibility result rigorously across:
- Single-slot mask-fixed primitives (Theorem 1).
- k-touch mask-fixed primitives (Theorem 2).
- Mask-extending primitives, both content-blind and content-aware (Theorem 3
  and Corollary 5).
- Scoped rebuild's asymptotic optimality in the dense-μ regime (Theorem 4).
- Option A (AND-encoding) under all common tie-breakers (Theorem 5).
- Option C (subtree-OR-encoding) under multi-rev insert+delete (Theorem 6 and
  Corollary 6).

The following questions remain open as future theoretical work:

- **Sparse-μ tight bound.** Theorem 4 establishes `Θ(n) = Θ(μ)` in the dense-μ
  regime (`μ → n`). For sparse-μ firings (`μ = o(n)`, the empirically common
  case), the asymptotic gap between `Ω(μ)` lower bound and `O(n)` rebuild
  remains. Closing this gap requires either constructing a smarter primitive
  with `O(μ)` cost (unknown whether feasible) or strengthening the lower bound
  to `Ω(n)` (would require new arguments about the slot-position constraints).
  Note this gap concerns *Class 1* (Direction-1 I8-unsafe) firings only; the
  *Class 2* cross-level-overlap case (§8.4) is already handled in `O(node-width)`
  by `mergeBiNodeAtExistingDiscBit` with no rebuild.

- **Information-theoretic version.** The proofs depend on Corollary 3 (subset-
  match conservation under content-preserving touches). A cleaner information-
  theoretic statement — comparing what a k-touch primitive can compute from
  `(densePK_N(K), S, mask)` against what I8-correct routing requires — could
  generalize the results to richer primitive taxonomies (e.g., randomized
  primitives, primitives with auxiliary state).

- **Soft-I8 reader formalization.** §9.2 sketches a soft-I8 reader fallback that
  walks slot boundaries with violations. A formal statement of what weakened
  invariant the reader can tolerate (and what algorithmic price is paid) would
  give an alternative correctness-preserving design and a head-to-head
  comparison with the scoped-rebuild path. Theorem 5/6 implicitly assume strict
  I7 + I8; a reader-tolerant version of the framework would yield different
  primitive feasibility.

- **Cross-encoding unified lower bound.** Theorems 5 and 6 cover Options A and
  C individually; §6.15.3 names the structural commonality (subset-match
  conservation, I7-vs-lex tension, deterministic-densePK routing) but does not
  unify them into a single abstract theorem. An attempted densePK-collision
  generalization (§6.15.3 retraction) failed because multi-value-leaf semantics
  permits keys with identical densePK to coexist in one slot — the natural
  collision case is resolved at the leaf level, not at the indirect node where
  our impossibility lives. A unified abstract theorem would require:
  (i) formalizing the encoding class precisely (= "deterministic densePK-routing
  encoding with content-derived stored partials"), (ii) exhibiting a single
  construction that defeats *every* encoding in the class with a single I8-vs-
  routing argument, (iii) handling the multi-value-leaf semantics correctly so
  the construction is non-trivially impossibility-resistant. The individual
  encoding-specific impossibilities (Theorems 5, 6) cover the practically
  relevant cases.

## References

- Robert Binna. *HOT: A Height Optimized Trie Index for Main-Memory Database Systems.*
  PhD thesis, 2018.
- `docs/HOT_FORMAL_FOUNDATION.md` — Theorems 1–5 for canonical HOT.
- `docs/HOT_ROUTING_ENCODING_REWRITE.md` — alternative routing encodings A / B / C.
- `docs/HOT_REBUILD_FALLBACK_ELIMINATION_PLAN.md` — Stage 4b iter-1..3.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/Direction1HitRateProbe.java`
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/Direction1ScaleValidationProbe.java`
- `bundles/sirix-core/src/main/java/io/sirix/index/hot/AbstractHOTIndexWriter.java`
  (method `dumpDirectionOneFallback`)
