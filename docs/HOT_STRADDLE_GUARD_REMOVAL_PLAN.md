# HOT straddle-guard removal — design plan

Historical design and review record from `fix/hot-strict-binna-conformance`.
The minimum-height conclusions were withdrawn after the lost-outer-spine regression;
current height and normalization requirements are owned by
[HOT Index Specification §2.6](HOT_INDEX_SPECIFICATION.md#26-partial-keys-and-routing).
**Supersedes:** `docs/HOT_ADDENTRY_STRADDLE_FIX.md` — that fix produces *correct* trees
(`rebuildSubtree` is canonical by construction) but it preserves a **non-invariant** at
**O(subtree)** cost per leaf split. **Empirically settled:** `StraddleCanonicityProbe`
(this campaign) + the guard-disabled canary run below.

This plan removed the guard to keep leaf-overflow folding incremental. Its original
no-commit-pass and minimum-height goals do not describe the current implementation;
see §5.2.

---

## §1. Problem

When a multi-value leaf `L` overflows, `splitLeafPage` splits `L ∪ {K}` at
`λ = msdb(L ∪ {K})` into `L₀` (`λ=0`) / `L₁` (`λ=1`); `integrate` folds
`BiNode(λ, L₀, L₁)` into `L`'s parent `N` via `addEntry`, adding `λ` as a new
discriminative bit. `addEntry` (`HOTIncrementalInsert.java:733`) carries a committed
guard — `splitBitIsSafe` (`:848`) — that scans every non-affected sibling for a
**straddle** of `λ` (`subtreeStraddles`, `:879`: the sibling's subtree has a key with
`λ=1` *and* a key with `λ=0`). On a straddle it throws `HOTStraddleException`;
`AbstractHOTIndexWriter` catches it and calls `rebuildSubtree` — an **O(subtree)**
collect-sort-`HOTBulkBuilder.build` recanonicalization.

The guard **fires on most leaf splits of any non-ascending workload.** A canonical trie
of uniform-width keys has equal-width sibling leaves sharing one key-set MSDB, so a
middle-insert split bit `λ` lands at or below a sibling's MSDB and the raw straddle scan
trips. (Ascending inserts *append* a leaf — `splitLeafPage` is a middle-insert op — so
they never fire it; this is why `HOTMicrobenchmark`, an ascending load, never saw the
cost.) Measured on this campaign's probe: **76 + 62 folds rejected → 76 + 62
`rebuildSubtree` calls** on workloads where the incremental fold was provably correct.

> **The defect, precisely.** The guard tests **raw constancy** ("every sibling key
> equal at `λ`"). That is *not* a HOT invariant — §3 proves the canonical builder
> violates it routinely — so the guard rejects perfectly canonical folds and pays an
> O(subtree) rebuild to reach a tree `addEntry` already produced in O(node).

---

## §2. Invariants and definitions

A HOT compound node `N` has discriminative bits `D(N)` (absolute MSB-first positions) and
children `c₀…cₙ₋₁`, each with a stored sparse partial `pᵢ`. Routing extracts
`densePK(K, M_N) = PEXT(K, D(N))` and picks the highest-index child with `pᵢ ⊆ densePK`
(`HOTIndirectPage.findChildByPartialKey`; Sirix additionally
prefers an exact match `pᵢ == densePK`, `:509`–`:525`).

Structural invariants (`HOT_FORMAL_FOUNDATION.md` §2): **I1** leaves key-disjoint, **I3**
partials distinct, **I4** `p₀=0`, **I7** partials ascending, **I8** children sorted by
firstKey, **I11** trie condition (`N`.MSB strictly more significant than every child
compound node's MSB), **R(S)** subtree contiguity, **I6** routing (`descend` reaches the
leaf holding `K`) — the *goal*.

**The two clashing definitions of I5 — the crux of this plan.**

- **I5-route (the real invariant).** `∀ child i, ∀ K ∈ subtree(cᵢ):
  (pᵢ & ~densePK(K, M_N)) == 0` — every bit *set in the stored partial* `pᵢ` is set in
  every subtree key's dense PK. This is `HOT_FORMAL_FOUNDATION.md` §2's I5; it is what
  `HOTInvariantValidator` checks — both the `I-Binna-sparse-path` form (firstKey,
  `:432`–`:438`) and the `I5-strict` form (every interior key, `:487`–`:523`) — and what
  `HOTMalformedSubtreeDetector.findI5FailingKey` (`:285`–`:323`) checks per key. It is a
  **one-sided** condition: it constrains only bits *set* in `pᵢ`.
- **I5-raw (a non-invariant).** `∀ child i, ∀ b ∈ D(N)`: all keys of `subtree(cᵢ)` are
  equal at bit `b`. This is `HOT_ADDENTRY_STRADDLE_FIX.md` §2's "I5 — child
  β-constancy", and the negation of what `subtreeStraddles` detects.

**I5-raw ⟹ I5-route** (constant ⇒ subset), but **not conversely**: a key may differ at a
bit that is *off* child `i`'s block-path — there `pᵢ`'s column is 0 (sparse-path
encoding), so `(pᵢ & ~densePK)` is unaffected. **I5-raw is strictly stronger than
routing requires, and §3 shows the canonical HOT does not even satisfy it.**

The historical analysis distinguished three properties:

- **invariant-clean** — satisfies I1–I8 + I11. `HOT_FORMAL_FOUNDATION.md` Theorem 2
  proves invariant-clean ⟹ I6 (routing-correct). This is **correctness**.
- **height-minimal** — the trie's height equals `minheight(S)`, the smallest height any
  fanout-≤32 HOT over `S` can have. This is the **minimum trie height** property.
- **canonical (SMHP)** — invariant-clean, height-minimal, *and* the one specific
  order-independent node-grouping `HOTBulkBuilder.build` produces, unique by Binna
  Lemma 2.

The incremental insert is insertion-order-dependent: a node's compound-block membership
depends on which leaf overflowed when. The accompanying minimum-height claim was not
established; see §5.2.

---

## §3. Root-cause analysis (formal + empirical)

### §3.1 The canonical HOT contains straddles — so I5-raw is not an invariant

A child `cᵢ` *straddles* a disc bit `b ∈ D(N)` when `subtree(cᵢ)` has keys differing at
`b`. `b` is **off-path** for `cᵢ` when `b` labels no block-BiNode on `cᵢ`'s block-path —
equivalently `pᵢ`'s `b`-column is 0. An off-path bit is one that branches inside a
*sibling's* sub-trie; `cᵢ`'s own sub-trie may branch on the same bit *position*
independently (a bit position labels many BiNodes across `R(S)` — Fact R2 orders bits
only along a path).

> **Lemma 1 (off-path straddles are canonical and harmless).** In a canonical HOT a
> child may straddle a disc bit `b` whenever `b` is off that child's block-path; routing
> stays correct.

*Proof.* Routing soundness (`HOT_FORMAL_FOUNDATION.md` Theorem 2) needs I5-route, not
I5-raw: its descent argument constrains, for the true child `i*`, only the block bits
**on `i*`'s path** (it shows no higher sibling `j` subset-matches because their LCA
block-bit `β*` — on `i*`'s path — has `K[β*]` fixed to `i*`'s side). An off-path bit `b`
of `cᵢ` has `pᵢ`'s `b`-column 0, so `b` never enters a subset test for `cᵢ`, and `cᵢ`'s
keys may take either value at `b` freely. ∎

**Empirical confirmation — `StraddleCanonicityProbe`.** Walking `HOTBulkBuilder` output
(the provably-canonical builder) over ascending / random / bimodal key sets:
**1240 straddles across 78 compound nodes, every one off-path (`colBitSet=false`), and
0 misroutes.** A canonical HOT routinely violates I5-raw and routes 100 % correctly.
I5-raw is therefore *not* a HOT invariant — it cannot be, since the canonical structure
breaks it.

### §3.2 The I5-route argument for `addEntry`

`addEntry` (`HOTIncrementalInsert.java:733`) re-encodes, for the new bit `λ` at column
`betaColumn`: the affected child's two replacements get `betaValue` 0 (`L₀`, `:777`–`:781`)
and 1 (`L₁`, `:782`–`:785`); **every non-affected sibling gets `betaValue = 0`**
(`:775`–`:777`, `:786`–`:789`) via `reencodeWithNewBit` (`:920`). Then it assembles the
node through `HOTBulkBuilder.assembleIndirect` (`:799`) — the *same* primitive
`HOTBulkBuilder` uses.

> **Theorem 1 (`addEntry` preserves I5-route).** Folding `BiNode(λ, L₀, L₁)` into a
> canonical not-full node `N` yields a node `N'` satisfying I5-route — **for every
> child, with no guard.**

*Proof.* Two groups.
- *Non-affected sibling `c`.* `addEntry` sets `c`'s `λ`-column to 0. So `p_c`'s `λ`-bit
  is 0 and `λ` contributes nothing to `(p_c & ~densePK)`. `c`'s other columns are `c`'s
  old partial, unchanged; `c`'s subtree is structurally unchanged; so `c`'s pre-insert
  I5-route survives verbatim. **Whether or not `c` straddles `λ` is irrelevant** — `λ`
  is off-path for `c` by construction (`addEntry` *made* it off-path: column 0). ∎
- *Affected replacements `L₀`, `L₁`.* `splitLeafPage` splits `L ∪ {K}` exactly at `λ`,
  so `L₀`'s keys are all `λ=0` (column 0 ✓) and `L₁`'s are all `λ=1` (column 1 — every
  key has the bit, I5-route holds). The `D(N)` columns: `L₀`/`L₁` inherit `L`'s region,
  which is `D(N)`-constant — `L` was a canonical child, and the merge-vs-branch bound
  (`β > leastSignificantDiscBit`, `AbstractHOTIndexWriter.java:722`) puts every `D(N)`
  bit above `K`'s mismatch, so `K` agrees with `L` on `D(N)`. ∎

I3/I4/I7/I8/I11 are preserved by `addEntry` **independently of the guard** — the
straddle guard never touched them; their preservation is the existing `addEntry`
correctness proof (`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`; `HOT_ADDENTRY_STRADDLE_FIX.md`
Theorem 1 §5.1 itself states "I3, I4, I7, I8 — unchanged … the guard does not touch
this"). With I1–I8 + I11, `N'` is invariant-clean ⟹ I6 (`HOT_FORMAL_FOUNDATION.md`
Theorem 2). ∎

### §3.3 Why the guard exists — the over-strict-I5 misdiagnosis

`HOT_ADDENTRY_STRADDLE_FIX.md` §2 *defined* I5 as I5-raw and §3.3 correctly proved
"`addEntry` preserves **I5-raw** iff no sibling straddles `λ`". The proof is sound — for
I5-raw. But I5-raw is not the routing invariant (§3.1); the guard preserves a property
the canonical HOT itself lacks. The diagnosis chain in `hot-betaisdiscbit-fullnode-blocked`
("first I5 violation at insert #1031") used a *raw-constancy* per-insert probe — it
flagged an **off-path straddle**, which is canonical. `subtreeStraddles` is *correct
code* (it accurately detects raw straddles); it simply tests the wrong predicate.

### §3.4 The guard provably never catches a real bug

An *on-path* sibling straddle of `λ` (the only routing-breaking case) would require a
sibling `c` with `p_c`'s `λ`-column = 1 and `c`'s keys differing at `λ`. But `addEntry`
sets **every** sibling's `λ`-column to 0 — an on-path sibling straddle of `λ` is
**structurally impossible** in `addEntry`'s output. The affected replacements `L₀`/`L₁`
*do* have `λ` on-path, and must be `λ`-constant — guaranteed by `splitLeafPage`
(`HOTLeafPageSplitFaithfulTest`) — and the guard *exempts the affected run anyway*. So:
the guard can only ever fire on a harmless off-path straddle, and could not catch a
malformed affected run even in principle. **Removing it loses zero protection.**
Empirically: `StraddleCanonicityProbe` step 4 — **76/76 straddling folds canonical** (0
malformed per `HOTMalformedSubtreeDetector`, 100 % routing) with the guard disabled.

---

## §4. The fix

**Delete the straddle guard.** `addEntry` keeps only its genuine preconditions —
`λ ∉ D(N)` and `N` not full (`HOTIncrementalInsert.java:740`–`:751`) — and always folds
incrementally in O(children). `integrate` always folds or cascades; no
`HOTStraddleException`, no straddle-driven `rebuildSubtree`. The result is the
**incremental** path the campaign wanted, with **no new machinery** — a
pure removal.

`rebuildSubtree` itself **stays** — it remains the target of the genuine
`IllegalArgumentException | IllegalStateException` self-heal (a real structural
inconsistency, e.g. the `integrate` trie-condition assertion `:1327`) and of
height-escalation; only its *straddle* caller is removed. On a tree kept canonical by
§3.2's induction the self-heal never fires — it is pure defense.

**Optional defensive check.** A debug-only assertion, gated behind a system property
(e.g. `-Dhot.assert.canonical=true`, off in production), may run
`HOTMalformedSubtreeDetector` on a freshly folded node and fail fast on a real
malformation. It is O(subtree) so it is *never* on the production hot path — it exists
only to catch a regression in `splitLeafPage`/`integrate` during testing.

---

## §5. Formal verification

Inductive hypothesis: the live trie is **canonical** before insert `i` (base: the empty
index is canonical). `doIndex` does one of: fast-path merge; leaf-overflow → `integrate`
→ `addEntry`/cascade; branch insert; the rare self-heal rebuild.

### §5.1 Theorem 2 — the leaf-overflow fold preserves invariant-cleanness

> `doIndex` of a key whose insert overflows a leaf `L` leaves the trie
> **invariant-clean** (I1–I8 + I11 ⟹ I6, routing-correct).

*Proof.* `splitLeafPage` produces `BiNode(λ, L₀, L₁)` with `λ = msdb(L ∪ {K})`; `L₀`,
`L₁` are valid `R`-subtrees, `λ`-constant (`HOTLeafPageSplitFaithfulTest`). `integrate`:

- **`N` not full** → `addEntry`. Invariant-cleanness: Theorem 1 (§3.2). Height:
  `addEntry` sets `height = 1 + maxChildHeight` (`:795`–`:800`); `λ` joins `N`'s block,
  which keeps `N` one compound node (a block of `m+1 ≤ 31` BiNodes is still one node).
  This local height calculation does not establish global minimality (§5.2).
- **`N` full** → capacity cascade: `splitIndirect(N)` at `N.MSB` + `foldIntoHalf` (the
  affected half absorbs `λ` via `addEntry`) + recurse. This is Binna's verified split
  (`HOT_INCREMENTAL_SPLIT_VERIFICATION.md`); removing the guard removes only the
  `catch` around `foldIntoHalf` — the half's `addEntry` is canonical by Theorem 1, and
  the cascade structure is unchanged.
- **`currentDepth == 0` / intermediate-node** cases add no disc bit — they cannot
  straddle and were never guarded; unchanged.

This is the local invariant-preservation argument; the global height induction is
withdrawn (§5.2).

### §5.2 Theorem 3 — minimum-height argument withdrawn

The original argument assumed every insertion handler preserved a globally minimal
height and that bulk construction supplied the same minimum. The lost-outer-spine
regression invalidated that induction; `HOTSubInsertSpineTest` supplies counterexamples.
See [HOT Index Specification §2.6](HOT_INDEX_SPECIFICATION.md#26-partial-keys-and-routing)
for the current contract and its limits.

The historical `StraddleCanonicityProbe` with the guard disabled observed unchanged
height after each `addEntry` fold and height 2 for both incremental and bulk builds
of its 20 K-random key set. That sample does not prove the withdrawn general claim.

### §5.3 Theorem 4 — height-equivalence argument withdrawn

This comparison with the straddle-driven rebuild depended on §5.2. The I5-route
argument in §3.2 remains separate from height or shape equivalence.

### §5.4 Theorem 5 — termination

Guard removed, `addEntry` is straight-line O(children); `integrate`'s cascade strictly
decreases spine depth; no path re-enters `rebuildSubtree` for a straddle. The self-heal
`rebuildSubtree` retains its existing height-escalation termination. ∎

**Empirical gate (guard disabled — `StraddleCanonicityProbe` + canary run):**
`HOTFormalVerificationTest` — `[comprehensive-rand-10K] violations=0`,
`[comprehensive-desc-10K] violations=0`, `[comprehensive-bimodal-5K+5K] violations=0`,
`[comprehensive-randrep-20K] N=17253 observedHeight=2 violations=0`,
`[comprehensive-bimodal-promoted-diag] N=55000 observedHeight=3 violations=0`,
`[retrievability-desc-10K] missedValues=0`; `HOTVersionedLeafStressTest`
`[SLIDING_SNAPSHOT] rev=1..10 violations=0`. **Zero violations, zero missed values, zero
misroutes — with the guard fully disabled.**

---

## §6. Corner cases (exhaustive)

| # | Case | Handling |
|---|---|---|
| C1 | `λ` is already a disc bit of `N` (the **branch** path's `betaIsDiscBit`) | Cannot occur on the **merge** leaf-overflow path: `λ = msdb(L ∪ {K})` and `L` is `D(N)`-constant + `K` agrees with `L` on `D(N)` (merge bound) ⇒ `λ ∉ D(N)`. On the branch path it is handled by `tryBranchIncremental`/`addChildAtCombination` — out of scope (§10). `addEntry`'s `IllegalArgumentException` precondition (`:748`) stays. |
| C2 | `N` is full (32 children) | `integrate` cascades — `splitIndirect` + `foldIntoHalf` (its `addEntry` into the non-full half is canonical, Theorem 1). The cascade is unchanged; only the removed `catch` around `foldIntoHalf` differs. |
| C3 | A non-affected sibling straddles `λ` | The diagnosed case. `addEntry` encodes the sibling's `λ`-column 0 (off-path); I5-route holds (Theorem 1); routing correct (Lemma 1). **No rebuild — the incremental fold is canonical.** |
| C4 | The **affected** child `L` straddles `λ` | Expected — `L` is split *on* `λ` into `λ`-constant `L₀`/`L₁`. The guard exempted the affected run anyway. |
| C5 | A split half exceeds 512 keys / page bytes | `splitLeafPage` recurses it through `HOTBulkBuilder` → the BiNode child is a compound subtree; `addEntry` accepts BiNode children of any height; Theorem 1's I11 clause covers a compound replacement (`λ` more significant than its MSB — split-half postcondition). |
| C6 | MultiMask-layout node (disc bits span > 8 bytes) | `discriminativeBits` / `reencodeWithNewBit` use absolute MSB-first indices — layout-agnostic. Unchanged. |
| C7 | The `HOTStraddleException` catch arms in `mergeIntoLeaf` / `branchAboveLeaf` | Become dead — removed. The separate `catch (IllegalArgumentException | IllegalStateException)` self-heal arms **stay** (a different, genuine failure class). |
| C8 | Tests that were weakened to tolerate the guard | `HOTIndirectPageSplitFaithfulTest`'s `addEntry` tests were made "total-function tolerant" (accept a depth-tagged rejection — `HOT_ADDENTRY_STRADDLE_FIX.md` §8.7). With no rejection path they are **re-tightened** to assert a clean canonical fold (§7.8). |
| C9 | The per-insert deep-I5 diagnostic probe (`-Dhot.diag.perInsertValidate`) | If it ever checks **raw constancy** it false-positives on canonical off-path straddles. It must check **I5-route** — reuse `HOTMalformedSubtreeDetector` (which already does). §7.9. |
| C10 | Tombstones in the overflowed leaf | `splitLeafPage` carries tombstones; `addEntry` is structure-only and indifferent. Unchanged (`hot-tombstone-preservation`). |
| C11 | Multi-revision / DIFFERENTIAL · INCREMENTAL versioning | `addEntry` emits fresh CoW pages into the TIL as before; removing the straddle `rebuildSubtree` removes its orphaned-leaf churn (strictly *fewer* orphans). `HOTVersionedLeafStressTest` green with the guard disabled (§5.4). |
| C12 | Sibling page not swizzled (TIL-only / on-disk) | The guard needed sibling *leaf* pages resolved; with the guard gone `addEntry` reads only child *references* (re-encode) — no sibling page access. `ensurePathChildrenLoaded` **stays** (the `integrate` height accounting still needs path children resolved) with its javadoc updated to drop the guard rationale. |
| C13 | `betaIsDiscBit` + full node — the branch path's remaining `rebuildSubtree` | **Out of scope (§10).** Its diagnosis (`hot-betaisdiscbit-fullnode-blocked`) rests on the *same* raw-constancy I5 — once this plan lands, re-examine it against I5-route (a follow-up). |
| C14 | The genuine self-heal `rebuildSubtree` (`IllegalArgumentException`/`IllegalStateException`) | Retained, unchanged. On a tree kept canonical by §5's induction it never fires; pure defense. |
| C15 | `subtreeMsdb` / `subtreeStraddles` / `straddleScan` used elsewhere | They are private and (per the source) reachable only from `splitBitIsSafe`. Verify zero other callers, then delete with the guard. If a stray caller exists, it is itself raw-constancy-based and must be re-reviewed. |
| C16 | A real malformation slips through (regression in `splitLeafPage` makes a non-`λ`-constant half) | The guard never caught this (it exempts the affected run — §3.4). The optional `-Dhot.assert.canonical` debug check (§4) catches it in testing; production correctness rests on `splitLeafPage`'s own verification. |

---

## §7. Implementation steps

Each step ends compiling and test-green.

1. **`HOTIncrementalInsert.addEntry`** — delete the `splitBitIsSafe` call, the
   `HOTStraddleException` throw, and the `STRADDLE_GUARD_REJECTIONS` increment
   (`:752`–`:761`). The body keeps only the `λ ∉ D(N)` and not-full preconditions.
2. **`HOTIncrementalInsert`** — delete `splitBitIsSafe` (`:848`), `subtreeMsdb`
   (`:815`), `subtreeStraddles` (`:879`), `straddleScan` (`:888`), and the
   `STRADDLE_GUARD_REJECTIONS` field, after confirming no other caller (C15).
3. **`HOTIncrementalInsert.integrate`** — delete both `catch (HOTStraddleException)`
   arms (`:1318`, `:1338`) and `tagStraddle` (`:1351`). `integrate` now folds /
   cascades without a straddle branch.
4. **`HOTStraddleException.java`** — delete the file.
5. **`AbstractHOTIndexWriter.mergeIntoLeaf` / `branchAboveLeaf`** — delete the
   `catch (HOTStraddleException straddle)` arms. **Keep** the
   `catch (IllegalArgumentException | IllegalStateException)` self-heal arms.
6. **`ensurePathChildrenLoaded`** — keep; update its javadoc to drop the straddle-guard
   rationale (it remains required for `integrate`'s height accounting).
7. **(Optional) `-Dhot.assert.canonical`** — a debug-only post-fold
   `HOTMalformedSubtreeDetector` assertion, off by default, never on the production path.
8. **`HOTIndirectPageSplitFaithfulTest`** — re-tighten the `addEntry` tests from
   "total-function tolerant" back to asserting a clean canonical fold (no rejection path
   remains).
9. **Per-insert probe** — if a `-Dhot.diag.perInsertValidate` hook is (re-)added, it
   must use `HOTMalformedSubtreeDetector` (I5-route), never a raw-constancy check (C9).
10. **`docs/HOT_ADDENTRY_STRADDLE_FIX.md`** — prepend a "Superseded by
    `HOT_STRADDLE_GUARD_REMOVAL_PLAN.md`" note.

The change is a **net deletion** — guard, exception, two helper methods, four catch
arms — with no new code on the hot path.

---

## §8. Test plan

A fix is accepted only when 1–5 are green and 6 confirms the cost is gone.

1. **`HOTFormalVerificationTest`** — all canaries (ascending, descending, random-shuffle,
   bimodal, fully-random, random-with-replacement, conformance sweep, multi-rev):
   `violations=0`, `missedValues=0`. (Confirmed green with the guard disabled — §5.4.)
2. **`HOTVersionedLeafStressTest`** — full suite green; every revision canonical.
3. **`StraddleCanonicityProbe`** — keep as a permanent regression test (guard-agnostic,
   green both ways): canonical trees straddle off-path only; incremental height ==
   `HOTBulkBuilder` height; every straddling `addEntry` fold is `HOTMalformedSubtreeDetector`-clean
   and routes 100 %.
4. **`HOTIndirectPageSplitFaithfulTest`** — the re-tightened `addEntry` tests assert a
   clean canonical fold (cross-checked against `HOTBulkBuilder` for height + routing).
5. **`HOTMicrobenchmark.smallCombinedMicrobench`** — ascending; `full-scan
   misses=0/500000`, `violations=0` (a non-regression check — ascending never fired the
   guard, so throughput is unchanged).
6. **New random-workload microbench** — the ascending microbench structurally cannot see
   the win. Add a *random* ~500 K single-transaction CAS microbench; assert (a) `full-scan
   misses=0`, (b) write CPU is no longer dominated by `rebuildSubtree` (profile or a
   `rebuildSubtree`-call counter ≈ 0 for straddles), (c) per-op write latency drops
   relative to the guard-enabled baseline.

---

## §9. Review passes

**Pass 1 (the core claim).** The guard preserves I5-raw; I5-raw is not the routing
invariant. — *Established:* §2 separates I5-raw from I5-route; `HOT_FORMAL_FOUNDATION.md`
§2 + Theorem 2 and both validators use I5-route; §3.1 Lemma 1 + the probe (1240 canonical
straddles, 0 misroutes) show the canonical HOT violates I5-raw.

**Pass 2 (does `addEntry` really not need the guard?).** — *Confirmed:* Theorem 1 —
`addEntry` sets every sibling's `λ`-column 0, so I5-route holds for siblings
unconditionally; the affected replacements are `λ`-constant by `splitLeafPage`.
I3/I4/I7/I8/I11 were never guard-dependent.

**Pass 3 (could the guard ever catch a real bug?).** — *Confirmed not:* §3.4 — an
on-path sibling straddle of `λ` is structurally impossible (`addEntry` makes `λ`
off-path for every sibling); a malformed affected run is exempt from the guard anyway.
Probe step 4: 76/76 folds canonical with the guard off.

**Pass 4 (the two constraints).** The minimum-height and no-commit-pass
conclusions are superseded; see §5.2.

**Pass 5 (what still uses `rebuildSubtree`?).** — *Checked:* the
`IllegalArgumentException | IllegalStateException` self-heal, `rebuildWholeIndex`, and
height-escalation. Only the straddle caller is removed; `rebuildSubtree` stays. On a
canonical tree the self-heal never fires.

**Pass 6 (regression surface).** The equivalence conclusion depended on
withdrawn Theorem 4 (§5.3). The optional debug assertion (§4) was proposed to detect
malformed inputs during testing.

**Pass 7 (leaf-packing vs Lemma 2).** Leaf boundaries are fill-history-dependent;
the accompanying height-equivalence conclusion is withdrawn (§5.2).

**Pass 8 (test honesty).** — *Caught:* `HOTIndirectPageSplitFaithfulTest`'s `addEntry`
tests were *weakened* to tolerate the guard. They must be re-tightened (§7.8), else the
suite would still pass even if `addEntry` regressed. The per-insert probe must use
I5-route (§7.9 / C9) or it false-positives on canonical straddles.

**Pass 9 (scope discipline).** — *Confirmed:* `betaIsDiscBit`-full (C13) and the
`IllegalArgumentException` self-heal are out of scope and untouched; this plan changes
exactly one thing — the leaf-overflow `addEntry` straddle response.

**Pass 10 (minimum height).** The review distinguished minimum height from
SMHP grouping but still relied on the invalid induction in §5.2. Its conclusion
that no height-related holes remained is withdrawn.

---

## §10. Scope boundary

**In scope.** Removing the `addEntry` straddle guard (`splitBitIsSafe` /
`subtreeStraddles` / `HOTStraddleException` / the `integrate` + driver catch arms) so the
leaf-overflow fold runs as an incremental O(children) operation.

**Out of scope — `betaIsDiscBit` + full node.** `tryBranchIncremental` still falls back
to `rebuildSubtree` there (`hot-betaisdiscbit-fullnode-blocked`). Its diagnosis rests on
the same raw-constancy I5; once this plan lands, re-examine it against I5-route — a
follow-up.

**Out of scope — the `IllegalArgumentException | IllegalStateException` self-heal.**
Retained as defense for a genuine structural inconsistency.

**Out of scope — routing-encoding rework.** Not needed: §3 shows `addEntry`'s existing
sparse-path encoding is already canonical.
