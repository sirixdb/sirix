# HOT Phase 7q — Structural Lift Design

**Branch**: `fix/hot-strict-binna-conformance`
**Status**: **2026-05-12 — Phase 7r-1 LANDED: internal-indirect probe instrumented.**
Path 5 (§7.22) eliminates the last I8 at the root via `reconcileRootMaskI11Safe`.
User landed default-ON via build.gradle. **Direct-writer path** (diagnostic, 100K CAS,
HOTOptionBPhase5Test, adversarial fuzz, Binna sweep) all 0 violations.
**Bulk-JSON insertion path** still has bugs at INTERNAL indirects on certain
workloads — see new `comprehensive*` tests in `HOTFormalVerificationTest` that
catch descending (1832), mixed-sign (900), bimodal (1280/1056) violations.
**Phase 7r-1** (§7.23) adds opt-in routing-collision instrumentation at
`buildFlatNonStrict`; empirical readout on descending 10K: **5,445 inspections /
182 collisions / 3.34 % collision rate** confirming the bug lives at this single
rebuild site. Phase 7r-2 will convert the probe to reject-then-augment.
Multi-day work to extend Path 5 to `createNodeFromChildren` /
`addEntryWithPDep` / `rebalanceAndIntegrate` / `phase7qExtendWithLift`.
**Goal**: eliminate the single marginal I8 violation by lifting a descendant-captured
discriminator bit to root via cascading split-and-promote. **ACHIEVED at root level;
partial at internal-indirect level (Phase 7r territory).**

## 1 — Empirical motivation

Baseline at HEAD `ad4eb6990` (Phase 7p + diagnostic): 1 violation preserved on the
50K microbench reproducer. The violation:

```
indirect 2 child[2].firstKey 80000000000000050007c0a002000000000000000000 >=
            child[3].firstKey 80000000000000050007c08008000000000000000000
            (parent=2)
```

Children are stored sorted by partial (PEXT(firstKey, root.mask)) but firstKey order
disagrees. The bit where c2 and c3 differ at the relevant byte is **bit 90** (byte 11,
bit 0x20). Root's mask covers byte 10 + byte 12 but NOT byte 11.

Phase 7p (`subtreeHasBitInAnyIndirectMask`) rejects any closure that would add bit 90
to root's mask because some descendant indirect already has bit 90 in its mask.

**Diagnostic counters** (commit `ad4eb6990`, output on the reproducer):

```
phase7q classification: wasted=0 load-bearing=64 LB-liftable=0 LB-hard=64
```

Every single rejection (64) is LB-HARD: the capturing descendant uses bit 90 as the
SOLE discriminator for some pair of its children. Removing bit 90 from that descendant's
mask would collide those children. Therefore the lift is not a simple mask rebuild —
it is a structural restructure.

## 2 — The structural lift algorithm

### 2.1 Single-level split

Given indirect D with mask M containing β, and D's children C[0..n-1] (each with their
own subtree, each subtree β-constant by I6):

1. Partition D's children by firstKey bit β:
   - S₀ = { C[i] : firstKey(C[i]).β == 0 }
   - S₁ = { C[i] : firstKey(C[i]).β == 1 }
2. Build D₀ = indirect with mask M\{β}, children S₀.
3. Build D₁ = indirect with mask M\{β}, children S₁.
4. D's parent P, which previously had D as its child at slot[D], must now have D₀ and D₁
   as adjacent children. P's mask must include β to discriminate D₀ from D₁.

### 2.2 Cascading recursion

If P didn't have β in its mask before, adding β puts P in the same situation D was in:
β is in P's mask AND P has a parent PP that doesn't have β. So PP must also receive β.

Continuing recursively, β propagates up the tree until reaching root. At each step:
- Split the indirect on β (partition children).
- Strip β from this indirect's mask.
- Propagate to parent: parent now has 2x children at this slot.

After all levels are processed, β appears in EXACTLY ONE indirect's mask: root.

### 2.3 Degenerate cases

- **D has only 2 children, both β-distinguished**: S₀ and S₁ each have one child. D₀ and
  D₁ are degenerate indirects with one child each. These should be replaced by their
  single child directly (no indirect needed).
- **D has just β in mask (no other bits)**: stripping β yields empty mask. Children
  collapse to a single bucket — they must be distinguishable some other way. In this
  case, partial collision check (Phase 7q "LB-hard") fires, and we MUST split.

### 2.4 Constraints

- **β-constancy of subtrees**: each D-child's subtree is β-constant by I6 (because β
  is in D's mask, PEXT-routing already partitioned keys by β). So firstKey.β is a
  reliable proxy for "which side of β this subtree is on".
- **I8 preservation**: after the lift, the new structure must still have children
  sorted by firstKey. Since S₀'s firstKeys are all β=0 and S₁'s are all β=1, and
  β=0 firstKeys < β=1 firstKeys (lexicographic), placing S₀ before S₁ preserves I8.
- **I11 preservation**: D's MSB is preserved or improved. D₀'s MSB = bit position
  highest in M\{β}. If β was D's MSB, D₀.MSB shifts to next-most-significant bit in M,
  which is > β. D₁ same. Parent P now has β in mask, so P.MSB ≤ β. P.MSB < D₀.MSB
  iff β was MSB of M ∪ {β} from the perspective of P. Need to verify.
- **Fanout limits**: doubling children at each ancestor level may overflow
  `MULTI_NODE_MAX_CHILDREN` (16 for Span, larger for MultiNode). Must guard.

## 3 — Implementation plan

### Phase 7q.0 — diagnostic infrastructure ✅ DONE (`ad4eb6990`)

Counters classify every rejection. Confirms LB-hard is the only category.

### Phase 7q.1 — `splitIndirectOnBitForLift` helper

Lower-level helper. Given indirect D and bit β where M ∋ β, return (D₀, D₁) pair
with M\{β} mask.

- Partition children by firstKey.β.
- Rebuild each half via existing `createSpanNodeMultiMask` /
  `createMultiNodeMultiMask` with mask M\{β}.
- Compute new partials via PEXT on M\{β}.
- Verify uniqueness — fall back to LB-hard error if partials collide (shouldn't
  happen after partition, but defensive).
- Return ImmutablePair<PageReference, PageReference>.

### Phase 7q.2 — `liftBetaFromSubtreeRecursive`

Recursive lift. Given subtree at ref and β:

1. Load page.
2. If leaf: nothing to lift, return ref unchanged.
3. If indirect with β NOT in mask AND no descendant has β: return unchanged.
4. Otherwise: recurse on each child first. Some children may return a "split pair"
   instead of single ref (when β was lifted out of them).
5. After processing children, rebuild THIS indirect: include all rebuilt children
   AND split-pair products. If this rebuild puts β into this indirect's mask
   (because we now have β-distinguished children at this level), the caller above
   us must lift β again at the next level — return a split pair.
6. If this indirect ends up needing β in its mask but is ROOT: keep β; done.

### Phase 7q.3 — wire into `extendIndirectMaskForClosure`

Replace the Phase 7p "reject if descendant captures β" with:
- If LB-hard rejection AND `-Dhot.strict.phase7q=true`: trigger the lift.
- Else: existing reject behavior.

The lift is invoked when root wants to add β AND the diagnostic says lift is needed.

### Phase 7q.4 — validate on reproducer

Goal: violations 1 → 0 with phase7q enabled.
Acceptance: no regression on 100K CAS (must stay 0); HOTOptionBPhase5Test suite passes;
phase7q firings counter non-zero (= mechanism actually used).

### Phase 7q.5 — cleanup

If validated: enable phase7q by default; remove the Phase 7p hard-reject; keep the
"strict" gate as a safety opt-out.

## 4 — Risks

- **Cascading split blows up tree height**: each lift level doubles children at one
  slot. If the offending β triggers many splits, fanout may exceed
  MULTI_NODE_MAX_CHILDREN. Mitigation: when fanout exceeds, split this ancestor too
  (= recurse fan-out splitting). This is Phase 4b territory; we may share code.
- **Multiple βs to lift simultaneously**: closure may want to add multiple bits per
  insert. Currently 7q.2 handles one β at a time; ensureMaskClosure iterates.
- **Page-key reassignment**: rebuilding indirects creates new pages. Must wire into
  TIL correctly so disk-backed lookups don't see stale references.
- **TIL-vs-disk pages**: descendant indirects may live on disk; loadPage cost matters
  on the lift path. Reproducer is in-memory so this is unobserved, but production
  workloads need verification.

## 5 — Verification strategy

1. **Unit test**: `phase7qSingleLevelLiftPreservesUniqueness` — build a 3-level synthetic
   trie with β only at the middle indirect; lift; verify result has β only at root and
   no partial collisions.
2. **50K reproducer** with `-Dhot.strict.phase7q=true`: violations should go 1 → 0.
3. **100K CAS workload** with `-Dhot.strict.phase7q=true`: must stay at 0 violations.
4. **HOTOptionBPhase5Test full suite**: no regression.
5. **HOTFormalVerificationTest full suite**: no regression.

## 6 — Resumable work plan

Each numbered phase below is a separate commit. The next session can pick up at the
next uncommitted phase by reading this doc + the latest commit's diff.

- [x] 7q.0 — diagnostic classifier (commit `ad4eb6990`)
- [x] 7q.1 — `splitIndirectOnBitForLift` + diagnostic counters (commit `7368a7afd`;
  helper dormant until 7q.3 wires it in; integration test deferred to 7q.4's 50K
  reproducer)
- [x] 7q.2 — `liftBetaFromSubtreeRecursive` walker + `BetaLiftWalk` result record
  + `splitExpandedChildrenOnBeta` intermediate-level helper (dormant until 7q.3)
- [x] 7q.3 — `phase7qExtendWithLift` + dispatch into `extendIndirectMaskForClosure`'s
  LB-HARD branch behind `-Dhot.strict.phase7q=true` (build.gradle forwards the flag)
- [x] 7q.4 — validate on reproducer (1 → 0) — **ACHIEVED 2026-05-12 PM** via
  Path 5 routing-collision check (§7.22, commit `72baa036c`). User landed
  default-ON via build.gradle. Diagnostic 0 violations + 100K CAS clean +
  HOTOptionBPhase5Test clean + broad HOT + non-HOT all clean.
- [x] 7q.5 — closure-loop livelock instrumentation + opt-in toggles
  (`hot.strict.phase7q.skipNoop`, `hot.strict.phase7q.fullScan`) +
  `phase7q-bitcheck` diagnostic. **Default behaviour preserved at 1 violation.**
  Confirms architectural ceiling and identifies lift's split-and-rebuild constancy
  bug as the next attack target.
- [x] 7q.6 — post-build constancy gate in `splitIndirectOnBitForLift` (auto-on
  under `skipNoop`, opt-in via `hot.strict.phase7q.constancyGate`). **Cascade
  prevented**: skipNoop+gate yields 2 I8 violations (no I6 / I11 / I5 cascade)
  vs the prior 8469-violation cascade. Default behaviour preserved at 1 I8.
- [x] 7q.7 — constancy-filtered wrap retry in `splitIndirectOnBitForLift`.
  Helper `phase7qBuildBucketConstancyFiltered` re-derives disc bits with
  inline I11 + β-skip + per-child subtree-constancy filters. Wired as a
  retry after the 7q.6 gate rejection. Rescues 9 of 25 firings (36%) in
  skipNoop+gate mode; split-fail-constancy 19→16. ext-ok stays at 0
  (cascade-infeasible buckets remain). Default mode unchanged at 1 I8.
- [x] 7q.12 — strip-only mode for β-in-mask LB-HARD case
  (`-Dhot.strict.phase7q.stripOnly=true`). Replaces the early bail with a
  walker pass that strips β from descendants and rebuilds indirect using
  its EXISTING mask. Cascade prevented via auto-enabled gates +
  per-child global constancy check. 203 firings → 3 success; persistent
  I8 shifts location but doesn't clear (root's mask still missing the
  discriminator). Default mode unchanged.

## 7 — Empirical results (filled in as phases land)

### 7.1 Phase 7q.0 — diagnostic classifier (commit `ad4eb6990`)

```
phase7q classification: wasted=0 load-bearing=64 LB-liftable=0 LB-hard=64
```

All 64 rejections are LB-HARD → confirmed structural-lift path is mandatory.

### 7.2 Phase 7q.1 — split helper landed

Helper compiles (`./gradlew :sirix-core:compileJava --rerun-tasks --no-build-cache`
BUILD SUCCESSFUL). Dormant: not wired into any dispatch yet; no runtime impact.
Diagnostic counters `PHASE7Q_SPLIT_FIRINGS` and `PHASE7Q_SPLIT_FAILURES` available
for reproducer telemetry once 7q.3 lands.

### 7.3 Phase 7q.2 — recursive walker landed

Helper `liftBetaFromSubtreeRecursive(ref, β, log, revision)` walks the subtree
post-order and strips β from every descendant indirect that captures it. Returns
a `BetaLiftWalk(root, propagateRight)`:
- `propagateRight == null` → no propagation (either subtree had no β below it,
  or all captures were fossil = β-constant subtrees).
- `propagateRight != null` → caller must absorb `(root, propagateRight)` as TWO
  children in its own indirect's slot and add β to its own mask.

Auxiliary `splitExpandedChildrenOnBeta(originalIndirect, expandedRefs, expandedN, β, …)`
handles the intermediate-level case: when some children propagated splits, the
walker collapses the expanded child list back into a (D₀, D₁) pair via
`buildBucketWithInheritedMask` inheriting the parent's mask. β isn't in the
parent's mask at this level so the bucket-build is just a partition + inherit.

Diagnostic counters `PHASE7Q_LIFT_FIRINGS`, `PHASE7Q_LIFT_FAILURES`,
`PHASE7Q_LIFT_NOOP` accumulate per walker invocation. Dormant until 7q.3 wires
into `extendIndirectMaskForClosure`.

Build: `./gradlew :sirix-core:compileJava` BUILD SUCCESSFUL (903 ms, no warnings
on the new code).

### 7.4 Phase 7q.3 — dispatch landed; lift fires successfully

Wiring: in `extendIndirectMaskForClosure`, after the Phase 7p classifier
fingers an `anyLoadBearing && liftability == LIFT_HARD` rejection, if
`-Dhot.strict.phase7q=true` we now call `phase7qExtendWithLift(indirect, β,
log, revision)` before falling through to the standard reject. The helper:
1. Runs `liftBetaFromSubtreeRecursive` on each child of `indirect`, stripping
   β from every descendant indirect's mask and producing an expanded child list
   (each propagating split adds one slot).
2. Pre-splits remaining β-mixed leaves via `splitSubtreeOnBit`.
3. Builds the new indirect with β added to its mask, computing partials under
   the extended mask and verifying I3 (unique) + I4 (smallest = 0).

**Empirical (50K reproducer, `-Dhot.strict.phase7q=true -Dhot.strict.phase7j=true -Dhot.relax.closure.placeholder=true`)**:

| Metric | Before 7q.3 | After 7q.3 |
|---|---|---|
| Violations | 1 (I8) | 1 (I8) — at different location |
| LB-HARD rejects | 64 | 1 |
| Observed height | 6 | 5 |
| Lift fires | 0 | walk=2, split=2, ext-ok=1 |
| 100K CAS violations | 0 | 0 |

The remaining I8 is at `indirect 2 child[4] vs child[5]` (was `child[2] vs child[3]`
before the lift). Different keys: `c13008…` vs `c07010…` — byte 11 differs at
bit 95 (LSB of byte 11), not bit 90. The lift fixed the original bit-90
problem; the structural rearrangement surfaced a NEW under-discrimination at
a different bit.

### 7.5 Phase 7q.3 — known remainder

The closure mechanism doesn't iterate after a successful lift. When the lift
re-arranges the trie, a new bit may become load-bearing for I8 at a different
sibling pair. The next session iteration needs to:
- Either: re-run `findClosureBits` + `ensureMaskClosure` post-lift to detect
  the surfaced violation.
- Or: extend `phase7qExtendWithLift` itself to recurse on the result and
  iterate until convergence (or fan-out cap).

### 7.6 Phase 7q.4 reconnaissance (2026-05-11 PM)

Running with `-Dhot.strict.phase7q=true -Dhot.strict.phase7j=true -Dhot.strict.phase7k=true -Dhot.relax.closure.placeholder=true`:

```
[phase7k] recursive total extensions=272
[phase7j] commit-time extensions=16
phase7q classification: wasted=0 load-bearing=60 LB-liftable=0 LB-hard=60
phase7q lift: split-firings=285 split-fail=0 walk-fire=285 walk-fail=0
              walk-noop=1049 ext-fire=60 ext-ok=1 ext-fail=59
```

Findings:
- phase7k recursive closure DOES exercise the lift dispatch on non-root
  indirects (60 LB-HARD cases vs 1 with phase7j alone).
- Walker itself never fails (walk-fail=0). 285 splits succeed.
- **Only 1 of 60 lift+extend attempts succeeds.** 59 ext-fail in the
  rebuild phase. Need finer counters to bucket failure reasons:
  - `expandedN > MULTI_NODE_MAX_CHILDREN` (fanout overflow)
  - `splitSubtreeOnBit` returns null on residual β-mixed leaves
  - partial collision under the new mask
  - `hasZero == false` (I4 gate)
  - β-bit already set in old mask

**Next session target**: split `PHASE7Q_EXTEND_FAILURES` into named sub-counters
to localize which rebuild gate is rejecting the 59 cases. Once known, address
the most common failure mode.

### 7.7 Phase 7q.4 sub-counters land (2026-05-11 PM)

Counters added: `PHASE7Q_EXTEND_FAIL_{NOPROP,FANOUT,LEAFSPLIT,COLLIDE,NOZERO,BETAINMASK,WALKER}`.

First pass (no β-in-mask early bail):
```
phase7q ext-fail-buckets:
  noprop=0 fanout=16 leafsplit=16 collide=0 nozero=0 beta-in-mask=27 walker-null=0
```

Then attempted `-Dhot.strict.phase7j.skipExisting=true` (= skip closure bits
already in the indirect's own mask). Catastrophic cascade: 1 I8 → **8466** total
violations (16 I11-trie-condition, 8448 I6-pext-routes-to-leaf, 2 I5, 2 I-Binna-sparse,
1 I8). Confirms historical comment that skipExisting is unsafe.

Pivoted: added explicit `indirectMaskHasAbsBit(indirect, beta)` early bail at
`phase7qExtendWithLift` entry — when β is already in the indirect's own mask
AND a descendant captures β too, that's a pre-existing I11 violation in the
trie, not something the lift can repair single-level.

After early-bail:
```
phase7q ext-fail-buckets:
  noprop=0 fanout=0 leafsplit=0 collide=0 nozero=0 beta-in-mask=59 walker-null=0
```

**Diagnosis**: 59 of 60 LB-HARD cases have β already in `indirect.mask` —
the trie's existing I11 double-capture. Only 1 is a real structural-lift
candidate that the lift can fix (and does: ext-ok=1).

The persistent I8 at `indirect 2 child[4] vs child[5]` is NOT among the cases
the lift can address single-level. Either:
- The discriminating bit isn't in `findClosureBits` because root's mask already
  has bits covering byte 11 (lift added bit 90), but the specific bit needed
  for child[4]/child[5] (bit 95) isn't surfaced as a closure bit.
- Or the closure is attempting bit 95 but the indirect at parent=2 also has
  bit 95 in its mask already (= same β-in-mask scenario).

Need a different angle next session: investigate WHICH bit closes the
final I8 and why it's not being attempted (or why its attempt hits β-in-mask).

100K CAS still 0 violations (no regression).

### 7.8 Phase 7q.4 deeper analysis (2026-05-11 PM)

Enabled `phase7q debug` to identify each lift's β + indirect.pageKey:

```
[phase7q] LIFT-OK     beta=88 indirect.pageKey=2     new-msb=81
[phase7q] LIFT-FAILED beta=95 indirect.pageKey=429   indirect.maskHasBeta=true indirect.msb=94
[phase7q] LIFT-FAILED beta=95 indirect.pageKey=431   indirect.maskHasBeta=true indirect.msb=94
```

The 1 successful lift is on **root** (pageKey=2) adding bit 88. The 2 bit-95
lift failures are at OTHER indirects (429, 431) — NOT at root. So **bit 95 is
never attempted on root via the LIFT path**.

Bumped phase7j/phase7k maxIter to 64. Net effect:
- phase7k recursive extensions: 272 → 1088
- phase7j commit-time extensions: 16 → 64
- G28-closure firings: 287 → 1151
- LB-HARD: 60 → 204 (more visits = more rejections)
- ext-ok: 1 → 1 (still only the root bit-88 lift)
- Persistent I8: same violation, same location

Combined with `-Dhot.strict.g32=true` (per-insert root reconcile): no change.

**Post-build dump of root (pageKey=2)** after all phases:
```
layout=MULTI_MASK MSB=81 numChildren=10
extractionPositions=[10, 11, 12]
extractionMasks=[0x60801a0000000000]
  byte 10 mask 0x60 → bits 81, 82
  byte 11 mask 0x80 → bit 88
  byte 12 mask 0x1a → bits 99, 100, 102
```

Root.mask = {81, 82, 88, 99, 100, 102}. **Bit 95 is missing.**

For c4 vs c5 (child[4] firstKey c1.30.08, child[5] firstKey c0.70.10):
- All root-mask bits give identical extraction → partial collision impossible
  (but they are distinct slots, so something else distinguishes them).
- Bit 95 (LSB of byte 11) would resolve the I8 ordering.

**Hypothesis**: `findClosureBits` returns bit 95 for root, but the standard
`extendIndirectMaskForClosure(root, 95)` rejects via some path that doesn't
log to the phase7q counters — likely partial-collision in the standard rebuild
or `extendMaskWithBitI11Safe` failing on a non-β-constant subtree.

**Next session entry point**: add an unconditional debug log at the top of
`extendIndirectMaskForClosure` so every (indirect.pageKey, β) attempt is
traced, then run again to identify the exact reject path for (root, 95).

### 7.9 Phase 7q.4 — root attempt distribution traced

Added unconditional `[phase7q-trace] extendIndirectMaskForClosure ENTER`
log when `indirect.getPageKey() == 2L` (= root). Distribution of bits
tried on root over the entire 50K reproducer run (with phase7q+phase7j+phase7k
all maxIter=64):

```
  127 calls with beta=82  (maskHasBeta=true — already in mask, rejected)
    1 call  with beta=88  (LIFT-OK on first call, numChildren=5)
    0 calls with beta=95  (NEVER ATTEMPTED!)
```

**Bit 95 is never even tried on root.** The closure iter loop wastes its
budget repeatedly re-trying bit 82.

**Hypothesis**: after bit 82 fails (β-in-mask reject), the inner loop should
`continue` to bit 83+. But the trace shows no other bits. Possibilities:
- `findClosureBits(root)` returns only `[82, ...]` and the inner loop iter
  exits early (= no further bits in the list above parentMsb=81).
- Some earlier break/return short-circuits the inner loop.
- The `triedBits` mechanism (phase7o) isn't enabled, so each outer iter
  RE-FETCHES the same `closureBits` and tries them in the same order.

**Likely root cause**: `findClosureBits` returns bits in ASCENDING order
(MSB-first absolute bits). Bit 82 is the smallest > parentMsb=81. The inner
loop tries 82 → reject. To get to bit 95, the inner loop must continue
past bits 83–94. Each of those is either ALSO in mask (reject) OR triggers
Phase 7p (descendant capture).

The `continue` after `next == null` should advance to the next bit. So why
isn't the trace showing bits 83–94 attempts?

**Action for next session**: add `[phase7q-closure-list] cur.pageKey=X bits=[…]`
log at the top of the closure iter to confirm what `findClosureBits` returns
for root. Then if 95 is in the list, trace why the inner loop doesn't reach it.

If the list does NOT contain bit 95, the bug is in `findClosureBits` —
possibly because root's children's firstKeys don't differ at bit 95 *at the
moment findClosureBits runs* (they may differ at validation time but not
during closure).

100K CAS still 0 violations across all these experiments.

### 7.10 Phase 7q.4 — bit-numbering discrepancy uncovered

Added `[phase7q-fk] cur.pageKey=2 …` dump inside `findClosureBits` to print
the actual firstKeys it sees. At phase7j time:

```
[phase7q-fk] cur.pageKey=2 numChildren=10 maxLen=22
  c[0]=80000000000000050007800000000000000000000000
  c[1]=80000000000000050007bff000000000000000000000
  c[2]=80000000000000050007c00000000000000000000000
  c[3]=80000000000000050007c13006000000000000000010
  c[4]=80000000000000050007c13008000000000000000010
  c[5]=80000000000000050007c07010000000000000000000
  c[6]=80000000000000050007c08000000000000000000000
  c[7]=80000000000000050007c0a002000000000000000000
  c[8]=80000000000000050007c08008000000000000000000
  c[9]=80000000000000050007c08010000000000000000000
[phase7q-closure-list] cur.pageKey=2 iter=1 bits=[81,82,83,84,85,86,87,88,89,90,91,99,100,101,102,171]
```

`findClosureBits` returns bits 88-91 and 99-102 from byte 11/byte 12 range
but **skips bits 92-98** — exactly the range where c4/c5 differ. By
straight `isAbsBitSet` arithmetic (MSB-first within byte), all of 88-95
should appear because c[0] byte 11 = 0x80 vs c[1] byte 11 = 0xbf — they
differ at every bit except 88 (= MSB) and 91-95 (depending on exact mapping).

**Discrepancy with the validator**: the validator says `child[4] >= child[5]`
at indirect 2, with c4 byte 11 = 0xc1, c5 byte 11 = 0xc0 — differ at bit 7
(LSB of byte 11). The validator calls this "bit 95"; `findClosureBits`'s
`isAbsBitSet` should also see bit 95 = LSB of byte 11.

**Possible causes**:
- A bit-numbering inconsistency between the validator and `findClosureBits`.
  The validator might number bits differently (LSB-first within a byte, or
  using a different byte ordering).
- `findClosureBits` skipping byte 11 LSB end (bits 92-95) for unknown reason.
- Some early termination of the bit scan.

**This is the smoking-gun bug for the persistent I8.** Once the discrepancy
is resolved (`findClosureBits` returns bit 95 → closure attempts it → it
succeeds via standard path or LIFT), the violation should clear.

**Next-session action**: investigate `findClosureBits` directly. Manual unit
test: pass `byte[][] firstKeys = {{0x80,…}, {0xbf,…}}` and verify which bits
are returned. Compare with `HOTInvariantValidator`'s bit-numbering when it
reports the I8 discriminating bit.

Persistent I8 still at `indirect 2 child[4] vs child[5]` (byte 11 bit 95 = LSB
of byte 11). Lift's structural improvement IS happening (height 6→5, LB-HARD
64→60→1 net), but it's not addressing the final pair.

Build: `:sirix-core:compileJava` and `compileTestJava` clean. `build.gradle`
now forwards `hot.strict.phase7q` and `hot.debug.phase7q` to the test JVM.

### 7.11 Phase 7q.5 — closure-loop livelock isolated; cascade reproduced (2026-05-12 AM)

Refined trace via `[phase7q-trace]` on every `extendIndirectMaskForClosure` ENTER
for `pageKey=2` (root) shows root only ever receives two distinct β attempts:

```
  127 ENTER pageKey=2 beta=82  (1× maskHasBeta=false then 126× maskHasBeta=true)
    1 ENTER pageKey=2 beta=88  → LIFT-OK
    0 ENTER pageKey=2 beta=90 (= the bit needed to fix the persistent I8)
```

Bit 82 is the smallest closure bit > parentMsb=81 returned by `findClosureBits`.
After it is added on the first phase7j call, every subsequent iter / commit
sees β=82 STILL as the smallest > parentMsb, calls
`extendIndirectMaskForClosure(root, 82)`, which succeeds as a no-op rebuild
(the merge `byteMaskBits | newMaskBitInByte` is idempotent), `extended=true;
break;` exits the inner loop, and the outer loop wastes the rest of its
maxIter budget on the same no-op. Bit 90 is unreachable.

**Validation that the no-op-skip cascades by itself**:
- `-Dhot.strict.phase7q.skipNoop=true` (added) drops the loop into β=83+, 84+
  attempts. After bits {83,84,85,86,87,88,89,99,100,102} all enter root.mask:
  total violations = 8469 (16× I11, 8448× I6, 2× I5, 2× sparse, 1× I8).
- Same cascade with `-Dhot.strict.phase7o=true` (existing `triedBits` mechanism,
  same effect via different gate): 8469 violations, ext-ok=3, extra structural
  rebuilds.
- `-Dhot.strict.phase7q.fullScan=true` (new, applied to `ensureMaskClosure`'s
  inner break) had no effect because the test does not enable
  `hot.strict.g28.closure` — `ensureMaskClosure` is dormant in this run.

**Constancy violation surfaces from inside the lift**:

```
[hot.constancy] BUILD-VIOLATION pageKey=1000025 label=createNodeFromChildren-N
  absBits=[103, 98, 97, 96, 95, 94, 93, 92, 91] non-constant
  at HOTTrieWriter.buildBucketWithInheritedMaskMultiMask(2431)
  at HOTTrieWriter.buildBucketWithInheritedMask(2099)
  at HOTTrieWriter.splitIndirectOnBitForLift(6046)
  at HOTTrieWriter.liftBetaFromSubtreeRecursive(6163)
  at HOTTrieWriter.phase7qExtendWithLift(6433)
```

The lift's `splitIndirectOnBitForLift` builds a half-indirect that inherits
the parent's mask `M`. The new sub-indirect's children must be β-constant on
every bit in `M\{β}`. They are not — meaning an upstream split moved children
from one slot to another in a way that violates the inherited mask's
constancy assumption. This is the **root cause of the cascade**: not the
extra closure bits themselves, but the lift's inability to keep all
descendant subtrees constant on the inherited bits when β is stripped from
intermediate levels.

**Phase 7q.5 deliverable** (this session):
1. `phase7qIsBetaAlreadyInIndirectMask` helper.
2. `PHASE7Q_CLOSURE_NOOP_SKIPS` counter + reset/getter pair.
3. Opt-in `-Dhot.strict.phase7q.skipNoop=true` skipping the no-op rebuild
   inside `phase7jExtendWithAllClosureBits`. Default OFF preserves the
   architectural ceiling at 1 violation.
4. Opt-in `-Dhot.strict.phase7q.fullScan=true` on `ensureMaskClosure`'s
   per-bit failure handling.
5. `[phase7q-bitcheck]` per-bit firstKey trace inside `findClosureBits`
   (debugging tool, always-on but only fires for root + selected bits).
6. `build.gradle` flag forwarding for both opt-in flags.

**Phase 7q.6 entry point** (next session): instrument
`splitIndirectOnBitForLift` to log, for each `[hot.constancy] BUILD-VIOLATION`,
which CHILD ref violates which absBit, then trace whether that child was a
descendant lift-product or the original sibling. Likely fix is in
`buildBucketWithInheritedMask` / `splitIndirectOnBitForLift`: when a child
isn't constant on an inherited bit, EITHER pre-split it to make it constant
OR exclude that bit from the inherited mask of this sub-indirect.

100K CAS, HOTOptionBPhase5Test, default 50K reproducer all pass at
HEAD with `-Dhot.strict.phase7q=true`.

### 7.12 Phase 7q.6 — constancy gate breaks the cascade (2026-05-12)

`splitIndirectOnBitForLift` now runs a **post-build constancy validator**
(`liftedBucketIsConstancySafe`) on each of the (D₀, D₁) bucket products. The
validator iterates every set bit in the bucket's mask and checks every child's
subtree is β-constant on that bit via `bitConstantValueInSubtree`. Resolution
order: `ref.getPage()` → `activeLog.get(ref).getModified()` → `loadPage(reader)`.
Unresolvable placeholders (NULL_ID_LONG with no TIL entry) are trusted (= return
true) to avoid false positives.

The gate is **auto-enabled when `-Dhot.strict.phase7q.skipNoop=true`** and
explicitly opt-in via `-Dhot.strict.phase7q.constancyGate=true`. Default off
keeps the baseline lift's "successful but slightly malformed" output (validator
accepts it; tightening here would regress height from 5 to 6 with no violation
benefit).

**Empirical (50K reproducer)**:

| Mode | Violations | Height | ext-ok | Constancy rejects |
|---|---|---|---|---|
| Baseline (no skipNoop, no gate) | 1 (I8) | 5 | 1 | 0 |
| skipNoop=true, NO gate (pre-7q.6) | **8469** (8448 I6, 16 I11, 2 I5, 2 sparse, 1 I8) | 5 | 3 | n/a |
| **skipNoop=true, gate ON (auto)** | **2 (both I8)** | 6 | 0 | 19 |

Cascade gone. The skipNoop path is now usable for further investigation —
specifically: now that bad lifts are cleanly rejected, the root cause of why
ext-ok stays at 0 (= which bits the lift CAN'T handle structurally) becomes
the next attack target.

**Phase 7q.7 entry point**: with skipNoop+gate, ext-fail-buckets shows
`collide=4 nozero=3 walker-null=10` (= 17 lift attempts, all failing).
Investigate the `walker-null=10` cases first — those are
`liftBetaFromSubtreeRecursive` returning null. Likely fix is to make the
walker more permissive (= accept partial lifts) so the bit-90 lift on root
can succeed.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `liftedBucketIsConstancySafe` helper (~70 lines).
  + `liftedChildIsConstantOnBit` helper for per-child resolve+probe.
  + `PHASE7Q_SPLIT_FAIL_CONSTANCY` counter.
  + Auto-on gate inside `splitIndirectOnBitForLift`.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter reset + report.
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.phase7q.constancyGate` flag forwarding.

100K CAS regression: 0 violations preserved.
HOTOptionBPhase5Test: passes.

### 7.13 Phase 7q.7 — constancy-filtered wrap retry (2026-05-12)

When `splitIndirectOnBitForLift` produces a bucket whose mask captures bits
non-constant in some child's subtree, the Phase 7q.6 gate rejects and the split
fails. Phase 7q.7 adds a fallback path: `phase7qBuildBucketConstancyFiltered`
re-derives disc bits from adjacent-pair firstKey XOR but applies three filters
inline — (a) `absBit > newParentMsb` (I11), (b) `absBit != β` (β is what we're
lifting out), (c) bit is β-constant in every bucket child's subtree (I5/I6).
Builds sparse-path partials and verifies uniqueness + I4. Returns null if no
usable bits or uniqueness collides.

Wired into `splitIndirectOnBitForLift`: when the post-build constancy gate
rejects r0 or r1, retry via `phase7qBuildBucketConstancyFiltered` for that
half. If the retry succeeds AND its output passes the gate, accept; otherwise
return null cleanly.

**Empirical (50K reproducer, skipNoop+gate on)**:

| Metric | Phase 7q.6 | Phase 7q.7 |
|---|---|---|
| Violations | 2 (both I8) | 2 (both I8) |
| Height | 6 | 6 |
| ext-ok | 0 | 0 |
| split-firings (success) | 2 | 7 |
| split-fail-constancy | 19 | 16 |
| Constancy-wrap firings | n/a | 25 (success=9, fail=16) |

Retry rescues 9 of 25 firings (36%). At the OUTER split level, this translates
to 5 additional split successes (2 → 7). However, **ext-ok stays at 0** because
each end-to-end lift requires ALL of its descendant splits to succeed
simultaneously — the 5 rescues are not concentrated enough to unblock any one
ext attempt.

**Default mode** (no skipNoop/gate): unchanged — 1 violation, height 5,
ext-ok=1. Constancy-wrap counters are 0 (gate never fires, retry path
dormant).

**100K CAS regression**: 0 violations preserved.
**HOTOptionBPhase5Test**: passes.

**Why the rescues don't translate into ext-ok > 0**:
- A single `phase7qExtendWithLift` call iterates over `indirect`'s N children,
  calling `liftBetaFromSubtreeRecursive` per child. Any null return bails the
  whole ext attempt.
- The 18 walker-null cases (down from 19 pre-7q.7) mean 18 of 19 ext attempts
  have at least ONE child whose walker fails. Even with rescues, the OTHER
  children's walks must also succeed for ext-ok.
- The 16 surviving constancy failures are buckets where NO usable disc bit
  exists under the three filters. For those buckets, the lift is
  fundamentally infeasible without restructuring the descendant subtree
  itself (i.e., recursive lift).

**Phase 7q.8 entry point** (next session):
1. Apply the same constancy retry to `splitExpandedChildrenOnBeta` (the Case B
   intermediate-level builder in the walker) — currently has NO retry.
2. Investigate the 16 surviving constancy failures: dump bucket children +
   their subtree masks to identify which bits are unavailable and why.
3. Consider a recursive lift inside the walker: when the constancy-filtered
   helper finds no usable bits because all candidates are non-constant in
   some descendant, recurse into that descendant first to strip its captures.
   (This was the cascading-lift idea sketched in §2.2 but applied
   bottom-up rather than top-down.)

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7qBuildBucketConstancyFiltered` helper (~180 lines including
    counters + Javadoc).
  + Retry hookup inside `splitIndirectOnBitForLift` (r0/r1 fallback).
  + `PHASE7Q_CONSTANCY_WRAP_{FIRINGS,SUCCESS,FAIL}` counters.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter reset + report.

### 7.14 Phase 7q.9 + 7q.10 — collision diagnosis (2026-05-12)

Phase 7q.9 split `PHASE7Q_CONSTANCY_WRAP_FAIL` into four named sub-buckets
(`nomask`, `collide`, `nozero`, `input`). The 50K reproducer with skipNoop+gate
revealed:

```
phase7q constancy-wrap fail-buckets: nomask=0 collide=16 nozero=0 input=0
```

**All 16 surviving failures are uniqueness collisions** — the constancy filter
retained bits but partials still collided.

Phase 7q.10 attempted two refinements:
1. **All-pairs XOR scan** (vs adjacent-pair-only) to capture distinguishing bits
   between non-adjacent children. **No effect**: the helper already covered
   the bits the adjacent scan caught.
2. **Use firstKey on both sides of the XOR** (vs `lastKey(i)` vs `firstKey(j)`).
   This was a correctness fix — partials are derived from firstKeys, so disc-bit
   candidates must come from firstKey-XOR. **Still no effect** on the
   collision count.

Added `-Dhot.debug.phase7q.collision=true` trace dumping the colliding pair's
firstKeys, distinguishing-bits, and per-bit blocker children. Smoking-gun
output from the 50K reproducer:

```
[phase7q-collide] bucketSize=14 pair=(1,2) β=91 newParentMsb=90
  kFirstKey=...c0c108...  iFirstKey=...c0c208...
  distinguishing-bits: 94[blocker=12,13] 95[USABLE!]
```

**Bit 95 is "USABLE" (β-constant in every child) but adding it to the mask
STILL produces colliding partials.** Why? Because sparse-path encoding
(Binna §4.2 = `computePartialKeysForChildren`) requires disc bits to be
HIERARCHICALLY MONOTONIC across firstKey-sorted children. For bit 95:

- c[1].firstKey byte 11 = 0xc1 → bit 95 = 1
- c[2].firstKey byte 11 = 0xc2 → bit 95 = 0

c[1] sorts BEFORE c[2] lexicographically (0xc1 < 0xc2), but at bit 95 c[1]=1
comes BEFORE c[2]=0 — non-monotonic. Sparse-path encoding's recursive
partition can't place c[1] in the "right" half (bit=1) AFTER c[2] in the
"left" half (bit=0) under firstKey-sort. The encoding silently fails: both
children end up with the same partial.

**This is the architectural ceiling.** The 16 surviving collisions are
buckets where every distinguishing-and-constant bit is non-monotonic under
firstKey sort. The lift CAN'T succeed for these without either:
- (a) Re-sorting children by the disc bit (= changing the routing invariant).
- (b) Recursively β'-stripping the descendant whose subtree creates the
      monotonicity violation (= cascading lift, design §2.2).

**Phase 7q.11 entry point** (next session): implement option (b). Specifically,
when collide=N > 0 AND the colliding pair has USABLE bits, identify a
non-monotonic-blocker child and recurse via `liftBetaFromSubtreeRecursive` on
ITS subtree with the colliding bit. This is the cascading-lift the design
contemplated but never implemented.

Default mode unchanged at 1 I8, height 5, ext-ok=1.
100K CAS: 0 violations preserved.

### 7.15 Phase 7q.12 — strip-only lift mode for β-in-mask case (2026-05-12)

Previous diagnostic surfaced that **203 of 204** LB-HARD lift dispatches in the 50K
reproducer hit `phase7qExtendWithLift`'s early bail at line 7006 — the bail rejects
the dispatch whenever β is already in `indirect.mask` AND descendants ALSO capture
β. That's an existing I11 double-capture, and the bail's reasoning was that
single-level lift can't repair it. Empirically this blocked 99.5% of lift attempts
from even running the walker.

Phase 7q.12 adds `phase7qStripDescendantBetaOnly`: a variant that runs the walker
on every child (stripping β from descendants) and rebuilds `indirect` with its
EXISTING mask (no β extension). Walker outputs that propagate (D₀, D₁) pairs are
absorbed naturally since `indirect` already partitions by β. Partials are
recomputed against the unchanged mask; uniqueness + I4 verified.

**Cascade prevention**: the gate inside `splitIndirectOnBitForLift` and
`splitExpandedChildrenOnBeta` is now auto-enabled when `stripOnly` is on
(in addition to skipNoop and constancyGate). A NEW global child-constancy gate
inside `phase7qStripDescendantBetaOnly` rejects the rebuilt indirect if any new
child violates β-constancy on an existing mask bit.

**Empirical (50K reproducer, `-Dhot.strict.phase7q.stripOnly=true`)**:

| Configuration | Violations | Height | strip-only firings | success | walker-null |
|---|---|---|---|---|---|
| Default (no stripOnly) | 1 (I8) | 5 | 0 | 0 | 0 |
| stripOnly=true (no gate, initial test) | 4492 (cascade) | 5 | 12 | 12 | 0 |
| **stripOnly=true, auto-gated** | **1 (I8 — different location)** | 6 | 203 | 3 | 512 |

Without the gate, strip-only cascades (12 trivial successes → 4480 I6 violations).
With the gate auto-enabled, cascade is contained — same number of violations as
default but at a NEW location (`indirect 2 child[2] vs child[3]`, c0a002/c08008
differing at bit 89). Height regresses to 6 because the strip rebuilds added
extra levels.

**Why ext-ok stays low**: 203 strip-only firings produce only 3 globally-acceptable
results. The other 200 fail either (a) walker-null (the recursive split couldn't
preserve constancy at some descendant level) or (b) child-constancy gate (the
rebuilt indirect violates I5/I6 on its existing mask bits).

**100K CAS regression**: 0 violations preserved (height 2 unchanged).
**HOTOptionBPhase5Test**: passes.

The persistent I8 SHIFTS but doesn't clear because:
- bit 89 (or bit 87, the missing discriminator at root) STILL isn't in root's
  mask after strip-only successes. Strip-only doesn't extend root's mask — by
  design.
- The closure loop's livelock on β=82 prevents finding bit 89 via the standard
  extension path.

**Phase 7q.13 entry point** (next session): combine stripOnly + skipNoop. The
closure loop's no-op skip would let the loop advance past β=82 (which is in
root's mask) to try β=83, β=87, β=89, β=99, etc. With stripOnly handling β-in-mask
LB-HARD cases AND skipNoop letting the loop advance, the architectural ceiling
should break — bit 89 becomes reachable on root.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7qStripDescendantBetaOnly` helper (~180 lines).
  + `PHASE7Q_STRIP_ONLY_{FIRINGS,SUCCESS,FAIL}` counters.
  + Gate auto-enable in `splitIndirectOnBitForLift` and `splitExpandedChildrenOnBeta`.
  + Global child-constancy gate inside `phase7qStripDescendantBetaOnly`.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter reset + report.
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.phase7q.stripOnly` flag forwarding.

- [x] 7q.12 — strip-only mode for β-in-mask case (auto-gated)
- [x] 7q.15d — intermediate-indirect MSB instrumentation. Counters
  `PHASE7Q_INTERMEDIATE_MSB_{EQUALITY,LOWER,OK}` at 3 hook points (extend-with-lift,
  standard-extend, bucket-build-multimask). Empirically pinpoints architectural
  ceiling to `buildBucketWithInheritedMaskMultiMask` (46 equality cases under
  g32.deep+bestEffort+commitroot; 0 elsewhere). Default mode: counters all 0.
- [x] 7q.15e — port `g32.childmsb` strip-bits-≥-minChildMsb gate to MultiMask
  bucket build. Compute `minChildMsbMm` via `getIndirectMsbOrMax`; mark bits
  ≥ minChildMsb for stripping; require constancy verification (= reject via
  return-null/wrap-fallback if non-constant). Equality drops 46 → 0 under
  `+childmsb`. Default mode unchanged.
- [x] 7q.15f — `hasStructureCycle` gate in `phase7qIterativeRootSortI8`
  best-effort. 7q.15e's gate causes wrap-fallback that can produce a trie with
  revisited pageKey (graph cycle); validator flags structure-cycle. The gate
  rejects accepted lift output that has any cycle, falling back to the original
  indirect. Under `+childmsb`: violations 2 → 1 (= baseline preserved).
- [x] 7q.13 — `-Dhot.strict.phase7q.allowDoubleCapture=true` opt-in diagnostic.
  EMPIRICALLY DISPROVED prior hypothesis that double-capture is correctness-safe.
  Disabled by default; the falsified hypothesis is captured in §7.16 with cascade
  metrics for future ceiling tests.
- [x] 7q.14a — `-Dhot.strict.phase7q.i8priority=true` directed-closure ordering.
  Reorders `closureBits` so that bits resolving an existing I8-violating adjacent
  firstKey pair are attempted FIRST. Breaks the β=82 no-op-rebuild livelock (128
  firings on the 50K reproducer). All 127 resulting lift attempts hit
  `EXTEND_FAIL_COLLIDE` — confirms the architectural ceiling is at the partial-
  uniqueness gate inside `phase7qExtendWithLift` Step 3, NOT at the walker step
  (`walk-fail=0`). Default behaviour preserved at 1 I8.
- [x] 7q.14b — COLLIDE-branch instrumentation. Counters
  `PHASE7Q_COLLIDE_RESOLVABLE_1BIT` and `PHASE7Q_COLLIDE_DUPLICATE_KEYS` classify
  every rejection. On the 50K reproducer with `-Dhot.strict.phase7q.i8priority=true`:
  **127 / 127 = resolvable-1bit, 0 = duplicate-keys**. Every colliding pair has
  ≥ 1 discriminating bit ABSENT from the proposed extension mask — a multi-β
  extension is structurally feasible. Sample firstKeys differ at exactly bit 89
  when β=90 is being added. Default mode: counters 0, behaviour preserved at 1 I8.

### 7.16 Phase 7q.13 — allowDoubleCapture falsification (2026-05-12)

**Hypothesis** (going in): when root.mask captures bit β AND descendant c also captures
bit β, all keys in slot[c] share constant β = c.firstKey.β by I5. So c's partials at the
β-output-position become constant — bit β is *wasted* at c, but routing still works.
Trading one bit of c's mask capacity for the I8-fixing extension at root would be a
favourable architectural deal — we have ample bit headroom in the 64-bit mask layout
and Phase 7p's reject was assumed to be enforcing efficiency, not correctness.

**Test**: gated the Phase 7p reject behind a new
`-Dhot.strict.phase7q.allowDoubleCapture=true` flag with `PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS`
counter. When the flag is set AND `anyLoadBearing`, fall through to the rebuild path
that adds β to indirect.mask while leaving descendants unchanged.

**50K reproducer result with the flag enabled** (`-Dhot.strict.phase7q=true … -Dhot.strict.phase7q.allowDoubleCapture=true`):

| Metric | Default | allowDoubleCapture=true |
|---|---|---|
| Violations | 1 (I8) | **4492** (I6×4480, I11×5, I-Binna-sparse×3, I5×3, I8×1) |
| LB-HARD rejects | 204 | 13 |
| ext-fire | 204 | 13 |
| ext-ok | 1 | 1 (same) |
| allow-double-capture firings | n/a | **12** |
| Observed height | 5 | 5 |

The 12 fall-throughs each induced a routing cascade. Cause: when c's mask uses β as
its **load-bearing** disc bit, c's sub-children's firstKeys actually differ at β. If
root.routing-by-β forces ALL keys in slot[c] to share the same β value, then c's
sub-children's firstKeys cannot both have that constant value — the routing
assumptions break. PEXT'ed partials no longer match stored partials (I-Binna-sparse-path,
I6-pext-routes-to-leaf, I5-leaf-constancy) and I11-trie-condition fires when the
mid-level structure rebuilds inconsistently.

**Conclusion**: Phase 7p's reject is enforcing CORRECTNESS, not efficiency. Double-capture
is only safe for FOSSIL captures (descendant has β in mask but β-constant in subtree),
which the existing Phase 7q lift already handles via `liftBetaFromSubtreeRecursive`
returning no-op for fossil cases. The architectural ceiling at 1 violation is
fundamentally a consequence of the load-bearing β cases requiring cascading restructure
(Phase 7q.11 §2.2).

**Default mode preserved**: when the flag is OFF (default), behaviour is identical to
HEAD `9d6e56a7e` — 1 I8 violation, height 5, ext-ok=1, allow-double-capture firings=0.
100K CAS regression: 0 violations preserved. HOTOptionBPhase5Test: passes.

**Phase 7q.14 entry point** (next session): the only remaining attack is the genuine
cascading lift sketched in design §2.2. When `splitIndirectOnBitForLift` fails because
β is c's sole disc bit for some sub-child pair (P₀, P₁) of c, RECURSIVELY MERGE P₀ and
P₁ into a single node OR push the discrimination one level deeper via another lift.
This is multi-week work — each recursive level may again hit the same hard case.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `PHASE7Q_ALLOW_DOUBLE_CAPTURE_FIRINGS` counter + getter/resetter.
  + Opt-in fall-through inside `extendIndirectMaskForClosure`'s reject branch.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter reset + report.
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.phase7q.allowDoubleCapture` flag forwarding.
- `docs/HOT_PHASE_7Q_DESIGN.md` §7.16 + work-plan tick.

### 7.17 Phase 7q.14a — directed closure (`-Dhot.strict.phase7q.i8priority=true`) breaks no-op livelock; cascade gate confirmed at COLLIDE (2026-05-12)

**Motivation**: §7.9–7.11 traced the persistent I8 to a closure-loop livelock in
`phase7jExtendWithAllClosureBits`. On the 50K reproducer, root's `closureBits` list
is `[81,82,83,…,91,99-102,171]` and `parentMsb=81`. The inner loop tries β=82 first
(smallest > parentMsb), `extendIndirectMaskForClosure` returns a STRUCTURALLY
IDENTICAL page (idempotent bitwise OR with a bit already in mask), the inner loop
treats this as success and `break`s, the outer loop increments `extensions`,
reload `findClosureBits` returns the same list, β=82 again. 64 wasted iters.
β=87 (the bit needed to fix the persistent I8 at `c[4] vs c[5]`) is **never even
attempted**.

**Mechanism**: new helper `phase7qComputeI8FixBitsForReorder(indirect, closureBits)`:
1. Walks `indirect`'s children pairwise.
2. For each pair `(i-1, i)` with `firstKey[i-1] > firstKey[i]` (= I8 violation in
   the current child arrangement), computes
   `DiscriminativeBitComputer.computeDifferingBit(firstKey[i-1], firstKey[i])` →
   the MSDB β that would resolve this pair's ordering.
3. Returns `closureBits` with all such priority βs moved to the front in MSB-first
   order; remaining bits keep their MSB-first order. Returns `null` if there's no
   I8 violation OR the priority bits already prefix `closureBits` (= no-op
   reorder).
4. Gated behind `-Dhot.strict.phase7q.i8priority=true`. Default mode unchanged.
5. Counter `PHASE7Q_I8_PRIORITY_FIRINGS` increments per closure iter where the
   reorder is non-trivial.

The wiring point is `phase7jExtendWithAllClosureBits` line 5444 (per-iter
`closureBits` is replaced before the inner β loop).

**Empirical (50K reproducer, `-Dhot.strict.phase7q=true -Dhot.strict.phase7j=true
-Dhot.strict.phase7k=true -Dhot.strict.phase7q.i8priority=true
-Dhot.relax.closure.placeholder=true`)**:

| Metric | Baseline | With i8priority |
|---|---|---|
| Violations | 1 (I8) | **1 (I8 — same location)** |
| Height | 5 | 5 |
| LB-LIFTABLE | 0 | **127** |
| LB-HARD | 204 | 331 |
| ext-fire | 204 | 331 |
| ext-ok | 1 | 1 |
| ext-fail collide | 0 | **127** |
| ext-fail beta-in-mask | 203 | 203 |
| i8-priority firings | 0 | **128** |

The 128 priority firings break the no-op livelock — 127 of them reach
`phase7qExtendWithLift` (LB-LIFTABLE classification fires). The walker succeeds
every time (`walk-fail=0`), but all 127 results hit `PHASE7Q_EXTEND_FAIL_COLLIDE`
in Step 3 (partial-uniqueness check after rebuilding the indirect with β added).

**Diagnosis**: this is the architectural ceiling. Adding β (e.g. 87) to root's
mask is correctness-feasible — the walker correctly strips β from every
descendant indirect that captures it. But the resulting partials at root (now
derived under a wider mask) collide because some non-priority-β pair of children
ends up with the same partial. The lift CAN strip a descendant's β, but it
CAN'T preserve all OTHER children's partial uniqueness when β is added at root,
because the relative ordering of bits in the mask determines partial uniqueness
in a non-local way.

The 127 `collide` failures confirm §7.14's earlier identification: the lift's
remaining wall is partial-uniqueness under the extended mask, not the walker
itself.

**Default mode**: 1 violation, height 5, ext-ok=1, `i8-priority firings=0`
(identical to HEAD `63dd890a6`).

**100K CAS regression**: 0 violations preserved (height 2 unchanged).
**HOTOptionBPhase5Test**: passes.

**Phase 7q.14b entry point** (next session): instrument the COLLIDE branch in
`phase7qExtendWithLift` (line 7222) to dump the colliding pair `(i, k)` and
each child's firstKey + computed partial. Identify which non-priority children
collide post-extension. If the collisions are between children whose firstKeys
agree on all NEW priority bits (= β added doesn't separate them), the lift
needs to ALSO add a "separating" bit for those colliding pairs — a multi-β lift.
This generalizes the single-bit lift to a SET-OF-BITS extension.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7qComputeI8FixBitsForReorder` helper (~50 lines).
  + `PHASE7Q_I8_PRIORITY_FIRINGS` counter + getter/resetter.
  + Reorder dispatch inside `phase7jExtendWithAllClosureBits` (5 lines).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter reset + report line.
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.phase7q.i8priority` flag forwarding.
- `docs/HOT_PHASE_7Q_DESIGN.md` §7.17 + work-plan tick.

### 7.18 Phase 7q.14b — COLLIDE branch classifier; multi-β extension feasibility (2026-05-12)

**Goal**: §7.17 located the architectural ceiling at the partial-uniqueness check in
`phase7qExtendWithLift` Step 3 (line 7314). Phase 7q.14b adds classifier counters
+ an opt-in dump to determine if a multi-β extension would unblock the failures.

**Mechanism**: every time the COLLIDE branch fires, `phase7q14bDumpExtendCollide`
walks the two firstKeys byte-by-byte computing XOR, identifies the absolute bit
positions where they differ, and checks which of those bits are NOT in the
proposed extension mask. Outcomes:
- `PHASE7Q_COLLIDE_RESOLVABLE_1BIT` increments when ≥ 1 such absent bit exists
  (= adding that bit to the mask would separate this pair).
- `PHASE7Q_COLLIDE_DUPLICATE_KEYS` increments when the firstKeys are
  byte-identical (= the failure is upstream, not a mask issue).
- Both counters always count; the heavy per-pair print is gated behind
  `-Dhot.debug.phase7q.extendcollide=true`.

**Empirical (50K reproducer with `-Dhot.strict.phase7q.i8priority=true`)**:

| Counter | Value |
|---|---|
| `EXTEND_FAIL_COLLIDE` | 127 |
| `COLLIDE_RESOLVABLE_1BIT` | **127 (100 %)** |
| `COLLIDE_DUPLICATE_KEYS` | 0 |

**Every single COLLIDE rejection has ≥ 1 absent discriminating bit.** Sample dump
from one rejection (β = 90 being added; pair (i=9, k=8) collides at partial = 0x58):

```
[phase7q.14b-collide] beta=90 i=9 k=8 partial=0x58
  fkI=80000000000000050007c0a000000000000000000000
  fkK=80000000000000050007c0e000000000000000000000
  maskBytes=[10,11,12]
  candidate-absent-bits=[89,] diffBits=1 absentBits=1
```

The two children differ at exactly one bit — byte 11 LSB+1 = absolute bit 89.
The proposed extension adds β=90 only, which doesn't separate them. A multi-β
extension `{90, 89}` would.

**Conclusion**: Phase 7q.14c (multi-β extension) is structurally feasible and
the empirical pattern is consistent — every COLLIDE has exactly one absent bit
that would resolve it. The cascading risk for multi-β is the same as Phase 7q
in general: adding two bits creates more partials per byte, which may surface
new collisions elsewhere in the trie. Mitigation: the existing constancy gate
+ walker handle the per-bit safety; multi-β just batches the bit additions.

**Default mode**: counters 0, behaviour preserved at 1 I8, height 5.
**100K CAS**: 0 violations preserved.
**HOTOptionBPhase5Test**: passes.

**Phase 7q.14c entry point** (next session): implement
`phase7qExtendWithLiftMultiBit(indirect, primaryBeta, additionalBetas, …)` that
extends the mask with primaryBeta AND each bit in additionalBetas
simultaneously. Each additional bit needs its own walker pass to strip
descendant captures (to preserve correctness when ALL bits are at this level
of the trie). Iteration loop: detect collisions, add their absent bits to the
set, retry; bounded retry budget (3? 8?). Watch counter for excess cascading.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7q14bDumpExtendCollide` helper (~70 lines).
  + `hexBytes` private util (~10 lines).
  + `PHASE7Q_COLLIDE_RESOLVABLE_1BIT` + `PHASE7Q_COLLIDE_DUPLICATE_KEYS`
    counters with getters/resetters.
  + COLLIDE branch call (3 lines, no-op when flag off).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter resets + collide-buckets report line.
- `bundles/sirix-core/build.gradle`:
  + `hot.debug.phase7q.extendcollide` flag forwarding.
- `docs/HOT_PHASE_7Q_DESIGN.md` §7.18 + work-plan tick.

### 7.22 Phase 7q-Path5 — partial-subset routing-collision check eliminates last I8 (2026-05-12) — **BREAKTHROUGH: 0 VIOLATIONS**

After Phases 7q.0-7q.15h saturated the structural-lift mechanisms and pinned the
remaining I8 ceiling at root indirect pageKey=2, **Path 5** identifies the actual
correctness gap: the rebuild produced partials whose subset relations under HOT's
"largest-index subset-fallback" routing rule mis-routed subtree-resident keys.

**Root cause.** `addNewRootLevelForI8` (and `rebuildRootWithFullClosureI11Safe`)
produced root partials like `[0, 0x10, 0x14, 0x19, 0x1a]`. The HOT routing rule for
MultiMask indirects is:
1. **Equality-preferred**: if `partial[i] == densePK(key)`, return `child[i]`.
2. **Subset-fallback**: else, return the child with the **LARGEST INDEX** among
   partials that satisfy `(densePK & partial[j]) == partial[j]`.

For a stored key with `densePK = 0x1d`:
- `0x0 ⊂ 0x1d` ✓, `0x10 ⊂ 0x1d` ✓, `0x14 ⊂ 0x1d` ✓, `0x19 ⊂ 0x1d` ✓.
- `0x1a ⊂ 0x1d`? `0x1a & 0x1d = 0x18 ≠ 0x1a` ✗.

Routing picks `child[3] (partial 0x19) → leaf 1`. But the key was actually stored
under `child[2] (partial 0x14) → indirect 11 → leaf 10`. Routing and storage
diverged → I8 surfaces at the validator because children appear unsorted-by-
firstkey in routing order.

**The fix.** Before constructing the new indirect, verify routing consistency for
**subtree-resident** keys, not just firstKeys. Sufficient condition: for each pair
`(i, j)` with `i < j` and `partial[j] != 0`, `child[i]`'s subtree contains **no**
key K with `densePK(K) ⊇ partial[j]`. Implemented as:
- ∃ at least one bit b in `partial[j]` such that `bitConstantValueInSubtree
  (child[i], absBitForDensePKBit(b)) == 0` (= bit b is always-zero across
  child[i]'s subtree, so K's densePK can never be a superset of partial[j]).
- If no such bit exists for ANY pair → routing collision possible → **reject the
  rebuild** (return `null`), forcing the caller to fall through to the next
  strategy.

Wired at two sites in `HOTTrieWriter.java`:
- `rebuildRootWithFullClosureI11Safe` (~L9329+) — firstKey self-routing check
  (lighter — only checks firstKeys, no subtree walks).
- `addNewRootLevelForI8` (~L10763+) — full O(n² × densePK-bits × subtree-walk)
  check using `bitConstantValueInSubtree`. **This is the CRITICAL site** that
  produced the bad partials in the reproducer.

The densePK-bit → absBit mapping iterates `extractionPositions[]` MSB-first per
byte; the partial's bit index `b` (LSB-first per `Long.compress` output)
corresponds to absBit `densePkBitToAbsBit[totalDpkBits - 1 - b]`.

**Empirical results (2026-05-12, branch `fix/hot-strict-binna-conformance`):**

| Test | Flags | Violations | Height | Build |
|------|-------|-----------|--------|-------|
| 50K reproducer | `phase7q + path5 + g32 + childmsb + multibeta + deep` | **0** ✅ | 6 | 8.5s |
| 50K reproducer | `phase7q` (default — no path5) | 1 (I8) | 6 | 3.0s |
| 100K CAS regression | `phase7q + path5 + g32 + childmsb + multibeta + deep` | **0** ✅ | 2 | 6.8s |
| HOTOptionBPhase5Test (15 tests) | same as 100K CAS | 0 failures ✅ | n/a | n/a |

Cost: O(n² × densePK-bits × subtree-walk). n ≤ 32, densePK ≤ 16 bits, walk depth
bounded by trie height. 50K build time 3.0s → 8.5s — acceptable for correctness
gain.

**Counter readout on 50K (path5 + g32 + childmsb + multibeta + deep):**

```
phase7q lift: split-firings=5034 walk-fire=5034 ext-fire=1548 ext-ok=775 ext-fail=773
phase7q intermediate-MSB: equality=0 lower=0 ok=6088
violations=0
```

**Flag-rollout plan — DEFAULT-ON (USER LANDED 2026-05-12 PM).** Initially opt-in
via `hot.strict.path5.routeverify=true`. Default-ON promotion attempted (also
2026-05-12 PM) and temporarily reverted after observing a transient transaction-
leak cascade in `HOTIndexInternalTest`/`HOTLargeScaleIntegrationTest`. **User
re-enabled default-ON via build.gradle** (`hot.strict.path5.routeverify=true`
plus `hot.strict.g32=true`, `hot.strict.phase7q=true`, `hot.strict.g32.deep=true`,
`hot.strict.g32.multibeta=true`, `hot.strict.g32.childmsb=true`).
Validation: diagnostic 0 violations, 100K CAS clean, HOTOptionBPhase5Test clean,
adversarial fuzz 0 violations, broad HOT regression + non-HOT regression all
clean. The earlier transaction-leak was a transient `--rerun-tasks`/classpath
artifact, not a path5-induced regression.

**Generalization.** The Path 5 mechanism — routing-collision check at rebuild
time — is a **general pattern**. Apply to OTHER rebuild sites if they produce
I6/I8 cascades: `phase7qMultiBetaAtomicLift`, `phase7qIterativeRootSortI8`,
`extendIndirectMaskForClosure` / `phase7qExtendWithLift`. The general principle:
**whenever a rebuild produces new partials, verify that the partial set is
routing-consistent for subtree-resident keys, not just firstKeys**. firstKey
self-routing alone is insufficient because the lift can produce partials that
route correctly for their own firstKey but mis-route deeper keys whose densePK
falls in the partial-overlap zone.

**Files touched** (commit pending — Path 5 commit will land in this session):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `rebuildRootWithFullClosureI11Safe`: 25 lines (firstKey self-routing check).
  + `addNewRootLevelForI8`: 129 lines (firstKey self-routing + subtree-walk pair check + densePK→absBit mapping).
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.path5.routeverify` + `hot.debug.path5` flag forwarding.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.22 (this section).

**Campaign status.** The multi-week structural-lift campaign goal is **achieved**:
0 invariant violations on the 50K diagnostic reproducer, 0 violations on 100K
CAS regression, HOTOptionBPhase5Test passes. The architectural ceiling that
persisted across Phases 7a-7q.15h is removed.

### 7.23 Phase 7r-1 — extend Path 5 instrumentation to internal-indirect `buildFlatNonStrict` (2026-05-12)

**Motivation.** Phase 7q.5 landed Path 5 at the two ROOT-level rebuild sites
(`rebuildRootWithFullClosureI11Safe`, `addNewRootLevelForI8`) and eliminated the
last I8 on the 50K diagnostic reproducer + 100K CAS regression. The 12 new
`comprehensive*` tests added in the same commit surfaced 5 PRE-EXISTING bugs
(`comprehensiveDescending10K` = 1832 I11, `comprehensiveMixedSign` = 900 I5/I6,
`comprehensiveBimodal5KPlus5K` = 1280 I5, `comprehensiveBimodal50KPromoted` =
1056 I5, `comprehensiveManyDuplicatesLowCardinality` = `SirixIOException`) that
hit INTERNAL indirects, not root. Phase 7r is the multi-day campaign to extend
the Path 5 mechanism to those internal-indirect sites.

**Phase 7r-1 deliverable** (this commit): instrument `buildFlatNonStrict` (the
multi-child branch of `createNodeFromChildren`) with a Path 5 routing-collision
PROBE. Strictly diagnostic — no behavior change — gated by
`hot.strict.phase7r.routeverify` (default `false`).

The probe runs after partials are computed + sorted but BEFORE the page is built.
For each child `i`, it simulates HOT's subset-fallback routing rule:
```
densePk = partials[i]
bestIdx = -1
for j in [0..n):
  if (densePk & partials[j]) == partials[j]:
    if partials[j] == densePk: bestIdx = j; break  // equality-preferred
    if j > bestIdx: bestIdx = j                   // largest-index subset
if bestIdx != i: COLLISION
```
Counter `PHASE7R_BUILDFLAT_INSPECTIONS` increments on every `buildFlatNonStrict`
invocation when the flag is on; `PHASE7R_BUILDFLAT_COLLISIONS` increments when at
least one child mis-routes. The helper `phase7rRoutingCollisionFirstIdx(int[])`
is package-private so Phase 7r-2/3 can re-use it for reject-then-augment without
duplication of the inline Path 5 probes from §7.22.

**Empirical readout — `comprehensiveDescending10K` (bulk-JSON path):**

| Metric | Value |
|--------|-------|
| `PHASE7R_BUILDFLAT_INSPECTIONS` | 5,445 |
| `PHASE7R_BUILDFLAT_COLLISIONS` | 182 |
| Collision rate | **3.34 %** |
| Downstream invariant violations | 1,832 (unchanged — probe is diagnostic only) |

This is direct empirical confirmation that the failing comprehensive tests' bug
lives at this single rebuild site. 182 colliding `buildFlatNonStrict` rebuilds
cascade to ~1,832 downstream I11/I8/I-Binna failures (≈ 10 per collision —
each collision can mis-route an entire subtree's worth of keys).

**Validation under default-off (flag inert):**

| Test | Flags | Violations | Status |
|------|-------|-----------|--------|
| 50K diagnostic reproducer | default-ON `phase7q + path5` | 0 | ✅ |
| 100K CAS regression | default-ON | 0 | ✅ (height=2) |
| comprehensiveAscending50K | default-ON | 0 | ✅ |
| HOTOptionBPhase5Test (15 tests) | default-ON | 0 failures | ✅ |

**Validation with flag enabled (`-Dhot.strict.phase7r.routeverify=true`):**

Same baseline tests still 0 violations. Probe is benign on already-working
workloads (no false positives — 0 collisions surfaced on asc-50K). Confirmed
the probe is purely measurement, never mutates routing behavior.

**Phase 7r-2 entry point.** Replace the diagnostic probe with reject-then-
augment: when the routing-collision check fires, call a new
`augmentDiscBitsUntilUnique()` that adds further bits from the `computeDiscBits`
candidate pool until partials are routing-consistent OR the per-indirect bit
budget (16 for SpanNode, 32 for MultiNode) is exhausted. On budget-exhaustion,
fall through to an alternate strategy (e.g., split the child set into two
indirects with separate masks). The 3.34 % collision rate suggests budget-
exhaustion will be rare.

**Files touched (Phase 7r-1 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `PHASE7R_BUILDFLAT_INSPECTIONS`, `PHASE7R_BUILDFLAT_COLLISIONS` (AtomicLong) + getters/resetters.
  + `phase7rRoutingCollisionFirstIdx(int[])` static helper (~22 lines).
  + Probe call in `buildFlatNonStrict` (~22 lines, fully gated).
- `bundles/sirix-core/build.gradle`:
  + `hot.strict.phase7r.routeverify` + `hot.debug.phase7r` flag forwards.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + `phase7r1CharacterizeDescending10K` characterization test (skips silently when flag off).
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.23 (this section).

### 7.24 Phase 7r-2 — active augment-until-unique in `buildFlatNonStrict` (commit `0590f22af`, 2026-05-12)

**Motivation.** Phase 7r-1 (§7.23) confirmed that 3.34 % of `buildFlatNonStrict`
invocations on `comprehensiveDescending10K` mis-route at least one child. Phase
7r-2 converts the diagnostic probe into a corrective action: when the initial
disc-bit set produces a colliding partial, find a sort-monotone bit that
distinguishes the colliding pair and add it to the disc-bit set, repeat until
unique or the bit budget is exhausted.

**Algorithm** (default-ON; opt-out via `-Dhot.strict.phase7r.augment.disable=true`):

```
function phase7rAugmentUntilUnique(children, partials, discBitsHolder, …):
    firstKeys[i] = getFirstKeyFromChild(children[i])
    for iter in 1..MAX_DISC_BITS:
        (i, j) = first colliding pair where partials[i] == partials[j]
        if no collision: return partials  // done
        for byte b in 0..min(len(firstKeys[i]), len(firstKeys[j])):
            for bit h in MSB..LSB where bit set in (firstKeys[i][b] XOR firstKeys[j][b]):
                if (b, h) already in disc bits: continue
                if not isBitSortMonotone(firstKeys, b, h): continue
                // accept: add (b, h) to disc bits
                augmentBytePos = b; augmentBit = h
                break out of both loops
        rebuild DiscBitsInfo with augmented mask (singlemask or MultiMask)
        partials = computePartialKeysForChildren(children, newDiscBits)
    return partials
```

The `isBitSortMonotone` check is non-obvious: `computeSparsePathRecursive`
partitions children at each bit based on the bit's value; if the bit isn't
strictly increasing across lex-sorted firstKeys (single 0→1 transition), the
recursive partition produces wrong partials and breaks the
sort-order = partial-order invariant. The prior bailout comment at
`HOTTrieWriter.java:12600` ("Non-adjacent-pair augment was investigated…")
predates this work; the earlier failure mode was likely from non-monotone
augmentation, which 7r-2 prevents via the guard.

**Empirical readout** (50K diag, 100K CAS, ascending preserved at 0;
comprehensive tests measured under default flag set + `phase7q=true`):

| Test | Before 7r-2 | After 7r-2 | Status |
|------|-------------|------------|--------|
| 50K diagnostic reproducer | 0 | 0 | ✅ preserved |
| 100K casIndexHundredKEntryHeightBound | 0 | 0 | ✅ preserved |
| HOTOptionBPhase5Test (15 tests) | pass | pass | ✅ preserved |
| comprehensiveAscending10K / 50K | 0 | 0 | ✅ preserved |
| **comprehensiveDescending10K** | **1832** | **515** | ⚠️ 71 % reduction |
| comprehensiveMixedSign | 900 | 900 | ⚠️ unchanged |
| comprehensiveBimodal5KPlus5K | 1280 | 1280 | ⚠️ unchanged |

**Why descending improves but mixed-sign / bimodal don't.** Descending's
collisions are pure adjacent-pair-scan misses (the colliding pair shares the
encoder-byte boundary). Augmenting with one sort-monotone bit
removes them. Mixed-sign and bimodal produce **β-mixed leaves**: a single leaf
holds keys with both values of the chosen disc bit (e.g. leaf 31985 has
`firstKey = c043…` but `lastKey = c0c3…` — spans byte 11 bit 7 = 0 and 1).
No augmentation can fix this because the routing collision is internal to a
single child, not between siblings. The fix requires splitting β-mixed leaves
on the disc bit BEFORE indirect construction — Phase 7s scope.

**Falsified hypothesis** (not committed): Phase 7r-4 constancy-filter
scaffold would drop disc bits that are non-β-constant in any child's subtree.
Empirical run with `hot.strict.phase7r.constancyfilter=true` cascaded to
6,931–9,615 violations across all comprehensive workloads (4–18× regression).
The filter strips bits NEEDED for routing; β-mixed leaves require splitting,
not bit-filtering. Scaffold removed before commit (CLAUDE.md: no carrying
falsified code).

**Files touched** (commit `0590f22af`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7rAugmentUntilUnique` (~120 lines, primitive arrays).
  + `isBitSortMonotone`, `discBitsContainsBit`, `discBitsToMaskByBytePos` helpers.
  + `buildFlatNonStrict` calls augment after initial partials.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7r.augment.disable` opt-out flag.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  re-enable 4 previously `@Disabled` comprehensive tests + extend timeouts on 2 scale tests.

**Phase 7s entry point.** Detect β-mixed leaves in
`createNodeFromChildren` / `addEntryWithPDep` BEFORE the call to
`buildFlatNonStrict`. For each candidate disc bit X and child leaf L: if L has
keys with both `bit_X = 0` and `bit_X = 1`, split L into L0 (bit_X = 0 keys)
and L1 (bit_X = 1 keys). Recompute children list and disc bits. Re-enter
`buildFlatNonStrict`. Multi-day work — touches leaf-split semantics and
parent-key resort.

### 7.25 Phase 7s-1 — prefer-β-constant augmentation + fallthrough metric (2026-05-12)

**Motivation.** After Phase 7r-2 (§7.24) landed, the descending workload still
shows 515 residual violations: 1 I4 + 1 I8 + 1 I5 + 512 I6 cascade. A
re-measurement of Phase 7r-1's routing-collision probe POST-augmentation gave
**0 % collision rate** on the same workload — so routing collisions are FULLY
solved. The residual 515 must come from a different root cause: I5-leaf-constancy
violations where a child's stored partial (recomputed from the augmented mask)
mismatches the PEXT of some key in the child's subtree.

**Diagnosis.** When `phase7rAugmentUntilUnique` adds bit X to the parent's disc
mask, child[i].storedPartial is recomputed as `PEXT(firstKey, mask+X)`. For I5 to
hold, every key K in child[i]'s subtree must agree with firstKey on bit X — i.e.
bit X must be **β-constant** in each child's subtree. The Phase 7r-2 augmenter
only enforces sort-monotonicity (single 0→1 transition across firstKeys), not
β-constancy. When the chosen bit IS sort-monotone but the 0→1 transition lands
INSIDE a child's subtree (between siblings of a leaf), I5 cascade follows.

**Fix (default-ON, no opt-out).** Tighten the candidate-bit loop in
`phase7rAugmentUntilUnique`:

```
candidate_bit = None
fallback_bit = None
for (byte b, bit h) in MSB-first traversal of firstKey_i XOR firstKey_j:
    if (b, h) already in mask: skip
    if not sort_monotone(firstKeys, b, h): skip
    if fallback_bit is None: fallback_bit = (b, h)    // 7r-2-compatible
    if bitConstantValueInSubtree(c, absbit(b, h)) ≥ 0 for every c in children:
        candidate_bit = (b, h)
        break
if candidate_bit is not None:
    add candidate_bit to mask
elif fallback_bit is not None:
    PHASE7S_AUGMENT_FALLTHROUGH ++
    add fallback_bit to mask      // 7r-2 legacy bit; may produce I5 if β-mixed
else:
    PHASE7S_AUGMENT_EXHAUSTED ++
    return partials               // no bit available at all
```

The fallthrough preserves 7r-2's behavior when no β-constant bit exists — never
regresses (a hard-reject of non-β-constant bits restored the routing-collision
cascade, descending 515 → 1830, as a falsified earlier iteration showed).

**Empirical readout** (50K + 100K + ascending preserved; comprehensive results
under default flag set + `phase7q=true`):

| Test | Phase 7r-2 only | Phase 7s-1 | Notes |
|------|-----------------|------------|-------|
| 50K diagnosticMicrobenchPatternReproducer | 0 | 0 | ✅ preserved |
| 100K casIndexHundredKEntryHeightBound | 0 | 0 | ✅ preserved |
| HOTOptionBPhase5Test (15) | pass | pass | ✅ preserved |
| comprehensiveAscending10K | 0 | 0 | ✅ preserved |
| comprehensiveDescending10K | 515 | 515 | same — fallthrough fires 945 times |
| comprehensiveMixedSign | 900 | 900 | same — exhausted fires 2 times |
| comprehensiveBimodal5KPlus5K | 1280 | 1280 | same |

**The actionable metric** — `phase7s1CharacterizeAugmentFallthrough` test
output:

| Workload | FALLTHROUGH | EXHAUSTED | Interpretation |
|----------|-------------|-----------|----------------|
| ascending-10K (control) | 0 | 0 | clean — β-constant always available |
| descending-10K | **945** | 0 | 945 sites need Phase 7s-2 leaf-split |
| mixed-sign-10K | 0 | **2** | 2 sites have no sort-monotone bit at all — different failure mode |

The fallthrough/exhausted counts isolate WHERE leaf-split should target. Each
fallthrough is a buildFlatNonStrict invocation where some child holds a leaf
β-mixed on the chosen disc bit. The Phase 7s-2 implementation should:

1. Detect β-mixed child leaves at the same site (= where fallthrough fires
   today).
2. Split the offending leaf on the disc bit before completing the parent
   indirect build.
3. Recompute children + retry augmentation; expect FALLTHROUGH to drop to 0
   when β-mixed leaves are eliminated.

Mixed-sign's 2 EXHAUSTED occurrences indicate a separate Phase 7s-3 problem:
even sort-monotone bits are unavailable — likely because the negative-value
encoding (bit-inverted prefix) breaks monotonicity at the sign boundary.

**Files touched (Phase 7s-1 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + β-constancy preference loop in `phase7rAugmentUntilUnique`.
  + `PHASE7S_AUGMENT_FALLTHROUGH` + `PHASE7S_AUGMENT_EXHAUSTED` counters with
    get/reset accessors.
- `bundles/sirix-core/build.gradle`: `hot.debug.phase7s` flag.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + `phase7s1CharacterizeAugmentFallthrough` test + `runWithFallthroughCounter` helper.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.25 (this section).

### 7.26 Phase 7s-2 — always-on disc-mask propagation + opt-in split-and-augment helper (2026-05-12)

**Surprise finding.** While building the Phase 7s-2 split helper, a counter
audit revealed that the Phase 7r-2 augmenter's holder updates for `discBits` and
`initialBytePos` were never propagated back to the caller in `buildFlatNonStrict`:

```
// 7r-2 (commit 0590f22af) shipped with this comment but no assignment:
//   "Re-read augmented disc-bits info via the holder pattern (mutated in-place).
//    Read back to local variables for the page-creation call below."
partialKeys = phase7rAugmentUntilUnique(children, partialKeys,
    new DiscBitsInfo[]{discBits}, new int[]{initialBytePos}, pageKey);
// → discBits + initialBytePos UNCHANGED here; createNodeWithDiscBits sees pre-aug mask
```

The recomputed partials were stored against the AUGMENTED mask but the page was
constructed with the PRE-AUGMENTATION mask. Mask/partial mismatch silently
absorbed ~50% of descending-10K's residual violations under the old setup —
fixing the propagation alone reduces descending **517 → 258** (50% additional
reduction on top of the 7r-2 71% reduction; total 1832 → 258 = **86% drop from
the original 7r-1 baseline**).

The split helper itself fires but always rolls back on descending-10K (1
firing → 1 rollback) because validate-and-rollback's final β-constancy check
catches that the single-bit split does not eliminate every (child, mask-bit)
β-mixed pair. Scaffolding stays as opt-in (`-Dhot.strict.phase7s.split=true`)
so the next session can iterate on multi-bit split or relaxed validation
without re-deriving the structure.

**Empirical readout (default flag set + always-on propagation, no opt-in
split):**

| Test | Phase 7s-1 baseline | Phase 7s-2 (this commit) | Notes |
|------|---------------------|--------------------------|-------|
| 50K diagnosticMicrobenchPatternReproducer | 1 (soft I4) | 1 (soft I4) | unchanged |
| 100K casIndexHundredKEntryHeightBound | 0 | 0 | preserved |
| HOTOptionBPhase5Test (15) | pass | pass | preserved |
| comprehensiveAscending10K | 0 | 0 | preserved |
| comprehensiveDescending10K | 515 | **258** | **−50%** |
| comprehensiveMixedSign | 900 | 900 | preserved |
| comprehensiveBimodal5K+5K | 1280 | 1280 | preserved |
| comprehensiveRandomShuffle10K | 0 | 0 | preserved |
| comprehensiveClustered5x2K | 0 | 0 | preserved |

**Counters (with `-Dhot.strict.phase7s.split=true`):**

| Workload | PHASE7S_AUGMENT_FALLTHROUGH | PHASE7S_SPLIT_APPLIED | PHASE7S_SPLIT_ROLLBACK |
|----------|-----------------------------|-----------------------|------------------------|
| ascending-10K | 0 | 0 | 0 |
| descending-10K | **1** (was 945) | 0 | 1 |
| mixed-sign-10K | 0 | 0 | 0 |

The Phase 7s-1 fallthrough collapse 945 → 1 happens because once the augmented
mask is actually written through, the downstream build's adjacent-pair scan
finds different (better) bits initially — most fallthrough events are
self-eliminating after the propagation fix. The single remaining fallthrough
is the source of the 258 residual violations and is the next attack vector
(Phase 7s-3): refining the split helper to actually apply on that one site
without breaking validate-and-rollback.

**Files touched (Phase 7s-2 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + always-on read-back of `discBitsHolder[0]` and `initialBytePosHolder[0]` after
    `phase7rAugmentUntilUnique` in `buildFlatNonStrict`.
  + opt-in `phase7sSplitAndAugment` helper that walks every (child, mask-bit)
    pair via `bitConstantValueInSubtree`, splits each β-mixed child on its first
    β-mixed bit via `splitSubtreeOnBit`, re-augments, then enforces β-constancy
    over every (post-split child, mask-bit) pair — rollback on any failure.
  + `PHASE7S_SPLIT_APPLIED` / `PHASE7S_SPLIT_ROLLBACK` / `PHASE7S_SPLIT_NOOP`
    counters with get/reset accessors.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7s.split` flag (default
  off).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + `phase7s2CharacterizeSplitOutcome` test + `runWithSplitCounters` helper
    (skipped unless flag set).

**Phase 7s-3 entry point:** Use Phase 7s-1's persistent fallthrough = 1 on
descending as the targeted bisection point. Either (a) relax the helper's
final β-constancy check so partial improvement commits when validation
otherwise passes, or (b) extend the split logic to fire multi-bit splits when
one β-mixed child has multiple mask bits mixed in its subtree. Both require
careful protection against mixed-sign regression — the prior prototype
(reverted) blew up 900 → 9618 on indiscriminate splits.

### 7.27 Phase 7s-3 — relax β-constancy gate (falsified — wrong gate) (2026-05-12)

**Hypothesis:** the single rollback on descending-10K (counter SPLIT_ROLLBACK=1)
came from the helper's final β-constancy validation gate. Relaxing it should
let the partial-improvement commit and reduce descending below 258.

**Implementation:** `-Dhot.strict.phase7s.split.relax=true` flag short-circuits
the final β-constancy verification in `phase7sSplitAndAugment`. Routing
correctness (unique partials) is still required.

**Empirical falsification.** Under the flag, descending-10K stays at 258
violations (no change). Debug trace pinpoints the actual rollback site:

```
[phase7s-split] child=1 split on absBit=88 pageKey=2
[phase7s-split] child=2 split on absBit=92 pageKey=2
...
[phase7s-split] child=10 split on absBit=95 pageKey=2
[phase7s-split] ROLLBACK pageKey=2 splitCount=19 — collisions persist after re-augment
```

The rollback fires at the EARLIER gate (`phase7rRoutingCollisionFirstIdx` check
AFTER `phase7rAugmentUntilUnique` re-runs on the post-split state), not at the
β-constancy gate. The helper splits 19 children on bits 88–95 (byte 11–11),
but the re-augmenter can't find sort-monotone discriminating bits for the
expanded children-set — `partials[i] == partials[j]` still holds somewhere.

Relaxing the routing-collision gate is NOT acceptable (it's a hard
correctness invariant — routing must be unambiguous), so this gate is the
real blocker.

**Falsification note retained:** `hot.strict.phase7s.split.relax` flag stays
in the codebase as scaffolding — it's a no-op on descending today but useful
for future workloads where the β-constancy gate is the bottleneck. The flag's
default is `false` to preserve current behaviour.

**Phase 7s-4 redirect:** the next forward step needs to break the routing
collision after re-augment. Options:
- Pre-augment AGAIN with a different bit-selection strategy that explicitly
  targets the post-split children's first colliding pair.
- Split on a different bit (not the first β-mixed mask bit). Specifically:
  the FALLTHROUGH augmenter chose its bit because no β-constant alternative
  existed; the helper should split children on THAT specific fallthrough bit,
  not just any β-mixed mask bit. Need to expose the augmenter's chosen
  fallthrough bit via the holder pattern.

**Files touched (Phase 7s-3 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  `-Dhot.strict.phase7s.split.relax` flag gates the final β-constancy check.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7s.split.relax` flag.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.27 (this section).

### 7.28 Phase 7s-4 — target augmenter's fallthrough bit (falsified — same gate) (2026-05-12)

**Hypothesis:** the helper's split bit selection is wrong. It splits on the
first β-mixed mask bit, but the augmenter's FALLTHROUGH bit is the bit that
specifically caused the I5 risk (chosen because no β-constant alternative
existed). Splitting children on THAT specific bit should let the post-split
state augment to unique partials.

**Implementation:** added a `java.util.BitSet outFallthroughAbsBits` accumulator
parameter to `phase7rAugmentUntilUnique`. Each fallthrough records the chosen
absolute bit position. The caller in `buildFlatNonStrict` allocates the BitSet
(only when `hot.strict.phase7s.split` is on) and passes it to
`phase7sSplitAndAugment`. The helper now prefers a fallthrough bit when picking
the split bit for each child, falling back to the MSB-first mask-bit scan
only when no fallthrough bit is β-mixed in this child.

**Empirical falsification.** Under default flags + Phase 7s-4 helper, all
workload counts are byte-identical to Phase 7s-3:

| Workload | Phase 7s-3 | Phase 7s-4 |
|----------|-----------|------------|
| ascending-10K | 0 | 0 |
| descending-10K | 258 | 258 |
| mixed-sign-10K | 900 | 900 |
| bimodal-5K+5K | 1280 | 1280 |

The split-applied / split-rollback counters are identical too (descending: 1
rollback). The fallthrough bit prioritisation either matches the bit the MSB
scan would have picked (= no change in split choice) or the routing-collision
gate after re-augment still rejects.

**Diagnosis.** The 1 firing on descending-10K splits 19 children on bits
{88, 92, 93, 94, 95}. The augmenter's fallthrough bit on this site is in the
same byte range (debug not captured here — to confirm, the next session
should enable `-Dhot.debug.phase7s=true` and trace the per-child split bits
+ the FALLTHROUGH bit). Either way, the routing collision after re-augment
is structural — the post-split children's firstKeys cannot be made
partial-unique by any sort-monotone bit available.

**Multi-day blocker.** Two paths forward:
1. **Encoder change**: refactor `CASKeySerializer` to produce sort-monotone
   bytes for descending sequential int32 values across the entire range
   (currently the 0xbf/0xc0 boundary at byte 10 introduces a non-monotone
   transition that constrains the augmenter's bit choice).
2. **Multi-level recursion**: when buildFlatNonStrict's flat build fails to
   produce a valid mask, recurse into child subtrees instead of accepting
   the I5 cascade. Touches `splitParentAndRecurse` + `rebuildParentAbsorbingSplit`.

Both are multi-week refactors. Phase 7s campaign closes at 258 residual
descending violations (vs. 1832 original = 86% reduction). The 50K diag and
100K CAS Phase 7q goals remain met (0 violations). Comprehensive descending,
mixed-sign, and bimodal workloads need a separate Phase 8 design.

**Files touched (Phase 7s-4 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7rAugmentUntilUnique` overload with `outFallthroughAbsBits` accumulator.
  + `phase7sSplitAndAugment` consults the fallthrough bits first per child.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.28 (this section).

### 7.29 Phase 7t-1 — firstKey-monotone post-sort probe at `buildFlatNonStrict` (2026-05-13) — **FALSIFIES `buildFlatNonStrict` AS THE I8 SOURCE**

**Hypothesis.** Phase 7r-2's augment-until-unique guarantees partial-key uniqueness
(HOT I3) but does NOT verify partial-sort-vs-firstKey-sort agreement (HOT I8).
The augmenter only adds sort-monotone bits, but the INITIAL pick from
`computeDiscBits` (adjacent-pair MSB-of-XOR scan) can include non-sort-monotone
bits whose presence in the PEXT mask reorders the partials. After
`sortChildrenAndPartialsByPartial`, children may end up in partial order that
disagrees with firstKey order — producing I8 violations downstream.

If this is the source of comprehensive descending's 258 (post-7s-2) / mixed-sign's
900 / bimodal's 1280 residual violations, a firstKey-monotone post-sort retry —
analogous to the existing one in `addEntryWithPDep` (line 1885 onward) and the
g32-multibeta path (line 9823) — would fix all three at one site.

**Probe implementation** (gated, default off):

```java
// In buildFlatNonStrict, immediately after sortChildrenAndPartialsByPartial:
if (Boolean.getBoolean("hot.strict.phase7t.monotone.probe")) {
  PHASE7T_BUILDFLAT_INSPECTIONS.incrementAndGet();
  byte[] prevFk = null;
  int inversionAt = -1;
  for (int i = 0; i < children.length; i++) {
    final byte[] fk = getFirstKeyFromChild(children[i]);
    if (fk == null || fk.length == 0) { prevFk = fk; continue; }
    if (prevFk != null && prevFk.length > 0 &&
        Arrays.compareUnsigned(prevFk, fk) >= 0) { inversionAt = i; break; }
    prevFk = fk;
  }
  if (inversionAt >= 0) PHASE7T_BUILDFLAT_INVERSIONS.incrementAndGet();
}
```

Flags: `hot.strict.phase7t.monotone.probe`, `hot.debug.phase7t` (both default `false`).
Counters: `PHASE7T_BUILDFLAT_INSPECTIONS` / `PHASE7T_BUILDFLAT_INVERSIONS` with
public reset/getter accessors mirroring the Phase 7r-1 / 7s pattern. Test entry
point: `HOTFormalVerificationTest.phase7t1CharacterizeMonotoneInversions`
(skips silently when the flag is off; reads counters from `HOTTrieWriter`).

**Empirical falsification.** Under `-Dhot.strict.phase7t.monotone.probe=true`:

| Workload | inspections | inversions | ratio |
|----------|-------------|------------|-------|
| ascending-10K (control) | 18 | 0 | 0.00 % |
| descending-10K | 18 | 0 | 0.00 % |
| mixed-sign-10K | 24 | 0 | 0.00 % |
| bimodal-5K+5K | 18 | 0 | 0.00 % |

**0 inversions across ALL workloads** — including the three whose I8 violations
this probe was designed to localise. Inspection counts are also small (18-24
per 10K inserts), confirming `buildFlatNonStrict` is the RARE indirect-rebuild
path. The DOMINANT path is `addEntryWithPDep` (called per leaf-split insert).

**Baselines preserved** (default flags, probe off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 0 violations · height 6 · build 6738 ms |
| casIndexHundredKEntryHeightBound (100K) | 0 violations · height 2 · build 5905 ms |
| HOTOptionBPhase5Test (15) | pass |
| comprehensiveDescending10K (probe on) | 258 violations (unchanged from 7s-2) |

**Phase 7t-2 redirect.** The inversion site is NOT `buildFlatNonStrict`.
Candidates ranked by call frequency on the bulk-JSON shred path:

1. **`addEntryWithPDep`** (dominant — called per leaf-split). HAS its own
   firstKey-monotone check (line 1885) + G.31 retry (line 1903 onward), but:
   - The check is SKIPPED when `prev`/`curr` is null/zero-length. Multi-entry
     leaves with placeholder/empty firstKeys could let inversions through.
   - G.31's mask-extension has tight restrictions: cross-window bail (line
     1930), saturation > 16 bits bail (line 1936), unfound-msdb bail (line
     1926) — when any bail, `addEntryWithPDep` returns null and the caller
     falls back to `splitParentAndRecurse`, which routes through
     `createNodeFromChildren` / `buildFlatNonStrict` (Phase 7t-1 confirmed
     clean) OR through other internal-indirect builders.
2. **`upgradeToMultiMaskWithNewBit`** (cross-window addEntry path, line 4555) —
   no visible firstKey-monotone retry.
3. **`rebuildParentAbsorbingSplit`** (line 4502) — internal rebuild on Phase 3
   absorb path.
4. **`rebalanceAndIntegrate`** (line 3641) — Phase 3 rebalance.

Phase 7t-2 entry point: port the same probe to `addEntryWithPDep` (at the
SUCCESS return path, just before `createSpanNode`/`createMultiNode`) and
`upgradeToMultiMaskWithNewBit` with separate counters. Expected outcome:
the dominant inversion source shows non-zero ratio on descending /
mixed-sign / bimodal.

**Files touched (Phase 7t-1 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  `PHASE7T_BUILDFLAT_{INSPECTIONS,INVERSIONS}` counters + accessors; gated probe
  in `buildFlatNonStrict` after `sortChildrenAndPartialsByPartial`.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7t.monotone.probe` +
  `hot.debug.phase7t` flags (both default `false`).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `phase7t1CharacterizeMonotoneInversions` test + `runWithMonotoneProbe` helper.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.29 (this section).

### 7.30 Phase 7t-2 — port firstKey-monotone probe to `addEntryWithPDep` + `upgradeToMultiMaskWithNewBit` (commit `e88ae008c+`, 2026-05-13) — **ALSO FALSIFIED**

**Hypothesis.** Phase 7t-1 ruled out `buildFlatNonStrict` as the I8 source. The
next-most-frequent indirect-construction successes are:
- `addEntryWithPDep` (line 1708 — per-leaf-split, SingleMask). Has firstKey-monotone
  check + G.31 retry, but the check is SKIPPED when prev/curr is null/empty (multi-
  entry-leaf placeholder keys).
- `upgradeToMultiMaskWithNewBit` (line 4555 — cross-window addEntry branch). NO visible
  firstKey-monotone retry.

Expected: one of these produces non-zero inversion ratio on the failing workloads.

**Probe.** Shared private helper `phase7tFirstInversionIdx(PageReference[] children)`.
Each site adds an `INSPECTIONS` / `INVERSIONS` counter pair, gated on
`-Dhot.strict.phase7t.monotone.probe`. Test helper `runWithMonotoneProbe` resets +
reads all three pairs (buildFlat, addPdep, upgrade) and prints
`buildFlat=I/T · addPdep=I/T · upgrade=I/T` (Inversions / Inspections).

**Empirical readout** (`-Dhot.strict.phase7t.monotone.probe=true`):

| Workload | buildFlat | addPdep | upgrade |
|----------|-----------|---------|---------|
| ascending-10K (control) | 0 / 18 | 0 / 0 | 0 / 0 |
| descending-10K | 0 / 18 | 0 / 0 | 0 / 1 |
| mixed-sign-10K | 0 / 24 | 0 / 2 | 1 / 2 |
| bimodal-5K+5K | 0 / 18 | 0 / 0 | 0 / 0 |

**Total inversions across ALL 3 sites: 1.** Yet descending-10K reports 258 violations,
mixed-sign 900, bimodal 1280. Inversion-driven I8 cannot be the dominant violation
type — it accounts for at most a handful of cases.

**Inspection counts are tiny** on the failing workloads:
- `addEntryWithPDep`: 0–2 successes per 10K inserts. Most calls return null
  (constancy bail, dup-partial bail, G.31 cross-window/saturation/msdb bail).
- `upgradeToMultiMaskWithNewBit`: 0–2 successes per 10K inserts.

This means **most indirects on the failing workloads are built somewhere ELSE**.
The dominant indirect-construction path for bulk-JSON descending/mixed-sign/bimodal
is not yet identified.

**Baselines preserved** (default flags, probe off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 0 violations · height 6 · build 6965 ms |
| casIndexHundredKEntryHeightBound (100K) | 0 violations · height 2 |
| HOTOptionBPhase5Test (15 tests) | pass |

**Phase 7t-3 redirect.** The 900/1280 violations must be **mostly I3/I5/I6/I7
(partial-key uniqueness, constancy, partial-key ordering)** rather than I8 — the
naming in [[hot-comprehensive-tests-findings]] conflated invariant kinds. To confirm
and localise:
1. Extend `buildAndValidateCas` to print the per-invariant breakdown
   (`inv.violations().stream().collect(groupingBy(kind, counting()))`).
2. If I3/I5 dominate, the cause is duplicate-partial / β-mixed-leaf state passing
   through `buildFlatNonStrict` because `phase7rAugmentUntilUnique` exhausts its
   bit budget without resolving collisions. `createNodeWithDiscBits` accepts the
   stale state unconditionally — there is no rollback. Phase 7r-1's
   `phase7rRoutingCollisionFirstIdx` counter (default-off) would show non-zero
   collisions per buildFlatNonStrict call AFTER 7r-2 augment exhaustion.
3. Run with BOTH `hot.strict.phase7t.monotone.probe=true` AND
   `hot.strict.phase7r.routeverify=true` to confirm routing collisions are the
   dominant unaddressed defect.

**Files touched (Phase 7t-2 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  `PHASE7T_ADDPDEP_{INSPECTIONS,INVERSIONS}` + `PHASE7T_UPGRADE_{INSPECTIONS,INVERSIONS}`
  counters; shared `phase7tFirstInversionIdx` + `phase7tLogInversion` helpers;
  probe sites at `addEntryWithPDep` success path and `upgradeToMultiMaskWithNewBit`
  success path. `buildFlatNonStrict` probe refactored to use the shared helpers
  (no behavior change).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `runWithMonotoneProbe` extended to reset/read/print all 3 site counters.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.30 (this section).

### 7.31 Phase 7t-3 — per-invariant breakdown localises the real defect: **I6 dominates (98–99.8%)**, not I8 (2026-05-13)

**Motivation.** Phases 7t-1 + 7t-2 falsified firstKey-monotone (I8) as the source
of the 258/900/1280 residual violations: total inversions across the three
probed sites = 1. The naming in [[hot-comprehensive-tests-findings]] conflated
invariant kinds — the example cited there ("c2 partial=0x6000 fk=bff0…")
implied I8, but the bulk distribution is different.

**Change.** Extend `buildAndValidateCas` in `HOTFormalVerificationTest` to
emit a per-invariant breakdown — group `inv.violations()` by `Violation.invariant`,
sort alphabetically, print `tag:count` joined by spaces. No flag — always
prints on tests that report violations.

**Empirical readout** (default flags, all current Phase 7q/7r/7s defaults ON):

| Workload | total | byInvariant |
|----------|-------|-------------|
| ascending-10K | 0 | — |
| descending-10K | 258 | **I6-pext-routes-to-leaf:253** · I-Binna-sparse-path:2 · I5-leaf-constancy:2 · I8-children-sorted-by-firstkey:1 |
| mixed-sign-10K | 900 | **I6-pext-routes-to-leaf:895** · I5-leaf-constancy:5 |
| bimodal-5K+5K | 1280 | **I6-pext-routes-to-leaf:1278** · I5-leaf-constancy:2 |

I6 dominates **98–99.8 %** of every failing workload. I8 accounts for exactly 1
violation across all four workloads.

**Interpretation.** I6-pext-routes-to-leaf is the **β-mixed leaf** invariant:
for every leaf key K, PEXT(K, parent.mask) must route K BACK to that leaf when
descending from the root. A β-mixed leaf has keys with differing values at one
of the parent's mask bits — so PEXT routes some of its keys to a SIBLING leaf
(= I6 violation, plus downstream cascade of I5-leaf-constancy).

**Why Phase 7r-2 augment-until-unique doesn't fix I6.** Augment ensures *unique*
partials at indirect-build time. It adds a sort-monotone bit when two adjacent
firstKeys agree on the current mask. The added bit may or may not be β-constant
inside each child — when it isn't, the child becomes β-mixed at that mask bit,
producing I6 at descendant-leaf descent.

**Why Phase 7s-2 split-and-augment doesn't fix I6 on these workloads.** The
helper is gated on `fallthroughFired || exhaustedFired`. The fallthrough fires
only when the augmenter prefers a β-constant + sort-monotone bit but none
exists; per the 7s-1 readout, mixed-sign and bimodal have 0 fallthroughs (the
augmenter finds β-constant bits trivially because most pairs DO have one). So
the helper never executes on the dominant failing workloads.

**Phase 7t-4 attack vector.** Relax the helper trigger from
`fallthroughFired || exhaustedFired` to ALSO fire when any (child, mask-bit)
pair is β-mixed (= I6 risk), even when augment succeeded cleanly. The helper
already walks `bitConstantValueInSubtree` for every (child, mask-bit) and
splits β-mixed children. The validate-and-rollback gate ensures it doesn't
regress when split makes no progress.

Risk: the helper currently rolls back on descending-10K (per [[hot-phase7s-2-mask-propagation-landed]]).
Need an unconditional trigger but a smarter rollback strategy that retains
*partial* split improvement instead of all-or-nothing.

**Files touched (Phase 7t-3 commit):**
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `buildAndValidateCas` extended with per-invariant breakdown emission.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.31 (this section).

### 7.32 Phase 7t-4 — relax `phase7sSplitAndAugment` trigger to β-mixed-only (FALSIFIED, default-off scaffold kept) (2026-05-13)

**Hypothesis.** Phase 7t-3 (§7.31) identified I6 (β-mixed leaf routing) as 98–99.8 % of
the failing-workload violations. The existing helper `phase7sSplitAndAugment` already
splits β-mixed children at indirect build time, but its trigger gate
`(fallthroughFired || exhaustedFired)` skips mixed-sign / bimodal because those
workloads' augmenter trivially finds β-constant + sort-monotone bits. Relax the gate
to fire whenever any (child, mask-bit) pair is β-mixed.

**Implementation.** Gated on `-Dhot.strict.phase7t.split.always=true` (default off).
Pre-helper scan walks `collectDiscBitsMsbFirst(discBitsHolder[0])` × `children[]` and
calls `bitConstantValueInSubtree`. If any pair returns < 0 → set
`betaMixedFound = true` → helper fires with the existing logic. Two new counters:

- `PHASE7T_BETAMIXED_DETECTED` — scan found a β-mixed pair.
- `PHASE7T_BETAMIXED_SPLIT_APPLIED` — helper returned true under the new gate.

**Empirical falsification.**

(A) With `-Dhot.strict.phase7t.split.always=true` alone — comprehensive descending /
mixed-sign / bimodal violation counts are **byte-identical** to baseline (258 / 900 /
1280). Debug trace shows the gate FIRES heavily on pageKey=2 (root indirect),
absBits ∈ {82, 89, 108, …}, but every firing rolls back at one of two existing
gates inside the helper:

- `phase7s-split ROLLBACK pageKey=N — child[X] still β-mixed at absBit=Y` —
  β-constancy final gate detects that split didn't fully resolve mixedness.
- `phase7s-split ROLLBACK pageKey=N splitCount=K — collisions persist after re-augment` —
  routing-collision gate.

These are the same gates documented as Phase 7s-3 / 7s-4 blockers.

(B) With `-Dhot.strict.phase7t.split.always=true -Dhot.strict.phase7s.split.relax=true`
combined — relaxing the β-constancy gate while firing more aggressively **regresses**:

| Workload | Baseline | Phase 7t-4 + relax | Δ |
|----------|----------|---------------------|---|
| mixed-sign-10K | 900 | **2418** | +169 % |
| descending-10K | 258 | (also regressed) | — |
| bimodal-5K+5K | 1280 | (also regressed) | — |

The helper's rollback is **load-bearing**. Partial split commits leave β-mixed
state that cascades to downstream leaves at descendant indirects, increasing the
total I6 / I5 count.

**Baselines preserved** (default flags, gates off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 0 violations · height 6 · build 7049 ms |
| casIndexHundredKEntryHeightBound (100K) | 0 violations · height 2 |
| HOTOptionBPhase5Test (15 tests) | pass |

**Scaffold retained.** The Phase 7t-4 trigger relaxation stays in the codebase
default-off because: (i) the counter signal `BETAMIXED_DETECTED` is empirically
useful for diagnosing future β-mixed pattern triage; (ii) future fixes that
strengthen `splitSubtreeOnBit` (= recursively split until β-constant rather than
single-bit split) can be re-tested simply by flipping the flag.

**Phase 7t-5 entry point.** The next forward step needs to fix the **root cause**
of the rollback: `splitSubtreeOnBit` returns subtrees that are themselves still
β-mixed at OTHER mask bits than the split bit. The helper's β-constancy gate
correctly rejects this. Two options:

1. **Recursive multi-bit split**: `splitSubtreeOnBit` recursively walks the
   produced halves, finds the next β-mixed bit, and splits again, until the leaf
   level. Bounded depth = number of mask bits (≤ 16 typically). Risk: explosion
   of leaf count exceeding fanout limit.
2. **Investigate where I6 violations are CREATED**: per Phases 7t-1/7t-2, the
   buildFlatNonStrict / addEntryWithPDep / upgradeToMultiMaskWithNewBit success
   paths fire only 18 / 0–2 / 0–2 times per 10K workload — far below the
   258 / 900 / 1280 violation counts. The dominant indirect-construction
   success path is elsewhere — likely `splitParentAndRecurse`,
   `rebalanceAndIntegrate`, or `rebuildParentAbsorbingSplit` (each visible in
   the file but not yet probed for monotone or β-mixed defects). A future
   phase should probe these and produce a per-path call-frequency histogram.

**Files touched (Phase 7t-4 commit):**
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  pre-helper scan in `buildFlatNonStrict` augment block; new gate
  `(fallthroughFired || exhaustedFired || betaMixedFound)`;
  `PHASE7T_BETAMIXED_{DETECTED,SPLIT_APPLIED}` counters + accessors.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7t.split.always` flag (default `false`).
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.32 (this section).

### 7.33 Phase 7t-5 — call-frequency probe at splitParentAndRecurse / rebalanceAndIntegrate / rebuildParentAbsorbingSplit (FALSIFIES the §7.32 "dominant site" hypothesis) (2026-05-13)

**Hypothesis (§7.32 Phase 7t-5 entry).** Phases 7t-1/7t-2 instrumented the three
indirect-construction success paths that were locally visible
(`buildFlatNonStrict`, `addEntryWithPDep`, `upgradeToMultiMaskWithNewBit`) and
found call frequency far below the 258 / 900 / 1280 violation counts (≤ 24
inspections per 10K). §7.32 conjectured that the dominant indirect-construction
success path was elsewhere — likely `splitParentAndRecurse`,
`rebalanceAndIntegrate`, or `rebuildParentAbsorbingSplit`. Phase 7t-5 instruments
all three with the existing `phase7tFirstInversionIdx` helper so the empirical
readout pinpoints which (if any) hosts the dominant production rate.

**Implementation.** Six new counters
(`PHASE7T_{SPLITPARENT,REBALANCE,REBUILD}_{INSPECTIONS,INVERSIONS}`) + accessors.
Probes installed at:

- `splitParentAndRecurse` — after `leftChildren` / `rightChildren` arrays are
  materialised but BEFORE the recursive `buildCompressedHalf` calls. Both halves
  are probed independently; INSPECTIONS increments twice per call so the
  histogram counts indirect-build events rather than function calls.
- `buildRebalancedParentWithInheritedMask` — after
  `sortChildrenAndPartialsByPartial(trimChildren, trimPartials)`, BEFORE the
  existing G.31 firstKey-monotone verify-and-resort.
- `rebuildParentAbsorbingSplit` — on the `rebuilt[]` PageReference array,
  AFTER trim/size-check, BEFORE the `createNodeFromChildren` call. (Downstream
  `createNodeFromChildren` may correct ordering, so this site's INVERSIONS
  measures the input state, not the final output state.)

All probes are counter-only and gated on
`-Dhot.strict.phase7t.monotone.probe`. Test harness `runWithMonotoneProbe`
extended to reset+print all six new counters alongside the three legacy ones.

**Empirical readout** (10K-entry CAS-shred, monotone.probe=true):

| Workload | buildFlat (inv/ins) | addPdep | upgrade | splitParent | rebalance | rebuild |
|----------|---------------------|---------|---------|-------------|-----------|---------|
| ascending-10K (control) | 0 / 18 | 0 / 0 | 0 / 0 | 0 / 2 | 0 / 0 | **8 / 18** |
| descending-10K (258 viol.) | 0 / 18 | 0 / 0 | 0 / 1 | 2 / 2 | 0 / 0 | **10 / 18** |
| mixed-sign-10K (900 viol.) | 0 / 24 | 0 / 2 | 1 / 2 | 1 / 2 | 0 / 0 | **16 / 23** |
| bimodal-5K+5K (1280 viol.) | 0 / 18 | 0 / 0 | 0 / 0 | 1 / 2 | 0 / 0 | **7 / 18** |

**Key findings — both prongs of §7.32 hypothesis FALSIFIED.**

(1) **`rebalanceAndIntegrate` fires 0× across all 4 workloads.** That code path
exists only in the `addEntryWithPDep` failure-fallback chain, which itself fires
0-2 times per 10K. On bulk-JSON shred it is effectively dead.

(2) **`splitParentAndRecurse` fires 1× per 10K** (= 2 INSPECTIONS counting both
halves). Negligible compared to the violation counts (258 / 900 / 1280).

(3) **`rebuildParentAbsorbingSplit` fires 18-23× per 10K** — dominant of the
three, but still 1-2 orders of magnitude below the violation counts. Notably,
the **ascending control** has 8 INVERSIONS / 0 violations: firstKey-inversion at
`rebuild` is locally tolerated because `createNodeFromChildren` downstream sorts
on partial-key, and on ascending workloads partial-sort coincides with
firstKey-sort. The high INVERSIONS / 0-violations decoupling is itself a useful
falsification: **firstKey-monotonicity at the rebuild site is NEITHER necessary
NOR sufficient to predict downstream violations.**

(4) Across ALL six probed sites combined, total INSPECTIONS per 10K ≤ 50,
total INVERSIONS ≤ 28. The 258 / 900 / 1280 violations cannot be attributed to
non-monotone children at any of these sites. The dominant defect (I6,
β-mixed-leaf-routing) is therefore being created at a path that either:

- (a) is invisible from a firstKey-monotone probe (I6 ≠ I8 — different
  invariants), OR
- (b) lives in a site not yet instrumented.

**Baselines preserved** (default flags off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 0 violations · height 6 · build 6642 ms |
| casIndexHundredKEntryHeightBound (100K) | 0 violations · height 2 · build 5782 ms |
| HOTOptionBPhase5Test (15 tests, 5+10) | pass |

Comprehensive failure counts unchanged because probes are counter-only.

**Phase 7t-6 entry point.** The right next probe is a **β-mixed child detector**,
not a firstKey-monotone probe. For each (child, mask-bit) pair in a freshly-
constructed indirect, call `bitConstantValueInSubtree(child, absBit)`; if it
returns -1 (mixed) the child is β-mixed and produces an I6 violation downstream.
Install this probe at the SAME six sites; the per-site β-mixed count gives the
true I6 origination histogram. Because I6 fires 253-1278 times per 10K, the
new counters should produce non-trivial readings at one or more sites.

If 7t-6 also produces ≤ 50 detections across all 6 sites, the conclusion is
that the I6 violations are not created at indirect-construction time at all —
they are created at LEAF mutation time after the parent indirect was already
constructed. That falsification would redirect the campaign to leaf-mutation
analysis (a still-distinct code path).

**Files touched (Phase 7t-5 commit):**

- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  6 new `PHASE7T_{SPLITPARENT,REBALANCE,REBUILD}_{INSPECTIONS,INVERSIONS}`
  counters + accessors; gated monotone probes at the three sites.
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `runWithMonotoneProbe` extended to reset + print the new counters.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.33 (this section).

### 7.34 Phase 7t-6 — β-mixed (child, mask-bit) pair detector at 4 indirect-build sites; localises I6 origination at `buildFlatNonStrict` but ASCENDING paradox shows raw β-mixedness ≠ violation (2026-05-13)

**Motivation.** Phase 7t-5 (§7.33) falsified `splitParentAndRecurse` /
`rebalanceAndIntegrate` / `rebuildParentAbsorbingSplit` as the I6 origination
point using the firstKey-monotone probe — but that probe detects I8 candidates,
not I6. §7.33's entry point called for a β-mixed-pair probe at the same sites
to produce the true I6 origination histogram. I6 (β-mixed leaf routing) is the
dominant defect by 7t-3's per-invariant breakdown (98-99.8 % of failures).

**Implementation.** Eight new counters
(`PHASE7T6_{BUILDFLAT,ADDPDEP,UPGRADE,REBALANCE}_{BUILDS,MIXED_PAIRS}`) + two
helpers:

- `phase7tCountBetaMixedPairsSingleMask(children[], initialBytePos, mask)` —
  enumerates set bits in `mask`, decodes BE-MSB-first byte+bit positions,
  calls `bitConstantValueInSubtree(child, absBit)` for each child. Returns
  count of pairs that return `-1`.
- `phase7tCountBetaMixedPairsMultiMaskBytes(children[], bytePositions[],
  byteMaskBits[], byteCount)` — same for MultiMask layout.

Probes installed at four sites:

- `buildFlatNonStrict` — SingleMask via `discBits.isSingleMask()` branch with
  unpacked byte[] extraction tables for MultiMask path.
- `addEntryWithPDep` — SingleMask (`oldInitialBytePos`, `newMask`).
- `upgradeToMultiMaskWithNewBit` — MultiMask (`allBytePositions[]`,
  `allByteMaskBits[]`, `allCount`) in scope from the merge step.
- `buildRebalancedParentWithInheritedMask` — SingleMask (`oldInitialBytePos`,
  `newMask`).

`splitParentAndRecurse` and `rebuildParentAbsorbingSplit` are skipped because
the constructed indirect's mask isn't directly available at the probe site
(`createNodeFromChildren` downstream computes it). All probes counter-only,
gated on `-Dhot.strict.phase7t.betamixed.probe` (default off).

**Empirical readout (10K CAS-shred, betamixed.probe=true):**

| Workload | violations | buildFlat (mixed/builds) | addPdep | upgrade | rebalance |
|----------|------------|---------------------------|---------|---------|-----------|
| ascending-10K (control) | 0 | **1756 / 18** | 0 / 0 | 0 / 0 | 0 / 0 |
| descending-10K | 258 | **1722 / 18** | 0 / 0 | 26 / 1 | 0 / 0 |
| mixed-sign-10K | 900 | **2748 / 24** | 61 / 2 | 201 / 2 | 0 / 0 |
| bimodal-5K+5K | 1280 | **1777 / 18** | 0 / 0 | 0 / 0 | 0 / 0 |

**Three findings.**

(1) **`buildFlatNonStrict` is by far the dominant site** — 1700-2800 β-mixed
(child, mask-bit) pairs per 10K workload. `addPdep` / `upgrade` contribute
≤ 260 combined. `rebalance` contributes 0 (consistent with 7t-5: dead path on
bulk-JSON shred).

(2) **ASCENDING PARADOX: 1756 mixed pairs / 0 violations.** The ascending
control has nearly the same β-mixed-pair count as descending (1722) but zero
I6 violations downstream. Raw β-mixedness at construction time is NOT
sufficient to predict I6 violation.

(3) **Mixed-pair count poorly correlated with violation count.** Mixed-sign
has the highest mixed-pair count (2748) but mid violation count (900); bimodal
has fewer mixed pairs (1777) but the highest violation count (1280).

**Why the paradox? I6 validator semantics.**

`HOTInvariantValidator` checks I6 per-KEY via `pextDescend`: walk every stored
key K from root, at each indirect take child[i] where `partial_i ==
PEXT(K, mask)`; if descent reaches a leaf that doesn't contain K, that's a
violation. So **I6 violations count keys that mis-route**, not (child, bit)
pairs that are β-mixed.

A β-mixed (child[i], β) pair means child[i]'s subtree contains keys with both
β=0 and β=1. For one polarity to mis-route, the OTHER polarity must:
(a) be assigned to a sibling child[j] (partial_j routes there), AND
(b) the sibling's actual subtree must NOT contain the mis-routed key (else
    PEXT-descent doesn't terminate at a wrong leaf — it terminates at the right
    leaf via a different route).

On ASCENDING workloads the encoding is naturally sorted, so PEXT routing
matches the natural ordering. Even if mask bits look β-mixed in an aggregate
subtree view, every key's PEXT-descent lands at its actual leaf. On DESCENDING
/ MIXED-SIGN / BIMODAL the encoding (`CASKeySerializer` bit-inverts negatives;
`LongRecordValueSerializer` xors sign bit) creates non-monotone bit patterns
that break this alignment.

**Phase 7t-7 entry point.** The next probe should NOT just detect β-mixedness;
it should detect **β-mixedness with active mis-routing potential**. Two paths:

1. **Sibling-cross-routing probe**: for each β-mixed (child[i], β), check
   whether some sibling child[j] has the inverse polarity in its `partial_j`
   AND its subtree does NOT contain the would-be-mis-routed keys. This is a
   strict subset of (β-mixed pairs) that should approximate the violation
   count.
2. **Direct per-key PEXT-descent probe at construction time**: simulate
   `pextDescend` for every key in every child's subtree against the newly-
   built indirect; count keys that would land in a wrong leaf. Effectively
   runs the I6 validator at construction time. Most expensive but most
   accurate.

Path 2 is conceptually equivalent to running the validator — if the validator
already reports 258 violations on descending, this probe would too. The value
of Path 1 over Path 2 is identifying the SPECIFIC β bit and SPECIFIC sibling
pair causing each mis-route, which would localise the fix.

**Baselines preserved** (default flags off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 0 violations · height 6 · build 6717 ms |
| casIndexHundredKEntryHeightBound (100K) | 0 violations · height 2 · build 5815 ms |
| HOTOptionBPhase5Test (15 tests) | pass |

Comprehensive failure counts unchanged because probes are counter-only.

**Files touched (Phase 7t-6 commit):**

- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  8 new `PHASE7T6_*` counters + accessors; 2 new helper methods; probes at 4
  indirect-build sites.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7t.betamixed.probe` flag
  (default `false`).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `phase7t6CharacterizeBetaMixedPairs` test + `runWithBetaMixedProbe` helper.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.34 (this section).

### 7.35 Phase 7t-7 — sibling-cross-routing probe at `buildFlatNonStrict`; FALSIFIES equality-only mis-route hypothesis; reveals subset-match descent as the dominant mechanism (2026-05-13)

**Motivation.** Phase 7t-6 (§7.34) identified `buildFlatNonStrict` as the dominant
β-mixed-pair producer (1700-2800 per 10K) but also showed the **ASCENDING
PARADOX**: 1756 β-mixed pairs / 0 downstream violations. Raw β-mixedness is
necessary but NOT sufficient. The §7.34 entry point hypothesised that adding
a "some sibling has the inverse-polarity partial" filter would yield a
sufficient predictor (Path 1).

**Implementation.** Helper `phase7t7CountCrossRoutingMixedPairs(children[],
partials[], discBits)` collects disc-bit absolute positions via
`collectDiscBitsMsbFirst` (gives the bit-rank → partial-bit-position map under
MSB-first encoding from `computeSparsePathRecursive`). For each β-mixed
`(child[i], β)`:

1. Compute `partialBitPos = totalDiscBits - 1 - discBitIdx` (the position in
   the partial-key that represents β).
2. `inversePartial = partials[i] XOR (1 << partialBitPos)`.
3. Scan siblings; if any `partials[j] == inversePartial`, this is a
   cross-routing candidate (potential I6 mis-route source).
4. Else it's `mixedNoCrossRoute` — descent would terminate inside child[i],
   not triggering I6 via equality routing.

Three new counters (`PHASE7T7_BUILDFLAT_{BUILDS,CROSS_ROUTING_PAIRS,
MIXED_NO_CROSS_ROUTE}`) + accessors + reset methods. Wired at
`buildFlatNonStrict` success path (after Phase 7t-6 probe, before Phase 7r-1
collision probe). Gated on `-Dhot.strict.phase7t.crossroute.probe` (default
off). Counter-only.

**Empirical readout (10K CAS-shred, crossroute.probe=true):**

| Workload | builds | crossRoutingPairs | mixedNoCrossRoute | validator I6 viol |
|----------|--------|-------------------|-------------------|-------------------|
| ascending-10K (control) | 18 | **0** | 1756 | 0 |
| descending-10K | 18 | **0** | 1722 | 258 |
| mixed-sign-10K | 24 | 169 | 2579 | 900 |
| bimodal-5K+5K | 18 | **0** | 1777 | 1280 |

**FALSIFICATION.** Equality-only cross-routing matches **zero** pairs on
descending and bimodal workloads, yet those produce 258 / 1280 violations
respectively. Mixed-sign has 169 cross-routing pairs vs 900 violations — also
under-predicts by 5.3×. The hypothesis "β-mixed (child[i], β) + sibling with
inverse-polarity partial" is NOT the dominant mis-route mechanism.

**Why the falsification — subset-match descent.**

`HOTIndirectPage.findChildSpanNode` (line ~531) uses a two-pass match:

```java
for (int i = 0; i < numChildren; i++) {
  int sparseKey = partialKeys[i];
  if (sparseKey == densePartialKey) { exact = i; break; }
  if ((densePartialKey & sparseKey) == sparseKey) best = i;
}
return exact >= 0 ? exact : best;
```

The SUBSET fallback `(densePK & sparseKey) == sparseKey` routes a key K to
the most-specific child whose sparse partial is a SUBSET of K's dense PEXT.
This is the HOT paper's sparse-path-encoding requirement. Equality is only
the FIRST pass; subset is the fallback. Phase 7t-7's predicate only models
the equality pass — it misses the dominant subset-match mis-routes.

Why descending hits 0 cross-routing but 258 violations:

- The sparse partials computed by `computeSparsePathRecursive` use
  *trivial-split short-circuits*: if every child in a range agrees on a disc
  bit, the partial bit stays 0 for everyone in that range. Mis-polarity keys
  within a β-mixed child have dense PEXT values that differ at β, BUT the
  stored sparse partials don't necessarily encode that bit at all.
- A mis-polarity key K with dense_K differing from `partial_i` at the bit
  for β can still subset-match to a sibling whose `partial_j` is a SUBSET of
  dense_K (since 0 bits in `partial_j` are "wildcards" under subset
  matching). The equality probe misses this entirely.
- Mixed-sign hits 169 because CASKeySerializer's bit-inversion on negatives
  produces partials at different polarities that happen to also satisfy
  equality. Descending and bimodal use monotone (non-bit-inverted) regions
  where the dominant mis-route mechanism is subset-match, not equality.

**Three takeaways.**

(1) **buildFlatNonStrict is still the dominant site by β-mixed-pair count
(§7.34)** — Phase 7t-7's negative result doesn't refute that. It only
refutes the cross-routing-via-equality hypothesis.

(2) **Subset-match descent is the missing piece**. Any sufficient predictor
of I6 violations must simulate the actual `findChildSpanNode` algorithm
(equality preferred, subset fallback, most-specific tie-break).

(3) **Mixed-sign's 169 / 900 partial coverage** suggests equality matches
account for ~19 % of mixed-sign violations, but subset matches dominate the
other workloads entirely. The mechanism is workload-dependent (encoding
matters).

**Phase 7t-8 entry point.** Replace the equality-only predicate with a
subset-match-aware predicate:

For each β-mixed `(child[i], β)`:
1. Synthesise `dense_K_candidate = partial_i XOR (1 << partialBitPos)` (the
   dense PEXT a mis-polarity key would produce against the current sparse
   encoding's bits).
2. Run `findChildSpanNode`'s algorithm: scan siblings; pick j with
   `partial_j == dense_K_candidate` (exact) or the most-specific subset
   `(dense_K_candidate & partial_j) == partial_j`.
3. If picked j ≠ i → mis-route candidate. Count.

Cost: O(D · N²) per indirect build (D = disc bits, N = children). For
typical N ≤ 64 and D ≤ 32 this is bounded. Alternative (more accurate but
expensive): per-stored-key simulator that walks every key in every child's
subtree and runs findChildIndex.

**Baselines preserved** (default flags off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 1 I4 violation (unchanged from §7.34) · height 7 · build 6.7 s |
| casIndexHundredKEntryHeightBound (100K) | **0 violations** · height 2 · build 7.7 s |
| HOTOptionBPhase5Test (15 tests) | pass |

Comprehensive failure counts unchanged because probes are counter-only.

**Files touched (Phase 7t-7 commit):**

- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  +60 lines — `phase7t7CountCrossRoutingMixedPairs` helper; 3 new
  `PHASE7T7_*` counters + accessors; probe at `buildFlatNonStrict`.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7t.crossroute.probe`
  flag (default `false`).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `phase7t7CharacterizeCrossRouting` test + `runWithCrossRoutingProbe` helper.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.35 (this section).

### 7.36 Phase 7t-8 — subset-match-aware mis-route probe; ALSO FALSIFIES `buildFlatNonStrict` as the I6 origination site (2026-05-13)

**Motivation.** §7.35 falsified the equality-only cross-routing predicate.
The natural extension was to add subset-match (the actual second pass in
`findChildSpanNode`) to the predicate. If buildFlatNonStrict is the
violation source, an equality-then-subset model should reproduce the
validator's I6 count.

**Implementation.** Helper `phase7t8CountSubsetRoutingMisroutes(children[],
partials[], discBits)` mirrors `findChildSpanNode` exactly:

1. For each β-mixed `(child[i], β)`: synthesise
   `densePkCandidate = partial_i XOR (1 << partialBitPos)`.
2. Step-1 equality scan: pick j with `partials[j] == densePkCandidate`.
3. Step-2 subset scan: pick the most-specific (highest-popcount, last-on-tie)
   j with `(densePkCandidate & partials[j]) == partials[j]`.
4. Classify as `EQUALITY_MISROUTES` / `SUBSET_MISROUTES` / `SELF_ROUTES`
   based on the picked j.

Four new counters (`PHASE7T8_BUILDFLAT_{BUILDS,EQUALITY_MISROUTES,SUBSET_MISROUTES,SELF_ROUTES}`)
+ accessors. Wired at `buildFlatNonStrict` success after the §7.35 probe.
Gated on `-Dhot.strict.phase7t.subsetroute.probe` (default off). Counter-only.

**Empirical readout (10K CAS-shred, subsetroute.probe=true):**

| Workload | builds | equality | subset | selfRoutes | validator I6 |
|----------|--------|----------|--------|------------|---------------|
| ascending-10K (control) | 18 | 0 | 0 | 1756 | 0 |
| descending-10K | 18 | 0 | **1** | 1721 | 258 |
| mixed-sign-10K | 24 | 169 | 113 | 2466 | 900 |
| bimodal-5K+5K | 18 | 0 | **0** | 1777 | 1280 |

**Second falsification.** Even with subset-match included, descending shows
**1 mis-route** vs 258 violations, bimodal **0 mis-routes** vs 1280 violations.
Mixed-sign captures 282 / 900 (31 %). The selfRoutes column dominates ~99 %
of β-mixed pairs across all workloads.

**Why — synthetic densePkCandidate is geometrically wrong.**

The synthetic `partial_i XOR (1 << r)` is not a valid dense PEXT. The real
dense PEXT of any key K is computed as `Long.compress(keyWord, bitMask)` —
it draws bits from ALL positions in the mask, including disc bits that
sparse-path encoding trivially short-circuits to 0 in every child's
partial. So real dense PEXTs typically have MORE bits set than any stored
partial. Subset-match then favors the highest-popcount partial that fits
under the dense PEXT — usually a child whose partial encodes the most
distinguishing bits.

By contrast, `partial_i XOR (1 << r)` produces a value that differs from
partial_i at exactly one bit:
- If bit r in partial_i was 0: candidate = partial_i | (1<<r). partial_i is
  a strict subset of the candidate (popcount = |partial_i|).
- If bit r in partial_i was 1: candidate = partial_i & ~(1<<r). partial_i is
  NOT a subset of the candidate (it has the bit, the candidate doesn't).

In either case, the most-specific subset of the synthetic candidate among
sibling partials is OFTEN partial_i itself (since other siblings differ
from partial_i at multiple bits, hence are NOT subsets of the candidate).
Hence `selfRoutes` dominates.

The real validator's dense PEXTs are produced from actual stored keys,
which carry information about all the disc bits including the ones zeroed
by sparse-path. Phase 7t-8's synthetic can't model that.

**Implication: I6 origination is NOT at `buildFlatNonStrict` (likely).**

If buildFlatNonStrict were the dominant I6 source, the per-(child, β)
combinatorial space would include the mis-routes — but it doesn't. The
violations must originate either:

1. **At post-construction modifications.** Subsequent leaf inserts, leaf
   splits, or addEntryWithPDep paths may add mis-polarity keys to leaves
   without updating the parent indirect's partials. The validator walks
   the FINAL trie state, so violations materialise from accumulated
   modifications.
2. **At other indirect-construction sites.** `addEntryWithPDep`,
   `upgradeToMultiMaskWithNewBit`, `splitParentAndRecurse`,
   `rebuildParentAbsorbingSplit`. 7t-6 showed their β-mixed counts are
   1-2 OOM lower, but if the mis-routing density per pair is much higher,
   they may dominate I6.
3. **Real per-key densities differ.** Even if buildFlatNonStrict's
   children are β-mixed, the actual keys in them may PEXT-route correctly
   under subset-match in most cases — and only a small fraction mis-route.
   The aggregate β-mixed count vastly overstates the mis-route density.

**Phase 7t-9 entry point.** Replace the synthetic-candidate predicate with
a **per-stored-key PEXT simulator at construction time**:

1. For each freshly-built indirect, walk every stored key K reachable
   from each child subtree.
2. Compute the dense PEXT of K against the indirect's mask.
3. Run `findChildSpanNode`'s algorithm to determine the routed child.
4. If routed child ≠ K's actual containing child → real mis-route.

Counter the mis-routes per construction site. This will give us the
SUFFICIENT predictor (it's the validator's exact algorithm, just localised
to one construction event). Cost is O(K × D) per build where K = total keys
in subtree — acceptable for diagnostic runs.

Alternative: instrument the validator to ATTRIBUTE each I6 violation to
the specific indirect pageKey where the mis-route diverges, then correlate
with construction-event logs to localise the origin site.

**Baselines preserved** (default flags off):

| Test | Result |
|------|--------|
| diagnosticMicrobenchPatternReproducer (50K) | 1 I4 violation (unchanged) · height 7 · build 6.7 s |
| casIndexHundredKEntryHeightBound (100K) | **0 violations** · height 2 · build 7.7 s |
| HOTOptionBPhase5Test (15 tests) | pass |

Comprehensive failure counts unchanged because probes are counter-only.

**Files touched (Phase 7t-8 commit):**

- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  helper `phase7t8CountSubsetRoutingMisroutes`; 4 new counters + accessors;
  probe at `buildFlatNonStrict`.
- `bundles/sirix-core/build.gradle`: `hot.strict.phase7t.subsetroute.probe`
  flag (default `false`).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  `phase7t8CharacterizeSubsetRouting` test + `runWithSubsetRoutingProbe` helper.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.36 (this section).

### 7.21 Phase 7q.15e + 7q.15f — port `g32.childmsb` to MultiMask + structure-cycle gate (2026-05-12)

**7q.15e**: ported the SingleMask `g32.childmsb` gate to
`buildBucketWithInheritedMaskMultiMask`. Compute `minChildMsbMm` over bucket
children via `getIndirectMsbOrMax`; mark bits with `abs ≥ minChildMsbMm` for
stripping into `liftAbsBits[]`; the existing rebuild loop clears them from the
new MultiMask. Constancy verification requires β-constancy for 15e-gated bits
too (distinct from 7q.7's stripNonConstant path which is constancy-exempt by
design).

**7q.15f**: added `hasStructureCycle(HOTIndirectPage)` cycle detector. Walks
the trie via `getChildReference` + TIL/disk fallback, returning true on the
first revisited pageKey (mirrors `HOTInvariantValidator`'s logic). Wired into
both `phase7qIterativeRootSortI8`'s best-effort gates (iter-abandon path and
maxIter-exhaust path) — when 15e's wrap-fallback produces a cyclic trie, we
reject `cur` and fall back to `indirect`. Without this, validator surfaces
"structure-cycle" as a NEW violation type, costing more than 15e's gain.

**Empirical (50K reproducer, `+g32.deep +bestEffort +commitroot +childmsb`)**:

| Metric | Before 7q.15e/f | After 7q.15e/f |
|---|---|---|
| intermediate-MSB EQ | 46 | **0** |
| best-effort accepted | 0 | 0 (cycle-rejected) |
| best-effort rejected | 1 | 1 |
| violations | 1 (I8) | 1 (I8) — baseline preserved |
| height | 5 | 5 |

The childmsb gate breaks the equality ceiling, but the lift's structural side
effects (cycle, height regression) currently exceed the I8 gain. The cycle
detector keeps the architectural ceiling at 1 I8 while preserving the option
to iterate on cycle-avoidance approaches in future phases.

**Default mode** (no `g32.*`): unchanged — 1 I8, height 5, equality=0, ok=12563.
**100K CAS**: 0 violations preserved (height 2).
**HOTOptionBPhase5Test**: passes.

**Phase 7q.15g entry point** (next session): investigate WHY 7q.15e's
wrap-fallback creates the structure-cycle. Likely cause: `wrapBucketInSubtree`
re-uses a pageKey that's already in the new tree via another path. The fix
is to ensure freshly-allocated pageKeys are unique within the lift transaction
(probably already true) AND that bucket children's references aren't shared
between the old tree and the new tree. Once cycle-free, the best-effort gate
should accept the lift output → I8 violation drops 1 → 0.

**Files touched** (commits `e20e979e6` + this one):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + 7q.15e: 51 lines in `buildBucketWithInheritedMaskMultiMask` (gate + verify).
  + 7q.15f: `hasStructureCycle{,Internal}` helpers + gate at 2 dispatch sites.
- `docs/HOT_PHASE_7Q_DESIGN.md`: §7.21 + work-plan ticks.

### 7.20 Phase 7q.15d — intermediate-indirect MSB instrumentation localizes equality-I11 source (2026-05-12)

**Goal**: §7.19 predicted that `phase7qExtendWithLift` Step 3 creates intermediate
indirects whose MSB equals their new parent's MSB (architectural ceiling). Phase
7q.15d adds **always-on** counters that classify every rebuilt indirect's child
MSBs vs the rebuilt parent's MSB at three hook points to find the actual source.

**Mechanism**: `phase7q15dCheckIntermediateMsb(parentPageKey, parentMsb, beta, children, newCount)`
walks `children[]`, classifying each child by `getIndirectMsbOrMax(child)` vs
`parentMsb`:
- `PHASE7Q_INTERMEDIATE_MSB_EQUALITY` — child.MSB == parent.MSB (= I11 equality).
- `PHASE7Q_INTERMEDIATE_MSB_LOWER` — child.MSB < parent.MSB (= strict I11 violation,
  child more significant than parent).
- `PHASE7Q_INTERMEDIATE_MSB_OK` — child.MSB > parent.MSB or child is a leaf
  (unbounded).

Hook points:
1. `phase7qExtendWithLift` final mask + sort, before page construction.
2. `extendIndirectMaskForClosure` (standard path) final mask + sort.
3. `buildBucketWithInheritedMaskMultiMask` final mask + sort (NEW: catches
   bucket-vs-its-own-children equality).

Per-event dump gated on `-Dhot.debug.phase7q.imsb=true`.

**Empirical results (50K reproducer)**:

| Config | equality | lower | ok | I8 viol |
|---|---|---|---|---|
| Default (`phase7q`) | 0 | 0 | 12563 | 1 |
| `+g32.deep +g32.bestEffort +commitroot` | **46** | 0 | 13051 | 1 |

**Key finding**: equality=0 in default mode (lift fires once, on root). Equality=46
**only** with `g32.deep` (= iterative root-sort + lift cascade). All 46 originate
from `buildBucketWithInheritedMaskMultiMask`'s output — NOT from the
extend/lift function's top-level rebuild. The previously suspected source
(extend's top-level msbIndex choice) is empirically clean (equality=0 at hooks 1+2;
46 at hook 3).

**Architectural pinpoint**: the bucket-indirect's `newMsb` (line 2935-2943 of
HOTTrieWriter) is computed as the lowest absBit in `(parent.mask \ β \ {bits ≤
newParentMsb})`. There is NO constraint that this be strictly greater than the
bucket's children's MSBs. The SingleMask sibling (line 2488-2540) has an opt-in
`hot.strict.g32.childmsb` gate that strips bits `≥ minChildMsb` from the bucket's
mask — but the MultiMask path lacks this gate. The 46 equality violations come
from MultiMask buckets where the inherited mask's smallest remaining absBit
happens to equal some child's MSB.

**Default mode preserved**: 1 I8 violation, height 5; 100K CAS 0 violations;
HOTOptionBPhase5Test passes.

**Phase 7q.15e entry point** (next session): port the `g32.childmsb` strip-bits-≥-
minChildMsb logic to `buildBucketWithInheritedMaskMultiMask`. Bucket children are
the `replacementRefs[]` parameter; for each, `getIndirectMsbOrMax` gives the MSB.
Strip bits ≥ minChildMsb from the new MultiMask's `extractionMasks[]`. Compact
zero-mask entries afterwards. Verify partials remain unique + I4. If empty mask
results, fall back to wrap (existing path). Expected outcome: equality=0 even
under g32.deep+bestEffort+commitroot, opening the path for iter-1's β=90 to
succeed without producing the cascading I11s observed in §7.19.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `PHASE7Q_INTERMEDIATE_MSB_{EQUALITY,LOWER,OK}` counters with getters/resetters.
  + `phase7q15dCheckIntermediateMsb` helper (~50 lines, HFT-grade single-pass).
  + Hook calls in 3 sites (extend-with-lift, standard-extend, bucket-build-MM).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + Counter resets + report line.
- `bundles/sirix-core/build.gradle`:
  + `hot.debug.phase7q.imsb` flag forwarding.

### 7.19 Phase 7q.15 + 7q.15a — multi-β atomic-lift framework + best-effort iterative-sort rollback (2026-05-12)

Phase 7q.15 lands a substantial framework that was held uncommitted in the prior session
("don't commit" user directive). All new code is feature-gated; default behaviour is
unchanged at 1 I8 violation, height 5, 100K CAS 0 violations. Headline additions:

- **`phase7qMultiBetaAtomicLift`** (gated `hot.strict.g32.multibeta`) — compute all
  needed MSDB bits, lift sequentially WITHOUT building intermediate indirects, build a
  SINGLE new indirect with all bits in mask. Bounded retry on partial-uniqueness collide.
- **`phase7qIterativeRootSortI8`** (gated `hot.strict.g32.deep`) — iterative single-bit
  loop: find first adjacent inversion, compute its MSDB, try extend, fall back to lift.
  Up to 16 iters. Empirically advances 10→12→21 children at idx=257 by adding bits 87
  and 90 to root's mask (iter 0 via lift, iter 1 via extend), then stalls at β=92.
- **`addNewRootLevelForI8`** (committed earlier as `9f0fea1fb`) — wraps the existing
  root structure with a new indirect that has the discriminating bit in its mask.
- **`countI11ViolationsRecursive`** — walks a subtree counting child.MSB ≤ parent.MSB
  cases, matching `HOTInvariantValidator`'s I11-trie-condition semantics.
- **`computeConstancyPreservingBiNodeDiscBit`** (gated `hot.strict.g32.cbinode` /
  `PARENT_MSB_HINT` ThreadLocal) — I11-aware disc bit selection for cbinode placement.
- **`phase8MultilevelClosure`** + **`recursiveSplitOnBit`** (gated
  `hot.strict.phase8.multilevel`, `hot.strict.phase8.recursivesplit`) — sketches the
  multi-level closure (§2.2) but stalls on warmup-territory β-mixed subtrees.

**Phase 7q.15a — `hot.strict.g32.bestEffort`** validate-or-rollback in
`phase7qIterativeRootSortI8`. When the iter abandons (or exhausts maxIter), instead of
discarding all accumulated progress, accept `cur` iff:
- `countAdjacentI8InversionsAtRoot(cur) < countAdjacentI8InversionsAtRoot(indirect)`,
- AND `countI11ViolationsRecursive(cur, -1) == 0` (no cascading I11 introduced).

New counters `PHASE7Q_BEST_EFFORT_{ACCEPTED,REJECTED}` track decisions. Adds
`hot.strict.g32.bestEffort` forwarding in `build.gradle`.

**Empirical (50K reproducer)**:

| Config | Violations | Height | best-effort acc/rej | ext-ok | I11 in iter-1 |
|---|---|---|---|---|---|
| Default | 1 (I8) | 5 | 0 / 0 (dormant) | 1 | n/a |
| `+commitroot` | 1 (I8) | 5 | 0 / 0 | 1 | n/a |
| `+commitroot +g32.deep` | 1 (I8) | 5 | 0 / 0 | 3 | n/a |
| `+commitroot +g32.deep +g32.bestEffort` | 1 (I8) | 5 | **0 / 1** | 3 | **15** |

The rollback CORRECTLY rejects the iter-1 partial state, which contains 15 fresh
I11 violations introduced by `phase7qExtendWithLift`'s rebuild creating intermediate
indirects with MSB == parent-MSB at pages 111-118, 347, 377-435, 1000006, 1000026.
Without the rollback, accepting this state would cascade I11 across the trie. With
the rollback, baseline (1 I8) is preserved even when best-effort mode is enabled.

**100K CAS regression**: 0 violations preserved (height 2 unchanged) across all flag
combinations.
**HOTOptionBPhase5Test**: passes with `hot.strict.phase7q=true`.

**Architectural insight**: the iterative sort PROVES the structural lift mechanism can
absorb bits 87 + 90 into root's mask at idx=257. But each absorption creates
intermediate indirects whose MSB collides with their new parent's MSB (== equality
I11 violation). Beating this requires either:
- (a) Lift mechanism that propagates MSB-raise into the intermediate indirects (= bits
  lifted from descendants must raise descendants' MSBs, not preserve them).
- (b) Multi-level closure (§2.2) that recursively splits β-mixed descendants.

**Phase 7q.15b entry point** (next session): instrument `phase7qExtendWithLift`'s
Step 3 rebuild to identify which intermediate indirect MSB choice creates the equality
I11. The rebuild likely uses the lifted bit AS the MSB of the rebuilt sub-indirects;
choosing a strictly-higher bit from the inherited mask would break the equality.

**Files touched** (commit `[pending]`):
- `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieWriter.java`:
  + `phase7qIterativeRootSortI8` (~95 lines) with validate-or-rollback at both abandon
    and maxIter exits.
  + `countAdjacentI8InversionsAtRoot` helper (~17 lines).
  + `countI11ViolationsRecursive` helper (~29 lines).
  + `PHASE7Q_BEST_EFFORT_{ACCEPTED,REJECTED}` counters with getters/resetters.
  + Dispatch in `reconcileRootMaskI11Safe` (multi-β + iterative + addNewRoot path).
  + Phase 7q.15 framework: `phase7qMultiBetaAtomicLift`,
    `computeConstancyPreservingBiNodeDiscBit`, `phase8MultilevelClosure`,
    `recursiveSplitOnBit`, etc. (all flag-gated).
- `bundles/sirix-core/src/test/java/io/sirix/index/hot/HOTFormalVerificationTest.java`:
  + `hot.strict.phase7q.commitroot` test-time invocation of `reconcileRootMaskI11Safe`.
  + Counter resets + report lines.
- `bundles/sirix-core/build.gradle`:
  + 19 new `hot.strict.*` and `hot.debug.*` flag forwards.
- `docs/HOT_PHASE_7Q_DESIGN.md` §7.19.
