# HOT Phase 7q — Structural Lift Design

**Branch**: `fix/hot-strict-binna-conformance`
**Status**: 2026-05-11 — diagnostic phase complete (commit `ad4eb6990`).
**Goal**: eliminate the single marginal I8 violation by lifting a descendant-captured
discriminator bit to root via cascading split-and-promote.

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
- [ ] 7q.4 — validate on reproducer (1 → 0) — **PARTIAL**: lift fires successfully
  on the 50K reproducer, dropping LB-HARD rejections 64 → 1 and trie height 6 → 5,
  but 1 I8 violation persists at a NEW location after structural rearrangement.
  Need iteration: closure mechanism must re-attempt after lift surfaces new
  under-discrimination
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

**Flag-rollout plan.** Default OFF initially — preserves the current "1 I8 at
indirect 2" baseline so other tests that depend on it aren't perturbed.
Recommended ON together with `hot.strict.g32`, `hot.strict.g32.childmsb`,
`hot.strict.g32.multibeta`, `hot.strict.g32.deep`. Promote to default-ON after
broader regression validation.

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
