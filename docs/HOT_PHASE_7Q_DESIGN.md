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
- [ ] 7q.6 — fix `splitIndirectOnBitForLift` build-violation cascade so multiple
  bits can be added to root without I6/I11 cascade.

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
