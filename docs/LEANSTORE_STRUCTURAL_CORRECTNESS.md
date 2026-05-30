# LeanStore Structural Mutation Correctness

## Scope

This document specifies correctness invariants for fixed-stride structural mutation (`72B` slots)
and gives a proof sketch for hash/descendant updates that now use direct slot access where possible.

It is a formalized engineering proof sketch, not a machine-checked theorem proof.

## Model

- Page kind: `KeyValueLeafPage` in `DOCUMENT` and `CHANGED_NODES` indexes.
- Mutable structural state for slot `s` is in `slotMemory` at
  `base = s * StructuralSlotLayout.SLOT_STRIDE`.
- Structural fields are:
  - parent, left/right sibling, first/last child
  - childCount, descendantCount
  - hash
  - nodeKind, revision metadata
- Optional mirror object: `records[s]` (may be `null`).

## Invariants

Let `P` be a page and `s` a slot.

1. Slot population invariant
   - If slot bitmap bit `s` is set and fixed-stride is active, structural slot bytes for `s`
     are either:
     - already populated in `P.slotMemory`, or
     - copied from the complete page before direct mutation.

2. Direct-write mirror invariant
   - For direct setters (`setHashDirect`, `setDescendantCountDirect`, sibling/parent setters):
     - `slotMemory` is updated.
     - If `records[s] != null` and has the field, mirror object is updated to same value.

3. Read-authority invariant
   - `getHashSynced`/`getDescendantCountSynced`/`getParentKeySynced` return:
     - object value when `records[s]` exists (authoritative in mixed mode),
     - otherwise fixed-stride memory value.

4. Copy-on-write structural availability
   - `prepareSlotForMutation` returns a valid `(page, slot)`.
   - If structural bytes are missing in modified page, it first attempts structural-copy from
     complete page.
   - If that is unavailable, it may use a bounded bootstrap compatibility materialization
     for very early page-0 keys (`<= 8`) to seed fixed-stride headers.
   - For all other cases: legacy fallback materialization (or fail-fast in strict mode).

5. Hash path invariant (rolling mode)
   - Each touched ancestor hash is computed from previous hash and delta terms exactly once.
   - Write of new hash is atomic at slot granularity (single 8-byte field write in memory segment).

6. Descendant-count invariant
   - For add/remove propagation, ancestor descendant count is adjusted by exactly
     `startDescendants + 1` with sign according to operation.

## Proof Sketch

### Lemma A: Direct mutation preserves mixed-mode consistency

Given direct setter call for slot `s`:
- setter writes `slotMemory` field at deterministic offset.
- if `records[s]` is present and type supports the field, setter updates object field to same value.
Therefore object and memory views remain equal for that field, preserving Invariant 2.

### Lemma B: Hash read/write correctness under mixed mode

`getHashForMutation(nodeKey)` calls `prepareSlotForMutation` and then:
- fixed-stride: `getHashSynced(slot)` (object if present, else memory),
- fallback: object record hash.
`setHashForMutation` writes to same logical field through direct setter (fixed-stride) or object setter.
By Lemma A and Invariant 3, read-modify-write uses authoritative source and preserves equality.

### Lemma C: Ancestor propagation formulas unchanged

In rolling add/remove/update methods:
- only storage access strategy changed (object vs direct slot),
- arithmetic formulas for new hash and descendant deltas are unchanged.
Thus algebraic behavior is identical to prior implementation.

### Theorem: Structural/hash correctness for fixed-stride hot paths

Assume:
- `prepareSlotForMutation` returns a valid slot and satisfies Invariant 4.
- Node traversal order and parent links are valid.

Then for each touched node during rolling hash/update/remove/add:
- resulting hash equals prior algorithm's result,
- resulting descendant count adjustments equal prior algorithm's adjustments,
- memory and optional object mirrors stay consistent.

Hence behavior is preserved while reducing object materialization in fixed-stride paths.

## Corner Cases

1. Legacy page without populated structural bytes
   - Non-strict mode: compatibility fallback materializes record and mirrors into slot memory.
   - Strict mode (`sirix.leanstore.strictStructuralMutation=true`): bounded bootstrap compatibility
     may still be used for very early page-0 keys; other cases throw fail-fast exception.

2. Mixed mode (`records[s]` still present)
   - Synced reads prefer object to avoid stale memory reads.
   - Direct writes mirror object to avoid divergence.

3. Non-structural nodes
   - Descendant updates guarded by `startNode instanceof StructNode`.
   - Hash updates still applied for value nodes.

4. Full versioning path (`complete == modified`)
   - Structural copy and fallback logic still valid; no extra assumptions on distinct pages.

## Performance Notes

- This refactor removes many `prepareRecordForModification` calls on rolling hash paths.
- Remaining allocations are `SlotHandle` records plus bounded bootstrap/compatibility fallback when required.
- Enabling strict mode is recommended for profiling pure fixed-stride behavior.
