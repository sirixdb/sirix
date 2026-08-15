# De-pinning the record-page read path

Replacing reference-counted `PageGuard` pinning on the record path with the stamp-validated
optimistic reads that `HOTLeafPage` and `HOTTrieReader` already use — the LeanStore/Umbra protocol,
where readers never write to shared state.

This document exists so the work can be resumed cold. It records what was measured, which designs
are dead and why, and what is left in the order it has to happen.

## Why: measured, not argued

`CursorGuardCostBenchmark` (commit `d725d97`), on a 4-core box:

| | uncontended | 4 threads, one page |
|---|---|---|
| `guardAcquireRelease` | 31.0 ns | 907.4 ns |
| `stampReadValidate` | 4.7 ns | 4.8 ns |

Two results matter. Uncontended, the stamp is **6.6× faster** — a monitor plus an atomic RMW costs
far more than an acquire-load and a compare even with nobody to contend with. This was predicted to
be a wash; it is not.

Under contention the guard degrades to **29× its own uncontended cost**, because the pair is a
*store* to a line every reader of the page shares, so each acquire invalidates it in every other
core. The stamp is **flat** — 4.711 → 4.772 ns, inside measurement error. That flatness is the
entire thesis of the protocol, and it is now measured rather than asserted.

The 190× gap is at four cores and widens with the number of cores that must invalidate the line.
Thread count is deliberately matched to core count: oversubscribing measures the scheduler, not the
protocol.

## The design, and the two options that are dead

`AbstractNodeReadOnlyTrx` keeps **one singleton shell per node kind** and rebinds it on every
`moveTo` (`readSingletonBinder` / `writeSingletonBinder`). `createSingletonSnapshot()` deep-copies
only when a caller retains a node across a move.

That fact kills one option and collapses two others:

- **Copying fixed-width header fields into the shell at bind time — REJECTED.** There is no
  per-record allocation to piggyback on, so it would add a memcpy to every `moveTo`, which is
  precisely what the singleton design exists to avoid.
- **"Validate on access" and "retain nothing" are the same design here.** The cursor already retains
  nothing across a move — it re-derives its binding each `moveTo`, and callers that genuinely hold a
  node past a move already have a snapshot with no page pointer in it. So there is **no `NodeCursor`
  API change**, and the retry machinery (`moveToSingletonSlowPath`) already exists.

The window needing protection is therefore `moveTo(k)` → accessors → next `moveTo`, which is exactly
what the guard covers today.

One earlier plan stage was dropped as a **no-op**: converting the slot-population probe, the legacy
deserialize branch and the header reads saves nothing, because the pin is taken in `getRecordPage`,
*upstream* of all of them.

## Done

- **`d6fe90f`** — `KeyValueLeafPage` gains the stamp binding (`stampCoordinates`,
  `stampBaseSegment`, `readStamp`, `validateStamp`), bound lazily to `FrameSlotAllocator` slot
  versions, with the ABA ordering (version acquire *before* the closed check) mirrored from
  `HOTLeafPage`. All six `slottedPage` assignments route through `publishSlottedPage`, which drops
  the binding — a stale binding validates against a slot no longer being read.
- **`384e64b`** — the stamp is made **self-describing**, which was the precondition for any reader
  depending on it. A stamp is a per-slot sequence number and means nothing without its slot; two
  slots' counters are unrelated and can hold equal values at once. One `long` cannot carry both, and
  packing a truncated coordinate hash would leave a false validation per rebind at the hash's
  collision rate — not a rate a database may accept. The reader carries the binding instead:

  ```java
  long binding = page.readStampBinding();
  long stamp   = page.readStamp();
  // ... any number of reads ...
  if (!page.validateStamp(binding, stamp)) retry;
  ```

  `stampBindingGeneration` is monotonic, bumped in `publishSlottedPage` (last, after the coordinate
  resets) and in `setStampBaseSegment`, and checked **first** — ahead of both stamp kinds, so a page
  swapping between unbacked and frame-backed is covered like any other rebind.
- **`d725d97`** — the benchmark above.

### An honest limit on the test

Deleting the generation check **fails nothing** in `KeyValueLeafPageStampTest`. Straight after a
swap the coordinates are `UNBOUND` and `validateStamp` already rejects on that alone; after a
re-bind, the old stamp fails only because the two slots' versions happen to differ. The check
converts a coincidence into a guarantee, and no deterministic assertion can pin it. Do not read the
green run as proof of the check — the reasoning is recorded beside the assertions in the test.

## Remaining, in order

- **(a)** Guard-free page resolve on `NodeStorageEngineReader`: a `getRecordPage` variant that does
  not touch `currentPageGuard`.
- **(b)** Hold `(page, binding, stamp)` beside `currentSingleton` at `moveTo`; add
  `validateCurrentNode()`.
- **(c)** Convert the accessors to snapshot → read → validate → retry. The model is
  `HOTTrieReader.containsKey`: swallow `RuntimeException` unless the stamp validates, because a torn
  read can fabricate offsets and lengths, and bound the retries (`MAX_STAMP_RETRIES = 64`).
- **(d)** Retry path is the existing `moveToSingletonSlowPath` rebind.
- **(e)** **The two escape routes — both must be closed before the guard can go, and this is where
  scope grows.** `getRecordFromSlottedPage`'s non-singleton branch binds a flyweight directly to page
  memory and hands it to arbitrary callers (`CASIndex.exactMatches` among them); and
  `lookupSlotWithGuard` hands a guard to callers **by contract** — its javadoc instructs them to
  close it. Each needs a copy-out projection or its own validation.
- **(f)** Delete `currentPageGuard` / `closeCurrentPageGuard`. This also dissolves the
  `CASIndex.exactMatches` writer-backed carve-out, which exists only because an index scan inside an
  updating transaction cannot borrow the caller's reader while guards are in play.
- **(g)** Re-run `CursorGuardCostBenchmark`'s `withinPageWalk` / `pageSwitchWalk` — the end-to-end
  cursor, not the primitive. A 190× gap on the primitive does not imply the same on a real `moveTo`,
  and this is the check that says whether the win survives where it counts.

## Also outstanding

`HOTLeafPage` carries the **same latent hole**: its `validateStamp(long)` takes no binding, and its
`slotMemory` is likewise non-volatile. It is live behind `HOTTrieReader`, so it is shipped code and
wants its own pass, but the fix and the reasoning are identical.

Note also what currently makes `publishSlottedPage` safe: an **invariant**, not a fence. Every caller
is a write or teardown path, and teardown publishes `CLOSED_BIT` so `readStamp` answers
`STAMP_INVALID`. A volatile store has release semantics only — it does not stop the following plain
store to `slottedPage` from being hoisted above it. That invariant is adequate for the current
callers and is *not* adequate once step (b) puts a reader on this path; publishing the segment with
release semantics is the remedy.
