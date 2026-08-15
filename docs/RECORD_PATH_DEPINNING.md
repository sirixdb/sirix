# De-pinning the record-page read path

Replacing reference-counted `PageGuard` pinning on the record path with the stamp-validated
optimistic reads that `HOTLeafPage` and `HOTTrieReader` already use — the LeanStore/Umbra protocol,
where readers never write to shared state.

This document exists so the work can be resumed cold. It records what was measured, which designs
are dead and why, and what is left in the order it has to happen.

**Status: the reader-side protocol is finished and correct on both page types; the cursor rewrite it
was built for is measured as a LOSS and is not being done.** Read *The verdict* before starting on
the remaining list — the list is kept because the reasoning behind each step is still worth having,
not because the steps are queued.

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

## The verdict: the primitive wins, the cursor does not

The table above prices the two primitives against each other and the stamp wins on both axes. It
does **not** price the swap, because the two are not paid at the same rate, and that is what decides
it:

- **The guard is paid per PAGE SWITCH.** `moveToSingleton` has a within-page fast path that skips
  guard management entirely, and a record page holds 1024 records.
- **The stamp would be paid per VALIDATED READ.** A stamp proves nothing until it is validated and
  the value it protects must not escape before then, so every accessor reading through page memory —
  the four structural keys, the value, the name — needs its own validate.

The same benchmark's end-to-end stages, from the run recorded in its javadoc:

```
withinPageWalk        25.9 ± 1.6  ns/hop   the bind floor, zero guard traffic
pageSwitchWalk       247.9 ± 31.5 ns/hop   + guard pair + page re-resolution, EVERY hop
```

A walk with any locality pays the guard once per ~1024 hops — **0.03 ns/hop** amortized uncontended,
**0.9 ns/hop** at the contended 907 ns — and would pay a validate on every accessor of every node,
which at three to six accessors is **13–26 ns/hop added** onto a 25.9 ns/hop floor. That is a 50–100 %
slowdown of the dominant case to recover under a nanosecond. Even in the shape most favourable to
de-pinning — `pageSwitchWalk`, a page switch on *every single hop* — a node touching four structural
keys and a value pays ~22 ns to save 31.6, netting under 4 % of the hop.

Batching to one validate per `moveTo` does not rescue it, because it is not sound: the accessors read
*after* the validate. The only way to make one validate cover them is to copy the record out at bind
time, which is the design rejected below, and pricing it does not save the trade either — a ~48-byte
header copy plus one validate is, on an unmeasured but generous estimate, still ~6 ns added per hop
against ~0.9 ns removed.

**So the protocol is right where locality is absent and wrong where it is high.** `HOTTrieReader` is
the former: a descent resolves a *different* leaf at every level, and one validation covers a whole
positioning decision. A record cursor is the latter, by construction — 1024 records to a page is the
locality the page size exists to create.

What the numbers indict is the guard's **implementation**, not its existence. 31 ns for an
UNCONTENDED acquire/release pair is enormous for what it does: `tryAcquireGuard` is a `synchronized`
method delegating to `acquireGuard`, itself `synchronized`, around a volatile read and an
`AtomicInteger`; `releaseGuard` is a third monitor entry plus an atomic decrement plus another
volatile read. `KeyValueLeafPage` has nine further `synchronized (this)` regions on that same
monitor, so contention on any of them inflates the lock that every `tryAcquireGuard` then pays for —
which is the shape of both the 31 ns and the 907 ns. Folding the guard count into the existing
packed state word (`stateFlags` uses three bits of an `int`; a `long` carries the count in the high
half) makes acquire and release a single CAS with no monitor, recovers most of that on this path AND
on `NodeStorageEngineReader.getRecordPage`, which pays the same pair per page resolution, and **also
closes a TOCTOU the monitor is currently papering over**: `close()` reads the flags, then reads the
count, then CASes the closed bit, and only the monitor keeps an `acquireGuard` out of that gap. That
is the change the measurements actually point at, and it keeps pinning rather than putting a retry
loop under every accessor.

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

  `stampBindingGeneration` is bumped by `publishSlottedPage` and `setStampBaseSegment`, and checked
  **first** — ahead of both stamp kinds, so a page swapping between unbacked and frame-backed is
  covered like any other rebind.
- **`d725d97`** — the benchmark above.
- **The publication is a seqlock, not an invariant** (both page types). The generation is driven
  ODD before the segment moves and EVEN once it and the coordinates agree again, with a
  `storeStore` fence pinning the odd marker ahead of the segment store and an `acquireFence` in
  `validateStamp` keeping the reader's loads from sinking below its check. A single trailing
  increment is not equivalent and the difference is the whole proof: with no marker published
  *before* the segment store, a reader whose data load already returned the new bytes can still
  observe the old generation at validation time and certify bytes the publisher was mid-way through
  writing. With the odd marker fenced ahead of it, a data load that returned the new bytes proves
  the odd store was globally visible first, and coherence then forces the reader's validation load
  to see at least that value. `validateStamp` rejects an odd binding outright.
- **The coordinates are bound by the PUBLISHER, never by a reader** (both page types). Lazy binding
  on first `readStamp()` is one map probe cheaper for a page nobody ever stamps, but it puts a STORE
  on the reader side, and a reader's store cannot be ordered against a concurrent publisher's: a
  reader that computed coordinates from the old segment can land them *after* the publisher has
  reset them, leaving the OLD slot's coordinates describing the NEW segment — after which every
  later reader validates its reads against a counter belonging to memory it is not reading. Binding
  inside the rebind window costs one `ConcurrentHashMap` probe per segment swap — allocate, grow and
  bulk-copy, each of which already does one on the allocator's own map, and none of which is on a
  read path — and takes a branch and a store off the read path.
- **The old segment is released AFTER the new one is published, not before.** `growSlottedPage`,
  `copySlottedPageFrom` and `setSlottedPage` all released first, leaving a window in which the page
  still pointed at — and still bound its stamp coordinates to — a slot the allocator had already
  taken back and could hand to another page, while a reader's stamp over it still validated. Only
  `close` was covered, and only because it publishes `CLOSED_BIT` before releasing, which makes
  `readStamp` answer `STAMP_INVALID`; it keeps that order for the same reason. The reorder costs one
  extra live slot for the duration of a copy and leaves the page holding its previous segment rather
  than nothing if the allocation throws. `copySlottedPageFrom` also loses an intermediate
  `publishSlottedPage(null)` that was a second rebind for no reader's benefit, and `setSlottedPage`
  now decides whether to release by ADDRESS rather than by identity — a stored segment has been
  through `reinterpret`, which returns a fresh object over the same memory, so an identity test
  answers "different" for a caller re-publishing the very segment the page already holds.
- **`HOTLeafPage` carries the binding too, and `HOTTrieReader` carries it through.** This was the
  live one: `validateStamp(long)` took no binding, and it is shipped code behind every HOT index
  read. `HOTTrieReader.loadPage` now snapshots `readStampBinding()` immediately before `readStamp()`,
  stores both, retries when either is odd, and hands both to `validateStamp`. Every other consumer
  (`HOTRangeCursor`, `AbstractHOTIndexReader`, `ProjectionIndexHOTStorage`,
  `NodeReferencesSerializer`) validates through `validateCurrentLeaf()` and needed no change. Every
  `slotMemory` assignment now routes through `publishSlotMemory`, the way `slottedPage` routes
  through `publishSlottedPage`.

### The test limit, and the case that closed it

The previous pass recorded that deleting the generation check **failed nothing**, because after a
swap the coordinates were `UNBOUND` and `validateStamp` rejected on that alone, and after a re-bind
the old stamp failed only because two slots' versions happened to differ. That gap is now closed by
the **unbacked ↔ frame-backed swap**, which needs no coincidence:

`setStampBaseSegment` pointed at a mid-buffer slice — exactly what a zero-copy deserializer produces
— resolves to no allocator slot, so `readStamp` answers with the UNBACKED sentinel, whose validation
rule is "is the page still open?". Correct the base afterwards and the page IS frame-backed again.
Without the generation check that outstanding UNBACKED stamp validates on the strength of the page
merely still being open, certifying reads taken while the page could not detect a torn one at all.
`KeyValueLeafPageStampTest.swappingBetweenUnbackedAndBackedInvalidatesAnOutstandingStamp` and
`HOTLeafPageStampTest.settingTheStampBaseInvalidatesAnOutstandingStamp` both fail deterministically
when the check is removed — verified by mutation, not by assumption.

What still has **no** deterministic test is the odd-generation rejection inside `validateStamp`: a
single thread cannot schedule itself inside a publication window, and there is no interposition hook
on the publisher. `aPublishedBindingIsEven` pins the half that is observable — every binding a reader
can see outside the window is even, so a publication that bumped by one instead of two fails — and
the rejection itself rests on the argument above rather than on an assertion. Do not read the green
run as proof of it.

## Remaining, in order — NOT queued; see *The verdict*

Kept because each step's reasoning outlives the decision, and because a future workload with no
record-page locality would revive the question.

- **(a)** Guard-free page resolve on `NodeStorageEngineReader`: a `getRecordPage` variant that does
  not touch `currentPageGuard`.
- **(b)** Hold `(page, binding, stamp)` beside `currentSingleton` at `moveTo`; add
  `validateCurrentNode()`.
- **(c)** Convert the accessors to snapshot → read → validate → retry. The model is
  `HOTTrieReader.containsKey`: swallow `RuntimeException` unless the stamp validates, because a torn
  read can fabricate offsets and lengths, and bound the retries (`MAX_STAMP_RETRIES = 64`). **This is
  the step the measurement rejects** — it is where the per-read cost enters.
- **(d)** Retry path is the existing `moveToSingletonSlowPath` rebind.
- **(e)** **The two escape routes — both must be closed before the guard can go, and this is where
  scope grows.** `getRecordFromSlottedPage`'s non-singleton branch binds a flyweight directly to page
  memory and hands it to arbitrary callers (`CASIndex.exactMatches` among them); and
  `lookupSlotWithGuard` hands a guard to callers **by contract** — its javadoc instructs them to
  close it. Each needs a copy-out projection or its own validation.
- **(f)** Delete `currentPageGuard` / `closeCurrentPageGuard`. This also dissolves the
  `CASIndex.exactMatches` writer-backed carve-out, which exists only because an index scan inside an
  updating transaction cannot borrow the caller's reader while guards are in play. That carve-out is
  the one benefit of (a)–(f) the measurement does not weigh against; it is a correctness/coverage
  wart, not a throughput one, and it can also be fixed by giving `exactMatches` its own reader on the
  writer-backed path rather than declining to filter.
- **(g)** Re-run `CursorGuardCostBenchmark`'s `withinPageWalk` / `pageSwitchWalk` — the end-to-end
  cursor, not the primitive. **Answered ahead of the rewrite, in *The verdict*, from the stages the
  benchmark already measures.** A 190× gap on the primitive does not imply the same on a real
  `moveTo`, and it does not: the two are paid at rates that differ by three orders of magnitude.

## Next, if this is picked up again

Fold the guard count into the packed state word and re-run the two primitive stages. That is the
change the numbers point at, it keeps pinning, and it needs no reader-side retry loop. Two things to
respect while doing it: `close()` must keep its `synchronized` (it is cold, and the monitor also
excludes the class's other `synchronized (this)` regions from running against a teardown), and the
closed bit must be taken by a CAS that *requires* a zero guard count, which is what makes the
lock-free acquire safe against it.
