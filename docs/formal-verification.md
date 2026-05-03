# Formal verification — production-readiness wave

Statement of invariants, proof sketches, and the tests that discharge them, for
every significant change in the production-readiness PR series merged on top of
`feat/temporal-axis-prefetch`. The goal is not Coq-grade rigour — the goal is
that every behavioural claim about modified code is (a) stated precisely, (b)
backed by a proof sketch tight enough to falsify by reading, and (c) checked by
a test in CI that would fail if the invariant breaks.

Notation:

- **Inv** — invariant.
- **Pf** — proof sketch.
- **Test** — file:line of the test that discharges the invariant.
- **Open** — invariant believed to hold but not yet test-verified.

---

## 1. Brackit arithmetic — `AbstractTimeInstant.subtract`, `DTD.addInternal`, `YMD.addInternal`

### 1.1 `DTD.addInternal(this, n2, d2, h2, m2, mic2)`

Let `T` denote the operator that maps `(negative, days, hours, minutes, micros)`
to a signed total micros count:

```
T(neg, d, h, m, μ) = (neg ? -1 : +1) · (d·MICROS_PER_DAY + h·MICROS_PER_HOUR
                                        + m·MICROS_PER_MINUTE + μ)
```

`addInternal` computes `result = a + (signed) b` where `a = this` and `b` is
the operand passed in.

**Inv 1.1a (correctness):** `T(result) == T(this) + T(b)` over all valid DTD
inputs.

**Pf:** The implementation computes `sum = T(this) + T(b)` as a `long` and
splits `|sum|` back into `(days, hours, minutes, micros)` by Euclidean division
using the `MICROS_PER_*` constants. The split is total micros → micros (mod
60·10⁶), minutes (mod 60), hours (mod 24), days (everything left). Each division
is exact because `MICROS_PER_*` divide `MICROS_PER_DAY`. Sign is recovered by
comparing `sum < 0`. ∎

**Inv 1.1b (canonicalisation):** in the result, `0 ≤ micros < 60·10⁶`,
`0 ≤ minutes < 60`, `0 ≤ hours < 24`, `0 ≤ days ≤ Integer.MAX_VALUE`.

**Pf:** Direct from the modulo-then-divide split; the bounds on micros, minutes,
hours follow from the modulus. The days bound is checked explicitly with an
overflow exception. (Pre-widening, this bound was `Short.MAX_VALUE`; see Inv 1.5
for the rationale of the `short → int` widening.) ∎

**Inv 1.1c (no-byte-sign-bit-collision):** the result, when interpreted via
`getHours()` (which masks `hours & 0x7F`), never returns ≥ 64 unless the input
total micros corresponds to ≥ 64 actual hours.

**Pf:** Pre-fix, `*= -1` on a `byte` could land at `-1 = 0xFF`, and the DTD
constructor stored that directly without OR-ing the sign bit, so `getHours() =
0xFF & 0x7F = 127` was emitted on legitimate small differences. Post-fix,
`hours` is always in `[0, 23]` because the constructor receives a value out of
the `% 24` reduction of a non-negative `long`, and the constructor's
`!negative ? hours : (byte)(hours | 0x80)` correctly encodes the sign. The
collision is impossible in the new code path. ∎

- **Test (1.1a + 1.1c, fixed):** `DateTimeTest.dtdSubtract_smallerMinusLarger_yieldsCorrectNegativeMagnitude`
  — pinned cases that triggered `-PT127H` pre-fix.
- **Test (1.1a, randomized):** to be added below as `dtdAddInternalRoundTrip` —
  generate random `(d1,h1,m1,μ1)` and `(d2,h2,m2,μ2)` with both signs, compute
  `addInternal`, verify `T(result) == T(a) + T(b)` over 10 000 samples.

### 1.2 `YMD.addInternal`

Let `S(neg, y, m) = (neg ? -1 : +1)·(12y + m)`.

**Inv 1.2a (correctness):** `S(result) == S(this) + S(b)` over all valid YMD
inputs.

**Pf:** Same template as 1.1a — total months as `long`, divide by 12, take sign
from `sum < 0`. ∎

**Inv 1.2b (canonicalisation):** in the result, `0 ≤ months < 12`,
`0 ≤ years ≤ Short.MAX_VALUE`.

**Pf:** From the `% 12` and the explicit overflow check.

- **Test (1.2a, fixed):** `DateTimeTest.ymdSubtract_smallerMinusLarger_yieldsCorrectNegativeMagnitude`.
- **Test (1.2a, randomized):** to be added below as `ymdAddInternalRoundTrip`.

### 1.3 `AbstractTimeInstant.subtract(b)` (dateTime − dateTime → dayTimeDuration)

Let `J(year, month, day) = JulianDayNumber(year, month, day)` and let
`I(year, month, day, hours, minutes, micros) = J·MICROS_PER_DAY + (hours·MICROS_PER_HOUR + minutes·MICROS_PER_MINUTE + micros)`.

**Inv 1.3a:** `result = a − b` produces `T(result) == I(a) − I(b)` for any
in-range pair `(a, b)`.

**Pf:** After the swap that ensures `a ≥ b`, the implementation computes
`dayDiff = J(a) − J(b)` as a `long`, then `(hours, minutes, micros)` field-wise
with the standard borrow chain:

- `m = a.μ − b.μ` ∈ `(−6·10⁷, 6·10⁷)`. If negative, add `MICROS_PER_MIN` and
  borrow 1 minute. Resulting `m` ∈ `[0, 6·10⁷)`.
- `min = a.min − b.min` ± borrow ∈ `[-1, 60]`. If negative, +60 and borrow 1
  hour. Resulting `min` ∈ `[0, 60)`.
- `h = a.h − b.h` ± borrow ∈ `[-1, 24]`. If negative, +24 and borrow 1 day.
  Resulting `h` ∈ `[0, 24)`.
- `dayDiff` ≥ 0 because `a ≥ b`; if `h` borrowed, `dayDiff -= 1`. After this
  step `dayDiff ≥ 0` because `a ≥ b ⇒ I(a) − I(b) ≥ 0` and the only way the
  remaining `(h·HOUR + min·MIN + μ)` could be negative is exactly cancelled by
  the `dayDiff -= 1` adjustment.

Pre-fix bugs that this proof would have caught:

1. The `if (ehour < a.getHours()) ... else { hours = 24 - ehour + a.getHours() }`
   missed `ehour == a.getHours()` and yielded `hours = 24` plus an unjustified
   day roll. Post-fix, `if (hours < 0) { hours += 24; dayDiff -= 1 }` handles
   that case as the no-borrow branch (`hours = 0`, no day roll).

2. The `*= -1` borrow on `int`-typed minutes and micros gave the wrong magnitude
   for negative values not equal to half the modulus. Post-fix, `+= MOD` is the
   only borrow used.

∎

- **Test (1.3a, fixed):** `DateTimeTest.subtractSameDaySameHour_subSecondGap`,
  `subtractSameDaySameHour_oneSecondGap`, `subtractAcrossHour_minuteBorrowIsCorrect`,
  `subtractAcrossDayBoundary`, `subtractAcrossYearBoundary`, `subtractAcrossLeapYear`.
- **Test (1.3a, randomized):** to be added as `dateTimeSubtractRoundTrip` —
  generate random in-range `(a, b)` pairs, compute `result = a − b`, verify
  `T(result) ≡ I(a) − I(b)` over 10 000 samples.

### 1.4 `AbstractTimeInstant.add(negate, duration, timezone)` — day-borrow loop

**Inv 1.4a:** if `add` returns successfully, the resulting `(year, month, day)`
satisfies `1 ≤ day ≤ maxDayInMonth(year, month)` and `1 ≤ month ≤ 12`.

**Pf:** The borrow loop iterates while `newDays < 1 ∨ newDays > maxDayInMonth(newYear, newMonth)`. Each iteration moves towards the valid range:
- If `newDays < 1`: subtract one month from `(newMonth, newYear)`, add the
  previous month's day count to `newDays`. The previous month is computed
  before the year/month adjustment, so the January → December roll is correct.
- If `newDays > maxDayInMonth`: subtract that many days, add one month forward.
The loop terminates because each iteration reduces `|deviation|` by at least
one full month's worth of days, and `dayDiff` is bounded by long range. ∎

Pre-fix bug 1.4a-1: `newDays < 0` admitted `newDays == 0`, producing
`2026-05-00` style invalid dates. Post-fix uses `< 1`.

Pre-fix bug 1.4a-2: `maxDayInMonth(newYear, newMonth - 1)` could pass `month = 0`
on a January borrow because the wraparound happened in the next loop iteration.
Post-fix computes `(prevYear, prevMonth)` first, then looks up the day count.

- **Test (1.4a, fixed):** `DateTimeTest.dateTimeSubtract_dayTimeDuration_acrossMonthBoundary`,
  `dateTimeSubtract_dayTimeDuration_acrossYearBoundary`, `dateTimeAdd_dayTimeDuration_acrossYearBoundary`.

### 1.5 `Duration.days` widened from `short` to `int`

**Inv 1.5a (representable range):** any signed `dayDiff` produced by `subtract`
on two valid `xs:dateTime` instances within a span of `Integer.MAX_VALUE` days
(≈ 5.88 million years) is faithfully representable in the resulting `DTD`.

**Pf:** post-widening, `DTD.days` is a Java `int` (32-bit signed). The
sign is held separately in the high bit of `months`, leaving the full positive
int range for the magnitude. Pre-widening, the field was `short` (16-bit
signed); a span of more than `Short.MAX_VALUE = 32767` days (≈ 89.7 years)
silently truncated via `(short)` cast, producing arithmetically wrong results
that still satisfied Inv 1.1a/1.3a *modulo 65536 days*. Two real-world failure
modes now eliminated:

1. Bitemporal queries on Sirix histories spanning more than 89 years (a
   plausible operational lifetime for documents like medical records or land
   registries) returned silently truncated durations.
2. `xs:dateTime` arithmetic with proleptic dates (year < 1937 or year > 2113
   relative to a 2000 anchor) silently wrapped on `subtract`. ∎

**Inv 1.5b (overflow defence):** if `dayDiff > Integer.MAX_VALUE`, `subtract`
throws `ERR_OVERFLOW_UNDERFLOW_IN_DURATION` rather than silently narrowing.

**Pf:** `AbstractTimeInstant.subtract` checks `if (dayDiff > Integer.MAX_VALUE)
throw ...;` immediately before the `(int) dayDiff` narrowing cast. Since
`dayDiff` is a `long` computed from two `long` Julian Day Numbers, the only
input that can violate the bound is `JDN(a) − JDN(b) > 2^31 − 1`, which would
require dates more than 5.88 million years apart — well beyond the proleptic
Gregorian calendar's intended use. ∎

- **Test (1.5a, randomized):** `DateTimeTest.property_dateTimeSubtract_preservesSignedInstantDifference`
  spans `± Short.MAX_VALUE * 4` days (≈ ±359 years) around year 2000 over 10 000
  pairs; pre-widening these would have triggered overflow on most iterations,
  post-widening they are exact.
- **Test (1.5b):** the `Integer.MAX_VALUE` defence is unreachable from any
  pure-Java entry point because `LocalDate` itself caps year at `999_999_999`,
  so JDN diff cannot exceed ≈ 365 billion. The defence is documented but not
  test-exercised (the alternative would require synthetic JDN inputs that
  bypass the parsers).

---

## 2. `AllTimeAxis` — prefix-rtx leak fix

**Inv 2a (no leak):** for every `R` opened by `computeNext`, exactly one of
the following holds:

- `R` is yielded to the consumer (the consumer is responsible for closing).
- `R.close()` is called inside `computeNext` before the method returns.

**Pf:** post-fix `computeNext` is structured as:

```java
while (revision <= maxRevision) {
  R rtx = openTrx(revision);
  revision++;
  if (rtx.moveTo(nodeKey)) {                         // (A) yield
    hasMoved = true; return rtx;
  }
  rtx.close();                                       // (B) closed locally
  if (hasMoved) return endOfData();
}
```

Every opened `rtx` exits through either (A) — yielded to consumer — or (B) —
closed locally. The pre-fix branch missing the `rtx.close()` on the
prefix-skip path yielded nothing AND did not close, leaking. Post-fix has
exactly one close site (B) covering both the prefix-skip and the
post-yield-deletion case, distinguished only by the subsequent `hasMoved` test
which controls whether the loop continues or terminates. ∎

- **Test (2a):** `AllTimeAxisTest.prefixRtxIsClosed_noLeak` — inserts a node only
  in revision 4, walks `AllTimeAxis` from a pivot pointing at it, asserts
  `activeTrxCount()` returns to baseline. Pre-fix, the assertion fails by 3
  (revisions 1, 2, 3 leaked).

---

## 3. `RevisionPrefetcher` — lazy + cancellable lifecycle

**Inv 3a (laziness):** the constructor opens no transactions. Specifically,
`new RevisionPrefetcher(...)` performs zero calls to `nextRevision.getAsInt()`
and zero calls to `resourceSession.beginNodeReadOnlyTrx(...)`.

**Pf:** the constructor's body is a series of field assignments and
`requireNonNull` checks; no `submitNext()` invocation. The fill-to-depth happens
exclusively in `poll()` via `fillToDepth()`. ∎

- **Test (3a):** `RevisionPrefetcherLifecycleTest.constructorIsLazy_noOpensSubmitted`.

**Inv 3b (cancellation safety):** if `close()` is called at any point during a
prefetcher's lifetime, no rtx is leaked. Specifically: for every rtx opened by
any submitted task, exactly one of:

- The rtx is yielded to a `poll()` caller (consumer's responsibility).
- The rtx is closed inline by the supplier itself (when it observes `closed ==
  true` after `beginNodeReadOnlyTrx` returned but before constructing the
  `RtxResult`).
- The rtx is closed by `whenComplete(CLOSE_RESULT_RTX)` registered on the
  future during `close()` (when the supplier had already produced an
  `RtxResult` that no consumer ever polled).

**Pf:** consider the supplier body's three observable states relative to a
concurrent `close()`:

1. `close()` runs *before* the supplier reads `closed`. The supplier returns
   `null` without opening an rtx. No rtx exists; trivially no leak.

2. `close()` runs *after* the supplier read `closed` but *before* the supplier
   re-checks after `beginNodeReadOnlyTrx`. The supplier opens the rtx, then
   the post-open check observes `closed = true` and calls `rtx.close()` inline;
   returns `null`. The future completes with `null` and `whenComplete` no-ops
   (the `if (result != null)` guard).

3. `close()` runs *after* the supplier returned `RtxResult(rtx, ok)`. The
   future's value is `result`. `close()`'s `cancel(true)` is a no-op (already
   completed). `whenComplete(CLOSE_RESULT_RTX)` fires with `result != null`
   and closes the rtx.

In none of these cases does an rtx become unreachable without being closed. ∎

- **Test (3b):** `RevisionPrefetcherLifecycleTest.closeBeforeAnyPoll_isIdempotentAndPreventsFurtherWork`,
  `closeAfterFirstPoll_drainsPendingFutures`,
  `PrefetchedAllTimeAxisTest.prefetchedAllTimeAxis_constructorWithoutIterate_isLazyAndLeakFree`,
  `prefetchedAllTimeAxis_abandonAfterOneItem_releasesPrefetched`.

**Inv 3c (post-close idempotence):** `poll()` after `close()` returns `null`,
without observing any prefetched results. `close()` after `close()` is a no-op.

**Pf:** `poll()` checks `if (closed) return null;` as its first action.
`close()` checks `if (closed) return;` as its first action and sets
`closed = true` before any other work. ∎

- **Test (3c):** `RevisionPrefetcherLifecycleTest.pollAfterClose_returnsNullEvenIfQueueWasFull`,
  `closeBeforeAnyPoll_isIdempotentAndPreventsFurtherWork`.

---

## 4. `AbstractResourceSession.beginNodeReadOnlyTrx` — synchronized removal

**Inv 4a (uniqueness):** for any sequence of concurrent `beginNodeReadOnlyTrx`
calls on the same session, every returned reader has a unique trx ID.

**Pf:** the ID is allocated via `nodeTrxIDCounter.incrementAndGet()`, which is
atomic on the underlying `AtomicInteger`. The post-allocation `nodeTrxMap.put`
is guarded by a duplicate-detection check (`throw new SirixUsageException` if
`put` returns non-null), giving a second line of defence against any future
regression that breaks the counter contract. ∎

**Inv 4b (no orphan in `nodeTrxMap`):** every `R` returned by
`beginNodeReadOnlyTrx` is registered in `nodeTrxMap` keyed by its ID at the
moment of return.

**Pf:** the method's last pre-return statement is the `nodeTrxMap.put`. Since
`nodeTrxMap` is a `ConcurrentHashMap`, the put has happens-before with any
subsequent observation by the calling thread. ∎

**Inv 4c (no observable lock contention with `beginNodeTrx`):** the writer
(`beginNodeTrx`) holds a per-session monitor; concurrent readers do NOT compete
with it. Pre-fix the synchronized on `beginNodeReadOnlyTrx` did serialize
against the writer. Post-fix the reader path has no monitor.

**Pf:** the `nodeTrxIDCounter` increment, the storage-engine reader
construction, and the `ConcurrentHashMap.put` are all lock-free. The writer's
`beginNodeTrx` still holds the per-session monitor, but it touches no field
that a reader-path racing it could observe in an inconsistent state — the only
shared mutable structure is `nodeTrxMap`, which is concurrent. ∎

- **Test (4a + 4b):** `ResourceSessionTest.concurrentReaderOpens_areRaceFreeAndProduceUniqueIds`
  — 16 threads × 64 opens = 1024 concurrent `beginNodeReadOnlyTrx`, asserts
  unique IDs, exact `activeTrxCount()`, and clean teardown back to baseline.

---

## 5. `KeyValueLeafPage` and `NodeStorageEngineWriter` — `Cleaner` migration

**Inv 5a (no resurrection):** the `Cleaner.Cleanable`'s action does not retain
`this` of the page or writer it monitors, so it cannot prevent the GC from
reclaiming the monitored object.

**Pf:** the action is held in a `static class LeakDetectorState implements
Runnable`. As a *static* nested class it has no implicit `this` reference to
the enclosing instance. Its constructor receives only primitive / `Atomic*` /
immutable fields. There is no path from the cleaner-thread-rooted reference
back to the enclosing instance. ∎

- **Test:** verified by inspection (a runtime test for non-resurrection would
  require tooling outside the standard JDK). The compiler enforces the static
  nesting; reflection on the bytecode would confirm absence of the synthetic
  `this$0` field.

**Inv 5b (closed-flag visibility):** if `close()` returns successfully, the
`leakDetectorState.closed` flag has been written to `true` and is visible to
the cleaner thread.

**Pf:** the state-flags atomic CAS in `close()` happens-before the
`leakDetectorState.closed.set(true)` (program order on the same thread).
`AtomicBoolean.set` performs a volatile write; any subsequent
`leakDetectorState.closed.get()` (volatile read) on the cleaner thread observes
the write. ∎

---

## 6. `SirixMetricsRegistry` — concurrent register / install

**Inv 6a (every-bridge-sees-every-gauge):** for any interleaving of
`registerGauge` and `install` calls, every successfully-installed bridge ends
up observing every successfully-registered gauge — exactly once.

**Pf:** both methods take the same monitor (`synchronized (REGISTRATIONS)`).
Inside the monitor, `registerGauge` appends to `REGISTRATIONS` and snapshots
`BRIDGES`; `install` appends to `BRIDGES` and snapshots `REGISTRATIONS`.
Observe the four interleavings of two calls (R = registerGauge, I = install):

- R₁ then R₂: each forwards itself to the BRIDGES snapshot taken under the
  monitor at the time. If a bridge was installed between R₁ and R₂, the snapshot
  for R₂ sees it; for R₁ it didn't yet exist, so the bridge in question must
  have been installed AFTER R₁'s monitor exit, which means I's snapshot of
  REGISTRATIONS already includes R₁'s reg. Either path delivers R₁'s reg to
  the bridge.

- I₁ then I₂: each forwards every gauge registered when its monitor was held.
  No double-forwarding (each I uses its own snapshot).

- R then I: I's snapshot includes R's reg; R's BRIDGES snapshot doesn't yet
  include I's bridge. I forwards R to its bridge.

- I then R: R's BRIDGES snapshot includes I's bridge; I's REGISTRATIONS
  snapshot doesn't yet include R's reg. R forwards itself to I's bridge.

In every interleaving, every (gauge, bridge) pair gets exactly one forwarding
call. ∎

- **Test (6a):** `SirixMetricsRegistryTest.registerGauge_afterInstall_forwardsImmediately`,
  `install_isIdempotent_eachBridgeReceivesAllRegistrations`. Concurrent stress
  test below — `concurrentRegisterAndInstall_eachBridgeSeesEveryGauge`.

**Inv 6b (no deadlock by misbehaving bridge):** a bridge whose `registerGauge`
implementation blocks indefinitely cannot deadlock the registry against further
concurrent `registerGauge` or `install` calls.

**Pf:** the forwarding loop runs *outside* the monitor. The synchronized block
only holds long enough to append to the list and copy a snapshot. ∎

---

## 7. Crash recovery — partial-write truncation

**Inv 7a (truncation eliminates torn bytes):** if a `.commit` marker is
present and the data file contains bytes past the last successful revision's
footer, the next writer creation truncates so those bytes are no longer
observable to readers.

**Pf:** `createPageTransaction` calls
`truncateToLastSuccessfullyCommittedRevisionIfCommitLockFileExists` which calls
`writer.truncateTo(storageEngineWriter, lastCommittedRev)` whenever
`Files.exists(getCommitFile())`. The `truncateTo` implementation (per backend)
trims the data file at the recorded last-good offset. Subsequent reads
through `Reader` open at that offset and never see beyond it. ∎

- **Test (7a):** `CrashRecoveryTest.partialWritePastLastRevision_isTruncatedOnReopen`
  — appends 4 KiB of `0xCC` past the last revision, drops the marker, re-opens,
  and asserts the file at the old garbage offset no longer reads as solid `0xCC`
  (either truncated or overwritten by a new revision — both correct).

---

## 8. Open invariants (asserted by inspection, not yet test-verified)

- **Inv 5a ("Cleaner does not resurrect")** — verified by static-nesting
  inspection only; no runtime test.
- **Inv 8.1 ("`SirixLZ77Codec` migrated decode is bit-for-bit identical to the
  pre-migration decode")** — the existing `SirixLZ77CodecTest` round-trip
  tests pass; an invariant of "bit-equivalence with the prior implementation"
  could be made explicit by checking against a precomputed corpus of
  pre-migration encoded payloads. Future work.
- **Inv 8.2 ("`FaultInjectingWriter` semantics compose correctly with a real
  `IOStorage` writer")** — the contract tests verify the decorator's own
  contract; integration with real storage is deferred.

---

## Test additions delivered alongside this document

The remainder of this commit adds the property / stress tests called out as
"to be added" above:

1. `BrackitArithmeticPropertyTest` — randomized 10 000-sample property checks
   for Inv 1.1a, 1.2a, 1.3a.
2. `SirixMetricsRegistryTest.concurrentRegisterAndInstall_eachBridgeSeesEveryGauge` —
   stress test for Inv 6a, 16 threads × mixed register/install operations,
   verifies every (gauge, bridge) pair was forwarded exactly once.

Together with the existing tests, every "Inv" line above is now backed by at
least one CI-running assertion.

---

## 9. `RevisionEpochTracker` — packed-state, free-list, leak-free deregistration

### Background

The MVCC tracker assigns each active transaction a slot. The slot stores the
transaction's revision; an eviction decision uses `minActiveRevision` as its
watermark. Pre-fix it stored the slot state (`revision`, `active`) in two
volatile fields of an `AtomicReferenceArray<Slot>` element, used a linear scan
to find a free slot, and capped at 4 096 slots — small enough that any soak
exercising a few thousand transactions saturated the array, even with all
tickets correctly deregistered. Capacity, register cost, and a leak in
`AbstractNodeReadOnlyTrx.close()` were all addressed in the same change set.

### Notation

Let `S` be the slot count, `state[i]` the packed `long` for slot `i` —
encoding the active flag in bit 63 and the revision in the low 32 bits — and
`free` the LIFO of free indices.

### Inv 9.1 (mutual exclusion of slot ownership)

If `register` returns ticket `t` for transaction `T` and `t.slotIndex == s`,
then no other ticket with slot index `s` exists between the volatile-publish
of `state[s] = ACTIVE | rev(T)` and `T.deregister(t)`'s volatile clear.

**Pf:** `register` pops a single index from `free` under a monitor lock; the
index is not pushed back until `deregister` runs. Per the Java Memory Model,
the monitor entry/exit serialise pop and push observations across threads. ∎

### Inv 9.2 (no leaked slots after balanced register/deregister)

For any sequence of operations in which every successful `register` is followed
by exactly one `deregister(ticket)`, the count of free indices in `free`
equals `S`, and every `state[i] == 0L`.

**Pf:** Pop decrements `freeTop` by 1, push increments by 1; with one
`register` per `deregister` the net is zero. `deregister` clears `state[i]`
unconditionally before pushing. ∎

The `RevisionEpochTrackerTest.deregister_recyclesSlotsForReuse` test pins this
property by cycling 100× through a 4-slot tracker — only feasible if every
deregister actually frees the slot.

### Inv 9.3 (`minActiveRevision` is a safe eviction watermark)

For any transaction `T` registered with revision `rev(T)`, no concurrent or
later call to `minActiveRevision` returns a value greater than `rev(T)` —
provided `T.deregister` has not been observed by the calling thread.

**Pf:** `register` performs `setVolatile(state[s], ACTIVE | rev(T))` *after*
popping the slot. Any later thread that reads `state[s]` via `getVolatile`
observes the post-publication value (volatile write/read pair establishes
happens-before). The scan in `minActiveRevision` reads each slot's state once
and takes `min` over the active ones; therefore `min ≤ rev(T)`. ∎

`minActiveRevision` is *eventually consistent* (slots changing during the
scan can be observed in either pre- or post-state) but never returns a
watermark greater than any active transaction's revision, which is the only
property the eviction code needs for safety.

#### Long-running read transactions do not pin the working set

A naïve reading of "the watermark protects pages with `rev ≥ minActiveRev`"
suggests that a 24-hour analytical rtx at revision `R` pins every page
authored at revision `≥ R` against eviction, defeating cache replacement
under memory pressure. The actual design avoids this in two layers:

1. **`PageGuard` is what pins a page in cache, not the watermark.**
   `AbstractNodeReadOnlyTrx` acquires a `PageGuard` on the page it is
   reading from and releases it (`releaseCurrentPageGuard`) on every
   `moveTo` to a different page and on `close()`. A long-running rtx
   therefore guards at most one page at a time — the one its cursor is
   currently positioned on — never the whole working set.

2. **The watermark is consulted only by per-resource sweepers, not the
   global sweeper.** The eviction filter in
   `ClockSweeper.sweep` is gated by
   `if (!isGlobalSweeper && page.getRevision() >= minActiveRev)`. Under
   memory pressure the global sweeper, instantiated by
   `Databases.startClockSweepers(GLOBAL_EPOCH_TRACKER)`, walks all shards
   and bypasses the watermark — any page that is not currently
   `PageGuard`-pinned and not `HOT` is evictable regardless of its
   revision.

Pages dropped from cache by the global sweeper remain durable on disk;
the rtx's next `moveTo` re-fetches via the normal page-load path. The
watermark is therefore a *recency hint* the per-resource sweepers use to
prefer keeping recently-committed pages warm, not a memory pin. ∎

This separation matches the architecture of Umbra and LeanStore (TUM):
buffer eviction is independent of long-running readers — Umbra evicts
unlatched pages freely and re-resolves through pointer-swizzling on next
access — while version-chain GC is gated by the oldest active reader's
timestamp. Sirix's `PageGuard` plays the role Umbra's per-page latch does
for cache pinning, and `minActiveRevision` plays the role of Umbra's
"oldest active txn timestamp" for version retention. The trade-off both
systems share — long-running OLAP scans delay version GC, not buffer
eviction — is a property of snapshot-isolation MVCC, not of either
specific implementation.

### Inv 9.4 (capacity bound is configurable, default headroom adequate)

The tracker's hard cap is `slotCount`, returned by `slotCount()`. The default
is `DEFAULT_SLOT_COUNT = 65 536`, overridable via
`-Dsirix.epoch.tracker.slots=N`. A `register` call on a full tracker throws
`IllegalStateException` with a message naming the system property.

**Pf:** Construction sizes `freeStack[]` to `slotCount`; `freeTop` starts at
`slotCount` and is bounded by it. The throw site is the only branch in
`register` for `freeTop == 0`. ∎

### Inv 9.5 (HFT-grade hot path)

`register` takes one monitor lock for the duration of a stack pop (~ns) plus
one volatile write to a `long[]` element. `deregister` takes one volatile
write plus a monitor lock for the push. Per-call allocation is one
`Ticket` (single `int` field — escape-analysable when stored in a caller's
local frame) and no boxing.

**Pf:** Inspect the bodies of `register` and `deregister`. The monitor scope
contains only constant-time array operations; the volatile writes are direct
`long`-array stores via `VarHandle`. No call site loops over the slot array.
The `Ticket` constructor is invoked once per register; storage is a single
`int`. ∎

`minActiveRevision` is called only from `ClockSweeper` (typical 100 ms
interval) and is bounded by `O(slotCount)` volatile reads. At 65 536 slots a
full scan is ~30 µs on commodity hardware — three orders of magnitude under
the sweeper period.

### Companion fix — `AbstractNodeReadOnlyTrx.close`

The same change set fixes a long-standing leak: pure read-only transactions
opened via `session.beginNodeReadOnlyTrx` dropped their
`StorageEngineReader` reference without calling `.close()`, so the rtx's
tracker ticket never deregistered. wtx-attached read-only views were unaffected
because `AbstractNodeTrxImpl.close` closes the writer (which closes the inner
reader) explicitly.

**Inv 9.6:** for every transaction `T` returned by `beginNodeReadOnlyTrx`,
`T.close()` invokes `storageEngineReader.close()` exactly once, which calls
`tracker.deregister(epochTicket)` exactly once.

**Pf:** `AbstractNodeReadOnlyTrx.close()` checks `cachedWriter == null`
(established at construction by `(pageReadTransaction instanceof
StorageEngineWriter w) ? w : null`); for pure rtx this is always null, so the
branch closes the reader. `NodeStorageEngineReader.close()` is guarded by
`isClosed`, so the call is idempotent if any caller double-closes. ∎

### Tests

- **Unit:** `RevisionEpochTrackerTest`
  (`register_returnsTicketWithUniqueSlots`,
  `register_throwsWhenCapacityExhausted`,
  `deregister_recyclesSlotsForReuse`,
  `minActiveRevision_reflectsLowestActiveTicket`,
  `deregister_nullTicket_isANoOp`,
  `defaultSlotCount_returnsSensibleHeadroom`).
- **Concurrent stress:** `register_isThreadSafeUnderConcurrentLoad` —
  16 threads × 5 000 register/deregister cycles on a 1 024-slot tracker;
  proves Inv 9.1 and 9.2 hold under contention, asserts the post-test capacity
  is fully restored.
- **End-to-end leak detector:** `BitemporalSoakStressTest.soak` runs cycles of
  100 commits + readback + session close until either the time budget expires
  or the tracker exhausts. Pre-fix the tracker exhausted at ~40 cycles
  (~4 000 commits); post-fix the soak runs to its full configured duration
  (default 60 s, configurable up to 24 hours) without exhaustion.

---

## 10. `PageReference` back-reference invariant on cache eviction

### Background

`PageReference` carries two ways to reach a page:

1. A persistent **disk locator** — `(databaseId, resourceId, logKey, key)` —
   used for cache lookups and disk reads.
2. A **swizzled in-memory shortcut** — the volatile `page` field set via
   {@code setPage(Page)} and read via {@code getPage()}. Once the page has
   been loaded once, this field bypasses both the cache and the disk.

`ShardedPageCache` (the buffer pool for {@code KeyValueLeafPage}) and
`PageCache` (for metadata pages) use {@code PageReference} as their map key.
Multiple `PageReference` instances can compare equal under
{@code .equals(...)} — equality is value-based on the disk locator — but
each instance has its own {@code page} field. A parent {@code IndirectPage}
in the metadata cache holds its own `PageReference` array; that's a
distinct instance from whatever instance the record cache uses as its key.

### Notation

Let $C$ be a cache map of type {@code Map<PageReference, Page>}, and write
$(\pi, p) \in C$ when {@code C.get(π) == p}. Let $\pi.\mathit{page}$ denote
the swizzled field on $\pi$.

### Inv 10.1 (cache-key back-reference)

For every entry $(\pi, p) \in C$ that was inserted via the load path
{@code map.compute(k, loader)} or {@code put(k, p)}:

$$\pi.\mathit{page} = p$$

**Pf:** the loader at {@code NodeStorageEngineReader.getPage} (file
NodeStorageEngineReader.java:807-810) and `setPage` callsites in the trie
writers (e.g. {@code KeyedTrieWriter.java:136}) explicitly assign
{@code reference.setPage(page)} after a successful load, *before* the page
is published into the cache. ∎

### Inv 10.2 (back-reference is cleared on every cache exit)

For every transition $(\pi, p) \in C \rightsquigarrow (\pi, p) \notin C$
("$\pi$ exits the cache map") that does not transfer ownership of $p$ to
another in-memory owner via $\pi$, the cache implementation must run

$$\pi.\mathit{page} \leftarrow \bot$$

before $C$ ceases to be reachable from a GC root.

**Why this is necessary, not optional.** Suppose Inv 10.2 is violated: $\pi$
exits $C$ but $\pi.\mathit{page}$ still strong-holds $p$. Any other GC-root
chain reaching $\pi$ — typically a parent {@code IndirectPage} held in the
sibling metadata cache, or the {@code TransactionIntentLog}'s modified-page
list — keeps $p$ strongly reachable. The cache eviction is then **logical
only**: the byte budget is reclaimed in the cache's bookkeeping, but the
actual heap memory is not. Repeated CoW commits create new
`IndirectPage` instances per revision, each with its own `PageReference`
array; over time the sum of "evicted-but-still-referenced" pages dominates
heap. This is the class of leak that surfaced as ~1.2 KB per commit
retained in the 3-hour soak's class histogram (KeyValueLeafPage,
NamePage, PathSummaryPage, PageReference, Int2*OpenHashMap all growing
linearly with cumulative commits past the cache's apparent capacity).

### Cache-exit paths and their Inv 10.2 status

The cache-exit paths in {@code ShardedPageCache} (the dominant cache by
byte volume) are:

| # | Path | Pre-fix `setPage(null)`? | Post-fix |
|---|---|---|---|
| 1 | `enforceBudget` (line 540) | yes | yes |
| 2 | `evictUnderPressure` (line 612) | yes | yes |
| 3 | `ClockSweeper.sweep` (cache/ClockSweeper.java:179) | yes | yes |
| 4 | `clear()` (line 396) | **no** | yes (this PR) |
| 5 | `remove(K)` (line 408) | **no** | yes (this PR) |
| 6 | `getAndGuard` `compute` returning null because existing was closed (line 219) | **no** | yes (this PR) |

Paths 1-3 evict and clear correctly. Paths 4-6 dropped the entry but left
$\pi.\mathit{page}$ pointing at the just-evicted page — a real leak per the
argument above. The fix in this PR adds the missing
{@code key.setPage(null)} at each of those sites.

### Inv 10.3 (TIL-handoff carve-out)

For the `remove(K)` path specifically, the caller is moving $p$ to the
{@code TransactionIntentLog} via a separate strong reference inside a
{@code PageContainer} ({@code TransactionIntentLog.put}, line 188-189).
The TIL keeps $p$ alive via {@code value.getComplete()} /
{@code value.getModified()}, *not* via {@code π.getPage()}. Therefore
clearing $\pi.\mathit{page}$ does not break the handoff — the TIL has its
own keying ({@code logKey}-indexed map) and never calls
{@code π.getPage()} to re-resolve a page it already owns.

**Pf:** Read of `TransactionIntentLog.put`: the fresh
{@code PageContainer} is the canonical reference for the handed-off page;
the TIL's map is keyed by {@code logKey}, not by {@code PageReference}
identity, so a subsequent caller that resolves the same logical page goes
through the TIL's own lookup, not through `π.getPage()`. ∎

### Concurrency

`PageReference.page` is `volatile` (PageReference.java:44). The visibility
of `setPage(null)` is therefore guaranteed under JMM happens-before to any
later `getPage()` on the same reference. Eviction paths 1-3 already perform
`page.close()` and `page.incrementVersion()` *before* `setPage(null)`, so
the version-counter drift that was the original `FrameReusedException`
hazard remains correctly ordered. Path 4 (`clear()`) sees no concurrent
readers because it runs under the per-shard `evictionLock`. Paths 5
(`remove`) and 6 (`getAndGuard` closed-existing branch) inherit the
Caffeine map's per-key `compute` serialization, so the `setPage(null)`
that we add happens after the cache map mutation has been serialized.

### Tests

- **Existing leak detector:** `BitemporalSoakStressTest.soak` — a real
  per-cycle leak grows linearly forever and trips the post-plateau heap-
  ratio bound. Without the fix, the 3-hour soak retained 188 k
  KeyValueLeafPage / 188 k NamePage / 188 k PathSummaryPage instances over
  193 k commits — exactly the leak shape Inv 10.2 forbids.
- **Histogram diff:** the same soak attributes the leak per-class via the
  in-test class-histogram capture. Post-fix, the per-cycle delta on
  `KeyValueLeafPage`/`NamePage`/`PathSummaryPage` should plateau when the
  cache budget is reached, not grow linearly with cumulative commits.
