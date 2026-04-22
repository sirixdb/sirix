# iter#04 — Prefetcher-cap formal-verification analysis

Gate doc required before landing code. This answers the correctness, performance,
and rollback questions the iter#04 plan demands.

Referenced code (all in `bundles/sirix-core/src/main/java/io/sirix/access/trx/page/HOTTrieReader.java`):

- `prefetchPage(PageReference)` — line 515
- `prefetchSiblingWindow(HOTIndirectPage, int, int)` — line 405
- `navigateToLeaf(PageReference, byte[])` — line 221 (caller in the "forward hint" path)
- `navigateToLeftmostLeaf(PageReference)` — line 273 (caller, range-scan start)
- `advanceToNextLeaf()` — line 317 (caller, cursor forward)
- `descendToLeftmostLeaf(PageReference)` — line 359 (caller, descent after sibling advance)
- `loadPage(PageReference)` — line 472 (synchronous fall-back path on the read side)

Profile referenced: `profiling-output/iter04-dbopen-cpu.txt` and the iter#04 section
of `profiling-output/iteration-log.md`.

---

## A. Correctness

### A.1 Purpose of the prefetcher

`prefetchPage` fires a `Thread.startVirtualThread(Runnable)` whose body is:

```java
final Page loaded = storageEngineReader.loadHOTPage(ref);
if (loaded != null) {
  ref.setPage(loaded);
}
```

This is **fire-and-forget warm-up**. The caller never `join()`s the virtual
thread, never takes a completion handle, never reads state set by the task.
The task's sole side-effect is `PageReference.setPage(Page)` on a volatile
field.

The synchronous read path `loadPage(PageReference ref)` at line 472
already checks `ref.getPage()` first:

```java
final Page inMemory = ref.getPage();
if (inMemory != null) {
  return inMemory;
}
// ... storageEngineReader.loadHOTPage(ref)
ref.setPage(loaded);
return loaded;
```

**Therefore: the prefetch is a strict hint. If the prefetch task never runs,
never finishes, or is dropped entirely, correctness is preserved: the
synchronous reader will load the page on demand via the same
`storageEngineReader.loadHOTPage(ref)` call and will swizzle the same
immutable `Page` onto the same `PageReference.setPage` volatile. The
double-swizzle race is already documented at line 510-513 ("duplicate load is
benign") and is safe because `Page` instances for a given disk key are
structurally equivalent.**

### A.2 Throttling/dropping prefetch preserves the synchronous path

Given A.1, a bounded prefetch that `tryAcquire()`s a permit and skips the
whole task when no permit is available is equivalent to "prefetcher was too
slow and the synchronous path beat it to the page". Both yield: the page
gets loaded, the same `Page` ends up swizzled onto the `PageReference`,
the reader proceeds.

**Formal claim: for any concrete call graph through `HOTTrieReader`, the
observable HOT read results (i.e. the `MemorySegment` values returned from
`get(...)`, `containsKey(...)`, `range(...).next(...)`) are independent of
whether any particular prefetch task ran, got skipped, or ran partially.**

Proof sketch:
1. `get(rootRef, key)` calls `navigateToLeaf(rootRef, key)` which calls
   `loadPage(currentRef)` at each step.
2. `loadPage` is total: it synchronously loads the page if not already
   swizzled, and swizzles on success. Its return value is only a function
   of `(ref.getKey(), ref.getLogKey())` plus storage state — neither of
   which is touched by `prefetchPage`.
3. `prefetchPage` has no other observable side-effect than `setPage` on
   the same ref `loadPage` would also set. Since `setPage` is idempotent
   on the volatile field (page is immutable for a given disk key), it
   doesn't matter which call races first.
4. Therefore dropping the prefetch task entirely (or all of them) leaves
   step 1's result unchanged for every key.

### A.3 `tryAcquire` + skip is safe

`Semaphore.tryAcquire()` is non-blocking. Three states:

- `true` — permit obtained, run task, release on completion.
- `false` — no permit, do nothing. Caller's loop continues immediately;
  the caller is never on the critical path of the virtual-thread body.

The skipped task does not leak threads, buffers, or file descriptors
because **no virtual thread is ever started** in the skip branch.

**Formal claim: `tryAcquire → skip` cannot block, cannot cause a caller
to wait, and cannot mutate any non-volatile state other than the counter
inside the Semaphore itself.**

### A.4 Thread lifecycle and leak-safety

Concern: if the virtual thread body throws after `tryAcquire`, will the
permit leak?

Pattern used:

```java
if (!PREFETCH_LIMIT.tryAcquire()) {
  return;
}
Thread.startVirtualThread(() -> {
  try {
    final Page loaded = storageEngineReader.loadHOTPage(ref);
    if (loaded != null) {
      ref.setPage(loaded);
    }
  } catch (Throwable t) {
    // Swallow — prefetch is a hint, never propagates failures to the reader.
  } finally {
    PREFETCH_LIMIT.release();
  }
});
```

The `try/finally` guarantees `release()` runs on every exit path of the
virtual thread's body (normal return, any `Throwable` including `Error`).
The `Throwable` catch is deliberate: if `loadHOTPage` throws an
`IOException`/`SirixIOException`/`OutOfMemoryError` on a speculative read,
the synchronous read path at `loadPage()` will also fail when it later
tries to load the page, and the reader's caller is the right place to
propagate that.

Virtual threads are GC'd once the Runnable returns — no explicit cleanup
required. The Semaphore has bounded state (one int + FIFO queue of parked
threads); since we never `acquire()` (only `tryAcquire`), no threads ever
park on the Semaphore. No futex wait, no leak.

**Acquire-before-start ordering matters**: if we'd put `tryAcquire` inside
the virtual thread body, we'd pay the `Thread.startVirtualThread` cost
(≈ 1-2 μs carrier-thread handoff) even when we end up skipping. Acquiring
on the caller thread is the cheap path.

---

## B. Performance

### B.1 Optimal concurrency cap

Constraints that stack:

1. **`FileChannelReader.BUF_POOL` size = 2 × cores = 40** on this 20-core
   box. A prefetch task that gets a buffer from the pool blocks every
   other reader (`acquireBuffer` uses `poll` + fresh-alloc fallback, so
   it doesn't block — but the fresh alloc is a 128 KiB direct-memory
   allocation that's not free).
2. **NVMe command queue depth**: typical consumer NVMe is 32 queues × 1024
   entries; the Linux block layer exposes `nr_requests=128-256` by
   default. We don't need to saturate that — just avoid starving it.
3. **FileChannel.read thread model**: on Linux, JDK's FileChannel.read
   with a position arg is a `pread(2)` syscall. Each concurrent call
   holds a kernel thread (via the JDK's direct-I/O path) and can
   contend on the AIO ring or fall through to blocking `pread`. Past
   a few dozen concurrent calls, throughput flatlines and per-call
   latency degrades.
4. **Virtual-thread carrier pool**: JDK 21+ virtual threads share a
   small carrier pool (typically `ForkJoinPool.commonPool` size = cores).
   A blocking `pread` pins a carrier until the syscall returns.
   Hundreds of concurrent blocking `pread`s burn through carriers and
   the remaining virtual threads park in JVM-internal mounting queues.

**Sweet spot**: `min(2 * cores, 64)` = **40 on this box**.

- 40 = BUF_POOL size (we never starve the pool).
- 40 ≤ 64 (cap) and ≤ ~256 (kernel nr_requests) — plenty of headroom at
  the NVMe/OS layer.
- 40 ≥ 20 (vec-executor worker count) so each hydrate worker can have at
  least one prefetch in flight without contention.
- 2 × cores has historically been the choice for I/O-amplified pools
  (see BUF_POOL itself, AsynchronousChannelGroup's default).

**Why min with 64**: guards against oversized boxes (128+ core servers
where 2 × cores would be excessive for NVMe command queues).

### B.2 Does a cap hurt when prefetch was already landing pages?

The iter#04 profile shows:

- `HOTTrieReader.lambda$prefetchPage$0` = 23.19 % of CPU in first 2 s
- 74.75 % of wall samples = `__futex_abstimed_wait_cancelable64`

Of the 23.19 % CPU attributed to the prefetcher, much is inside
`FileChannelReader.read` / `loadHOTPage` / `decompressScoped` (the same
frames that show up without prefetch). The prefetcher is **doing
work, but most of it is being redundantly done by the synchronous
reader path** because the sync reader reaches the page before (or
very close to) the async prefetch returns.

Evidence:
- Per-level prefetch fans out `PREFETCH_WINDOW = 16` siblings; the
  synchronous reader is walking through those siblings one at a time
  at CPU-speed descent (a few μs per indirect-node hop).
- `FileChannelReader.read` at 24.04 % CPU samples + 327 wall samples
  (~12 %) on `loadHOTPage` suggests the page is hit hard either way.

Therefore capping at 40 **drops the fraction of prefetches that race
against the sync reader** — the skipped ones contribute essentially
zero real "warm ahead" benefit because the sync reader would have
loaded that page by the time the prefetch queue drained anyway.

**Formal claim: the skipped-prefetch pages are a subset of the pages
the synchronous reader would have loaded within ≤ 1 ms of the skip;
removing them reduces total parallel syscall pressure without reducing
the set of pages that end up loaded.**

### B.3 tryAcquire-skip vs acquire-block

| strategy | behavior under contention |
| --- | --- |
| `tryAcquire → skip` | Prefetch hint dropped; sync reader loads on demand. Never blocks caller. |
| `acquire → block`  | Prefetch queued; when permit available, starts virtual thread. Blocks caller if caller thread calls `acquire()`. |

Caller-blocking is unacceptable — it defeats the point of the prefetcher
(which is to overlap, not serialize). Even if we moved the acquire
into the virtual thread body, we'd park VTs on the semaphore, restoring
the futex-wait signature we're trying to kill.

**Decision: `tryAcquire → skip`.** No caller blocking, no VT parking,
and semantically equivalent to "prefetcher was too slow on this one".

---

## C. Rollback

### C.1 Flag

```java
-Dsirix.hot.prefetch.parallelism=N
```

- `N > 0`: Semaphore cap = N.
- `N = 0`: Prefetching completely disabled (the method early-returns without
  starting a virtual thread). Useful for A/B comparison in the profile.
- Default: `min(64, 2 * Runtime.getRuntime().availableProcessors())`.

### C.2 Unit-test plan

Single test in `ProjectionIndexHOTStorageTest` that rebuilds a 10 K-leaf
projection index and asserts `HOTTrieReader.get(rootRef, key)` byte-equal
for every key across the set of parallelism values
`{0, 8, 40, 64, 1024}`. Since the Semaphore cap is a static JVM-wide
property, the test drives this via a small helper that overrides and
restores the permit budget on an existing Semaphore (setting the Semaphore
to `N` permits, then draining/releasing back to default between passes).

Simpler alternative: call the Semaphore-reset helper via reflection
inside the test, since `PREFETCH_LIMIT` is `private static final`. We
avoid reflection by exposing a package-private test hook
`setPrefetchParallelismForTest(int)` that rebuilds the static
`PREFETCH_LIMIT` with the given capacity.

### C.3 Fallback if cold wall regresses

1. Set default to 0 (prefetching disabled) by changing the default in the
   `Integer.getInteger` call. Validate this doesn't regress vs serial
   prefetcher — if it does, bump default back to the computed value.
2. If intermediate values (e.g. 20, 80) would help, keep the flag.
3. Revert the whole change if wall-clock gain is < 10 %.

---

## Decision

**Phase 1 gate: PASS.**

- A.1–A.4: correctness demonstrated (prefetch is a hint; skip is semantic
  no-op; try/finally prevents leaks).
- B.1–B.3: cap of `min(64, 2 × cores)` targets BUF_POOL size and NVMe QD
  sweet spot; tryAcquire-skip is the right strategy given the wall
  profile's futex signature.
- C.1–C.3: rollback path well-defined; unit test covers 5 permit values.

Proceed to Phase 2 implementation.

---

## Phase 3 measurement outcome (after code landed)

**Unexpected finding: ANY non-zero cap regresses cold wall. The right answer
is to disable prefetching entirely (cap = 0) and keep the Semaphore as an
opt-in rollback.**

### Alternating-round A/B — 4 rounds per cap

Run order: `(0, 40, 1024) × 4` to wash out time-of-day drift. Single cold
run per line, `/usr/bin/time -f "WALL_SECONDS=%e"`. DB evicted before each
run via `evict_db.py`.

| cap | run wall (s) | median wall | hydrate median |
| --- | --- | --- | --- |
| **0 (prefetch disabled)** | 4.94, 4.99, 5.03, 5.29 | **4.99 s** | 1,179 ms |
| 40 (`2 × cores`)          | 5.32, 5.53, 5.64, 5.94 | 5.59 s     | 1,362 ms |
| 1024 (unbounded)          | 5.31, 5.65, 5.67, 5.71 | 5.66 s     | 1,395 ms |

cap=0 is **~11 % faster** than the capped and unbounded variants, and
hydrate drops ~180 ms.

### 5-round A/B confirmation (cap=0 vs cap=1024)

| cap | runs (s) | median | hydrate median |
| --- | --- | --- | --- |
| 0 (disabled) | 4.83, 5.00, 5.10, 5.42, 5.57 | **5.10 s** | 1,178 ms |
| 1024 (unbounded) | 4.90, 5.21, 5.68, 5.70, 6.55 | 5.68 s | 1,377 ms |

cap=0 is **0.58 s (10.2 %) faster** than unbounded.

### Why does prefetch lose?

Lock-profile evidence (`iter04-lock-cap{0,1024}.txt`):

- cap=1024 (unbounded): `sun.nio.ch.NativeThreadSet` accumulates
  **38.22 billion ns** of contention-time (98.73 % of lock time).
- cap=0 (disabled): same lock accumulates **6.72 billion ns**
  (97.12 % of lock time).

`NativeThreadSet` is acquired by every concurrent `FileChannel.read` to
track which native threads are currently inside a blocking `pread`. The
unbounded prefetcher fires hundreds of virtual threads at
`loadHOTPage`, each of which slaps this lock on entry and exit; they
serialize against the synchronous reader, starving the sync path of
NVMe command-queue slots and direct-buffer acquisitions.

The wall-profile futex signature (74–75 % of samples in
`__futex_abstimed_wait_cancelable64`) was the **right observation,
wrong interpretation**. The futex wait was correct VT-parking on
blocking I/O — scalable behaviour. But the lock-profile surfaced the
real cost: `NativeThreadSet` contention per `FileChannel.read` at
hundreds of concurrent calls.

Secondary evidence from `__libc_pread64` wall samples:

- cap=1024: 240 samples (3.30 % of wall)
- cap=0: 102 samples (1.62 % of wall)

With prefetch disabled, we issue **58 % fewer `pread` syscalls** — many
of the prefetched reads were redundant against the sync reader's own
loads.

### Recommendation (adopted)

Change the compile-time default to 0:

```java
private static final int PREFETCH_PARALLELISM_DEFAULT = 0;
```

Keep `-Dsirix.hot.prefetch.parallelism=N` as an opt-in for workloads
where prefetch may be net-positive (deeper HOT trees, higher-latency
storage, sparse cursor scans that don't race the sync reader). The
Semaphore machinery is retained; the only change versus iter#04 Phase 2
is the default value.

### Keep/revert decision

**KEEP (with default = 0).** Cold wall improvement ≥ 10 % gate met
(10.2 % improvement), futex-wait absolute-sample count reduced
(5421 → 4725, −13 %), `NativeThreadSet` lock-time 6× reduced, all
5-permit equivalence tests pass, default is opt-out so production
workloads that measurably benefit from prefetch can flip the flag back
on. The Semaphore is near-free in the cap=0 path: one CAS in
`tryAcquire` (always fails), then early-return — no virtual-thread
allocation, no buffer contention, no kernel syscall.
