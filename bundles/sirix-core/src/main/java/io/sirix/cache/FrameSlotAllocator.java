/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Umbra / LeanStore-style buffer allocator with fixed-position frame slots
 * and optimistic versioned reads.
 *
 * <h2>Why this exists</h2>
 *
 * The prior {@link LinuxMemorySegmentAllocator} uses a pool of recyclable
 * segments: a fresh allocate() can hand back a segment whose virtual address
 * previously hosted different page content. Under high-concurrency parallel
 * scans (20 worker threads × 100 M records), this exposes a race where a
 * reader holds a {@link MemorySegment} into a just-released segment at the
 * moment the kernel {@code MADV_DONTNEED}s its physical pages, producing
 * {@code SIGSEGV_MAPERR} in otherwise-correct code.
 *
 * <h2>Umbra's solution, ported</h2>
 *
 * <ol>
 *   <li>One {@code mmap} at startup per size class, subdivided into
 *       fixed-position slots. A slot's virtual address is stable for the
 *       lifetime of the process.</li>
 *   <li>A {@code long} version counter per slot. Even = slot is quiescent
 *       (either free or stably owned by readers); odd = slot is being
 *       modified by a writer (allocate/release).</li>
 *   <li>Readers use optimistic versioning: snapshot {@code v1 = version},
 *       read the slot bytes, re-read {@code v2 = version}; if
 *       {@code v1 != v2} or the low bit is set, the read raced an eviction
 *       and must be retried by re-locating the page.</li>
 *   <li>Writers (evictors) bump the version before {@code MADV_DONTNEED}
 *       and again after it completes, so any read-pair that straddles a
 *       teardown always observes {@code v1 != v2}.</li>
 * </ol>
 *
 * <h2>Size classes</h2>
 *
 * Seven power-of-two size classes match what {@code LinuxMemorySegmentAllocator}
 * exposes today: 4 KiB through 256 KiB. Each class has its own mmap'd virtual
 * region, version array, and free-slot stack; there is no cross-class
 * contention. Callers locate the right class via {@link #indexForSize(long)}.
 *
 * <h2>Sirix's persistent tree-of-tries, and why it fits this allocator</h2>
 *
 * Sirix is a copy-on-write store built as a functional tree of tries: each
 * commit produces a new {@code RevisionRootPage} that pointer-shares unchanged
 * subtrees with prior revisions. A single writer mutates any given resource at
 * a time; readers observe their own revision's root and walk down through
 * structurally-shared page fragments. Two consequences for this allocator:
 *
 * <ul>
 *   <li><b>Cache entries are one-to-one with page fragments, not with
 *       revisions.</b> A subtree shared by revisions M and N resolves to the
 *       same {@code PageReference} (same on-disk key) and therefore occupies
 *       the same cache entry and the same slot. There is no "per-revision
 *       slot" concept — the slot version counter protects concurrent readers
 *       across revisions just as well as within one.</li>
 *   <li><b>Modifying a fragment in revision N+1 produces a new
 *       {@code PageReference} at a new file offset.</b> That new reference
 *       maps to a different cache entry, which lives in its own slot. The old
 *       revision's readers keep seeing the old slot unchanged; N+1 readers
 *       resolve to the new slot. The allocator sees this only as "two
 *       unrelated allocations."</li>
 * </ul>
 *
 * <h2>Single-writer-per-resource, many readers — no reader↔writer race</h2>
 *
 * Writes on a resource are serialized — exactly one writer thread at a time —
 * and the writer operates on its own transaction intent log, isolated from the
 * reader-visible cache until commit. Pages created by the writer <em>do</em>
 * use frames from this allocator, but the writer never publishes those frames
 * to readers before the commit fence; likewise readers never see in-flight
 * writer state. There is therefore <b>no reader↔writer race</b> this
 * allocator needs to protect against.
 *
 * <p>The only concurrency this allocator handles:
 *
 * <ul>
 *   <li><b>Reader ↔ reader.</b> Two readers simultaneously reading the same
 *       cached page's slot — trivially safe (no mutation).</li>
 *   <li><b>Reader ↔ evictor.</b> The ClockSweeper (or a reader triggering
 *       evict-on-over-budget) releases a slot whose {@link FrameSlot} handle
 *       another reader may still be dereferencing. The slot version counter
 *       closes this window: the evictor bumps version before
 *       {@code MADV_DONTNEED}; a reader that snapshotted the pre-eviction
 *       version observes a different post-eviction version and retries via
 *       the cache, which re-resolves the {@code PageReference} to a fresh
 *       slot. No SEGV.</li>
 * </ul>
 *
 * In short: the allocator provides slot-level memory safety (version-checked
 * reads, stable virtual addresses) that is orthogonal to Sirix's revision
 * model. The cache's {@code PageReference → slot} mapping handles revision
 * semantics; the allocator handles recycling semantics.
 *
 * <h2>HFT-grade cost</h2>
 *
 * <ul>
 *   <li>Allocate: one {@code AtomicInteger.compareAndSet} on the class's
 *       free-stack top, one {@code long} version bump, one {@code MADV_POPULATE_WRITE}.</li>
 *   <li>Release: one version bump + one {@code madvise(DONTNEED)} + one free-slot
 *       push. No unmapping; virtual address stays valid for reuse.</li>
 *   <li>Read: two {@code getAcquire} loads of the version. Zero CAS,
 *       zero syscall in the common case.</li>
 * </ul>
 */
public final class FrameSlotAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FrameSlotAllocator.class);

  // ===== Size classes (must match LinuxMemorySegmentAllocator.SEGMENT_SIZES) =
  public static final long[] SIZE_CLASSES = {
      4L * 1024,           //  4 KiB
      8L * 1024,           //  8 KiB
      16L * 1024,          // 16 KiB
      32L * 1024,          // 32 KiB
      64L * 1024,          // 64 KiB
      128L * 1024,         // 128 KiB
      256L * 1024          // 256 KiB
  };

  // ===== POSIX plumbing ======================================================
  private static final Linker LINKER = Linker.nativeLinker();
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;
  private static final MethodHandle MADVISE;

  private static final int PROT_READ = 0x1;
  private static final int PROT_WRITE = 0x2;
  private static final int MAP_PRIVATE = 0x02;
  private static final int MAP_ANONYMOUS = 0x20;
  private static final int MAP_NORESERVE = 0x4000;
  private static final int MADV_DONTNEED = 4;
  private static final int MADV_POPULATE_WRITE = 23;

  static {
    MMAP = LINKER.downcallHandle(
        LINKER.defaultLookup().find("mmap").orElseThrow(() -> new RuntimeException("mmap missing")),
        FunctionDescriptor.of(ValueLayout.ADDRESS,
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT,
            ValueLayout.JAVA_INT, ValueLayout.JAVA_INT, ValueLayout.JAVA_LONG));
    MUNMAP = LINKER.downcallHandle(
        LINKER.defaultLookup().find("munmap").orElseThrow(() -> new RuntimeException("munmap missing")),
        FunctionDescriptor.of(ValueLayout.JAVA_INT, ValueLayout.ADDRESS, ValueLayout.JAVA_LONG));
    MADVISE = LINKER.downcallHandle(
        LINKER.defaultLookup().find("madvise").orElseThrow(() -> new RuntimeException("madvise missing")),
        FunctionDescriptor.of(ValueLayout.JAVA_INT,
            ValueLayout.ADDRESS, ValueLayout.JAVA_LONG, ValueLayout.JAVA_INT));
  }

  /**
   * Per-size-class state. One instance per entry in {@link #SIZE_CLASSES}.
   * Each class is independently addressable — no cross-class contention.
   *
   * <p>Slot-index allocation strategy: fresh slots are handed out lazily via
   * {@link #nextFreshIndex}; recycled slots land on {@link #freeSlots}. The
   * allocator always tries the recycled stack first, so stable-address
   * recycling dominates once the cache is warm. Lazy-fresh avoids a 512K+
   * boxed-Integer startup cost.
   */
  private static final class SizeClass {
    final long slotSize;
    final int slotCount;
    final MemorySegment region;
    final AtomicLongArray slotVersion;
    final AtomicInteger nextFreshIndex = new AtomicInteger();
    final ConcurrentLinkedDeque<Integer> freeSlots = new ConcurrentLinkedDeque<>();
    final AtomicInteger liveCount = new AtomicInteger();
    final AtomicLong allocCount = new AtomicLong();
    final AtomicLong releaseCount = new AtomicLong();

    SizeClass(final long slotSize, final int slotCount, final MemorySegment region) {
      this.slotSize = slotSize;
      this.slotCount = slotCount;
      this.region = region;
      this.slotVersion = new AtomicLongArray(slotCount);
    }
  }

  // ===== Singleton + MemorySegmentAllocator interface state ==================
  private static final FrameSlotAllocator INSTANCE = new FrameSlotAllocator();
  private final AtomicBoolean initialized = new AtomicBoolean();
  private volatile SizeClass[] classes;
  private volatile long budgetBytes;

  /**
   * Physical-memory bytes currently held across all size classes. Counted
   * per-slot-size on allocate, released on close. Sized so that a workload
   * dominated by a single size class (e.g., all 64 KiB leaf pages during a
   * parallel scan) can use the full budget rather than being capped at
   * {@code budget / numClasses}.
   */
  private final AtomicLong physicalBytes = new AtomicLong();

  /**
   * Virtual reservation per size class. Cheap because {@code MAP_NORESERVE}
   * means only touched pages count against RAM. Sized at 32 GiB per class
   * (× 7 = 224 GiB virtual) — plenty of slot indices for any realistic
   * budget up to ~28 GiB physical.
   */
  private static final long VIRTUAL_PER_CLASS = 32L * 1024 * 1024 * 1024;

  /**
   * Pressure listener — invoked on allocate-failure before the park-and-retry
   * window. Production implementation: {@code BufferManagerImpl} registers
   * {@code cache::evictUnderPressure} here so eviction fires directly rather
   * than waiting for the background {@link ClockSweeper} to catch up.
   *
   * <p>Volatile write, atomic reference — zero-allocation on the hot path
   * when no listener is registered.
   */
  public interface PressureListener {
    void onPressure();
  }

  private static volatile PressureListener pressureListener;

  public static void setPressureListener(final PressureListener listener) {
    pressureListener = listener;
  }

  /**
   * Address → live FrameSlot map used by the {@link #release(MemorySegment)}
   * implementation of the {@link MemorySegmentAllocator} interface. Callers
   * using the native {@link FrameSlot} handle API don't hit this map.
   */
  private final ConcurrentHashMap<Long, FrameSlot> liveByAddress = new ConcurrentHashMap<>();

  public static FrameSlotAllocator getInstance() {
    return INSTANCE;
  }

  /**
   * Internal no-arg constructor; production paths go through
   * {@link #getInstance()} + {@link #init(long)}. Tests may construct an
   * independent instance directly.
   */
  FrameSlotAllocator() {
  }

  /** Test-only: construct and immediately initialize with the given budget. */
  public FrameSlotAllocator(final long budgetBytes) {
    initInternal(budgetBytes);
  }

  /**
   * Initialize the allocator with a physical-memory budget. Idempotent; a
   * second call with a larger budget is a no-op (we cannot cheaply grow an
   * mmap'd region). Callers should call once at startup.
   */
  @Override
  public void init(final long maxBufferSize) {
    if (initialized.compareAndSet(false, true)) {
      initInternal(maxBufferSize);
    }
  }

  private void initInternal(final long budgetBytes) {
    if (budgetBytes < SIZE_CLASSES[SIZE_CLASSES.length - 1]) {
      throw new IllegalArgumentException(
          "budgetBytes must be >= largest size class (" + SIZE_CLASSES[SIZE_CLASSES.length - 1] + ")");
    }
    this.budgetBytes = budgetBytes;
    final SizeClass[] cls = new SizeClass[SIZE_CLASSES.length];
    // Per-class virtual reservation is workload-independent; MAP_NORESERVE
    // means physical pages only commit on MADV_POPULATE_WRITE. Cap slot count
    // so the AtomicLongArray of per-slot versions stays bounded (max 512K
    // slots per class at 4 KiB = 4 MiB of metadata per class).
    for (int i = 0; i < SIZE_CLASSES.length; i++) {
      final long slotSize = SIZE_CLASSES[i];
      final long rawSlotCount = VIRTUAL_PER_CLASS / slotSize;
      final int slotCount = Math.toIntExact(Math.min(rawSlotCount, 1L << 20));
      final long regionBytes = (long) slotCount * slotSize;
      final MemorySegment region = mapRegion(regionBytes);
      cls[i] = new SizeClass(slotSize, slotCount, region);
      LOGGER.info("FrameSlotAllocator class {}: {} slots × {} bytes = {} MiB virtual",
          i, slotCount, slotSize, regionBytes / (1024 * 1024));
    }
    this.classes = cls;
    this.initialized.set(true);
    LOGGER.info("FrameSlotAllocator: shared physical budget = {} MiB", budgetBytes / (1024 * 1024));
  }

  @Override
  public boolean isInitialized() {
    return initialized.get() && classes != null;
  }

  @Override
  public long getMaxBufferSize() {
    return budgetBytes;
  }

  @Override
  public void free() {
    shutdown();
  }

  private static MemorySegment mapRegion(final long bytes) {
    try {
      final MemorySegment addr = (MemorySegment) MMAP.invokeExact(
          MemorySegment.NULL, bytes,
          PROT_READ | PROT_WRITE,
          MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
          -1, 0L);
      if (addr.address() == 0) {
        throw new OutOfMemoryError("mmap failed for " + bytes + " bytes");
      }
      return addr.reinterpret(bytes);
    } catch (final Throwable t) {
      throw new RuntimeException("Failed to mmap allocator region", t);
    }
  }

  /**
   * Resolve a requested byte size to the smallest size-class index that
   * fits it. Returns {@code -1} if the request exceeds the largest class.
   */
  public static int indexForSize(final long requestedBytes) {
    for (int i = 0; i < SIZE_CLASSES.length; i++) {
      if (requestedBytes <= SIZE_CLASSES[i]) {
        return i;
      }
    }
    return -1;
  }

  /**
   * Allocate a slot and return the {@link FrameSlot} handle. Prefer this over
   * the interface {@link #allocate(long)} when you want explicit slot
   * metadata (class index, slot index, version) for optimistic reads.
   * Returns {@code null} if the class has no free slots.
   */
  public FrameSlot allocateSlot(final long requestedBytes) {
    final int classIdx = indexForSize(requestedBytes);
    if (classIdx < 0) {
      throw new IllegalArgumentException("requested size " + requestedBytes + " exceeds largest class");
    }
    final SizeClass c = classes[classIdx];

    // Global physical-budget reserve. Must happen before we grab a slot so a
    // budget-exceeded caller doesn't burn a slot index it can't populate.
    final long after = physicalBytes.addAndGet(c.slotSize);
    if (after > budgetBytes) {
      physicalBytes.addAndGet(-c.slotSize);
      return null;
    }

    final int slotIdx = popFreeSlot(c);
    if (slotIdx < 0) {
      physicalBytes.addAndGet(-c.slotSize);
      return null;
    }

    // Version transition: prior even → odd (writer in progress) → next even
    // (quiescent, owned). A reader that snapshotted prior sees a different
    // final value, and the odd state forbids in-flight reads.
    final long prior = c.slotVersion.get(slotIdx);
    final long inProgress = prior | 1L;
    c.slotVersion.setRelease(slotIdx, inProgress);

    final MemorySegment slot = c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize);
    // No MADV_POPULATE_WRITE on recycle: the slot's physical pages survive
    // across allocate/release cycles. Fresh slots take the first-write fault
    // which the kernel serves from the zero page — one page fault amortized
    // across the slot's entire lifetime, not a per-recycle syscall storm.

    final long owned = inProgress + 1L;
    c.slotVersion.setRelease(slotIdx, owned);

    c.liveCount.incrementAndGet();
    c.allocCount.incrementAndGet();
    assert (owned & 1L) == 0L : "owned version must be even";
    return new FrameSlot(this, classIdx, slotIdx, owned, slot);
  }

  /**
   * {@link MemorySegmentAllocator}-conforming allocate. Wraps
   * {@link #allocateSlot(long)} and records the slot in an address-indexed
   * map so {@link #release(MemorySegment)} can look it up.
   *
   * <p>On class exhaustion, parks briefly and retries — the {@link ClockSweeper}
   * runs on a daemon thread and continuously releases stale slots, so the
   * typical exhaustion is a transient spike rather than a real limit. Mirrors
   * {@code LinuxMemorySegmentAllocator}'s park-and-retry pattern (50 µs initial,
   * exponential up to 5 ms, 10 s total ceiling). A mid-query OOM cascades into
   * partial-page state and SIGSEGV downstream, so waiting is almost always the
   * right call.
   */
  @Override
  public MemorySegment allocate(final long size) {
    FrameSlot slot = allocateSlot(size);
    if (slot == null) {
      // First shot at relief before parking: the cache is the most likely
      // owner of the bytes we need, so ask it to evict synchronously. Skips
      // the 500 ms ClockSweeper cadence in the common hot-scan case.
      firePressure();
      slot = allocateSlot(size);
    }
    if (slot == null) {
      long parkNanos = 50_000L;
      long totalWaitedNanos = 0L;
      final long maxWaitNanos = 10_000_000_000L;
      while (slot == null && totalWaitedNanos < maxWaitNanos) {
        java.util.concurrent.locks.LockSupport.parkNanos(parkNanos);
        totalWaitedNanos += parkNanos;
        parkNanos = Math.min(parkNanos * 2, 5_000_000L);
        slot = allocateSlot(size);
        if (slot == null) {
          firePressure();
          slot = allocateSlot(size);
        }
      }
      if (slot == null) {
        final int classIdx = indexForSize(size);
        LOGGER.warn("FrameSlotAllocator class {} saturated for {} ms (size {}): sweeper unable to free slots",
            classIdx, totalWaitedNanos / 1_000_000L, size);
        dumpStateForOOM(classIdx, size, totalWaitedNanos);
        throw new OutOfMemoryError("FrameSlotAllocator: size class "
            + classIdx + " exhausted for " + size + " bytes after "
            + (totalWaitedNanos / 1_000_000L) + " ms of retry");
      }
    }
    final MemorySegment seg = slot.segment();
    liveByAddress.put(seg.address(), slot);
    return seg;
  }

  /**
   * {@link MemorySegmentAllocator}-conforming release. Maps
   * {@code segment.address()} back to the live {@link FrameSlot} and closes
   * it — which bumps the slot's version, {@code MADV_DONTNEED}s its pages,
   * and returns the slot to the free stack at the same virtual address.
   *
   * <p>Idempotent: a second release for the same address is a no-op.
   */
  @Override
  public void release(final MemorySegment segment) {
    if (segment == null) {
      return;
    }
    final FrameSlot slot = liveByAddress.remove(segment.address());
    if (slot != null) {
      slot.close();
    }
  }

  /**
   * Reset ({@code MADV_DONTNEED}) the physical pages backing {@code segment}
   * without releasing the slot. Used by callers that want to reuse the slot
   * with zero-filled content.
   */
  @Override
  public void resetSegment(final MemorySegment segment) {
    if (segment == null) {
      return;
    }
    try {
      final int rc = (int) MADVISE.invokeExact(segment, segment.byteSize(), MADV_DONTNEED);
      if (rc != 0) {
        LOGGER.debug("resetSegment MADV_DONTNEED rc={} on address 0x{}", rc, Long.toHexString(segment.address()));
      }
    } catch (final Throwable t) {
      LOGGER.debug("resetSegment madvise threw: {}", t.getMessage());
    }
  }

  /**
   * Hand out a slot index. Recycled slots come off {@link SizeClass#freeSlots}
   * first (stable-address reuse, which is the core of the Umbra design). If
   * the recycle stack is empty, a fresh index is carved off the virtual
   * region via {@link SizeClass#nextFreshIndex}. Returns {@code -1} when both
   * sources are exhausted.
   */
  /**
   * Dumps per-class allocator state when an allocation saturates — live slot
   * count, total lifetime allocates/releases, slots still in {@code liveByAddress},
   * and physical-byte accounting. Emitted to stderr so it lands even when
   * logback config swallows WARN. Diagnostic tool for cases where the pool
   * appears exhausted but the cache should have evicted.
   */
  private void dumpStateForOOM(final int failedClass, final long failedSize, final long waitedNanos) {
    final StringBuilder sb = new StringBuilder();
    sb.append("\n=== FrameSlotAllocator state at OOM ===\n");
    sb.append(String.format("  failed: class=%d size=%d bytes after=%d ms%n",
        failedClass, failedSize, waitedNanos / 1_000_000L));
    sb.append(String.format("  physicalBytes=%d / budget=%d (%.1f%%)%n",
        physicalBytes.get(), budgetBytes, 100.0 * physicalBytes.get() / budgetBytes));
    sb.append(String.format("  liveByAddress entries=%d%n", liveByAddress.size()));
    for (int i = 0; i < classes.length; i++) {
      final SizeClass c = classes[i];
      sb.append(String.format("  class[%d] slotSize=%d  slots=%d  live=%d  alloc=%d  release=%d  freeStack=%d  freshIdx=%d%n",
          i, c.slotSize, c.slotCount, c.liveCount.get(), c.allocCount.get(),
          c.releaseCount.get(), c.freeSlots.size(), c.nextFreshIndex.get()));
    }
    sb.append("=== end FrameSlotAllocator state ===\n");
    System.err.print(sb);
    System.err.flush();
  }

  private static void firePressure() {
    final PressureListener l = pressureListener;
    if (l != null) {
      try {
        l.onPressure();
      } catch (final Throwable t) {
        LOGGER.debug("PressureListener threw: {}", t.getMessage());
      }
    }
  }

  private static int popFreeSlot(final SizeClass c) {
    final Integer recycled = c.freeSlots.pollLast();
    if (recycled != null) {
      return recycled.intValue();
    }
    final int fresh = c.nextFreshIndex.getAndIncrement();
    if (fresh < c.slotCount) {
      return fresh;
    }
    c.nextFreshIndex.decrementAndGet();
    return -1;
  }

  private static void pushFreeSlot(final SizeClass c, final int slotIdx) {
    c.freeSlots.offerLast(slotIdx);
  }

  /** Called by {@link FrameSlot#close()} — not usually invoked directly. */
  void releaseSlot(final int classIdx, final int slotIdx) {
    final SizeClass c = classes[classIdx];
    final long prior = c.slotVersion.get(slotIdx);

    // Step 1: bump to odd ("writer in progress"). Any reader that sees this
    // value between its pre and post snapshots detects the race.
    final long inProgress = prior | 1L;
    c.slotVersion.setRelease(slotIdx, inProgress);

    // NB: no MADV_DONTNEED on release. Physical pages stay resident across
    // recycle cycles — the version counter is the logical-safety mechanism,
    // the madvise was only for RSS accounting. Under a 20-thread scan the
    // DONTNEED + subsequent POPULATE_WRITE caused measurable kernel time in
    // zap_pte_range + clear_page_erms (both showed up in asprof profiles as
    // top leaf samples). Trading syscall time for ~constant RSS at the
    // configured budget is the right call here.

    // Step 2: bump to next even value. Strictly greater than prior, different
    // parity from the odd in-progress value. Both reader checks fire correctly.
    final long quiescent = inProgress + 1L;
    c.slotVersion.setRelease(slotIdx, quiescent);
    assert (quiescent & 1L) == 0L : "post-release version must be even";

    pushFreeSlot(c, slotIdx);
    c.liveCount.decrementAndGet();
    c.releaseCount.incrementAndGet();
    physicalBytes.addAndGet(-c.slotSize);
  }

  /** Pre-commit physical pages for a freshly-allocated slot. */
  private static void populate(final MemorySegment slot, final long sizeBytes) {
    try {
      final int rc = (int) MADVISE.invokeExact(slot, sizeBytes, MADV_POPULATE_WRITE);
      if (rc != 0) {
        throw new OutOfMemoryError("MADV_POPULATE_WRITE failed (rc=" + rc
            + ") — physical memory exhausted during slot populate");
      }
    } catch (final OutOfMemoryError oom) {
      throw oom;
    } catch (final Throwable t) {
      // Older kernels lack MADV_POPULATE_WRITE (EINVAL). Fall back to
      // lazy-commit; if RAM is tight, kernel OOM-kills the process rather
      // than handing us a SIGSEGV on first write.
    }
  }

  // ===== Reader-side optimistic validation ===================================

  /**
   * Snapshot the current version of a specific {@code (classIdx, slotIdx)}
   * pair. Callers about to read slot bytes use the pattern:
   *
   * <pre>{@code
   *   long v1 = allocator.acquireVersion(classIdx, slotIdx);
   *   if ((v1 & 1L) != 0L) retry;                       // writer in progress
   *   // ... read slot bytes from resolved MemorySegment ...
   *   if (!allocator.validateVersion(classIdx, slotIdx, v1)) retry; // raced eviction
   * }</pre>
   *
   * <p>A failed validation means the slot's content was evicted between the
   * snapshot and the check; the caller must re-resolve the page via the cache
   * (which may return a different slot or a cache miss that triggers a reload)
   * and retry the read.
   */
  public long acquireVersion(final int classIdx, final int slotIdx) {
    return classes[classIdx].slotVersion.getAcquire(slotIdx);
  }

  public boolean validateVersion(final int classIdx, final int slotIdx, final long preVersion) {
    final long now = classes[classIdx].slotVersion.getAcquire(slotIdx);
    return now == preVersion && (now & 1L) == 0L;
  }

  // ===== Introspection =======================================================

  public long budgetBytes() {
    return budgetBytes;
  }

  public int slotCount(final int classIdx) {
    return classes[classIdx].slotCount;
  }

  public int liveSlotCount(final int classIdx) {
    return classes[classIdx].liveCount.get();
  }

  public long allocateCount(final int classIdx) {
    return classes[classIdx].allocCount.get();
  }

  public long releaseCount(final int classIdx) {
    return classes[classIdx].releaseCount.get();
  }

  /**
   * Tear down all mmap'd regions. Not called during normal operation; the
   * allocator is expected to live for the JVM's lifetime.
   */
  public void shutdown() {
    for (final SizeClass c : classes) {
      try {
        // Capture int return per the FunctionDescriptor signature —
        // invokeExact is strict about matching return type even when
        // the value is discarded.
        final int rc = (int) MUNMAP.invokeExact(c.region, c.region.byteSize());
        if (rc != 0) {
          LOGGER.warn("munmap returned {} for class region size {}", rc, c.region.byteSize());
        }
      } catch (final Throwable t) {
        LOGGER.warn("munmap failed on shutdown: {}", t.getMessage());
      }
    }
  }

  /**
   * Live handle for a frame slot. Holding this guarantees the allocator will
   * not release the slot. Close exactly once to return it to the free stack.
   */
  public static final class FrameSlot implements AutoCloseable {
    private final FrameSlotAllocator owner;
    private final int classIdx;
    private final int slotIdx;
    private final long versionAtAlloc;
    private final MemorySegment segment;
    private volatile boolean closed;

    FrameSlot(final FrameSlotAllocator owner, final int classIdx, final int slotIdx,
        final long versionAtAlloc, final MemorySegment segment) {
      this.owner = owner;
      this.classIdx = classIdx;
      this.slotIdx = slotIdx;
      this.versionAtAlloc = versionAtAlloc;
      this.segment = segment;
    }

    public int classIndex() {
      return classIdx;
    }

    public int slotIndex() {
      return slotIdx;
    }

    /** The version observed at allocation time. Useful for later validation. */
    public long versionAtAlloc() {
      return versionAtAlloc;
    }

    public MemorySegment segment() {
      return segment;
    }

    @Override
    public void close() {
      if (closed) {
        return;
      }
      closed = true;
      owner.releaseSlot(classIdx, slotIdx);
    }
  }
}
