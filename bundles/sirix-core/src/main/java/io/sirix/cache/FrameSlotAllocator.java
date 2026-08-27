/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.HftBoundaryTelemetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * Umbra / LeanStore-style buffer allocator with fixed-position frame slots and optimistic versioned
 * reads.
 *
 * <h2>Why this exists</h2>
 *
 * The prior {@link LinuxMemorySegmentAllocator} uses a pool of recyclable segments: a fresh
 * allocate() can hand back a segment whose virtual address previously hosted different page
 * content. Under high-concurrency parallel scans (20 worker threads × 100 M records), this exposes
 * a race where a reader holds a {@link MemorySegment} into a just-released segment at the moment
 * the kernel {@code MADV_DONTNEED}s its physical pages, producing {@code SIGSEGV_MAPERR} in
 * otherwise-correct code.
 *
 * <h2>Umbra's solution, ported</h2>
 *
 * <ol>
 * <li>One virtual-memory reservation at startup per size class ({@code mmap} on POSIX,
 * {@code VirtualAlloc(MEM_RESERVE)} on Windows — see {@link VirtualMemory}), subdivided into
 * fixed-position slots. A slot's virtual address is stable for the lifetime of the process.</li>
 * <li>A {@code long} version counter per slot. Even = slot is quiescent (either free or stably
 * owned by readers); odd = slot is being modified by a writer (allocate/release).</li>
 * <li>Readers use optimistic versioning: snapshot {@code v1 = version}, read the slot bytes,
 * re-read {@code v2 = version}; if {@code v1 != v2} or the low bit is set, the read raced an
 * eviction and must be retried by re-locating the page.</li>
 * <li>Writers (evictors) bump the version before {@code MADV_DONTNEED} and again after it
 * completes, so any read-pair that straddles a teardown always observes {@code v1 != v2}.</li>
 * </ol>
 *
 * <h2>Size classes</h2>
 *
 * Seven power-of-two size classes match what {@code LinuxMemorySegmentAllocator} exposes today: 4
 * KiB through 256 KiB. Each class has its own mmap'd virtual region, version array, and free-slot
 * stack; there is no cross-class contention. Callers locate the right class via
 * {@link #indexForSize(long)}.
 *
 * <h2>Sirix's persistent tree-of-tries, and why it fits this allocator</h2>
 *
 * Sirix is a copy-on-write store built as a functional tree of tries: each commit produces a new
 * {@code RevisionRootPage} that pointer-shares unchanged subtrees with prior revisions. A single
 * writer mutates any given resource at a time; readers observe their own revision's root and walk
 * down through structurally-shared page fragments. Two consequences for this allocator:
 *
 * <ul>
 * <li><b>Cache entries are one-to-one with page fragments, not with revisions.</b> A subtree shared
 * by revisions M and N resolves to the same {@code PageReference} (same on-disk key) and therefore
 * occupies the same cache entry and the same slot. There is no "per-revision slot" concept — the
 * slot version counter protects concurrent readers across revisions just as well as within
 * one.</li>
 * <li><b>Modifying a fragment in revision N+1 produces a new {@code PageReference} at a new file
 * offset.</b> That new reference maps to a different cache entry, which lives in its own slot. The
 * old revision's readers keep seeing the old slot unchanged; N+1 readers resolve to the new slot.
 * The allocator sees this only as "two unrelated allocations."</li>
 * </ul>
 *
 * <h2>Single-writer-per-resource, many readers — no reader↔writer race</h2>
 *
 * Writes on a resource are serialized — exactly one writer thread at a time — and the writer
 * operates on its own transaction intent log, isolated from the reader-visible cache until commit.
 * Pages created by the writer <em>do</em> use frames from this allocator, but the writer never
 * publishes those frames to readers before the commit fence; likewise readers never see in-flight
 * writer state. There is therefore <b>no reader↔writer race</b> this allocator needs to protect
 * against.
 *
 * <p>
 * The only concurrency this allocator handles:
 *
 * <ul>
 * <li><b>Reader ↔ reader.</b> Two readers simultaneously reading the same cached page's slot —
 * trivially safe (no mutation).</li>
 * <li><b>Reader ↔ evictor.</b> The ClockSweeper (or a reader triggering evict-on-over-budget)
 * releases a slot whose {@link FrameSlot} handle another reader may still be dereferencing. The
 * slot version counter closes this window: the evictor bumps version before {@code MADV_DONTNEED};
 * a reader that snapshotted the pre-eviction version observes a different post-eviction version and
 * retries via the cache, which re-resolves the {@code PageReference} to a fresh slot. No SEGV.</li>
 * </ul>
 *
 * In short: the allocator provides slot-level memory safety (version-checked reads, stable virtual
 * addresses) that is orthogonal to Sirix's revision model. The cache's {@code PageReference → slot}
 * mapping handles revision semantics; the allocator handles recycling semantics.
 *
 * <h2>HFT-grade cost</h2>
 *
 * <ul>
 * <li>Allocate: one free-stack pop + one {@code long} version bump; a fresh slot's one-time commit
 * happens in the {@link VirtualMemory} backend (no-op on POSIX).</li>
 * <li>Release: one version bump + one free-slot push. No unmapping, no decommit; the virtual
 * address stays valid and committed for reuse.</li>
 * <li>Read: two {@code getAcquire} loads of the version. Zero CAS, zero syscall in the common
 * case.</li>
 * </ul>
 */
public final class FrameSlotAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(FrameSlotAllocator.class);

  private static final int SCOPE_CHUNK_SHIFT = 12;
  private static final int SCOPE_CHUNK_SIZE = 1 << SCOPE_CHUNK_SHIFT;
  private static final int SCOPE_CHUNK_MASK = SCOPE_CHUNK_SIZE - 1;
  private static final int VERSION_CHUNK_SHIFT = 15;
  private static final int VERSION_CHUNK_SIZE = 1 << VERSION_CHUNK_SHIFT;
  private static final int VERSION_CHUNK_MASK = VERSION_CHUNK_SIZE - 1;
  private static final int MAX_RECYCLED_SCAN_PASSES = 4;
  private static final VarHandle SCOPE_CHUNK = MethodHandles.arrayElementVarHandle(MemorySegment.Scope[][].class);
  private static final VarHandle SCOPE_ENTRY = MethodHandles.arrayElementVarHandle(MemorySegment.Scope[].class);
  private static final VarHandle VERSION_ENTRY = MethodHandles.arrayElementVarHandle(long[].class);

  // ===== Size classes (must match LinuxMemorySegmentAllocator.SEGMENT_SIZES) =
  public static final long[] SIZE_CLASSES = {4L * 1024, // 4 KiB
      8L * 1024, // 8 KiB
      16L * 1024, // 16 KiB
      32L * 1024, // 32 KiB
      64L * 1024, // 64 KiB
      128L * 1024, // 128 KiB
      256L * 1024 // 256 KiB
  };

  // ===== Virtual-memory plumbing =============================================
  // All syscall-shaped work (reserve / commit-fresh / discard / release) lives behind the
  // per-platform VirtualMemory backend; everything below is portable Java.
  private static final VirtualMemory VM = VirtualMemory.forCurrentPlatform();

  /**
   * Fixed, eagerly materialized chunks for per-slot optimistic versions.
   *
   * <p>
   * A single {@code AtomicLongArray} for the one-million-slot classes has an 8 MiB backing array. G1
   * therefore retains it as multiple humongous regions for the allocator's entire lifetime. A chunk
   * contains at most 32,768 longs (256 KiB of payload), safely below even the 512 KiB humongous
   * threshold of G1's smallest 1 MiB region size. Eager construction is deliberate: it keeps
   * allocation, release, and optimistic reads allocation-free, including the first access to a new
   * slot range.
   *
   * <p>
   * The VarHandle access modes exactly mirror the former {@link AtomicLongArray}: {@link #get(int)}
   * is volatile, reader snapshots use acquire, ownership publications use release, and release claims
   * use a full atomic compare-and-set.
   */
  static final class ChunkedAtomicLongArray {
    private final long[][] chunks;
    private final int length;

    ChunkedAtomicLongArray(final int length) {
      if (length < 0) {
        throw new IllegalArgumentException("length must be non-negative: " + length);
      }
      this.length = length;
      final int chunkCount = length == 0
          ? 0
          : ((length - 1) >>> VERSION_CHUNK_SHIFT) + 1;
      this.chunks = new long[chunkCount][];
      for (int chunkIndex = 0; chunkIndex < chunks.length; chunkIndex++) {
        final int remaining = length - (chunkIndex << VERSION_CHUNK_SHIFT);
        chunks[chunkIndex] = new long[Math.min(VERSION_CHUNK_SIZE, remaining)];
      }
    }

    long get(final int index) {
      final long[] chunk = chunk(index);
      return (long) VERSION_ENTRY.getVolatile(chunk, index & VERSION_CHUNK_MASK);
    }

    long getAcquire(final int index) {
      final long[] chunk = chunk(index);
      return (long) VERSION_ENTRY.getAcquire(chunk, index & VERSION_CHUNK_MASK);
    }

    void setRelease(final int index, final long value) {
      final long[] chunk = chunk(index);
      VERSION_ENTRY.setRelease(chunk, index & VERSION_CHUNK_MASK, value);
    }

    boolean compareAndSet(final int index, final long expectedValue, final long newValue) {
      final long[] chunk = chunk(index);
      return VERSION_ENTRY.compareAndSet(chunk, index & VERSION_CHUNK_MASK, expectedValue, newValue);
    }

    int length() {
      return length;
    }

    int chunkCount() {
      return chunks.length;
    }

    int chunkLength(final int chunkIndex) {
      return chunks[chunkIndex].length;
    }

    private long[] chunk(final int index) {
      // The outer and exact-length final-chunk array checks jointly cover the full logical range.
      // Avoid a separate branch on every optimistic read.
      return chunks[index >>> VERSION_CHUNK_SHIFT];
    }
  }

  /**
   * Per-size-class state. One instance per entry in {@link #SIZE_CLASSES}. Each class is
   * independently addressable — no cross-class contention.
   *
   * <p>
   * Slot-index allocation strategy: fresh slots are handed out lazily via {@link #nextFreshIndex};
   * recycled slots are represented by one bit in {@link #recycledSlots}. The allocator always tries a
   * recycled slot first, biased toward {@link #recycleHint}, so stable-address recycling dominates
   * once the cache is warm. The bit set is both bounded and allocation-free: unlike a linked stack,
   * it has neither boxed indices nor per-release nodes, and a FREE-to-OWNED CAS has no linked-head
   * ABA state to stamp.
   *
   * <p>
   * Interface allocations publish their allocation-era identity in {@link #liveScopes}. The outer
   * table is fixed at initialization, while 4,096-slot reference chunks are materialized only when an
   * interface allocation first reaches that range. Thus unused virtual capacity does not pin a
   * multi-million-entry reference array, and warmed allocation/release performs no metadata
   * allocation.
   */
  private static final class SizeClass {
    final long slotSize;
    final int slotCount;
    final MemorySegment region;
    final long baseAddress;
    final long regionBytes;
    final ChunkedAtomicLongArray slotVersion;
    final AtomicInteger nextFreshIndex = new AtomicInteger();
    final AtomicLongArray recycledSlots;
    final AtomicInteger recycledCount = new AtomicInteger();
    final AtomicInteger recycleHint = new AtomicInteger(-1);
    final AtomicInteger recycleScanCursor = new AtomicInteger();
    final MemorySegment.Scope[][] liveScopes;
    final AtomicInteger liveCount = new AtomicInteger();
    final AtomicLong allocCount = new AtomicLong();
    final AtomicLong releaseCount = new AtomicLong();
    final AtomicLong committedBytes = new AtomicLong();

    SizeClass(final long slotSize, final int slotCount, final MemorySegment region) {
      this.slotSize = slotSize;
      this.slotCount = slotCount;
      this.region = region;
      this.baseAddress = region.address();
      this.regionBytes = region.byteSize();
      this.slotVersion = new ChunkedAtomicLongArray(slotCount);
      this.recycledSlots = new AtomicLongArray((slotCount + Long.SIZE - 1) / Long.SIZE);
      this.liveScopes = new MemorySegment.Scope[(slotCount + SCOPE_CHUNK_SIZE - 1) >>> SCOPE_CHUNK_SHIFT][];
    }
  }

  // ===== Singleton + MemorySegmentAllocator interface state ==================
  private static final FrameSlotAllocator INSTANCE = new FrameSlotAllocator();
  private final AtomicBoolean initialized = new AtomicBoolean();
  private final AtomicBoolean terminated = new AtomicBoolean();
  private volatile SizeClass[] classes;
  private volatile long budgetBytes;

  /** Budget share only oversized allocations may commit — see {@link #initInternal}. */
  private volatile long oversizedHeadroomBytes;

  /**
   * Physical/touchable capacity retained by committed frame slots plus live oversized arenas. A frame
   * slot remains committed across recycle cycles, so its bytes leave this counter only when the whole
   * size-class region is released at shutdown. This is the counter constrained by the global budget
   * and exposed through {@link #getPhysicalMemoryBytes()}.
   */
  private final AtomicLong committedBytes = new AtomicLong();

  /** Bytes currently owned by callers, distinct from retained committed capacity. */
  private final AtomicLong activeBytes = new AtomicLong();

  /** Active bytes backed by frame slots; shutdown must never unmap while this is nonzero. */
  private final AtomicLong activeSlotBytes = new AtomicLong();

  /** Cold-path oversized allocations currently between terminal-state validation and publication. */
  private final AtomicInteger oversizedAllocationOperations = new AtomicInteger();

  /**
   * Virtual reservation per size class. Cheap because {@code MAP_NORESERVE} means only touched pages
   * count against RAM. Sized at 32 GiB per class (× 7 = 224 GiB virtual) — plenty of slot indices for
   * any realistic budget up to ~28 GiB physical.
   */
  private static final long VIRTUAL_PER_CLASS = 32L * 1024 * 1024 * 1024;

  /**
   * Pressure listener — invoked on allocate-failure before the park-and-retry window. Production
   * implementation: {@code BufferManagerImpl} registers {@code cache::evictUnderPressure} here so
   * eviction fires directly rather than waiting for the background {@link ClockSweeper} to catch up.
   *
   * <p>
   * Volatile write, atomic reference — zero-allocation on the hot path when no listener is
   * registered.
   */
  public interface PressureListener {
    void onPressure();
  }

  private static volatile PressureListener pressureListener;

  public static void setPressureListener(final PressureListener listener) {
    pressureListener = listener;
  }

  /**
   * Test-only diagnostic. When {@code true}, {@link #releaseSlot} zero-fills a slot's physical pages
   * as it is freed. Production deliberately leaves a released slot's bytes intact (no
   * {@code MADV_DONTNEED} — see {@link #releaseSlot}), so a use-after-close reads stale-but-valid
   * data and only a real memory-pressure recycle ever clobbers it. Turning this on makes any
   * use-after-free deterministic instead of pressure-gated: the freed slot reads back as zeros
   * immediately. Off by default; one volatile read per release when off, no hot-path cost otherwise.
   */
  private static volatile boolean poisonOnRelease;

  /** Test-only: see {@link #poisonOnRelease}. */
  public static void setPoisonOnReleaseForTesting(final boolean poison) {
    poisonOnRelease = poison;
  }

  /**
   * Number of live interface-issued slots, maintained for diagnostics without scanning scope chunks.
   */
  private final AtomicInteger issuedSlotCount = new AtomicInteger();

  /**
   * Address → arena map for allocations LARGER than the largest size class (e.g. decompression
   * buffers for {@code OverflowPage}s carrying large values, #1076). Frame slots cannot serve them,
   * so each is backed by its own CONFINED arena, closed deterministically on
   * {@link #release(MemorySegment)} — closing a confined arena is not gated by GraalVM's
   * {@code SharedArenaSupport} (only shared-arena close is, and that flag is mutually exclusive with
   * the Vector API), so this keeps both deterministic reclaim and the SIMD kernels in the native
   * image. Allocation, access, and release are confined to the allocating thread; a release (or
   * access) from another thread fails loudly with {@link WrongThreadException}, which is the intended
   * failure for a caller violating the scoped decompress path's same-thread contract. Oversized
   * allocations are rare by design — every slotted page fits a size class — so the extra map lookup
   * on release is off the hot path.
   */
  private record OversizedAllocation(Arena arena, Thread owner, long bytes) {
  }

  private final ConcurrentHashMap<Long, OversizedAllocation> oversizedByAddress = new ConcurrentHashMap<>();

  public static FrameSlotAllocator getInstance() {
    return INSTANCE;
  }

  /**
   * Internal no-arg constructor; production paths go through {@link #getInstance()} +
   * {@link #init(long)}. Tests may construct an independent instance directly.
   */
  FrameSlotAllocator() {}

  /** Test-only: construct and immediately initialize with the given budget. */
  public FrameSlotAllocator(final long budgetBytes) {
    initInternal(budgetBytes);
  }

  /**
   * Initialize the allocator with a physical-memory budget. Idempotent; a second call with a larger
   * budget is a no-op (we cannot cheaply grow an mmap'd region). Callers should call once at startup.
   */
  @Override
  public void init(final long maxBufferSize) {
    if (terminated.get()) {
      throw new IllegalStateException("FrameSlotAllocator has been shut down");
    }
    if (initialized.compareAndSet(false, true)) {
      final long granted = MemorySegmentAllocator.clampToPhysicalHeadroom(maxBufferSize);
      if (granted < maxBufferSize) {
        LOGGER.warn("Off-heap arena clamped from {} MiB to {} MiB to leave headroom for the heap and the OS",
            maxBufferSize / (1024 * 1024), granted / (1024 * 1024));
      }
      initInternal(granted);
    } else {
      // Another thread already claimed initialization but may not have published `classes` yet
      // (initInternal maps several large regions before assigning it). Wait for the volatile
      // `classes` write so a caller never returns from init() — and then dereferences classes —
      // while it is still null (the concurrent allocateSlot NPE window).
      while (classes == null) {
        Thread.onSpinWait();
      }
    }
  }

  private void initInternal(final long budgetBytes) {
    if (budgetBytes < SIZE_CLASSES[SIZE_CLASSES.length - 1]) {
      throw new IllegalArgumentException(
          "budgetBytes must be >= largest size class (" + SIZE_CLASSES[SIZE_CLASSES.length - 1] + ")");
    }
    this.budgetBytes = budgetBytes;
    final long configuredHeadroom = Long.getLong("sirix.allocator.oversizedHeadroomBytes", -1L);
    // Slab-region commitments never release before shutdown (no portable way to decommit pages of
    // a live mmap'd region), so an unchecked slab peak would permanently starve the oversized path
    // — the large-value (de)compression buffers a post-ingest query still needs. Fresh-slot
    // commitment stops short of the full budget; only oversized allocations (whose bytes DO
    // return on release) may use the headroom. Default: 1/16th of the budget, capped at 1 GiB —
    // zero for tiny test budgets, so their exhaustion semantics are unchanged.
    // Only budgets comfortably above embedded/test scale reserve headroom by default: an
    // allocator budgeted EXACTLY for its slabs (focused tests, tiny embedded configs) must keep
    // its full slab capacity.
    this.oversizedHeadroomBytes = configuredHeadroom >= 0
        ? Math.min(configuredHeadroom, budgetBytes)
        : (budgetBytes >= 64L * 1024 * 1024
            ? Math.min(budgetBytes / 16, 1L << 30)
            : 0L);
    final SizeClass[] cls = new SizeClass[SIZE_CLASSES.length];
    // Per-class virtual reservation is workload-independent; MAP_NORESERVE means physical pages
    // only commit when touched. Cap the number of slots to bound version metadata; its fixed-size
    // chunks avoid persistent G1 humongous arrays without adding hot-path allocation.
    for (int i = 0; i < SIZE_CLASSES.length; i++) {
      final long slotSize = SIZE_CLASSES[i];
      final long rawSlotCount = VIRTUAL_PER_CLASS / slotSize;
      final int slotCount = Math.toIntExact(Math.min(rawSlotCount, 1L << 20));
      final long regionBytes = (long) slotCount * slotSize;
      final MemorySegment region = mapRegion(regionBytes);
      cls[i] = new SizeClass(slotSize, slotCount, region);
      LOGGER.info("FrameSlotAllocator class {}: {} slots × {} bytes = {} MiB virtual", i, slotCount, slotSize,
          regionBytes / (1024 * 1024));
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
  public long getPhysicalMemoryBytes() {
    return committedBytes.get();
  }

  /** Bytes currently owned by live frame-slot and oversized-allocation callers. */
  public long getActiveMemoryBytes() {
    return activeBytes.get();
  }

  @Override
  public void free() {
    shutdown();
  }

  private static MemorySegment mapRegion(final long bytes) {
    final MemorySegment region = VM.reserve(bytes);
    HftBoundaryTelemetry.nativeAllocation();
    return region;
  }

  /**
   * Resolve a requested byte size to the smallest size-class index that fits it. Returns {@code -1}
   * if the request exceeds the largest class.
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
   * Allocate a slot and return the {@link FrameSlot} handle. Prefer this over the interface
   * {@link #allocate(long)} when you want explicit slot metadata (class index, slot index, version)
   * for optimistic reads. Returns {@code null} if the class has no free slots.
   */
  public FrameSlot allocateSlot(final long requestedBytes) {
    ensureInitialized();
    final int classIdx = indexForSize(requestedBytes);
    if (classIdx < 0) {
      throw new IllegalArgumentException("requested size " + requestedBytes + " exceeds largest class");
    }
    final SizeClass c = classes[classIdx];
    final int slotIdx = acquireSlot(c);
    if (slotIdx < 0) {
      return null;
    }
    final long owned = c.slotVersion.getAcquire(slotIdx);
    final MemorySegment slot = c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize);
    return new FrameSlot(this, classIdx, slotIdx, owned, slot);
  }

  /**
   * Reserve one slot in {@code c}, returning its primitive index. The slot is exclusively owned when
   * this method returns; no handle, map entry, boxed address, or free-list node is created.
   */
  private int acquireSlot(final SizeClass c) {
    // Publish in-flight ownership before touching a region. shutdown() closes admission first and
    // then reads this counter, so an allocation that passed ensureInitialized() immediately before
    // the terminal transition either becomes visible here or observes termination and backs out.
    activeSlotBytes.addAndGet(c.slotSize);
    activeBytes.addAndGet(c.slotSize);
    if (terminated.get()) {
      activeBytes.addAndGet(-c.slotSize);
      activeSlotBytes.addAndGet(-c.slotSize);
      throw new IllegalStateException("FrameSlotAllocator has been shut down");
    }

    boolean acquired = false;
    try {
      int slotIdx = popRecycledSlot(c);
      if (slotIdx < 0) {
        // A fresh slot adds retained commitment. Recycled slots do not: their pages deliberately
        // remain committed, and charging them again would eventually consume the budget with the
        // same physical slot over and over.
        if (!reserveSlabCommittedBytes(c.slotSize)) {
          return -1;
        }
        slotIdx = popFreshSlot(c);
        if (slotIdx < 0) {
          committedBytes.addAndGet(-c.slotSize);
          return -1;
        }
        try {
          if (VM.commitFresh(c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize))) {
            HftBoundaryTelemetry.nativeAllocation();
          }
          c.committedBytes.addAndGet(c.slotSize);
        } catch (final RuntimeException | Error failure) {
          // The fresh index is intentionally stranded: on Windows a failed MEM_COMMIT slot is not
          // touch-safe and therefore must never enter the recycled set.
          committedBytes.addAndGet(-c.slotSize);
          throw failure;
        }
      }

      // Version transition: prior even → odd (writer in progress) → next even
      // (quiescent, owned). A reader that snapshotted prior sees a different
      // final value, and the odd state forbids in-flight reads.
      final long prior = c.slotVersion.get(slotIdx);
      final long inProgress = prior | 1L;
      c.slotVersion.setRelease(slotIdx, inProgress);

      // No MADV_POPULATE_WRITE on recycle: the slot's physical pages survive
      // across allocate/release cycles. Fresh slots take the first-write fault
      // which the kernel serves from the zero page — one page fault amortized
      // across the slot's entire lifetime, not a per-recycle syscall storm.

      final long owned = inProgress + 1L;
      c.slotVersion.setRelease(slotIdx, owned);

      c.liveCount.incrementAndGet();
      c.allocCount.incrementAndGet();
      acquired = true;
      HftBoundaryTelemetry.allocatorAllocation();
      assert (owned & 1L) == 0L : "owned version must be even";
      return slotIdx;
    } finally {
      if (!acquired) {
        activeBytes.addAndGet(-c.slotSize);
        activeSlotBytes.addAndGet(-c.slotSize);
      }
    }
  }

  boolean reserveCommittedBytes(final long bytes) {
    return reserveCommittedBytesUpTo(bytes, budgetBytes);
  }

  /**
   * Fresh-slot (slab) commitment is permanent until shutdown, so it may never consume the oversized
   * headroom — the slice that keeps large-value buffers allocatable after a slab peak.
   * Package-private so the witness test can drive the decision table directly.
   */
  boolean reserveSlabCommittedBytes(final long bytes) {
    return reserveCommittedBytesUpTo(bytes, budgetBytes - oversizedHeadroomBytes);
  }

  long oversizedHeadroomBytes() {
    return oversizedHeadroomBytes;
  }

  private boolean reserveCommittedBytesUpTo(final long bytes, final long limit) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("allocation size must be positive: " + bytes);
    }
    long current = committedBytes.getAcquire();
    while (true) {
      if (current > limit || bytes > limit - current) {
        return false;
      }
      if (committedBytes.compareAndSet(current, current + bytes)) {
        return true;
      }
      current = committedBytes.getAcquire();
    }
  }

  // Defensive lazy-init: production code calls Databases.initAllocator first, but focused page
  // tests construct pages directly. Match LinuxMemorySegmentAllocator's lazy behavior without
  // allowing a concurrent caller to observe classes before all per-class metadata is published.
  private void ensureInitialized() {
    if (terminated.get()) {
      throw new IllegalStateException("FrameSlotAllocator has been shut down");
    }
    if (classes == null) {
      synchronized (this) {
        if (terminated.get()) {
          throw new IllegalStateException("FrameSlotAllocator has been shut down");
        }
        if (classes == null) {
          LOGGER.warn("FrameSlotAllocator not initialized — auto-initializing with default 16 GiB budget");
          init(16L * 1024 * 1024 * 1024);
        }
      }
    }
  }

  /**
   * {@link MemorySegmentAllocator}-conforming allocate. Acquires a primitive slot index directly and
   * publishes the allocation scope in that slot's pre-indexed metadata.
   *
   * <p>
   * On class exhaustion, parks briefly and retries — the {@link ClockSweeper} runs on a daemon thread
   * and continuously releases stale slots, so the typical exhaustion is a transient spike rather than
   * a real limit. Mirrors {@code LinuxMemorySegmentAllocator}'s park-and-retry pattern (50 µs
   * initial, exponential up to 5 ms, 10 s total ceiling). A mid-query OOM cascades into partial-page
   * state and SIGSEGV downstream, so waiting is almost always the right call.
   */
  @Override
  public MemorySegment allocate(final long size) {
    if (size <= 0) {
      throw new IllegalArgumentException("allocation size must be positive: " + size);
    }
    ensureInitialized();
    if (size > SIZE_CLASSES[SIZE_CLASSES.length - 1]) {
      oversizedAllocationOperations.incrementAndGet();
      if (terminated.get()) {
        oversizedAllocationOperations.decrementAndGet();
        throw new IllegalStateException("FrameSlotAllocator has been shut down");
      }
      try {
        // Oversized request — larger than any frame-slot size class. These come from large-value
        // overflow storage (#1076): OverflowPage (de)compression buffers can exceed 256 KiB. Served
        // from a per-allocation CONFINED arena, closed deterministically in release(). Two reasons
        // over ofShared: (1) closing a shared arena is a thread-handshake — measurable on a path a
        // large blob read hits once per query; (2) native-image forbids Arena.ofShared close unless
        // -H:+SharedArenaSupport is on, and THAT flag is mutually exclusive with Vector API support
        // in GraalVM 25 — this method was the image's only shared-arena close, and it silently cost
        // the SIMD kernels. Confined-arena close carries neither cost, and the scoped decompress
        // path allocates and releases on the same thread; a cross-thread caller fails loudly with
        // WrongThreadException rather than leaking or deferring reclaim to GC.
        if (!reserveCommittedBytes(size)) {
          firePressure();
          // The retry can only observe relief from concurrent OVERSIZED releases (their bytes
          // return to committedBytes on close). Page eviction returns frame SLOTS, and
          // slab-region commitments never leave committedBytes before shutdown — report the
          // split so a post-peak OOM here is diagnosable as slab pressure, not a leak.
          if (!reserveCommittedBytes(size)) {
            throw new OutOfMemoryError("FrameSlotAllocator: oversized allocation of " + size + " bytes exceeds the "
                + budgetBytes + "-byte physical budget (committed=" + committedBytes.get()
                + " bytes — slab-region commitments release only at " + "shutdown; active=" + activeBytes.get()
                + " bytes)");
          }
        }
        Arena arena = null;
        MemorySegment segment = null;
        OversizedAllocation allocation = null;
        try {
          arena = Arena.ofConfined();
          segment = arena.allocate(size);
          allocation = new OversizedAllocation(arena, Thread.currentThread(), size);
          final OversizedAllocation prior = oversizedByAddress.putIfAbsent(segment.address(), allocation);
          if (prior != null) {
            throw new IllegalStateException("duplicate oversized allocation address " + segment.address());
          }
          HftBoundaryTelemetry.allocatorAllocation();
          HftBoundaryTelemetry.nativeAllocation();
          activeBytes.addAndGet(size);
          return segment;
        } catch (final RuntimeException | Error failure) {
          if (segment != null && allocation != null) {
            oversizedByAddress.remove(segment.address(), allocation);
          }
          if (arena != null) {
            try {
              arena.close();
            } catch (final RuntimeException cleanupFailure) {
              if (cleanupFailure != failure) {
                failure.addSuppressed(cleanupFailure);
              }
            }
          }
          committedBytes.addAndGet(-size);
          throw failure;
        }
      } finally {
        oversizedAllocationOperations.decrementAndGet();
      }
    }
    final int classIdx = indexForSize(size);
    if (classIdx < 0) {
      throw new IllegalArgumentException("requested size " + size + " exceeds largest class");
    }
    final SizeClass c = classes[classIdx];
    int slotIdx = acquireSlot(c);
    if (slotIdx < 0) {
      // First shot at relief before parking: the cache is the most likely
      // owner of the bytes we need, so ask it to evict synchronously. Skips
      // the 500 ms ClockSweeper cadence in the common hot-scan case.
      firePressure();
      slotIdx = acquireSlot(c);
    }
    if (slotIdx < 0) {
      long parkNanos = 50_000L;
      long totalWaitedNanos = 0L;
      final long maxWaitNanos = 10_000_000_000L;
      while (slotIdx < 0 && totalWaitedNanos < maxWaitNanos) {
        java.util.concurrent.locks.LockSupport.parkNanos(parkNanos);
        totalWaitedNanos += parkNanos;
        parkNanos = Math.min(parkNanos * 2, 5_000_000L);
        slotIdx = acquireSlot(c);
        if (slotIdx < 0) {
          firePressure();
          slotIdx = acquireSlot(c);
        }
      }
      if (slotIdx < 0) {
        LOGGER.warn("FrameSlotAllocator class {} saturated for {} ms (size {}): sweeper unable to free slots", classIdx,
            totalWaitedNanos / 1_000_000L, size);
        dumpStateForOOM(classIdx, size, totalWaitedNanos);
        throw new OutOfMemoryError("FrameSlotAllocator: size class " + classIdx + " exhausted for " + size
            + " bytes after " + (totalWaitedNanos / 1_000_000L) + " ms of retry");
      }
    }
    // A fresh scope is the allocation era's identity token. Slice/reinterpret-derived segments keep
    // it, while a stale segment at this recycled address carries a different scope (#1073 Defect A).
    // The primitive slot was already charged above, so every failure in the FFM wrapping or lazy
    // scope-table publication must return that exact slot and budget reservation.
    final long ownedVersion = c.slotVersion.getAcquire(slotIdx);
    try {
      final Arena scopeArena = Arena.ofShared();
      final MemorySegment segment =
          c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize).reinterpret(c.slotSize, scopeArena, null);
      if (!publishScope(c, slotIdx, segment.scope())) {
        throw new IllegalStateException(
            "slot " + slotIdx + " in size class " + classIdx + " retained a live allocation scope while marked free");
      }
      issuedSlotCount.incrementAndGet();
      return segment;
    } catch (final RuntimeException | Error failure) {
      // Do not close a shared identity arena here: GraalVM native-image requires
      // -H:+SharedArenaSupport for any shared-arena close call, which is mutually exclusive with the
      // Vector API configuration used by Sirix. No native memory was allocated from this arena.
      releaseSlot(classIdx, slotIdx, ownedVersion);
      throw failure;
    }
  }

  /**
   * {@link MemorySegmentAllocator}-conforming release. Decodes {@code segment.address()} directly
   * against the seven reserved regions and atomically clears the slot's exact allocation-era scope
   * before returning it to the recycled-slot set.
   *
   * <p>
   * Idempotent: a second release for the same address is a no-op. A release whose segment belongs to
   * a PRIOR allocation era of the address (a stale, dangling reference arriving after the slot was
   * recycled and re-issued) is detected via the per-allocation scope token and rejected — it must not
   * close the current owner's live slot (#1073 Defect A).
   */
  @Override
  public void release(final MemorySegment segment) {
    if (segment == null) {
      return;
    }
    final long address = segment.address();
    final int classIdx = classIndexForAddress(address);
    if (classIdx >= 0) {
      releaseFrameSegment(segment, address, classIdx);
      return;
    }

    // Oversized allocation (#1076): confined-arena-backed, not slot-backed — close the arena to
    // reclaim the native memory deterministically. Confined close must run on the allocating
    // thread; a cross-thread release throws WrongThreadException, surfacing the misuse loudly. This
    // boxed map probe is deliberately after primitive reserved-region decoding, so normal frame
    // release never creates a Long key merely to prove that it is not oversized.
    final OversizedAllocation oversized = oversizedByAddress.get(address);
    if (oversized != null) {
      if (oversized.owner() != Thread.currentThread()) {
        throw new java.lang.WrongThreadException();
      }
      // Remove the address mapping BEFORE closing the arena: close frees the native memory, and
      // a fresh oversized allocation can immediately reuse the same address — its putIfAbsent
      // would then collide with this stale entry and fail a VALID allocation with "duplicate
      // oversized allocation address". Remove-first means only a genuine double-release loses
      // the removal race and fails loudly.
      if (!oversizedByAddress.remove(address, oversized)) {
        throw new IllegalStateException("oversized allocation ownership changed during release at " + address);
      }
      oversized.arena().close();
      activeBytes.addAndGet(-oversized.bytes());
      committedBytes.addAndGet(-oversized.bytes());
      HftBoundaryTelemetry.allocatorRelease();
      HftBoundaryTelemetry.nativeRelease();
    }
  }

  private void releaseFrameSegment(final MemorySegment segment, final long address, final int classIdx) {
    final SizeClass c = classes[classIdx];
    final int slotIdx = slotIndexForAddress(c, address);
    if (slotIdx < 0) {
      return;
    }
    final MemorySegment.Scope allocationScope = segment.scope();
    if (!clearScope(c, slotIdx, allocationScope)) {
      final MemorySegment.Scope currentScope = currentScope(c, slotIdx);
      if (currentScope != null && currentScope != allocationScope) {
        // Stale double-release from a previous era of this recycled address. The current scope
        // belongs to a different, live allocation; leave it and all counters untouched.
        LOGGER.warn("Rejected stale release of address {} ({} bytes): the segment belongs to a prior "
            + "allocation era of this slot (double-release detected).", address, segment.byteSize());
      }
      return;
    }

    final long ownedVersion = c.slotVersion.getAcquire(slotIdx);
    if (!releaseSlot(classIdx, slotIdx, ownedVersion)) {
      // The exact current scope was claimed, so this can only indicate internal metadata corruption.
      // Restore ownership instead of leaking the live slot behind a null identity entry.
      if (!publishExistingScope(c, slotIdx, allocationScope)) {
        throw new IllegalStateException(
            "unable to restore allocation scope after failed slot release for class " + classIdx + ", slot " + slotIdx);
      }
      LOGGER.warn("Rejected stale release of address {} ({} bytes): the segment belongs to a prior "
          + "allocation era of this slot (double-release detected).", address, segment.byteSize());
      return;
    }
    issuedSlotCount.decrementAndGet();
  }

  /**
   * Reset ({@code MADV_DONTNEED}) the physical pages backing {@code segment} without releasing the
   * slot. Used by callers that want to reuse the slot with zero-filled content.
   */
  @Override
  public void resetSegment(final MemorySegment segment) {
    if (segment == null) {
      return;
    }
    VM.discardToZeros(segment);
  }

  /**
   * Dumps per-class allocator state when an allocation saturates — live slot count, total lifetime
   * allocates/releases, live interface scopes, and committed/active-byte accounting. Emitted to
   * stderr so it lands even when logback config swallows WARN. Diagnostic tool for cases where the
   * pool appears exhausted but the cache should have evicted.
   */
  private void dumpStateForOOM(final int failedClass, final long failedSize, final long waitedNanos) {
    final StringBuilder sb = new StringBuilder();
    sb.append("\n=== FrameSlotAllocator state at OOM ===\n");
    sb.append(String.format("  failed: class=%d size=%d bytes after=%d ms%n", failedClass, failedSize,
        waitedNanos / 1_000_000L));
    sb.append(String.format("  committedBytes=%d / budget=%d (%.1f%%), activeBytes=%d%n", committedBytes.get(),
        budgetBytes, 100.0 * committedBytes.get() / budgetBytes, activeBytes.get()));
    sb.append(String.format("  interface-issued slots=%d%n", issuedSlotCount.get()));
    for (int i = 0; i < classes.length; i++) {
      final SizeClass c = classes[i];
      sb.append(
          String.format("  class[%d] slotSize=%d  slots=%d  live=%d  alloc=%d  release=%d  recycled=%d  freshIdx=%d%n",
              i, c.slotSize, c.slotCount, c.liveCount.get(), c.allocCount.get(), c.releaseCount.get(),
              c.recycledCount.get(), c.nextFreshIndex.get()));
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

  /** Atomically carve a never-used index from the virtual region. */
  private static int popFreshSlot(final SizeClass c) {
    int fresh;
    do {
      fresh = c.nextFreshIndex.getAcquire();
      if (fresh >= c.slotCount) {
        return -1;
      }
    } while (!c.nextFreshIndex.compareAndSet(fresh, fresh + 1));

    return fresh;
  }

  private static int popRecycledSlot(final SizeClass c) {
    int available;
    do {
      available = c.recycledCount.getAcquire();
      if (available == 0) {
        return -1;
      }
    } while (!c.recycledCount.compareAndSet(available, available - 1));

    // Reserving one count credit guarantees that at least one free bit remains for this caller.
    // Other consumers may win individual bit races, so retry scans until this caller linearizes one
    // FREE-to-OWNED CAS; choosing a fresh slot here would strand a known recycled slot.
    final int hinted = c.recycleHint.getAndSet(-1);
    if (hinted >= 0 && tryClaimRecycledSlot(c, hinted)) {
      return hinted;
    }

    final int wordCount = c.recycledSlots.length();
    int firstWord = c.recycleScanCursor.getAcquire();
    for (int pass = 0; pass < MAX_RECYCLED_SCAN_PASSES; pass++) {
      for (int offset = 0; offset < wordCount; offset++) {
        final int wordIndex = (firstWord + offset) % wordCount;
        long freeBits = c.recycledSlots.get(wordIndex);
        while (freeBits != 0L) {
          final int bitIndex = Long.numberOfTrailingZeros(freeBits);
          final long claimedBits = freeBits & ~(1L << bitIndex);
          if (c.recycledSlots.compareAndSet(wordIndex, freeBits, claimedBits)) {
            final int slotIdx = (wordIndex << 6) + bitIndex;
            assert slotIdx < c.slotCount : "padding bit must never be published as free";
            updateRecycleScanCursor(c, wordIndex, claimedBits);
            return slotIdx;
          }
          freeBits = c.recycledSlots.get(wordIndex);
        }
      }
      Thread.onSpinWait();
      firstWord = c.recycleScanCursor.getAcquire();
    }

    // A caller can lose every observed bit to competing reserved consumers. Return this caller's
    // count credit after bounded full scans rather than spin indefinitely; a later attempt can take
    // the remaining recycled slot, while this attempt may safely fall back to a fresh index.
    c.recycledCount.incrementAndGet();
    return -1;
  }

  private static boolean tryClaimRecycledSlot(final SizeClass c, final int slotIdx) {
    final int wordIndex = slotIdx >>> 6;
    final long mask = 1L << slotIdx;
    long freeBits = c.recycledSlots.get(wordIndex);
    while ((freeBits & mask) != 0L) {
      final long claimedBits = freeBits & ~mask;
      if (c.recycledSlots.compareAndSet(wordIndex, freeBits, claimedBits)) {
        updateRecycleScanCursor(c, wordIndex, claimedBits);
        return true;
      }
      freeBits = c.recycledSlots.get(wordIndex);
    }
    return false;
  }

  private static void updateRecycleScanCursor(final SizeClass c, final int claimedWord, final long claimedBits) {
    if (claimedBits != 0L) {
      c.recycleScanCursor.setRelease(claimedWord);
      return;
    }
    final int nextWord = claimedWord + 1 == c.recycledSlots.length()
        ? 0
        : claimedWord + 1;
    c.recycleScanCursor.setRelease(nextWord);
  }

  private static void pushRecycledSlot(final SizeClass c, final int slotIdx) {
    final int wordIndex = slotIdx >>> 6;
    final long mask = 1L << slotIdx;
    long freeBits = c.recycledSlots.get(wordIndex);
    while (true) {
      if ((freeBits & mask) != 0L) {
        throw new IllegalStateException("slot " + slotIdx + " was already present in the recycled-slot set");
      }
      if (c.recycledSlots.compareAndSet(wordIndex, freeBits, freeBits | mask)) {
        break;
      }
      freeBits = c.recycledSlots.get(wordIndex);
    }
    // A release identifies a definitely nonempty word. Concurrent claims may make the hint stale,
    // but the bit CAS remains authoritative and the bounded scan will move on if needed.
    c.recycleScanCursor.setRelease(wordIndex);
    c.recycleHint.set(slotIdx);
    c.recycledCount.incrementAndGet();
  }

  /** Called by {@link FrameSlot#close()} — not usually invoked directly. */
  boolean releaseSlot(final int classIdx, final int slotIdx, final long versionAtAlloc) {
    final SizeClass c = classes[classIdx];
    final long prior = c.slotVersion.getAcquire(slotIdx);

    // Era check (#1073 Defect A, handle-API layer): a FrameSlot may only release the slot if the
    // slot's version is still the one it was allocated with. A stale handle from a previous
    // allocation era (the slot was already released and re-issued) sees an advanced version and
    // must be a no-op — otherwise it would push the slot index onto the free stack a second time
    // (two owners of one slot) and double-decrement the physical budget. The handle's own CAS
    // close-guard only protects against double-close of the SAME handle object.
    if (prior != versionAtAlloc) {
      LOGGER.warn("Rejected stale slot release: class {} slot {} (handle era {}, current era {}).", classIdx, slotIdx,
          versionAtAlloc, prior);
      return false;
    }

    // Step 1: bump to odd ("writer in progress"). Any reader that sees this
    // value between its pre and post snapshots detects the race.
    final long inProgress = prior | 1L;
    if (!c.slotVersion.compareAndSet(slotIdx, prior, inProgress)) {
      LOGGER.warn("Rejected concurrent slot release: class {} slot {} (handle era {}).", classIdx, slotIdx,
          versionAtAlloc);
      return false;
    }

    // Test-only: scribble the freed slot so a use-after-close reads zeros
    // deterministically. The version is already odd ("writer in progress"), so a
    // racing optimistic reader retries rather than observing the half-wiped slot.
    if (poisonOnRelease) {
      c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize).fill((byte) 0);
    }

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

    pushRecycledSlot(c, slotIdx);
    c.liveCount.decrementAndGet();
    c.releaseCount.incrementAndGet();
    activeSlotBytes.addAndGet(-c.slotSize);
    activeBytes.addAndGet(-c.slotSize);
    HftBoundaryTelemetry.allocatorRelease();
    return true;
  }

  private static boolean publishScope(final SizeClass c, final int slotIdx, final MemorySegment.Scope allocationScope) {
    final int chunkIndex = slotIdx >>> SCOPE_CHUNK_SHIFT;
    MemorySegment.Scope[] chunk = (MemorySegment.Scope[]) SCOPE_CHUNK.getAcquire(c.liveScopes, chunkIndex);
    if (chunk == null) {
      final MemorySegment.Scope[] newChunk = new MemorySegment.Scope[SCOPE_CHUNK_SIZE];
      if (SCOPE_CHUNK.compareAndSet(c.liveScopes, chunkIndex, null, newChunk)) {
        chunk = newChunk;
      } else {
        chunk = (MemorySegment.Scope[]) SCOPE_CHUNK.getAcquire(c.liveScopes, chunkIndex);
      }
    }
    return SCOPE_ENTRY.compareAndSet(chunk, slotIdx & SCOPE_CHUNK_MASK, null, allocationScope);
  }

  private static boolean publishExistingScope(final SizeClass c, final int slotIdx,
      final MemorySegment.Scope allocationScope) {
    final MemorySegment.Scope[] chunk =
        (MemorySegment.Scope[]) SCOPE_CHUNK.getAcquire(c.liveScopes, slotIdx >>> SCOPE_CHUNK_SHIFT);
    return chunk != null && SCOPE_ENTRY.compareAndSet(chunk, slotIdx & SCOPE_CHUNK_MASK, null, allocationScope);
  }

  private static boolean clearScope(final SizeClass c, final int slotIdx, final MemorySegment.Scope allocationScope) {
    final MemorySegment.Scope[] chunk =
        (MemorySegment.Scope[]) SCOPE_CHUNK.getAcquire(c.liveScopes, slotIdx >>> SCOPE_CHUNK_SHIFT);
    return chunk != null && SCOPE_ENTRY.compareAndSet(chunk, slotIdx & SCOPE_CHUNK_MASK, allocationScope, null);
  }

  private static MemorySegment.Scope currentScope(final SizeClass c, final int slotIdx) {
    final MemorySegment.Scope[] chunk =
        (MemorySegment.Scope[]) SCOPE_CHUNK.getAcquire(c.liveScopes, slotIdx >>> SCOPE_CHUNK_SHIFT);
    if (chunk == null) {
      return null;
    }
    return (MemorySegment.Scope) SCOPE_ENTRY.getAcquire(chunk, slotIdx & SCOPE_CHUNK_MASK);
  }

  private int classIndexForAddress(final long address) {
    final SizeClass[] currentClasses = classes;
    if (currentClasses == null) {
      return -1;
    }
    for (int classIdx = 0; classIdx < currentClasses.length; classIdx++) {
      final SizeClass c = currentClasses[classIdx];
      final long offset = address - c.baseAddress;
      if (Long.compareUnsigned(offset, c.regionBytes) < 0) {
        return classIdx;
      }
    }
    return -1;
  }

  /**
   * Return the exact slot whose first byte is {@code address}; interior and out-of-region addresses
   * fail.
   */
  private static int slotIndexForAddress(final SizeClass c, final long address) {
    final long offset = address - c.baseAddress;
    if (Long.compareUnsigned(offset, c.regionBytes) >= 0 || (offset & (c.slotSize - 1L)) != 0L) {
      return -1;
    }
    return Math.toIntExact(offset / c.slotSize);
  }


  // ===== Reader-side optimistic validation ===================================

  /**
   * Snapshot the current version of a specific {@code (classIdx, slotIdx)} pair. Callers about to
   * read slot bytes use the pattern:
   *
   * <pre>{@code
   *   long v1 = allocator.acquireVersion(classIdx, slotIdx);
   *   if ((v1 & 1L) != 0L) retry;                       // writer in progress
   *   // ... read slot bytes from resolved MemorySegment ...
   *   if (!allocator.validateVersion(classIdx, slotIdx, v1)) retry; // raced eviction
   * }</pre>
   *
   * <p>
   * A failed validation means the slot's content was evicted between the snapshot and the check; the
   * caller must re-resolve the page via the cache (which may return a different slot or a cache miss
   * that triggers a reload) and retry the read.
   */
  /**
   * Sentinel for {@link #slotCoordinates(MemorySegment)}: the segment is not (or no longer) a live
   * slot of this allocator — a heap-backed test segment, a pool-allocator segment, or a slot already
   * released. Pages backed by such segments cannot be torn by slot reuse, so stamp validation for
   * them is trivially true.
   */
  public static final long NO_SLOT_COORDINATES = -1L;

  /**
   * Locate the live slot backing {@code segment}, packed as {@code (classIdx << 32) | slotIdx}.
   *
   * <p>
   * Seven range checks at most, followed by one pre-indexed scope load. Callers bind the result ONCE
   * per page and thereafter read/validate the slot version with two plain array indexes — this lookup
   * must never sit on a per-read hot path. Scope identity is checked as well as address, so a stale
   * segment cannot bind to a newer owner at the same recycled address.
   *
   * @param segment a segment previously returned by {@link #allocate(long)}
   * @return packed coordinates, or {@link #NO_SLOT_COORDINATES} when the segment is not a live slot
   */
  public long slotCoordinates(final MemorySegment segment) {
    if (segment == null) {
      return NO_SLOT_COORDINATES;
    }
    final long address = segment.address();
    final int classIdx = classIndexForAddress(address);
    if (classIdx < 0) {
      return NO_SLOT_COORDINATES;
    }
    final SizeClass c = classes[classIdx];
    final int slotIdx = slotIndexForAddress(c, address);
    if (slotIdx < 0 || currentScope(c, slotIdx) != segment.scope()) {
      return NO_SLOT_COORDINATES;
    }
    return (((long) classIdx) << 32) | (slotIdx & 0xFFFF_FFFFL);
  }

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

  int recycledSlotCount(final int classIdx) {
    return classes[classIdx].recycledCount.get();
  }

  int recycledSlotBitCount(final int classIdx) {
    final AtomicLongArray recycledSlots = classes[classIdx].recycledSlots;
    int count = 0;
    for (int wordIndex = 0; wordIndex < recycledSlots.length(); wordIndex++) {
      count += Long.bitCount(recycledSlots.get(wordIndex));
    }
    return count;
  }

  int recycledScanWord(final int classIdx) {
    return classes[classIdx].recycleScanCursor.getAcquire();
  }

  /**
   * Tear down all mmap'd regions. Not called during normal operation; the allocator is expected to
   * live for the JVM's lifetime.
   */
  public synchronized void shutdown() {
    if (terminated.get()) {
      return;
    }

    // Full volatile operations are intentional. Admission and shutdown form a two-variable
    // handshake (terminal flag versus in-flight owner count); acquire/release-only accesses permit
    // the store-buffering outcome in which both sides miss the other's publication.
    terminated.set(true);
    final long liveSlotBytes = activeSlotBytes.get();
    if (liveSlotBytes != 0L) {
      terminated.set(false);
      throw new IllegalStateException("FrameSlotAllocator shutdown with " + liveSlotBytes
          + " active frame-slot bytes; release all slot owners before shutdown");
    }
    final int oversizedOperations = oversizedAllocationOperations.get();
    if (oversizedOperations != 0) {
      terminated.set(false);
      throw new IllegalStateException("FrameSlotAllocator shutdown raced " + oversizedOperations
          + " oversized allocation operation(s); retry after admission completes");
    }

    // Confined arenas preserve the same-thread, allocation-free normal release path. Validate all
    // owners before closing any so a foreign-thread leak fails atomically instead of leaving a
    // half-shut-down allocator. The owning thread can release it and retry shutdown.
    final Thread shutdownThread = Thread.currentThread();
    for (final OversizedAllocation oversized : oversizedByAddress.values()) {
      if (oversized.owner() != shutdownThread) {
        terminated.set(false);
        throw new IllegalStateException("FrameSlotAllocator shutdown cannot close an oversized allocation owned by "
            + oversized.owner().getName() + "; release it on its allocating thread first");
      }
    }

    Throwable cleanupFailure = null;
    for (final var entry : oversizedByAddress.entrySet()) {
      final long address = entry.getKey();
      final OversizedAllocation oversized = entry.getValue();
      try {
        oversized.arena().close();
        if (!oversizedByAddress.remove(address, oversized)) {
          throw new IllegalStateException("oversized allocation ownership changed during shutdown at " + address);
        }
        activeBytes.addAndGet(-oversized.bytes());
        committedBytes.addAndGet(-oversized.bytes());
        HftBoundaryTelemetry.allocatorRelease();
        HftBoundaryTelemetry.nativeRelease();
      } catch (final RuntimeException | Error failure) {
        cleanupFailure = retainCleanupFailure(cleanupFailure, failure);
      }
    }

    if (cleanupFailure != null) {
      terminated.set(false);
      rethrowCleanupFailure(cleanupFailure);
    }

    final SizeClass[] currentClasses = classes;
    if (currentClasses != null) {
      for (final SizeClass c : currentClasses) {
        if (VM.release(c.region)) {
          committedBytes.addAndGet(-c.committedBytes.getAndSet(0L));
          HftBoundaryTelemetry.nativeRelease();
        }
      }
    }
    classes = null;
    initialized.setRelease(false);
  }

  private static Throwable retainCleanupFailure(final Throwable primary, final Throwable secondary) {
    if (primary == null) {
      return secondary;
    }
    if (primary != secondary) {
      try {
        primary.addSuppressed(secondary);
      } catch (final RuntimeException | Error ignored) {
        // Preserve the first cleanup failure even if suppression itself is unavailable.
      }
    }
    return primary;
  }

  @SuppressWarnings("unchecked")
  private static <T extends Throwable> void rethrowCleanupFailure(final Throwable failure) throws T {
    throw (T) failure;
  }

  /**
   * Live handle for a frame slot. Holding this guarantees the allocator will not release the slot.
   * Close exactly once to return it to the free stack.
   */
  public static final class FrameSlot implements AutoCloseable {
    private static final VarHandle CLOSED;

    static {
      try {
        CLOSED = MethodHandles.lookup().findVarHandle(FrameSlot.class, "closed", boolean.class);
      } catch (final ReflectiveOperationException e) {
        throw new ExceptionInInitializerError(e);
      }
    }

    private final FrameSlotAllocator owner;
    private final int classIdx;
    private final int slotIdx;
    private final long versionAtAlloc;
    private final MemorySegment segment;
    @SuppressWarnings("unused") // accessed via the CLOSED VarHandle
    private volatile boolean closed;

    FrameSlot(final FrameSlotAllocator owner, final int classIdx, final int slotIdx, final long versionAtAlloc,
        final MemorySegment segment) {
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
      // Atomic close guard: a non-atomic check-then-set let two threads closing the same handle
      // both reach releaseSlot, pushing the slot index onto the free stack twice (two allocations
      // then own one slot) and double-decrementing the budget. CAS ensures exactly one closer.
      if (!CLOSED.compareAndSet(this, false, true)) {
        return;
      }
      owner.releaseSlot(classIdx, slotIdx, versionAtAlloc);
    }
  }
}
