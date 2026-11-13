/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.cache;

import io.sirix.HftBoundaryTelemetry;
import org.jspecify.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BiFunction;

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
 * <li>Handing a slot out and releasing it each move its version through an odd value to the next
 * even one, so any read pair that straddles a release or a re-issue always observes
 * {@code v1 != v2}.</li>
 * </ol>
 *
 * <h2>What a release does, and what it does not</h2>
 *
 * This is where the port departs from Umbra. Releasing a slot advances its version and puts its
 * index back into the class's recycled set; it does <b>not</b> discard the slot's pages (no
 * {@code MADV_DONTNEED}, no decommit). The pages stay committed and resident, the next owner of the
 * slot overwrites them in place, and a stale reader meanwhile sees stale-but-mapped bytes that its
 * version check rejects. Three consequences follow, and the rest of this class relies on each:
 *
 * <ul>
 * <li><b>The version is the only safety mechanism.</b> It detects stale content; it does not pin
 * memory. That is sufficient exactly because the pages are never unmapped while the allocator
 * lives.</li>
 * <li><b>Slot commitment is permanent.</b> The bytes of a slot enter {@link #committedBytes} when
 * the slot is first handed out and leave only at {@link #shutdown()}. Evicting pages therefore
 * frees <em>slots of their class</em>, never budget: a class that committed slots at a peak keeps
 * them, and another class cannot commit against them. The commitment limits below exist because of
 * this.</li>
 * <li><b>Recycled memory is dirty.</b> Nothing zeroes a slot between owners. The single exception
 * is {@link #resetSegment}, which discards pages on request and is not part of releasing or
 * recycling.</li>
 * </ul>
 *
 * Only memory outside the slot regions returns its commitment when released: oversized allocations,
 * cache-owned slabs and the mapped scratch regions.
 *
 * <h2>Size classes</h2>
 *
 * Seven power-of-two size classes match what {@code LinuxMemorySegmentAllocator} exposes today: 4
 * KiB through 256 KiB. Each class has its own mmap'd virtual region, version array, and
 * recycled-slot set; there is no cross-class contention. Callers locate the right class via
 * {@link #indexForSize(long)}.
 *
 * <h2>One owner for every reusable buffer</h2>
 *
 * Page frames are one of four things this allocator owns, all charged to the one physical budget:
 *
 * <ul>
 * <li><b>Page frames</b>: slots, held by cached pages and returned by eviction.</li>
 * <li><b>Scratch buffers</b> ({@link ScratchLease}, {@link FrameScratch}): every transient buffer
 * of the engine. Each buffer has its own mapped region, normalized from 4 KiB through 16 MiB,
 * exactly sized beyond; exclusively lent, kept resident between borrows, never evicted while lent.
 * No reader, serializer or codec keeps spare buffers of its own.</li>
 * <li><b>Cache-owned slabs</b> ({@link #allocateSlab}): page-granular regions whose owner's leases
 * decide their lifetime.</li>
 * <li><b>Oversized allocations</b> ({@link #allocate} beyond the largest class): confined to the
 * allocating thread.</li>
 * </ul>
 *
 * The budget is split by who may commit its last bytes. Page-frame slot commitment is permanent;
 * idle scratch and cache slabs return their commitment at shared admission. One eighth is an
 * execution floor, usable only through exclusive aggregation grants. Of the remaining seven
 * eighths, fresh page frames leave {@code oversizedHeadroom}, and cache slabs and oversized
 * allocations leave {@code scratchReserve}. Ordinary scratch may use those seven eighths. Thus
 * caches cannot consume the execution floor or the read/serialization progress reserve, even when
 * no scan is active. Idle aggregation regions must transfer their charge out of the execution floor
 * before being retained for reuse.
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
 * slot version counter closes this window: the release advances the version; a reader that
 * snapshotted the pre-eviction version observes a different post-eviction version and retries via
 * the cache, which re-resolves the {@code PageReference} to a fresh slot. The reader may have read
 * the next owner's bytes in between, but never an unmapped page: the slot's pages stay committed.
 * No SEGV.</li>
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

  /**
   * Budget share fresh page-frame slots may not commit — see {@link #initInternal}. Oversized
   * allocations, slabs and scratch buffers share it.
   */
  private volatile long oversizedHeadroomBytes;

  /**
   * The last bytes of the non-execution allowance, which only ordinary scratch may commit; part of
   * {@link #oversizedHeadroomBytes}. See {@link #SCRATCH_RESERVE_BYTES_PROPERTY}.
   */
  private volatile long scratchReserveBytes;

  /**
   * Explicit size of the budget share reserved for scratch buffers; the default is a quarter of the
   * oversized headroom, at most 256 MiB. Never more than that headroom, so page frames are
   * unaffected.
   */
  public static final String SCRATCH_RESERVE_BYTES_PROPERTY = "sirix.allocator.scratchReserveBytes";

  /** This allocator's scratch buffers; created with the size classes. */
  // Volatile publishes the scratch owner; its operations synchronize mutable state.
  @SuppressWarnings("java:S3077")
  private volatile FrameScratch scratch;

  /** Idle scratch buffers to keep per capacity; {@code 0} selects the configured default. */
  private final int scratchIdleBuffers;

  /**
   * Physical/touchable capacity retained by committed frame slots plus live oversized arenas. A frame
   * slot remains committed across recycle cycles, so its bytes leave this counter only when the whole
   * size-class region is released at shutdown. This is the counter constrained by the global budget
   * and exposed through {@link #getPhysicalMemoryBytes()}.
   */
  private final AtomicLong committedBytes = new AtomicLong();

  /** Non-execution commitment has its own ceiling; grant-backed regions never charge it. */
  private final AtomicLong nonExecutionCommittedBytes = new AtomicLong();
  private final AggregationBudget aggregationBudget = new AggregationBudget(this);

  /** Active charges, including the lifetime charge of small scratch regions. */
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

  /** Test seam: the registered listener, so a test can restore it instead of dropping it. */
  static PressureListener pressureListener() {
    return pressureListener;
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
  private static final BiFunction<Long, OversizedAllocation, OversizedAllocation> CLOSE_OVERSIZED =
      (address, allocation) -> {
        allocation.arena().close();
        return null;
      };

  /**
   * A cache-owned slab: one page-granular region with a global-scope segment, charged to the same
   * physical budget as oversized allocations until its backing is unmapped. No arena: the owner's
   * leases decide when nothing can still dereference it, and a global-scope segment carries no
   * per-access liveness check. Release may come from any thread.
   */
  private record SlabAllocation(MemorySegment segment, long bytes) {
  }

  private static final long SLAB_PAGE_BYTES = 4096L;

  private final ConcurrentHashMap<Long, SlabAllocation> slabsByAddress = new ConcurrentHashMap<>();

  private final AtomicLong slabBytes = new AtomicLong();
  private final AtomicInteger slabOperations = new AtomicInteger();
  // Volatile publishes the test callback reference; it does not protect mutable callback state.
  @SuppressWarnings("java:S3077")
  private volatile @Nullable Runnable slabPublicationHook;

  /**
   * Released slabs kept mapped for the next slab of the same page-rounded size (decision 104, the
   * off-heap lanes round): a query-local slab is mapped, zero-faulted and unmapped on every query
   * otherwise, which measured as +10 % on a 50 ms query. Their pages stay committed and dirty, so a
   * reuse pays neither the system calls nor the faults; the bytes stay in {@link #committedBytes} and
   * are counted in {@link #recycledSlabBytes}, never in {@link #slabBytes} (live slabs only). Bounded
   * by {@link #slabRecycleLimitBytes}; a release beyond the bound unmaps as before. Commitment
   * admission reclaims the shortfall only if idle backing can cover it
   * ({@link #reserveCommittedBytesUpTo}); {@link #shutdown()} drains it all. The holder of a recycled
   * slab is not a lease: a caller that releases a slab must never read it again, recycled or not.
   */
  private final ConcurrentHashMap<Long, ConcurrentLinkedDeque<SlabAllocation>> recycledSlabs =
      new ConcurrentHashMap<>();

  private final AtomicLong recycledSlabBytes = new AtomicLong();

  /** Bound on the recycled bytes; see {@link #SLAB_RECYCLE_BYTES_PROPERTY}. */
  private volatile long slabRecycleLimitBytes;

  /**
   * Explicit bound on the recycled slab bytes; the default is one sixteenth of the budget, at most
   * 512 MiB. Zero disables recycling.
   */
  public static final String SLAB_RECYCLE_BYTES_PROPERTY = "sirix.allocator.slabRecycleBytes";

  /**
   * Pre-fault a fresh slab's pages in one call ({@link VirtualMemory#populate}) instead of taking one
   * fault per 4 KiB page on the lane fill's first touch (the first-run admission of 180 MiB of join
   * lanes paid 44k faults, 2.4 us each); {@code false} turns it off. A recycled slab is resident
   * already and is never populated again.
   */
  public static final String SLAB_POPULATE_PROPERTY = "sirix.allocator.slabPopulate";

  private static volatile boolean populateSlabs =
      !"false".equalsIgnoreCase(System.getProperty(SLAB_POPULATE_PROPERTY, "true"));

  /**
   * Pre-fault a small scratch buffer's pages in one call ({@link VirtualMemory#populate}) the first
   * time a read borrows it. A positional read copies its bytes out in the kernel, so every page of
   * the buffer a read is the first to reach costs a fault inside that read (2.4 us each measured); in
   * a fresh process those reads are its first query's. The reader's pool of direct buffers this
   * scratch replaced was zeroed, and with that resident, when the reader class initialized.
   * {@code false} turns it off. A buffer is populated once: it stays mapped until the allocator shuts
   * down or reclaims it.
   */
  public static final String READ_SCRATCH_POPULATE_PROPERTY = "sirix.allocator.readScratchPopulate";

  /**
   * Bytes of fresh slabs whose pages were populated on allocation; recycled reuses are not counted.
   */
  private final AtomicLong populatedSlabBytes = new AtomicLong();

  static boolean setSlabPagePreparationForTesting(final boolean populate) {
    final boolean previous = populateSlabs;
    populateSlabs = populate;
    return previous;
  }

  /** Bytes of fresh slabs populated on allocation; see {@link #SLAB_POPULATE_PROPERTY}. */
  long getPopulatedSlabBytes() {
    return populatedSlabBytes.get();
  }

  /**
   * Test seam: fill a recycled slab with {@code 0x5A} before handing it out, so a consumer that
   * assumes zeroed memory fails instead of passing on a lucky layout. Never on in production.
   */
  private static volatile boolean poisonRecycledSlabs = Boolean.getBoolean("sirix.allocator.poisonRecycledSlabs");

  /** Test seam for {@link #poisonRecycledSlabs}; returns the previous setting. */
  static boolean setPoisonRecycledSlabsForTesting(final boolean value) {
    final boolean previous = poisonRecycledSlabs;
    poisonRecycledSlabs = value;
    return previous;
  }

  /** Test seam for the recycle bound; returns the previous bound. Lowering it drains the excess. */
  long setSlabRecycleLimitBytesForTesting(final long bytes) {
    if (bytes < 0) {
      throw new IllegalArgumentException("negative recycle bound " + bytes);
    }
    final long previous = slabRecycleLimitBytes;
    slabRecycleLimitBytes = bytes;
    final long excess = recycledSlabBytes.get() - bytes;
    if (excess > 0) {
      drainRecycledSlabs(excess, false);
    }
    return previous;
  }

  /**
   * Bytes held mapped for reuse by nobody in particular: recycled (released, still mapped) slabs and
   * idle scratch regions, which share the recycle bound. Included in
   * {@link #getPhysicalMemoryBytes()}.
   */
  @Override
  public long getRecycledSlabBytes() {
    return recycledSlabBytes.get();
  }

  /**
   * Unmap recycled slabs, then idle scratch regions, until at least {@code wanted} bytes of
   * commitment are returned or none are left; cold path (budget pressure, bound change, shutdown).
   * Memory somebody holds is never touched.
   *
   * @return the bytes returned to the budget
   */
  private long drainRecycledSlabs(final long wanted, final boolean includeSmallIdle) {
    long freed = 0L;
    for (final ConcurrentLinkedDeque<SlabAllocation> free : recycledSlabs.values()) {
      SlabAllocation recycled;
      while (freed < wanted && (recycled = free.pollLast()) != null) {
        recycledSlabBytes.addAndGet(-recycled.bytes());
        unmapSlab(recycled.segment(), recycled.bytes());
        freed += recycled.bytes();
      }
      if (freed >= wanted) {
        break;
      }
    }
    final FrameScratch scratchBuffers = scratch;
    if (freed < wanted && scratchBuffers != null) {
      freed += scratchBuffers.reclaimIdleRegions(wanted - freed, includeSmallIdle);
    }
    return freed;
  }

  /**
   * Count {@code bytes} of an idle scratch region against the recycle bound it shares with the
   * recycled slabs.
   *
   * @return whether the bound had room; on {@code false} the caller unmaps the region instead
   */
  boolean tryReserveRecycledBytes(final long bytes) {
    final long limit = slabRecycleLimitBytes;
    for (;;) {
      final long current = recycledSlabBytes.get();
      if (bytes > limit - current) {
        return false;
      }
      if (recycledSlabBytes.compareAndSet(current, current + bytes)) {
        return true;
      }
    }
  }

  /** Take {@code bytes} of an idle scratch region out of the recycle bound again. */
  void unreserveRecycledBytes(final long bytes) {
    recycledSlabBytes.addAndGet(-bytes);
  }

  void setSlabPublicationHookForTesting(final @Nullable Runnable hook) {
    slabPublicationHook = hook;
  }

  private void beforeSlabPublication() {
    final Runnable hook = slabPublicationHook;
    if (hook != null) {
      hook.run();
    }
  }

  /** Publish a live slab: the exact instance the caller received, by address. */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  private void publishSlab(final MemorySegment segment, final long mapped) {
    final Long address = segment.address();
    final SlabAllocation allocation = new SlabAllocation(segment, mapped);
    try {
      beforeSlabPublication();
      if (slabsByAddress.putIfAbsent(address, allocation) != null) {
        throw new IllegalStateException("duplicate slab address " + address);
      }
      HftBoundaryTelemetry.allocatorAllocation();
    } catch (final RuntimeException | Error failure) {
      slabsByAddress.remove(address, allocation);
      throw failure;
    }
    activeBytes.addAndGet(mapped);
    slabBytes.addAndGet(mapped);
  }

  @Override
  public @Nullable MemorySegment allocateSlab(final long bytes) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("slab size must be positive: " + bytes);
    }
    ensureInitialized();
    slabOperations.incrementAndGet();
    try {
      if (terminated.get()) {
        throw new IllegalStateException("FrameSlotAllocator has been shut down");
      }
      final long mapped = bytes > Long.MAX_VALUE - SLAB_PAGE_BYTES
          ? Long.MAX_VALUE
          : (bytes + SLAB_PAGE_BYTES - 1) & -SLAB_PAGE_BYTES;
      if (mapped > contentCommitLimit()) {
        return null;
      }
      final MemorySegment reused = reuseRecycledSlab(mapped);
      return reused != null
          ? reused
          : mapFreshSlab(mapped);
    } finally {
      slabOperations.decrementAndGet();
    }
  }

  /**
   * A recycled slab of {@code mapped} bytes, or {@code null} when none is idle: its pages are mapped
   * and committed already (dirty, never zeroed), so the reuse costs no system call and no page fault.
   * The caller gets a FRESH global-scope view over the same pages: the instance handed out before
   * stays rejected by the exact-instance check of releaseSlab, so a stale double release cannot free
   * the new owner's slab.
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure
  // is rethrown.
  @SuppressWarnings("java:S1181")
  private @Nullable MemorySegment reuseRecycledSlab(final long mapped) {
    final ConcurrentLinkedDeque<SlabAllocation> free = recycledSlabs.get(mapped);
    final SlabAllocation recycled = free == null
        ? null
        : free.pollFirst();
    if (recycled == null) {
      return null;
    }
    recycledSlabBytes.addAndGet(-mapped);
    try {
      final MemorySegment reused = MemorySegment.ofAddress(recycled.segment().address()).reinterpret(mapped);
      if (poisonRecycledSlabs) {
        reused.fill((byte) 0x5A);
      }
      publishSlab(reused, mapped);
      return reused;
    } catch (final RuntimeException | Error failure) {
      unmapSlab(recycled.segment(), mapped);
      throw failure;
    }
  }

  /**
   * A freshly mapped and committed slab of {@code mapped} bytes, or {@code null} when the budget or
   * the kernel refuses it. Charged against the whole budget like an oversized allocation: slab bytes
   * return on release. A refusal is reported, never retried through page-cache pressure: the caller
   * owns reclaimable images of its own and must give those up before a persisted page loses its
   * frame. Recycled slabs that would not fit beside the request are unmapped first
   * (reserveCommittedBytesUpTo).
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure
  // is rethrown.
  @SuppressWarnings("java:S1181")
  private @Nullable MemorySegment mapFreshSlab(final long mapped) {
    if (!reserveCommittedBytes(mapped)) {
      return null;
    }
    final MemorySegment segment;
    try {
      segment = VM.reserve(mapped);
    } catch (final RuntimeException | Error failure) {
      refundCommittedBytes(mapped);
      throw failure;
    }
    try {
      VM.commitFresh(segment);
      if (!prepareSlabPages(segment, mapped)) {
        // The kernel cannot back the pages: reported like a budget refusal, never thrown.
        VM.release(segment);
        refundCommittedBytes(mapped);
        return null;
      }
      HftBoundaryTelemetry.nativeAllocation();
      publishSlab(segment, mapped);
    } catch (final RuntimeException | Error failure) {
      VM.release(segment);
      refundCommittedBytes(mapped);
      throw failure;
    }
    return segment;
  }

  /**
   * Prepare a fresh slab's pages in one call, so the lane fill that follows writes resident pages
   * instead of taking one fault per page.
   *
   * @return {@code false} when the kernel cannot back the slab's pages; the caller releases the slab
   */
  private boolean prepareSlabPages(final MemorySegment segment, final long mapped) {
    if (!populateSlabs) {
      return true;
    }
    try {
      if (VM.populate(segment)) {
        populatedSlabBytes.addAndGet(mapped);
      }
      return true;
    } catch (final OutOfMemoryError refused) {
      LOGGER.debug("slab of {} bytes refused while populating its pages: {}", mapped, refused.getMessage());
      return false;
    }
  }

  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  @Override
  public boolean releaseSlab(final MemorySegment slab) {
    slabOperations.incrementAndGet();
    try {
      final long address = slab.address();
      final SlabAllocation allocation = slabsByAddress.get(address);
      // Exact instance: a stale handle whose address a newer slab reuses must not unmap that slab.
      if (allocation == null || allocation.segment() != slab) {
        LOGGER.warn("Rejected stale or repeated slab release of address {} ({} bytes)", address, slab.byteSize());
        return false;
      }
      if (!slabsByAddress.remove(address, allocation)) {
        return false;
      }
      final long bytes = allocation.bytes();
      activeBytes.addAndGet(-bytes);
      try {
        boolean recycled = false;
        try {
          HftBoundaryTelemetry.allocatorRelease();
          if (!terminated.get() && tryReserveRecycledBytes(bytes)) {
            recycled = true;
            beforeSlabPublication();
            recycledSlabs.computeIfAbsent(bytes, size -> new ConcurrentLinkedDeque<>()).addFirst(allocation);
            return true;
          }
        } catch (final RuntimeException | Error failure) {
          if (recycled) {
            recycledSlabBytes.addAndGet(-bytes);
          }
          unmapSlab(allocation.segment(), bytes);
          throw failure;
        }
        return unmapSlab(allocation.segment(), bytes);
      } finally {
        slabBytes.addAndGet(-bytes);
      }
    } finally {
      slabOperations.decrementAndGet();
    }
  }

  private boolean unmapSlab(final MemorySegment segment, final long bytes) {
    final boolean released = VM.release(segment);
    refundCommittedBytes(bytes);
    HftBoundaryTelemetry.nativeRelease();
    return released;
  }

  @Override
  public long getSlabBytes() {
    return slabBytes.get();
  }

  public static FrameSlotAllocator getInstance() {
    return INSTANCE;
  }

  /**
   * Internal no-arg constructor; production paths go through {@link #getInstance()} +
   * {@link #init(long)}. Tests may construct an independent instance directly.
   */
  FrameSlotAllocator() {
    this.scratchIdleBuffers = 0;
  }

  /** Test-only: construct and immediately initialize with the given budget. */
  public FrameSlotAllocator(final long budgetBytes) {
    this(budgetBytes, 0);
  }

  /**
   * Test-only: as {@link #FrameSlotAllocator(long)}, keeping {@code scratchIdleBuffers} idle scratch
   * buffers per capacity instead of the processor-dependent default ({@code 0}).
   */
  FrameSlotAllocator(final long budgetBytes, final int scratchIdleBuffers) {
    if (scratchIdleBuffers < 0) {
      throw new IllegalArgumentException("negative idle scratch buffer count " + scratchIdleBuffers);
    }
    this.scratchIdleBuffers = scratchIdleBuffers;
    initInternal(budgetBytes);
  }

  /**
   * Initialize the allocator with a physical-memory budget. Idempotent; a second call with a larger
   * budget is a no-op (we cannot cheaply grow an mmap'd region). Callers should call once at startup.
   */
  @Override
  public synchronized void init(final long maxBufferSize) {
    if (terminated.get()) {
      throw new IllegalStateException("FrameSlotAllocator has been shut down");
    }
    if (initialized.get()) {
      return;
    }
    final long granted = MemorySegmentAllocator.clampToPhysicalHeadroom(maxBufferSize);
    if (granted < maxBufferSize) {
      LOGGER.warn("Off-heap arena clamped from {} MiB to {} MiB to leave headroom for the heap and the OS",
          maxBufferSize / (1024 * 1024), granted / (1024 * 1024));
    }
    initInternal(granted);
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
    final long configuredRecycle = Long.getLong(SLAB_RECYCLE_BYTES_PROPERTY, -1L);
    // Recycled slabs are commitment that returns on pressure, so the bound may share the budget with
    // everything else; one sixteenth keeps a handful of query-local slabs (a few MiB each) resident.
    this.slabRecycleLimitBytes = configuredRecycle >= 0
        ? Math.min(configuredRecycle, budgetBytes)
        : Math.min(budgetBytes / 16, 512L << 20);
    // The last bytes of the budget belong to scratch alone. A read needs its buffer to make progress
    // at all, and evicting pages returns slots of their class, not budget; without this share a
    // budget filled by frames and slabs would leave a reader waiting for memory nothing can free.
    // Carved out of the oversized headroom, so page frames commit exactly what they did before, and
    // zero wherever that headroom is zero (tiny test budgets keep their exhaustion semantics).
    final long configuredScratchReserve = Long.getLong(SCRATCH_RESERVE_BYTES_PROPERTY, -1L);
    final long defaultScratchReserve = Math.min(oversizedHeadroomBytes / 4, 256L << 20);
    this.scratchReserveBytes = Math.min(oversizedHeadroomBytes, configuredScratchReserve >= 0
        ? configuredScratchReserve
        : defaultScratchReserve);
    this.scratch = new FrameScratch(this, scratchIdleBuffers > 0
        ? scratchIdleBuffers
        : FrameScratch.defaultIdleBuffers());
    this.classes = reserveSizeClasses(budgetBytes);
    this.initialized.set(true);
  }

  /**
   * Reserves the virtual region of every size class; a failure releases the regions reserved so far.
   * Per-class virtual reservation is workload-independent; MAP_NORESERVE means physical pages only
   * commit when touched. The number of slots is capped to bound version metadata; its fixed-size
   * chunks avoid persistent G1 humongous arrays without adding hot-path allocation.
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure
  // is rethrown.
  @SuppressWarnings("java:S1181")
  private static SizeClass[] reserveSizeClasses(final long budgetBytes) {
    final SizeClass[] cls = new SizeClass[SIZE_CLASSES.length];
    MemorySegment pending = null;
    try {
      for (int i = 0; i < SIZE_CLASSES.length; i++) {
        final long slotSize = SIZE_CLASSES[i];
        final long rawSlotCount = VIRTUAL_PER_CLASS / slotSize;
        final int slotCount = Math.toIntExact(Math.min(rawSlotCount, 1L << 20));
        final long regionBytes = (long) slotCount * slotSize;
        pending = VM.reserve(regionBytes);
        HftBoundaryTelemetry.nativeAllocation();
        cls[i] = new SizeClass(slotSize, slotCount, pending);
        pending = null;
        LOGGER.info("FrameSlotAllocator class {}: {} slots × {} bytes = {} MiB virtual", i, slotCount, slotSize,
            regionBytes / (1024 * 1024));
      }
      LOGGER.info("FrameSlotAllocator: shared physical budget = {} MiB", budgetBytes / (1024 * 1024));
    } catch (final RuntimeException | Error failure) {
      if (pending != null) {
        VM.release(pending);
      }
      for (final SizeClass allocated : cls) {
        if (allocated != null) {
          VM.release(allocated.region);
        }
      }
      throw failure;
    }
    return cls;
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

  /**
   * Bytes currently owned by callers: page frames, oversized allocations, slabs and lent scratch
   * buffers. Idle small-buffer charges are subtracted on demand; the gauge is exact while no scratch
   * borrow or return is in flight.
   */
  public long getActiveMemoryBytes() {
    final FrameScratch scratchBuffers = scratch;
    return activeBytes.get() - (scratchBuffers == null
        ? 0L
        : scratchBuffers.idleSmallBytes());
  }

  /** What this allocator's scratch buffers were asked for, hold and refused. */
  @Override
  public ScratchStats scratchStats() {
    final FrameScratch scratchBuffers = scratch;
    return scratchBuffers == null
        ? ScratchStats.EMPTY
        : scratchBuffers.stats();
  }

  /** This allocator's scratch buffers; initializes the allocator like any allocation does. */
  FrameScratch scratch() {
    ensureInitialized();
    return scratch;
  }

  /** The last bytes of the budget, which only scratch buffers may commit. */
  long scratchReserveBytes() {
    return scratchReserveBytes;
  }

  static VirtualMemory virtualMemory() {
    return VM;
  }

  boolean isTerminated() {
    return terminated.get();
  }

  /** Move the caller-owned byte gauge by a scratch region that was lent or returned. */
  void adjustActiveBytes(final long delta) {
    activeBytes.addAndGet(delta);
  }

  /** Return commitment taken with one of the {@code reserve…CommittedBytes} methods. */
  void refundCommittedBytes(final long bytes) {
    committedBytes.addAndGet(-bytes);
    nonExecutionCommittedBytes.addAndGet(-bytes);
  }

  @Override
  public AggregationBudget aggregationBudget() {
    ensureInitialized();
    return aggregationBudget;
  }

  /** Called only after the owning grant has charged the whole rounded region. */
  void commitAggregationBytes(final long bytes) {
    committedBytes.addAndGet(bytes);
  }

  void refundAggregationBytes(final long bytes) {
    committedBytes.addAndGet(-bytes);
  }

  /** An idle mapped buffer becomes execution memory without remapping it. */
  void transferToAggregation(final long bytes) {
    nonExecutionCommittedBytes.addAndGet(-bytes);
  }

  /** Idle execution memory may be retained only if it fits outside the protected execution floor. */
  boolean transferFromAggregation(final long bytes) {
    return tryReserveNonExecutionBytes(bytes, scratchCommitLimit());
  }

  public long nonExecutionCommittedBytes() {
    return nonExecutionCommittedBytes.get();
  }

  @Override
  public void free() {
    shutdown();
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
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
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
    try {
      final MemorySegment slot = c.region.asSlice((long) slotIdx * c.slotSize, c.slotSize);
      return new FrameSlot(this, classIdx, slotIdx, owned, slot);
    } catch (final RuntimeException | Error failure) {
      releaseSlot(classIdx, slotIdx, owned);
      throw failure;
    }
  }

  /**
   * Reserve one slot in {@code c}, returning its primitive index. The slot is exclusively owned when
   * this method returns; no handle, map entry, boxed address, or free-list node is created.
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
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
        slotIdx = commitFreshSlot(c);
        if (slotIdx < 0) {
          return -1;
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

      try {
        HftBoundaryTelemetry.allocatorAllocation();
      } catch (final RuntimeException | Error failure) {
        pushRecycledSlot(c, slotIdx);
        throw failure;
      }
      c.liveCount.incrementAndGet();
      c.allocCount.incrementAndGet();
      acquired = true;
      assert (owned & 1L) == 0L : "owned version must be even";
      return slotIdx;
    } finally {
      if (!acquired) {
        activeBytes.addAndGet(-c.slotSize);
        activeSlotBytes.addAndGet(-c.slotSize);
      }
    }
  }

  /**
   * Commit a never-used slot of {@code c}: a fresh slot adds retained commitment. Recycled slots do
   * not, because their pages deliberately remain committed, and charging them again would eventually
   * consume the budget with the same physical slot over and over.
   *
   * @return the slot's index, or {@code -1} when the budget or the class's region has no room
   */
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  private int commitFreshSlot(final SizeClass c) {
    final boolean reserved = reserveSlabCommittedBytes(c.slotSize);
    if (!reserved) {
      return -1;
    }
    final int slotIdx = popFreshSlot(c);
    if (slotIdx < 0) {
      refundCommittedBytes(c.slotSize);
      return -1;
    }
    final boolean committed;
    try {
      committed = VM.commitFresh(c.region.asSlice(slotIdx * c.slotSize, c.slotSize));
    } catch (final RuntimeException | Error failure) {
      refundCommittedBytes(c.slotSize);
      throw failure;
    }
    c.committedBytes.addAndGet(c.slotSize);
    if (committed) {
      try {
        HftBoundaryTelemetry.nativeAllocation();
      } catch (final RuntimeException | Error failure) {
        pushRecycledSlot(c, slotIdx);
        throw failure;
      }
    }
    return slotIdx;
  }

  /**
   * Commitment that returns when it is released: oversized allocations and slabs. It may use the
   * oversized headroom but not the scratch reserve at the very end of the budget.
   */
  boolean reserveCommittedBytes(final long bytes) {
    return reserveCommittedBytesUpTo(bytes, contentCommitLimit());
  }

  /**
   * Fresh-slot (slab) commitment is permanent until shutdown, so it may never consume the oversized
   * headroom — the slice that keeps large-value buffers allocatable after a slab peak.
   * Package-private so the witness test can drive the decision table directly.
   */
  boolean reserveSlabCommittedBytes(final long bytes) {
    return reserveCommittedBytesUpTo(bytes, frameCommitLimit());
  }

  /**
   * Commitment for ordinary scratch: the non-execution allowance, including its progress reserve.
   */
  boolean reserveScratchCommittedBytes(final long bytes) {
    return reserveCommittedBytesUpTo(bytes, scratchCommitLimit());
  }

  private long scratchCommitLimit() {
    return budgetBytes - budgetBytes / AggregationBudget.DIVISOR;
  }

  private long contentCommitLimit() {
    return scratchCommitLimit() - scratchReserveBytes;
  }

  private long frameCommitLimit() {
    return scratchCommitLimit() - oversizedHeadroomBytes;
  }

  private static OutOfMemoryError impossibleAllocation(final String kind, final long bytes, final long ceiling) {
    return new OutOfMemoryError("FrameSlotAllocator: " + kind + " capacity " + bytes + " bytes exceeds its " + ceiling
        + "-byte admission ceiling");
  }

  long oversizedHeadroomBytes() {
    return oversizedHeadroomBytes;
  }

  /**
   * Commitment for a scratch region of {@code bytes}.
   *
   * @param mandatory whether to ask the page caches for relief and wait, as a page frame does, before
   *        failing; an optional reservation is refused at once
   * @return whether the bytes are reserved; {@code false} only for an optional reservation
   * @throws OutOfMemoryError when a mandatory reservation cannot be served
   */
  boolean reserveScratchCommittedBytes(final long bytes, final boolean mandatory) {
    if (terminated.get()) {
      throw new IllegalStateException("FrameSlotAllocator has been shut down");
    }
    final long ceiling = scratchCommitLimit();
    if (bytes > ceiling) {
      if (!mandatory) {
        return false;
      }
      throw impossibleAllocation("scratch buffer", bytes, ceiling);
    }
    if (reserveScratchCommittedBytes(bytes)) {
      return true;
    }
    if (!mandatory) {
      return false;
    }
    long waitedNanos = 0L;
    firePressure();
    long parkNanos = INITIAL_PARK_NANOS;
    while (waitedNanos < MAX_WAIT_NANOS) {
      if (reserveScratchCommittedBytes(bytes)) {
        return true;
      }
      LockSupport.parkNanos(parkNanos);
      waitedNanos += parkNanos;
      parkNanos = Math.min(parkNanos * 2, MAX_PARK_NANOS);
      firePressure();
    }
    if (reserveScratchCommittedBytes(bytes)) {
      return true;
    }
    dumpStateForOOM(-1, bytes, waitedNanos);
    throw new OutOfMemoryError("FrameSlotAllocator: scratch buffer of " + bytes + " bytes does not fit the "
        + budgetBytes + "-byte physical budget (committed=" + committedBytes.get() + " bytes, active="
        + activeBytes.get() + " bytes) after " + (waitedNanos / 1_000_000L) + " ms of retry");
  }

  private static final long INITIAL_PARK_NANOS = 50_000L;
  private static final long MAX_PARK_NANOS = 5_000_000L;
  private static final long MAX_WAIT_NANOS = 10_000_000_000L;

  /**
   * A slot of class {@code classIdx}, asking the page caches for relief and waiting when the class is
   * exhausted: the {@link ClockSweeper} runs on a daemon thread and continuously releases stale
   * slots, so the typical exhaustion is a transient spike rather than a real limit. Parks 50 µs
   * initially, exponentially up to 5 ms, 10 s in total. A mid-query OOM cascades into partial-page
   * state downstream, so waiting is almost always the right call.
   *
   * @throws OutOfMemoryError when the class stays exhausted
   */
  private int acquireSlotOrWait(final int classIdx, final long size) {
    final SizeClass c = classes[classIdx];
    final long ceiling = frameCommitLimit();
    if (c.slotSize > ceiling) {
      throw impossibleAllocation("page frame", c.slotSize, ceiling);
    }
    int slotIdx = acquireSlot(c);
    if (slotIdx < 0) {
      // First shot at relief before parking: the cache is the most likely
      // owner of the bytes we need, so ask it to evict synchronously. Skips
      // the 500 ms ClockSweeper cadence in the common hot-scan case.
      firePressure();
      slotIdx = acquireSlot(c);
    }
    if (slotIdx < 0) {
      long parkNanos = INITIAL_PARK_NANOS;
      long totalWaitedNanos = 0L;
      while (slotIdx < 0 && totalWaitedNanos < MAX_WAIT_NANOS) {
        LockSupport.parkNanos(parkNanos);
        totalWaitedNanos += parkNanos;
        parkNanos = Math.min(parkNanos * 2, MAX_PARK_NANOS);
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
    return slotIdx;
  }

  private boolean reserveCommittedBytesUpTo(final long bytes, final long limit) {
    if (bytes <= 0) {
      throw new IllegalArgumentException("allocation size must be positive: " + bytes);
    }
    if (bytes > limit) {
      // Nothing that could be given back makes this fit; keep what is resident.
      return false;
    }
    // Recycled slabs and idle scratch regions hold commitment nothing reads: give back what the
    // request is short of before refusing anyone, and no more than that, so the rest stays resident
    // for its next borrower. The shortfall is re-read each round because other threads commit too.
    while (!tryReserveCommittedBytes(bytes, limit)) {
      final long shortBy = nonExecutionCommittedBytes.getAcquire() - (limit - bytes);
      if (shortBy <= 0L) {
        continue;
      }
      final FrameScratch scratchBuffers = scratch;
      final long reclaimable = recycledSlabBytes.get() + (scratchBuffers == null
          ? 0L
          : scratchBuffers.unchargedIdleSmallBytes());
      if (shortBy > reclaimable || drainRecycledSlabs(shortBy, true) == 0L) {
        return false;
      }
    }
    return true;
  }

  private boolean tryReserveCommittedBytes(final long bytes, final long limit) {
    if (!tryReserveNonExecutionBytes(bytes, limit)) {
      return false;
    }
    committedBytes.addAndGet(bytes);
    return true;
  }

  private boolean tryReserveNonExecutionBytes(final long bytes, final long limit) {
    long current = nonExecutionCommittedBytes.getAcquire();
    while (true) {
      if (current > limit || bytes > limit - current) {
        return false;
      }
      if (nonExecutionCommittedBytes.compareAndSet(current, current + bytes)) {
        return true;
      }
      current = nonExecutionCommittedBytes.getAcquire();
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
   * On class exhaustion, asks the page caches for relief, parks briefly and retries
   * ({@link #acquireSlotOrWait}), mirroring {@code LinuxMemorySegmentAllocator}'s park-and-retry
   * pattern.
   */
  // The oversized branch's confined arena is not a try-with-resources resource by design: it is
  // handed to the OversizedAllocation registered in #oversizedByAddress and outlives this call,
  // until release(MemorySegment) closes it on the allocating thread. Its own catch block closes it
  // on every failure path before registration completes.
  // Sonar S1181: an Error must not leak what is released here, which no collector frees. The first
  // failure is rethrown with later ones attached.
  // Sonar S3776: runs for every page and is kept as one compiled unit. Splitting it changes what the
  // compiler inlines on that path.
  @SuppressWarnings({"MustBeClosedChecker", "java:S1181", "java:S3776"})
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
        final long ceiling = contentCommitLimit();
        if (size > ceiling) {
          throw impossibleAllocation("oversized allocation", size, ceiling);
        }
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
        Long address = null;
        try {
          arena = Arena.ofConfined();
          segment = arena.allocate(size);
          address = segment.address();
          allocation = new OversizedAllocation(arena, Thread.currentThread(), size);
          final OversizedAllocation prior = oversizedByAddress.putIfAbsent(address, allocation);
          if (prior != null) {
            throw new IllegalStateException("duplicate oversized allocation address " + segment.address());
          }
          HftBoundaryTelemetry.allocatorAllocation();
          HftBoundaryTelemetry.nativeAllocation();
          activeBytes.addAndGet(size);
          return segment;
        } catch (final RuntimeException | Error failure) {
          if (address != null && allocation != null) {
            oversizedByAddress.remove(address, allocation);
          }
          if (arena != null) {
            try {
              arena.close();
            } catch (final RuntimeException | Error cleanupFailure) {
              retainCleanupFailure(failure, cleanupFailure);
            }
          }
          refundCommittedBytes(size);
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
    final int slotIdx = acquireSlotOrWait(classIdx, size);
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
        throw new WrongThreadException();
      }
      oversizedByAddress.computeIfPresent(address, CLOSE_OVERSIZED);
      activeBytes.addAndGet(-oversized.bytes());
      refundCommittedBytes(oversized.bytes());
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
   * Discard ({@code MADV_DONTNEED}) the physical pages backing {@code segment} without releasing the
   * slot, so that the caller, who must own the segment, reads zeros from it next and the pages fault
   * back in on the next write.
   *
   * <p>
   * This is <em>not</em> how a slot is recycled: {@link #release} keeps the pages, and nothing in the
   * allocator calls this. It is unsafe on memory another thread may still read optimistically, since
   * that reader would observe zeros with an unchanged version.
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
    sb.append(String.format("  slabBytes=%d live, recycledSlabBytes=%d of %d%n", slabBytes.get(),
        recycledSlabBytes.get(), slabRecycleLimitBytes));
    sb.append(
        String.format("  oversizedHeadroom=%d, scratchReserve=%d%n", oversizedHeadroomBytes, scratchReserveBytes));
    sb.append("  ").append(scratch.describe()).append('\n');
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

    for (int pass = 0; pass < MAX_RECYCLED_SCAN_PASSES; pass++) {
      // Never-allocated slots cannot be free. Fresh allocation advances this monotonic bound
      // before a release can publish a recycled count credit; refresh it on each retry because
      // competing consumers may claim old free bits while newly allocated slots are released.
      final int wordCount = (c.nextFreshIndex.getAcquire() + Long.SIZE - 1) >>> 6;
      final int firstWord = c.recycleScanCursor.getAcquire();
      int wordIndex = firstWord < wordCount
          ? firstWord
          : 0;
      for (int offset = 0; offset < wordCount; offset++) {
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
        if (++wordIndex == wordCount) {
          wordIndex = 0;
        }
      }
      Thread.onSpinWait();
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

  /**
   * Release a slot: advance its version and return its index to the class's recycled set. The slot's
   * pages stay committed and keep their contents.
   */
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
      final long end = (slotIdx + 1) * c.slotSize;
      for (long offset = slotIdx * c.slotSize; offset < end; offset += Long.BYTES) {
        c.region.set(ValueLayout.JAVA_LONG, offset, 0L);
      }
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
    activeBytes.addAndGet(-c.slotSize);
    activeSlotBytes.addAndGet(-c.slotSize);
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
  // Sonar S1181: an Error must not leak what is rolled back here, which no collector frees. The
  // failure is rethrown.
  @SuppressWarnings("java:S1181")
  public synchronized void shutdown() {
    if (terminated.get()) {
      return;
    }

    // Full volatile operations are intentional. Admission and shutdown form a two-variable
    // handshake (terminal flag versus in-flight owner count); acquire/release-only accesses permit
    // the store-buffering outcome in which both sides miss the other's publication.
    terminated.set(true);
    try {
      requireNoLiveOwners();
      closeOversizedAllocations();
    } catch (final RuntimeException | Error failure) {
      terminated.set(false);
      throw failure;
    }

    final SizeClass[] currentClasses = classes;
    if (currentClasses != null) {
      for (final SizeClass c : currentClasses) {
        if (VM.release(c.region)) {
          refundCommittedBytes(c.committedBytes.getAndSet(0L));
          HftBoundaryTelemetry.nativeRelease();
        }
      }
    }
    classes = null;
    initialized.setRelease(false);
  }

  /**
   * The first half of {@link #shutdown}: return what nobody reads (idle scratch, recycled slabs) and
   * fail while anything else is still held or an admission is in flight.
   */
  private void requireNoLiveOwners() {
    // A granted scan owns capacity even before its first mapping. Admission checks the terminal
    // flag under the budget monitor; taking that monitor here completes the shutdown handshake.
    final long liveAggregationCapacity = aggregationBudget.grantedBytes();
    if (liveAggregationCapacity != 0L) {
      throw new IllegalStateException("FrameSlotAllocator shutdown with " + liveAggregationCapacity
          + " bytes granted to aggregation; close the scans before shutdown");
    }
    // Idle scratch buffers are read by nobody: give their regions back first, so that only
    // memory somebody still holds can stop the shutdown below.
    final FrameScratch scratchBuffers = scratch;
    if (scratchBuffers != null) {
      scratchBuffers.reclaimAllIdle();
    }
    final long liveSlotBytes = activeSlotBytes.get();
    if (liveSlotBytes != 0L) {
      throw new IllegalStateException("FrameSlotAllocator shutdown with " + liveSlotBytes
          + " active frame-slot bytes; release all slot owners before shutdown");
    }
    final long liveScratchRegionBytes = scratchBuffers == null
        ? 0L
        : scratchBuffers.regionBytes();
    if (liveScratchRegionBytes != 0L) {
      throw new IllegalStateException("FrameSlotAllocator shutdown with " + liveScratchRegionBytes
          + " bytes of lent scratch regions; close their leases before shutdown");
    }
    final int oversizedOperations = oversizedAllocationOperations.get();
    if (oversizedOperations != 0) {
      throw new IllegalStateException("FrameSlotAllocator shutdown raced " + oversizedOperations
          + " oversized allocation operation(s); retry after admission completes");
    }
    final int pendingSlabOperations = slabOperations.get();
    if (pendingSlabOperations != 0) {
      throw new IllegalStateException("FrameSlotAllocator shutdown raced " + pendingSlabOperations
          + " slab operation(s); retry after admission completes");
    }
    // Recycled slabs are read by nobody: unmap them now. A live slab may still be read through a
    // lease; unmapping it here would be a use-after-free.
    drainRecycledSlabs(Long.MAX_VALUE, true);
    final long liveSlabBytes = slabBytes.get();
    if (liveSlabBytes != 0L) {
      throw new IllegalStateException("FrameSlotAllocator shutdown with " + liveSlabBytes
          + " live slab bytes; clear the derived caches that own them before shutdown");
    }
  }

  /**
   * The second half of {@link #shutdown}: close the oversized allocations of the shutting-down
   * thread, after checking that no other thread owns one.
   */
  private void closeOversizedAllocations() {
    // Confined arenas preserve the same-thread, allocation-free normal release path. Validate all
    // owners before closing any so a foreign-thread leak fails atomically instead of leaving a
    // half-shut-down allocator. The owning thread can release it and retry shutdown.
    final Thread shutdownThread = Thread.currentThread();
    for (final OversizedAllocation oversized : oversizedByAddress.values()) {
      if (oversized.owner() != shutdownThread) {
        throw new IllegalStateException("FrameSlotAllocator shutdown cannot close an oversized allocation owned by "
            + oversized.owner().getName() + "; release it on its allocating thread first");
      }
    }

    Throwable cleanupFailure = null;
    for (final var entry : oversizedByAddress.entrySet()) {
      final Long address = entry.getKey();
      final OversizedAllocation oversized = entry.getValue();
      try {
        oversized.arena().close();
        if (!oversizedByAddress.remove(address, oversized)) {
          throw new IllegalStateException("oversized allocation ownership changed during shutdown at " + address);
        }
        activeBytes.addAndGet(-oversized.bytes());
        refundCommittedBytes(oversized.bytes());
        HftBoundaryTelemetry.allocatorRelease();
        HftBoundaryTelemetry.nativeRelease();
      } catch (final RuntimeException | Error failure) {
        cleanupFailure = retainCleanupFailure(cleanupFailure, failure);
      }
    }

    if (cleanupFailure != null) {
      rethrowCleanupFailure(cleanupFailure);
    }
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
