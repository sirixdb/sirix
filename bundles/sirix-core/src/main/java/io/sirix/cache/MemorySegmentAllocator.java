package io.sirix.cache;

import com.sun.management.OperatingSystemMXBean;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.management.ManagementFactory;

public interface MemorySegmentAllocator {

  int FOUR_KB = 4096;
  int EIGHT_KB = 8192;
  int SIXTEEN_KB = 16384;
  int THIRTYTWO_KB = 32768;
  int SIXTYFOUR_KB = 65536;
  int ONE_TWENTYEIGHT_KB = 131072;
  int TWO_FIFTYSIX_KB = 262144;

  int[] SEGMENT_SIZES =
      {FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB};

  void init(long maxBufferSize);

  /** Physical RAM left to the OS and page cache by {@link #clampToPhysicalHeadroom(long)}. */
  long MIN_OS_RESERVE_BYTES = 2L << 30;

  /** Arena floor: below this the size classes cannot hold a working set and every read thrashes. */
  long MIN_OFFHEAP_BYTES = 512L << 20;

  /**
   * Cap an arena request at what this machine can actually back, leaving room for the Java heap and
   * the OS. Implementations must apply this in {@link #init(long)} — the request reaching them is a
   * fixed size chosen without knowledge of the host (a persisted database config, or a benchmark
   * runner's default), and committing all of it on a smaller box gets the process OOM-killed by the
   * kernel. That failure is invisible to every in-process budget, because nothing in the JVM is over
   * ITS budget.
   *
   * @param requestedBytes the arena size the caller asked for
   * @return the size to actually reserve, never above the host's headroom
   */
  static long clampToPhysicalHeadroom(final long requestedBytes) {
    if (!(ManagementFactory.getOperatingSystemMXBean() instanceof OperatingSystemMXBean os)) {
      return requestedBytes; // unknown host — honour the request rather than guess
    }
    final long physical = os.getTotalMemorySize();
    if (physical <= 0) {
      return requestedBytes;
    }
    final long headroom = physical - Runtime.getRuntime().maxMemory() - Math.max(MIN_OS_RESERVE_BYTES, physical / 10);
    if (headroom < MIN_OFFHEAP_BYTES) {
      // The heap alone already crowds the box. Give the arena the floor it needs to function, so the
      // operator's -Xmx is what gets blamed rather than a silently unusable buffer pool.
      return Math.min(requestedBytes, MIN_OFFHEAP_BYTES);
    }
    return Math.min(requestedBytes, headroom);
  }

  /**
   * Check if the allocator has been initialized.
   * 
   * @return true if init() has been called and completed successfully
   */
  boolean isInitialized();

  void free();

  MemorySegment allocate(long size);

  void release(MemorySegment segment);

  long getMaxBufferSize();

  /**
   * Allocate a cache-owned slab: a page-granular native region whose segment has the global scope, so
   * reads carry no per-access liveness check and the owner frees it explicitly, from any thread, with
   * {@link #releaseSlab(MemorySegment)}. The bytes are charged to this allocator's physical budget
   * before the region is mapped. Release removes the live-slab charge exactly once; implementations
   * may keep recycled backing committed until it is unmapped.
   *
   * @param bytes the requested size; the charged size is rounded up to whole pages
   * @return the slab, or {@code null} when the budget cannot admit it. The caller reclaims its own
   *         retained images first; this allocator never evicts page caches on behalf of a slab.
   */
  default @Nullable MemorySegment allocateSlab(final long bytes) {
    return null;
  }

  /**
   * Allocate a slab with the ownership and release protocol of {@link #allocateSlab(long)} whose
   * bytes this allocator does NOT charge: the caller admitted them against its own byte ceiling and
   * is the single owner of that budget.
   *
   * <p>
   * Use it for storage a cache already bounds — a pool of blocks for that cache's entries — so the
   * pool cannot take bytes a page frame needs, and so nothing has to be handed back under pressure.
   * The slab is still tracked, released and reported like any other: only the commitment accounting
   * is the owner's. Releasing it refunds nothing here and never recycles its backing.
   * </p>
   *
   * @param bytes the positive requested size, rounded up to whole pages
   * @return the slab, or {@code null} on native reservation, commitment, population or shutdown
   *         refusal; heap and publication failures propagate
   */
  default @Nullable MemorySegment allocateOwnerBudgetedSlab(final long bytes) {
    return null;
  }

  /**
   * Release a slab obtained from {@link #allocateSlab(long)} or
   * {@link #allocateOwnerBudgetedSlab(long)}.
   * Idempotent: a stale or repeated release is rejected and returns {@code false}.
   *
   * @param slab the exact segment returned by either slab allocation method
   * @return whether this call returned the slab to the allocator and removed its live-slab charge;
   *         recycled backing may remain committed for reuse
   */
  default boolean releaseSlab(final MemorySegment slab) {
    throw new UnsupportedOperationException(getClass().getName() + " does not own slabs");
  }

  /** Bytes currently committed to live slabs, included in {@link #getPhysicalMemoryBytes()}. */
  default long getSlabBytes() {
    return 0L;
  }

  /**
   * Bytes of released slabs and idle scratch charged to an implementation's shared recycle bound,
   * included in {@link #getPhysicalMemoryBytes()} and returned to the budget under pressure. Small
   * scratch buffers retained outside that bound are reported by {@link #scratchStats()} instead.
   */
  default long getRecycledSlabBytes() {
    return 0L;
  }

  /**
   * What this allocator's scratch buffers ({@link ScratchLease}) were asked for, hold and refused.
   * Implementations that lend no scratch report {@link ScratchStats#EMPTY}.
   */
  default ScratchStats scratchStats() {
    return ScratchStats.EMPTY;
  }

  /** Exclusive aggregation capacity, owned by this allocator. */
  default AggregationBudget aggregationBudget() {
    throw new UnsupportedOperationException(getClass().getName() + " grants no execution capacity");
  }

  /**
   * Physical off-heap bytes currently committed by this allocator. Used by metrics; implementations
   * that don't track physical commitment return 0.
   *
   * @return committed physical bytes, or 0 if not tracked
   */
  default long getPhysicalMemoryBytes() {
    return 0L;
  }

  /**
   * Reset a memory segment by clearing its contents. Implementations should use the most efficient
   * approach available (e.g., madvise on Linux). Thread-safe: can be called concurrently on different
   * segments.
   * 
   * @param segment the segment to reset
   */
  void resetSegment(MemorySegment segment);
}
