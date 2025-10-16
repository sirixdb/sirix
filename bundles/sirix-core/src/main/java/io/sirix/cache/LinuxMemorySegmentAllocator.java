package io.sirix.cache;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.foreign.ValueLayout.*;

/**
 * Memory segment allocator using per-segment madvise (UmbraDB approach) with rebalancing.
 * Tracks borrowed segments globally to prevent duplicate returns.
 * Implements pool rebalancing to stay within memory budget.
 */
public final class LinuxMemorySegmentAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LinuxMemorySegmentAllocator.class.getName());

  private static final Linker LINKER = Linker.nativeLinker();

  // Static final MethodHandles for mmap, munmap, and madvise
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;
  private static final MethodHandle MADVISE;

  private static final int PROT_READ = 0x1;   // Page can be read
  private static final int PROT_WRITE = 0x2;  // Page can be written
  private static final int MAP_PRIVATE = 0x02; // Changes are private
  private static final int MAP_ANONYMOUS = 0x20; // Anonymous mapping
  private static final int MADV_DONTNEED = 4;  // Linux: release physical memory, keep virtual

  static {
    MMAP = LINKER.downcallHandle(LINKER.defaultLookup()
                                       .find("mmap")
                                       .orElseThrow(() -> new RuntimeException("mmap not found")),
                                 FunctionDescriptor.of(ADDRESS,
                                                       ADDRESS,
                                                       JAVA_LONG,
                                                       JAVA_INT,
                                                       JAVA_INT,
                                                       JAVA_INT,
                                                       JAVA_LONG));

    MUNMAP = LINKER.downcallHandle(LINKER.defaultLookup()
                                         .find("munmap")
                                         .orElseThrow(() -> new RuntimeException("munmap not found")),
                                   FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG));

    MADVISE = LINKER.downcallHandle(LINKER.defaultLookup()
                                          .find("madvise")
                                          .orElseThrow(() -> new RuntimeException("madvise not found")),
                                    FunctionDescriptor.of(JAVA_INT, ADDRESS, JAVA_LONG, JAVA_INT));
  }

  // Define power-of-two sizes: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
  private static final long[] SEGMENT_SIZES =
      { FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB };
  
  // Fixed region size per pool: 2MB (good balance for most workloads)
  private static final long REGION_SIZE = 2 * 1024 * 1024;

  // Singleton instance
  private static final LinuxMemorySegmentAllocator INSTANCE = new LinuxMemorySegmentAllocator();

  // State tracking
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final AtomicLong physicalMemoryBytes = new AtomicLong(0);
  private final AtomicLong maxBufferSize = new AtomicLong(Long.MAX_VALUE);

  // Size-class pools (one per segment size)
  @SuppressWarnings("unchecked")
  private final Deque<MemorySegment>[] segmentPools = new Deque[SEGMENT_SIZES.length];
  private final AtomicInteger[] poolSizes = new AtomicInteger[SEGMENT_SIZES.length];

  // Global borrowed segment tracking (prevents duplicate returns)
  private final ConcurrentHashMap<Long, Boolean> borrowedSegments = new ConcurrentHashMap<>();

  // NEW: Rebalancing infrastructure
  private final Map<Long, MemorySegment> mmappedBases = new ConcurrentHashMap<>();
  @SuppressWarnings("unchecked")
  private final AtomicLong[] totalBorrows = new AtomicLong[SEGMENT_SIZES.length];
  @SuppressWarnings("unchecked")
  private final AtomicLong[] totalReturns = new AtomicLong[SEGMENT_SIZES.length];

  /**
   * Private constructor to enforce singleton pattern.
   */
  private LinuxMemorySegmentAllocator() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Shutting down LinuxMemorySegmentAllocator...");
      free();
      LOGGER.info("LinuxMemorySegmentAllocator shutdown complete.");
    }));
  }

  public static LinuxMemorySegmentAllocator getInstance() {
    return INSTANCE;
  }

  @Override
  public void init(long maxBufferSize) {
    if (!isInitialized.compareAndSet(false, true)) {
      return;
    }

    LOGGER.info("Initializing LinuxMemorySegmentAllocator with budget: {} MB", 
               maxBufferSize / (1024 * 1024));
    
    this.maxBufferSize.set(maxBufferSize);

    // Initialize pools
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
      poolSizes[i] = new AtomicInteger(0);
      totalBorrows[i] = new AtomicLong(0);
      totalReturns[i] = new AtomicLong(0);
    }

    LOGGER.info("Allocator initialized - on-demand allocation with rebalancing");
  }

  @Override
  public long getMaxBufferSize() {
    return maxBufferSize.get();
  }

  /**
   * Get current pool sizes for diagnostics.
   */
  public int[] getPoolSizes() {
    int[] sizes = new int[SEGMENT_SIZES.length];
    for (int i = 0; i < segmentPools.length; i++) {
      sizes[i] = poolSizes[i].get();
    }
    return sizes;
  }

  /**
   * Print diagnostic information about pool state.
   */
  public void printPoolDiagnostics() {
    System.out.println("\n========== MEMORY SEGMENT POOL DIAGNOSTICS ==========");
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      System.out.println("Pool " + i + " (size " + SEGMENT_SIZES[i] + "): " + 
                         poolSizes[i].get() + " segments available");
    }
    System.out.println("Physical memory: " + (physicalMemoryBytes.get() / (1024 * 1024)) + " MB");
    System.out.println("Borrowed segments: " + borrowedSegments.size());
    System.out.println("mmap'd bases: " + mmappedBases.size());
    System.out.println("====================================================\n");
  }

  @Override
  public MemorySegment allocate(long size) {
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }

    Deque<MemorySegment> pool = segmentPools[index];
    MemorySegment segment = pool.poll();

    if (segment == null) {
      // Pool empty - allocate new segments
      synchronized (pool) {
        // Double-check after lock
        segment = pool.poll();
        if (segment == null) {
          LOGGER.debug("Pool {} empty, allocating new segments", index);
          allocateSegmentsForPool(index);
          segment = pool.poll();
          if (segment == null) {
            throw new IllegalStateException("Pool still empty after allocation!");
          }
        }
      }
    }

    // Decrement pool counter
    poolSizes[index].decrementAndGet();

    // Track as borrowed (detect duplicates)
    long address = segment.address();
    Boolean previous = borrowedSegments.putIfAbsent(address, Boolean.TRUE);
    if (previous != null) {
      LOGGER.error("CRITICAL: Segment {} was already borrowed! (pool {})", address, index);
    }

    // Track utilization
    totalBorrows[index].incrementAndGet();

    // Allocate logging disabled for performance
    // LOGGER.trace("ALLOCATE: address={}, pool={}, size={}", address, index, size);

    // Note: mmap with MAP_ANONYMOUS gives zero-filled fresh segments.
    // Reused segments should still have previous data (no madvise in release).
    return segment;
  }

  /**
   * Allocate a batch of segments for the given pool.
   * UmbraDB approach: mmap a region, slice it, add to pool.
   * New segments are zero-filled by OS (MAP_ANONYMOUS).
   * Reused segments are zeroed via madvise in release().
   */
  private void allocateSegmentsForPool(int poolIndex) {
    long segmentSize = SEGMENT_SIZES[poolIndex];
    int segmentsPerRegion = (int) (REGION_SIZE / segmentSize);

    // Allocate base segment
    MemorySegment base = mapMemory(REGION_SIZE);
    mmappedBases.put(base.address(), base);
    
    LOGGER.info("Allocated {} MB region for pool {} ({} x {} byte segments)", 
               REGION_SIZE / (1024 * 1024), poolIndex, segmentsPerRegion, segmentSize);

    // Slice into segments and add to pool
    Deque<MemorySegment> pool = segmentPools[poolIndex];
    for (int i = 0; i < segmentsPerRegion; i++) {
      MemorySegment slice = base.asSlice(i * segmentSize, segmentSize);
      pool.offer(slice);
    }

    poolSizes[poolIndex].addAndGet(segmentsPerRegion);
    physicalMemoryBytes.addAndGet(REGION_SIZE);
  }


  /**
   * Map memory using mmap.
   * Public for testing purposes.
   */
  public MemorySegment mapMemory(long totalSize) {
    MemorySegment addr;
    try {
      addr = (MemorySegment) MMAP.invoke(MemorySegment.NULL,
                                         totalSize,
                                         PROT_READ | PROT_WRITE,
                                         MAP_PRIVATE | MAP_ANONYMOUS,
                                         -1,
                                         0);
    } catch (Throwable e) {
      throw new RuntimeException("Failed to allocate memory via mmap", e);
    }
    if (addr == MemorySegment.NULL) {
      throw new OutOfMemoryError("Failed to allocate memory via mmap");
    }
    
    return addr.reinterpret(totalSize);
  }

  /**
   * Release memory using munmap.
   * Public for testing purposes.
   */
  public void releaseMemory(MemorySegment addr, long size) {
    try {
      int result = (int) MUNMAP.invoke(addr, size);
      if (result != 0) {
        throw new RuntimeException("Failed to release memory via munmap");
      }
    } catch (Throwable e) {
      throw new RuntimeException("Failed to release memory via munmap", e);
    }
  }

  /**
   * UmbraDB approach: Release physical memory but keep virtual address mapping.
   * Next access gets fresh zero page from OS.
   */
  private void releasePhysicalMemory(MemorySegment segment, long size) {
    try {
      int result = (int) MADVISE.invoke(segment, size, MADV_DONTNEED);
      if (result != 0) {
        LOGGER.warn("madvise MADV_DONTNEED failed for address {}, size {} (error code: {})",
                   segment.address(), size, result);
      }
    } catch (Throwable e) {
      LOGGER.error("madvise invocation failed", e);
    }
  }

  @Override
  public void resetSegment(MemorySegment segment) {
    if (segment == null) {
      return; // Already released/nulled
    }
    // NO-OP: Don't call madvise here
    // Memory is zeroed in release() when segment is returned to pool
    // This avoids over-zealous zeroing when clear() is called multiple times
    // LOGGER.trace("RESET_SEGMENT: address={}, size={} (NO-OP)", segment.address(), segment.byteSize());
  }

  @Override
  public void release(MemorySegment segment) {
    if (segment == null) {
      return; // Already released/nulled
    }

    long size = segment.byteSize();
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      LOGGER.error("CANNOT RETURN SEGMENT! Invalid size: {}", size);
      throw new IllegalArgumentException("Segment size not supported for reuse: " + size);
    }

    // Check if this segment was borrowed (detect duplicates)
    long address = segment.address();
    Boolean removed = borrowedSegments.remove(address);
    if (removed == null) {
      LOGGER.warn("RELEASE_DUPLICATE: address={}, pool={}, from={}", 
                  address, index, Thread.currentThread().getName());
      return; // Ignore duplicate return
    }

    // Release logging disabled for performance
    // LOGGER.trace("RELEASE: address={}, pool={}, size={}", address, index, size);

    // Track utilization
    totalReturns[index].incrementAndGet();

    releasePhysicalMemory(segment, size);
    physicalMemoryBytes.addAndGet(-size);
    segmentPools[index].offer(segment);
    int poolSize = poolSizes[index].incrementAndGet();

    // Periodic logging
    if (poolSize % 1000 == 0) {
      LOGGER.info("Segment returned: size={}, pool {} now has {} segments", size, index, poolSize);
    }
  }

  @Override
  public void free() {
    if (!isInitialized.get()) {
      LOGGER.debug("Allocator not initialized, nothing to free");
      return;
    }

    LOGGER.info("Cleaning up allocator (physical memory: {} MB, borrowed: {})",
               physicalMemoryBytes.get() / (1024 * 1024),
               borrowedSegments.size());

    // Clear pools
    for (int i = 0; i < segmentPools.length; i++) {
      if (segmentPools[i] != null) {
        segmentPools[i].clear();
        poolSizes[i].set(0);
        totalBorrows[i].set(0);
        totalReturns[i].set(0);
      }
    }

    borrowedSegments.clear();
    mmappedBases.clear();
    physicalMemoryBytes.set(0);
    isInitialized.set(false);
    
    LOGGER.info("LinuxMemorySegmentAllocator cleanup complete");
  }

  /**
   * Print current memory statistics.
   */
  public void printMemoryStats() {
    LOGGER.info("Memory: Physical={} MB, Borrowed segments={}, mmap'd bases={}",
               physicalMemoryBytes.get() / (1024 * 1024),
               borrowedSegments.size(),
               mmappedBases.size());
  }

  public static void main(String[] args) {
    LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();
    allocator.init(1L << 30);

    MemorySegment segment4KB = allocator.allocate(4096);
    System.out.println("Allocated 4KB segment: " + segment4KB);

    MemorySegment segment128KB = allocator.allocate(131072);
    System.out.println("Allocated 128KB segment: " + segment128KB);

    allocator.release(segment4KB);
    allocator.release(segment128KB);
    
    allocator.printMemoryStats();
  }
}
