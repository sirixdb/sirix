package io.sirix.cache;

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.lang.foreign.ValueLayout.*;

public final class LinuxMemorySegmentAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LinuxMemorySegmentAllocator.class.getName());

  private static final Linker LINKER = Linker.nativeLinker();

  // Static final MethodHandles for mmap and munmap to avoid repeated lookups
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

  // Define power-of-two sizes for mapping: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
  private static final long[] SEGMENT_SIZES =
      { FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB };
  private static final long MAX_BUFFER_SIZE = 1L << 30; // 1GB   FIXME: Make this configurable
  
  /**
   * UmbraDB-style: Calculate region size dynamically based on segment size.
   * Target 32 slices per region for optimal parallel workload performance.
   * Cap at 8MB for memory efficiency.
   */
  private static long getRegionSizeForPool(long segmentSize) {
    long optimalSize = segmentSize * 32;  // 32 slices per region
    return Math.min(8 * 1024 * 1024, Math.max(1024 * 1024, optimalSize));
  }

  /**
   * Represents a memory region that has been mmap'd and sliced into segments.
   * 
   * CRITICAL: MemorySegments from mmap() have an implicit global Arena.
   * They remain valid until we explicitly munmap them.
   * We do NOT create explicit Arenas - that would cause premature invalidation!
   */
  private static class MemoryRegion {
    final MemorySegment baseSegment;  // mmap'd segment with implicit global Arena
    final long segmentSize;
    final int poolIndex;
    final AtomicInteger unusedSlices;  // Count of slices currently in pool (available)
    final int totalSlices;
    final Set<Long> sliceAddresses;  // Track slice addresses for fast removal
    
    MemoryRegion(int poolIndex, long segmentSize, MemorySegment mmappedSegment) {
      this.poolIndex = poolIndex;
      this.segmentSize = segmentSize;
      this.totalSlices = (int) (mmappedSegment.byteSize() / segmentSize);
      this.unusedSlices = new AtomicInteger(totalSlices);
      this.sliceAddresses = ConcurrentHashMap.newKeySet();  // Thread-safe set
      
      // Use the mmap'd segment directly - it has an implicit global Arena
      // Segments stay valid until we munmap in freeRegion()
      this.baseSegment = mmappedSegment;
    }
    
    boolean allSlicesReturned() {
      return unusedSlices.get() == totalSlices;
    }
  }

  private final List<MemoryRegion> activeRegions = new ArrayList<>();  // Synchronized access
  private final Map<Long, MemoryRegion> regionByBaseAddress = new ConcurrentHashMap<>();  // Fast O(1) lookup
  private final AtomicLong totalMappedBytes = new AtomicLong(0);

  private final Deque<MemorySegment>[] segmentPools = new Deque[SEGMENT_SIZES.length];
  private final AtomicInteger[] poolSizes = new AtomicInteger[SEGMENT_SIZES.length];  // Track pool sizes without O(n) traversal

  private static final LinuxMemorySegmentAllocator INSTANCE = new LinuxMemorySegmentAllocator();

  private final AtomicBoolean isInitialized = new AtomicBoolean(false);

  private final AtomicLong maxBufferSize = new AtomicLong(MAX_BUFFER_SIZE);

  /**
   * Private constructor to enforce singleton pattern.
   * Use getInstance() to obtain the singleton instance.
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

    LOGGER.info("Initializing on-demand LinuxMemorySegmentAllocator with budget: {} bytes", maxBufferSize);

    this.maxBufferSize.set(maxBufferSize);

    // Initialize empty segment pools for each size class
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
      poolSizes[i] = new AtomicInteger(0);
    }

    LOGGER.info("On-demand allocator initialized - regions will be mmap'd as needed");
  }

  /**
   * Allocate one or more memory regions for the specified pool index.
   * UmbraDB-style: Use dynamic region sizes based on segment size.
   * 
   * @param poolIndex the pool to allocate for
   * @param minSegmentsNeeded minimum number of segments to ensure are available
   */
  private void allocateNewRegion(int poolIndex, int minSegmentsNeeded) {
    long segmentSize = SEGMENT_SIZES[poolIndex];
    long regionSize = getRegionSizeForPool(segmentSize);  // Dynamic size!
    int slicesPerRegion = (int) (regionSize / segmentSize);
    
    // For parallel workloads, allocate multiple regions if needed
    int regionsToAllocate = Math.max(1, (minSegmentsNeeded + slicesPerRegion - 1) / slicesPerRegion);
    
    LOGGER.info("Pool {} empty, allocating {} region(s) of {} MB for {} segments ({} slices/region)",
                poolIndex, regionsToAllocate, regionSize / (1024 * 1024), 
                minSegmentsNeeded, slicesPerRegion);
    
    for (int r = 0; r < regionsToAllocate; r++) {
      // mmap the region
      MemorySegment mmappedRegion = mapMemory(regionSize);
      
      // Create MemoryRegion wrapper
      MemoryRegion region = new MemoryRegion(poolIndex, segmentSize, mmappedRegion);
      
      // Slice region and add all slices to pool
      Deque<MemorySegment> pool = segmentPools[poolIndex];
      for (int i = 0; i < slicesPerRegion; i++) {
        MemorySegment slice = region.baseSegment.asSlice(i * segmentSize, segmentSize);
        region.sliceAddresses.add(slice.address());  // Track slice address
        pool.offer(slice);
      }
      poolSizes[poolIndex].addAndGet(slicesPerRegion);  // Update counter
      
      // Add to tracking structures
      synchronized (activeRegions) {
        activeRegions.add(region);
      }
      regionByBaseAddress.put(region.baseSegment.address(), region);
      totalMappedBytes.addAndGet(regionSize);
      
      LOGGER.info("Allocated region {} for pool {}: {} MB with {} slices (total mapped: {} MB)", 
                  r + 1, poolIndex, regionSize / (1024 * 1024), slicesPerRegion,
                  totalMappedBytes.get() / (1024 * 1024));
    }
  }

  @Override
  public long getMaxBufferSize() {
    return maxBufferSize.get();
  }

  /**
   * Get current pool sizes for diagnostics.
   * @return array of pool sizes, one per segment size
   */
  public int[] getPoolSizes() {
    int[] sizes = new int[SEGMENT_SIZES.length];
    for (int i = 0; i < segmentPools.length; i++) {
      sizes[i] = poolSizes[i].get();  // O(1) instead of O(n)
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
                         poolSizes[i].get() + " segments available");  // O(1) instead of O(n)
    }
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
      // Pool empty - need to allocate new region
      // CRITICAL: Synchronize to prevent multiple threads from racing to allocate
      // In parallel serialization, many threads hit empty pool simultaneously
      synchronized (pool) {
        // Double-check after acquiring lock - another thread might have allocated
        segment = pool.poll();
        if (segment == null) {
          LOGGER.debug("Pool {} empty, allocating new region", index);
          
          // Estimate parallel threads (ForkJoinPool.commonPool size + buffer)
          int parallelism = Math.max(8, Runtime.getRuntime().availableProcessors() * 2);
          
          // UmbraDB-style: Dynamic region size based on segment size
          long regionSize = getRegionSizeForPool(SEGMENT_SIZES[index]);
          long currentMapped = totalMappedBytes.get();
          
          if (currentMapped + regionSize > maxBufferSize.get()) {
            // UmbraDB approach: Try freeing unused regions from ANY pool
            // madvise makes this safe - virtual addresses stay valid
            long deficit = (currentMapped + regionSize) - maxBufferSize.get();
            
            LOGGER.info("Budget pressure: {} MB deficit, attempting to free unused regions", 
                       deficit / (1024 * 1024));
            
            freeUnusedRegionsForBudget(regionSize);
            
            // Check if we freed enough
            if (totalMappedBytes.get() + regionSize > maxBufferSize.get()) {
              LOGGER.error("Budget exceeded: {} MB limit, {} MB used, {} MB needed",
                          maxBufferSize.get() / (1024 * 1024),
                          totalMappedBytes.get() / (1024 * 1024),
                          regionSize / (1024 * 1024));
              LOGGER.error("Active regions: {}, all in use - consider increasing budget", 
                          activeRegions.size());
              throw new OutOfMemoryError("Memory budget exceeded: " + maxBufferSize.get() + 
                                        " bytes limit, all regions in use");
            }
          }
          
          // Allocate enough regions for parallel workload
          allocateNewRegion(index, parallelism);
          
          // Try again after allocation
          segment = pool.poll();
          if (segment == null) {
            throw new IllegalStateException("Pool still empty after region allocation! Pool size: " + poolSizes[index].get());
          }
        }
      }
    }

    // Decrement pool counter (O(1) instead of O(n) traversal)
    int poolSize = poolSizes[index].decrementAndGet();
    
    // CRITICAL: Also decrement the region's unused counter
    // This ensures freeUnusedRegions() won't free regions with segments in use!
    MemoryRegion region = findRegionForSegment(segment);
    if (region != null) {
      region.unusedSlices.decrementAndGet();
    }
    
    // Only log when pool is getting low (avoid overhead on every allocation)
    if (poolSize < 10) {
      LOGGER.warn("LOW ON SEGMENTS! Size: {}, Pool {} has only {} segments left", size, index, poolSize);
    }

    // Note: mmap with MAP_ANONYMOUS already zeros fresh pages on first access.
    // For reused segments, KeyValueLeafPage is responsible for clearing before use.
    // Not clearing here avoids massive performance overhead.
    return segment;
  }

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
    
    // Return the segment directly - we manage lifecycle explicitly via munmap
    // No Arena needed since we call munmap ourselves in releaseMemory()
    return addr.reinterpret(totalSize);
  }

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
   * This allows MemorySegments to remain valid - next access gets fresh zero page from OS.
   * CRITICAL for pooling: No SIGSEGV since virtual addresses never become invalid.
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
  public void release(MemorySegment segment) {
    long size = segment.byteSize();
    int index = SegmentAllocators.getIndexForSize(size);

    if (index >= 0 && index < segmentPools.length) {
      var returned = segmentPools[index].offer(segment);

      assert returned : "Must return segment to pool.";

      // Increment counter (O(1) instead of O(n) traversal)
      int poolSize = poolSizes[index].incrementAndGet();

      // Track that this slice was returned to its region
      MemoryRegion region = findRegionForSegment(segment);
      if (region != null) {
        region.unusedSlices.incrementAndGet();
        
        // CRITICAL FIX: Do NOT free regions immediately!
        // There's a race condition where segments can be allocated from pool
        // but not yet used when we free the region, causing SIGSEGV.
        // Only free regions when we need budget space (in freeUnusedRegions)
      }

      // Periodic logging to verify segments are being returned (less frequent)
      if (poolSize % 1000 == 0) {
        LOGGER.info("Segment returned: size={}, pool {} now has {} segments", size, index, poolSize);
      }
    } else {
      LOGGER.error("CANNOT RETURN SEGMENT! Invalid size: {}", size);
      throw new IllegalArgumentException("Segment size not supported for reuse: " + size);
    }
  }

  private MemoryRegion findRegionForSegment(MemorySegment segment) {
    long addr = segment.address();
    
    // Fast path: check if segment address is tracked in any region
    // This is O(n) in number of regions but much faster than checking ranges
    for (MemoryRegion region : regionByBaseAddress.values()) {
      if (region.sliceAddresses.contains(addr)) {
        return region;
      }
    }
    
    // Fallback: range-based lookup (should rarely be needed)
    for (MemoryRegion region : regionByBaseAddress.values()) {
      long baseAddr = region.baseSegment.address();
      long endAddr = baseAddr + region.baseSegment.byteSize();
      if (addr >= baseAddr && addr < endAddr) {
        return region;
      }
    }
    
    return null;
  }

  /**
   * UmbraDB approach: Release physical memory using madvise, keep virtual mapping.
   * This is safe for pooling - MemorySegments remain valid after madvise.
   */
  private void freeRegion(MemoryRegion region) {
    try {
      Deque<MemorySegment> pool = segmentPools[region.poolIndex];
      
      // Performance: Use unusedSlices count (O(1)) instead of stream
      int removedCount = region.unusedSlices.get();
      
      // Remove slices from pool
      pool.removeIf(seg -> region.sliceAddresses.contains(seg.address()));
      poolSizes[region.poolIndex].addAndGet(-removedCount);
      
      // UmbraDB approach: madvise instead of munmap
      // Releases physical memory but keeps virtual addresses valid
      // Segments in pool remain usable - NO SIGSEGV!
      releasePhysicalMemory(region.baseSegment, region.baseSegment.byteSize());
      
      // Update budget tracking (physical memory freed)
      long freedBytes = region.baseSegment.byteSize();
      totalMappedBytes.addAndGet(-freedBytes);
      
      // DON'T remove from activeRegions or regionByBaseAddress!
      // Virtual memory still mapped - regions can be reused
      
      LOGGER.info("Released {} MB physical memory from pool {} (virtual kept, total physical: {} MB)", 
                 freedBytes / (1024 * 1024), region.poolIndex,
                 totalMappedBytes.get() / (1024 * 1024));
    } catch (Exception e) {
      LOGGER.error("Failed to release region physical memory", e);
    }
  }

  /**
   * UmbraDB-style: Simple global budget enforcement.
   * Free unused regions from ANY pool until we have enough budget.
   * Performance-optimized with fast-path check and minimal locking.
   */
  private void freeUnusedRegionsForBudget(long memoryNeeded) {
    // PERFORMANCE: Fast-path check - avoid expensive work if all in use
    boolean hasUnused = false;
    synchronized (activeRegions) {
      for (MemoryRegion region : activeRegions) {
        if (region.allSlicesReturned()) {
          hasUnused = true;
          break;  // Early exit
        }
      }
    }
    
    if (!hasUnused) {
      LOGGER.debug("No unused regions available for budget reclaim");
      return;  // Fast path - nothing to free
    }
    
    // Collect candidates with minimal lock time
    List<MemoryRegion> candidates;
    synchronized (activeRegions) {
      candidates = new ArrayList<>(activeRegions.size());
      for (MemoryRegion region : activeRegions) {
        if (region.allSlicesReturned()) {
          candidates.add(region);
        }
      }
    }
    
    // Sort outside lock for better concurrency (largest first for efficiency)
    candidates.sort((r1, r2) -> Long.compare(r2.baseSegment.byteSize(), 
                                              r1.baseSegment.byteSize()));
    
    LOGGER.info("Budget reclaim: need {} MB, found {} unused regions",
               memoryNeeded / (1024 * 1024), candidates.size());
    
    // Free until budget satisfied
    long freed = 0;
    for (MemoryRegion region : candidates) {
      freeRegion(region);
      freed += region.baseSegment.byteSize();
      if (freed >= memoryNeeded) {
        LOGGER.info("Freed {} MB, budget satisfied", freed / (1024 * 1024));
        break;  // Early exit when enough freed
      }
    }
  }

  /**
   * Cleanup all mmap'd memory regions.
   * Called automatically during JVM shutdown.
   */
  @Override
  public void free() {
    if (!isInitialized.get()) {
      LOGGER.debug("Allocator is not initialized, nothing to free.");
      return;
    }

    List<MemoryRegion> regionsToFree;
    synchronized (activeRegions) {
      LOGGER.info("Cleaning up {} memory regions (total: {} MB)...", 
                 activeRegions.size(), totalMappedBytes.get() / (1024 * 1024));
      regionsToFree = new ArrayList<>(activeRegions);
      activeRegions.clear();
    }
    
    // Free all regions (munmap only - no arena to close)
    for (MemoryRegion region : regionsToFree) {
      try {
        releaseMemory(region.baseSegment, region.baseSegment.byteSize());
      } catch (Exception e) {
        LOGGER.error("Failed to release region: {}", e.getMessage());
      }
    }
    regionByBaseAddress.clear();

    // Clear all pools
    for (Deque<MemorySegment> pool : segmentPools) {
      pool.clear();
    }

    totalMappedBytes.set(0);
    isInitialized.set(false);
    LOGGER.info("LinuxMemorySegmentAllocator cleanup complete.");
  }

  public static void main(String[] args) {
    LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();

    MemorySegment segment4KB = allocator.allocate(4096);
    System.out.println("Allocated 4KB segment: " + segment4KB);

    MemorySegment segment128KB = allocator.allocate(131072);
    System.out.println("Allocated 128KB segment: " + segment128KB);

    allocator.release(segment4KB);
    allocator.release(segment128KB);
  }
}
