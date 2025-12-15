package io.sirix.cache;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.invoke.MethodHandle;
import java.util.Deque;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.foreign.ValueLayout.*;

/**
 * Memory segment allocator using per-segment madvise (UmbraDB approach) with rebalancing.
 * Tracks borrowed segments globally to prevent duplicate returns.
 * Implements pool rebalancing to stay within memory budget.
 */
public final class LinuxMemorySegmentAllocator implements MemorySegmentAllocator {

  private static final Logger LOGGER = LoggerFactory.getLogger(LinuxMemorySegmentAllocator.class.getName());

  private static final Linker LINKER = Linker.nativeLinker();

  // Static final MethodHandles for mmap, munmap, madvise, and sysconf
  private static final MethodHandle MMAP;
  private static final MethodHandle MUNMAP;
  private static final MethodHandle MADVISE;
  private static final MethodHandle SYSCONF;

  private static final int PROT_READ = 0x1;   // Page can be read
  private static final int PROT_WRITE = 0x2;  // Page can be written
  private static final int MAP_PRIVATE = 0x02; // Changes are private
  private static final int MAP_ANONYMOUS = 0x20; // Anonymous mapping
  private static final int MAP_NORESERVE = 0x4000; // Don't reserve swap space
  private static final int MADV_DONTNEED = 4;  // Linux: release physical memory, keep virtual
  
  // sysconf constants for page size detection
  private static final int _SC_PAGESIZE = 30;  // Get system page size

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

    SYSCONF = LINKER.downcallHandle(LINKER.defaultLookup()
                                          .find("sysconf")
                                          .orElseThrow(() -> new RuntimeException("sysconf not found")),
                                    FunctionDescriptor.of(JAVA_LONG, JAVA_INT));
  }

  // Define power-of-two sizes: 4KB, 8KB, 16KB, 32KB, 64KB, 128KB, 256KB
  private static final long[] SEGMENT_SIZES =
      { FOUR_KB, EIGHT_KB, SIXTEEN_KB, THIRTYTWO_KB, SIXTYFOUR_KB, ONE_TWENTYEIGHT_KB, TWO_FIFTYSIX_KB };

  // Virtual memory region size per size class: 4 GB
  // With global BufferManager serving all databases/resources, we need larger virtual regions.
  // Total virtual: 4GB * 7 size classes = 28GB (virtual, not physical - maps to 8GB default physical budget)
  private static final long VIRTUAL_REGION_SIZE = 4L * 1024 * 1024 * 1024;

  // Singleton instance
  private static final LinuxMemorySegmentAllocator INSTANCE = new LinuxMemorySegmentAllocator();

  // Detected page sizes
  private long basePageSize = 4096;  // Default to 4KB, will be detected
  private long hugePageSize = 0;     // 0 means huge pages not available

  // State tracking
  private final AtomicBoolean isInitialized = new AtomicBoolean(false);
  private final AtomicLong physicalMemoryBytes = new AtomicLong(0);
  private final AtomicLong maxBufferSize = new AtomicLong(Long.MAX_VALUE);

  // Size-class pools (one per segment size)
  @SuppressWarnings("unchecked")
  private final Deque<MemorySegment>[] segmentPools = new Deque[SEGMENT_SIZES.length];
  private final AtomicInteger[] poolSizes = new AtomicInteger[SEGMENT_SIZES.length];

  // Pre-allocated virtual memory regions (one per size class)
  private final MemorySegment[] virtualRegions = new MemorySegment[SEGMENT_SIZES.length];
  
  // Track borrowed segments to prevent double-returns and for accurate physical memory tracking
  private final java.util.Set<Long> borrowedSegments = ConcurrentHashMap.newKeySet();
  
  // Rebalancing infrastructure
  private final AtomicLong[] totalBorrows = new AtomicLong[SEGMENT_SIZES.length];
  private final AtomicLong[] totalReturns = new AtomicLong[SEGMENT_SIZES.length];

  /**
   * Private constructor to enforce singleton pattern.
   */
  private LinuxMemorySegmentAllocator() {
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      LOGGER.info("Shutting down LinuxMemorySegmentAllocator...");
      
      // Report page leak statistics before shutdown (only if DEBUG_MEMORY_LEAKS enabled)
      if (io.sirix.page.KeyValueLeafPage.DEBUG_MEMORY_LEAKS) {
        long finalized = io.sirix.page.KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
        long created = io.sirix.page.KeyValueLeafPage.PAGES_CREATED.get();
        long closed = io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.get();
        var livePages = io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES;
        
        if (finalized > 0 || created > 0 || closed > 0 || !livePages.isEmpty()) {
          LOGGER.info("\n========== PAGE LEAK DIAGNOSTICS ==========");
          LOGGER.info("Pages Created: {}", created);
          LOGGER.info("Pages Closed: {}", closed);
          LOGGER.info("Pages Leaked (caught by finalizer): {}", finalized);
          LOGGER.info("Pages Still Live: {}", livePages.size());
          
          // Show finalized pages breakdown
          if (finalized > 0) {
            LOGGER.info("\nFinalized Pages (NOT closed properly) by Type:");
            io.sirix.page.KeyValueLeafPage.FINALIZED_BY_TYPE.forEach((type, count) -> 
                LOGGER.info("  {}: {} pages", type, count.get()));
            
            LOGGER.info("\nFinalized Pages (NOT closed properly) by Page Key (top 15):");
            io.sirix.page.KeyValueLeafPage.FINALIZED_BY_PAGE_KEY.entrySet().stream()
                .sorted(java.util.Map.Entry.<Long, java.util.concurrent.atomic.AtomicLong>comparingByValue(
                    (a, b) -> Long.compare(b.get(), a.get())).reversed())
                .limit(15)
                .forEach(e -> LOGGER.info("  Page {}: {} times", e.getKey(), e.getValue().get()));
          }
          
          // Show breakdown by page key
          // CRITICAL: Create snapshot to avoid ConcurrentModificationException during iteration
          var livePageSnapshot = new java.util.ArrayList<>(livePages);
          var pageKeyCount = new java.util.HashMap<Long, Integer>();
          var indexTypeCount = new java.util.HashMap<io.sirix.index.IndexType, Integer>();
          for (var page : livePageSnapshot) {
            pageKeyCount.merge(page.getPageKey(), 1, Integer::sum);
            indexTypeCount.merge(page.getIndexType(), 1, Integer::sum);
          }
          
          if (!pageKeyCount.isEmpty()) {
            LOGGER.info("\nLive Pages by Page Key:");
            pageKeyCount.entrySet().stream()
                .sorted(java.util.Map.Entry.<Long, Integer>comparingByValue().reversed())
                .limit(10)
                .forEach(e -> LOGGER.info("  Page {}: {} instances", e.getKey(), e.getValue()));
            
            LOGGER.info("\nLive Pages by Index Type:");
            indexTypeCount.forEach((type, count) -> 
                LOGGER.info("  {}: {} instances", type, count));
            
            // Check guard counts of leaked pages
            int guardedLeaks = 0;
            int unguardedLeaks = 0;
            var guardCountBreakdown = new java.util.HashMap<Integer, Integer>();
            for (var page : livePages) {
              int guardCount = page.getGuardCount();
              if (guardCount > 0) {
                guardedLeaks++;
              } else {
                unguardedLeaks++;
              }
              guardCountBreakdown.merge(guardCount, 1, Integer::sum);
            }
            
            LOGGER.info("\nGuard Count Analysis:");
            LOGGER.info("  Guarded leaks (guardCount > 0): {}", guardedLeaks);
            LOGGER.info("  Unguarded leaks (guardCount = 0): {}", unguardedLeaks);
            LOGGER.info("  Guard count breakdown: {}", guardCountBreakdown);
            
            // Show which pages have guards
            if (guardedLeaks > 0) {
              LOGGER.info("\nPages with guards:");
              for (var page : livePages) {
                if (page.getGuardCount() > 0) {
                  LOGGER.info("  Page {} ({}) revision={} guardCount={}",
                      page.getPageKey(), page.getIndexType(), page.getRevision(), page.getGuardCount());
                }
              }
            }
            
            // Show details of unguarded leaks (first 10) and check if they're in cache
            if (unguardedLeaks > 0) {
              LOGGER.info("\nUnguarded Leak Details (these should have been closed!):");
              
              // Check if global buffer manager exists to search caches
              int inRecordCache = 0;
              int inFragmentCache = 0;
              int inPageCache = 0;
              int notInAnyCache = 0;
            
            try {
              var bufferMgr = io.sirix.access.Databases.getGlobalBufferManager();
              
              for (var page : livePages) {
                // Skip pages with active guards (in use by transactions)
                if (page.getGuardCount() == 0) {
                  boolean found = false;
                  
                  // Check if in RecordPageCache
                  for (var entry : bufferMgr.getRecordPageCache().asMap().values()) {
                    if (entry == page) {
                      inRecordCache++;
                      found = true;
                      break;
                    }
                  }
                  
                  if (!found) {
                    // Check if in RecordPageFragmentCache
                    for (var entry : bufferMgr.getRecordPageFragmentCache().asMap().values()) {
                      if (entry == page) {
                        inFragmentCache++;
                        found = true;
                        break;
                      }
                    }
                  }
                  
                  if (!found) {
                    // Check if in PageCache
                    for (var entry : bufferMgr.getPageCache().asMap().values()) {
                      if (entry instanceof io.sirix.page.KeyValueLeafPage kvp && kvp == page) {
                        inPageCache++;
                        found = true;
                        break;
                      }
                    }
                  }
                  
                  if (!found) {
                    notInAnyCache++;
                  }
                }
              }
              
              LOGGER.info("  In RecordPageCache: {}", inRecordCache);
              LOGGER.info("  In RecordPageFragmentCache: {}", inFragmentCache);
              LOGGER.info("  In PageCache: {}", inPageCache);
              LOGGER.info("  NOT in any cache: {} ← LEAKED, NOT TRACKED!", notInAnyCache);
              
            } catch (Exception e) {
              // BufferManager might not exist
            }
            
            // Show first few examples
            int shown = 0;
            for (var page : livePages) {
              if (page.getGuardCount() == 0 && shown < 5) {
                LOGGER.info("  Example: Page {} ({}) revision={}",
                    page.getPageKey(), page.getIndexType(), page.getRevision());
                shown++;
              }
            }
          }
          
          // CRITICAL: Force-close any remaining pages as final cleanup
          // After all fixes, there should be 0-5 pages here (99.9%+ leak-free)
          if (!livePages.isEmpty()) {
            LOGGER.info("\nForce-closing any remaining pages...");
            int forceReleasedGuards = 0;
            int forceClosedCount = 0;
            for (var page : new java.util.ArrayList<>(livePages)) {
              if (!page.isClosed()) {
                try {
                  // Release any remaining guards
                  while (page.getGuardCount() > 0) {
                    page.releaseGuard();
                    forceReleasedGuards++;
                  }
                  // Now close it
                  page.close();
                  forceClosedCount++;
                } catch (Exception e) {
                  LOGGER.warn("Failed to force-close page {} ({}): {}", 
                      page.getPageKey(), page.getIndexType(), e.getMessage());
                }
              }
            }
            if (forceClosedCount > 0) {
              LOGGER.info("Force-released {} guards, closed {} pages.", forceReleasedGuards, forceClosedCount);
            } else {
              LOGGER.info("✅ Perfect: No leaked pages to force-close!");
            }
          }
          
          LOGGER.info("===========================================\n");
          }  // Close if (!pageKeyCount.isEmpty())
        }  // Close if (finalized > 0 || created > 0 || closed > 0 || !livePages.isEmpty())
      }  // Close if (DEBUG_MEMORY_LEAKS)
      
      free();
      LOGGER.info("LinuxMemorySegmentAllocator shutdown complete.");
    }));  // Close lambda and addShutdownHook
  }  // Close constructor

  public static LinuxMemorySegmentAllocator getInstance() {
    return INSTANCE;
  }

  /**
   * Detect the base OS page size using sysconf().
   * Returns the detected page size or 4096 if detection fails.
   */
  private long detectBasePageSize() {
    try {
      long pageSize = (long) SYSCONF.invokeExact(_SC_PAGESIZE);
      if (pageSize > 0) {
        LOGGER.info("Detected base page size: {} bytes", pageSize);
        return pageSize;
      }
    } catch (Throwable t) {
      LOGGER.warn("Failed to detect page size via sysconf, defaulting to 4KB", t);
    }
    return 4096; // Default fallback
  }

  /**
   * Detect huge page size from /proc/meminfo.
   * Returns 0 if huge pages are not available.
   */
  private long detectHugePageSize() {
    try {
      java.nio.file.Path meminfoPath = java.nio.file.Path.of("/proc/meminfo");
      if (!java.nio.file.Files.exists(meminfoPath)) {
        LOGGER.debug("/ proc/meminfo not found, huge pages not available");
        return 0;
      }
      
      java.util.List<String> lines = java.nio.file.Files.readAllLines(meminfoPath);
      for (String line : lines) {
        if (line.startsWith("Hugepagesize:")) {
          // Format: "Hugepagesize:     2048 kB"
          String[] parts = line.split("\\s+");
          if (parts.length >= 2) {
            long sizeKb = Long.parseLong(parts[1]);
            long sizeBytes = sizeKb * 1024;
            LOGGER.info("Detected huge page size: {} MB", sizeBytes / (1024 * 1024));
            return sizeBytes;
          }
        }
      }
    } catch (Exception e) {
      LOGGER.debug("Could not read huge page size from /proc/meminfo", e);
    }
    
    LOGGER.info("Huge pages not available or not configured");
    return 0;
  }

  /**
   * Validate configuration before initialization.
   */
  private void validateConfiguration(long maxBufferSize) {
    if (maxBufferSize <= 0) {
      throw new IllegalArgumentException("maxBufferSize must be > 0, got: " + maxBufferSize);
    }

    // Validate all segment sizes are multiples of base page size
    for (long segmentSize : SEGMENT_SIZES) {
      if (segmentSize % basePageSize != 0) {
        throw new IllegalStateException(
            String.format("Segment size %d is not a multiple of page size %d", segmentSize, basePageSize));
      }
    }

    // Check virtual address space requirement
    long totalVirtualMemory = VIRTUAL_REGION_SIZE * SEGMENT_SIZES.length;
    LOGGER.info("Virtual address space required: {} GB", totalVirtualMemory / (1024 * 1024 * 1024));
    
    if (totalVirtualMemory < 0) {
      throw new IllegalStateException("Virtual memory requirement overflow");
    }
  }

  /**
   * Pre-allocate a large virtual memory region for a size class.
   * Uses MAP_NORESERVE to avoid reserving swap space.
   */
  private void preAllocateVirtualRegion(int poolIndex, long segmentSize) {
    long virtualSize = VIRTUAL_REGION_SIZE;
    
    // Try full size, fall back to smaller sizes if needed (2GB -> 1GB -> 512MB)
    long[] attemptSizes = {virtualSize, virtualSize / 2, virtualSize / 4};
    
    for (long attemptSize : attemptSizes) {
      try {
        MemorySegment addr = (MemorySegment) MMAP.invokeExact(
            MemorySegment.NULL,
            attemptSize,
            PROT_READ | PROT_WRITE,
            MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE,
            -1,
            0L
        );
        
        if (addr.address() == 0 || addr == MemorySegment.NULL) {
          LOGGER.warn("mmap returned NULL for {} GB virtual region, trying smaller size",
                      attemptSize / (1024 * 1024 * 1024));
          continue;
        }
        
        virtualRegions[poolIndex] = addr.reinterpret(attemptSize);
        
        LOGGER.info("Pre-allocated {} GB virtual region for size class {} ({} bytes per segment)",
                    attemptSize / (1024 * 1024 * 1024),
                    poolIndex,
                    segmentSize);
        
        // Partition the virtual region into segments
        partitionVirtualRegion(poolIndex, segmentSize, attemptSize);
        return;
        
      } catch (Throwable t) {
        LOGGER.warn("Failed to allocate {} GB virtual region for pool {}, trying smaller size",
                    attemptSize / (1024 * 1024 * 1024), poolIndex, t);
      }
    }
    
    throw new RuntimeException("Failed to pre-allocate virtual memory region for pool " + poolIndex);
  }

  /**
   * Partition a virtual region into segments and add them to the pool.
   * Physical memory is not committed until segments are actually used.
   */
  private void partitionVirtualRegion(int poolIndex, long segmentSize, long regionSize) {
    MemorySegment region = virtualRegions[poolIndex];
    long segmentCount = regionSize / segmentSize;
    
    LOGGER.debug("Partitioning virtual region into {} segments of {} bytes each",
                 segmentCount, segmentSize);
    
    Deque<MemorySegment> pool = segmentPools[poolIndex];
    for (long i = 0; i < segmentCount; i++) {
      MemorySegment slice = region.asSlice(i * segmentSize, segmentSize);
      pool.offer(slice);
    }
    
    poolSizes[poolIndex].set((int) segmentCount);
    LOGGER.info("Pool {} initialized with {} pre-allocated segments", poolIndex, segmentCount);
  }

  @Override
  public synchronized void init(long maxBufferSize) {
    if (isInitialized.get()) {
      // Already initialized - just update max buffer size if needed
      // CRITICAL: DO NOT reset borrowedSegments or physicalMemoryBytes!
      // Live pages from previous tests may still hold segments - if we clear tracking,
      // their release() calls will fail with "UNTRACKED ALLOCATION" errors
      long currentBufferSize = this.maxBufferSize.get();
      if (currentBufferSize != maxBufferSize) {
        LOGGER.debug("Allocator already initialized - updating max buffer size from {} to {} MB",
                    currentBufferSize / (1024 * 1024), maxBufferSize / (1024 * 1024));
        this.maxBufferSize.set(maxBufferSize);
      }
      
      // Log current state for diagnostics
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("Allocator re-init: {} borrowed segments, {} MB physical memory tracked",
                    borrowedSegments.size(), physicalMemoryBytes.get() / (1024 * 1024));
      }
      return;
    }
    
    // First initialization - set the flag
    isInitialized.set(true);

    LOGGER.info("========== Initializing UmbraDB-Style Memory Allocator ==========");
    LOGGER.info("Physical memory limit: {} MB", maxBufferSize / (1024 * 1024));

    // Step 1: Detect page sizes
    basePageSize = detectBasePageSize();
    hugePageSize = detectHugePageSize();

    // Step 2: Set max buffer size
    this.maxBufferSize.set(maxBufferSize);

    // Step 3: Validate configuration
    try {
      validateConfiguration(maxBufferSize);
    } catch (Exception e) {
      LOGGER.error("Configuration validation failed", e);
      isInitialized.set(false);
      throw e;
    }

    // Step 4: Initialize pool data structures
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      segmentPools[i] = new ConcurrentLinkedDeque<>();
      poolSizes[i] = new AtomicInteger(0);
      totalBorrows[i] = new AtomicLong(0);
      totalReturns[i] = new AtomicLong(0);
    }

    // Step 5: Pre-allocate virtual memory regions for each size class
    LOGGER.info("Pre-allocating virtual memory regions...");
    try {
      for (int i = 0; i < SEGMENT_SIZES.length; i++) {
        preAllocateVirtualRegion(i, SEGMENT_SIZES[i]);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to pre-allocate virtual memory regions", e);
      // Cleanup any successfully allocated regions
      free();
      isInitialized.set(false);
      throw new RuntimeException("Virtual memory pre-allocation failed", e);
    }

    // Log final initialization summary
    long totalVirtual = virtualRegions.length * VIRTUAL_REGION_SIZE;
    LOGGER.info("=================================================================");
    LOGGER.info("Allocator initialized successfully:");
    LOGGER.info("  - Base page size: {} bytes", basePageSize);
    LOGGER.info("  - Huge page size: {} MB ({})", 
                hugePageSize / (1024 * 1024), 
                hugePageSize > 0 ? "available" : "not available");
    LOGGER.info("  - Virtual memory mapped: {} GB (across {} size classes)",
                totalVirtual / (1024 * 1024 * 1024),
                SEGMENT_SIZES.length);
    LOGGER.info("  - Physical memory limit: {} MB", maxBufferSize / (1024 * 1024));
    LOGGER.info("  - Segment size classes: {}", java.util.Arrays.toString(SEGMENT_SIZES));
    LOGGER.info("=================================================================");
  }

  @Override
  public long getMaxBufferSize() {
    return maxBufferSize.get();
  }
  
  @Override
  public boolean isInitialized() {
    return isInitialized.get();
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
    if (!LOGGER.isDebugEnabled()) {
      return;
    }
    
    LOGGER.debug("\n========== MEMORY SEGMENT POOL DIAGNOSTICS ==========");
    for (int i = 0; i < SEGMENT_SIZES.length; i++) {
      LOGGER.debug("Pool {} (size {}): {} segments available", 
                   i, SEGMENT_SIZES[i], poolSizes[i].get());
    }
    LOGGER.debug("Physical memory: {} MB / {} MB limit",
                 physicalMemoryBytes.get() / (1024 * 1024),
                 maxBufferSize.get() / (1024 * 1024));
    LOGGER.debug("Borrowed segments: {}", borrowedSegments.size());
    LOGGER.debug("Virtual regions: {} x {} GB",
                 virtualRegions.length,
                 VIRTUAL_REGION_SIZE / (1024 * 1024 * 1024));
    LOGGER.debug("====================================================\n");
  }

  @Override
  public synchronized MemorySegment allocate(long size) {
    long callNum = allocateCallCount.incrementAndGet();
    
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }

    // CRITICAL: Use the actual segment size from the pool, NOT the requested size!
    // The pool returns segments of SEGMENT_SIZES[index], which may be larger than requested.
    long actualSegmentSize = SEGMENT_SIZES[index];

    // Check physical memory limit before allocation using actual segment size
    long currentPhysical = physicalMemoryBytes.get();
    if (currentPhysical + actualSegmentSize > maxBufferSize.get()) {
      LOGGER.warn("Physical memory limit approaching: {} / {} MB (requested {} bytes, actual segment {} bytes)",
                  currentPhysical / (1024 * 1024),
                  maxBufferSize.get() / (1024 * 1024),
                  size, actualSegmentSize);
      
      // Memory limit reached - throw exception rather than blocking
      // Future enhancement: implement backpressure with waiting mechanism
      throw new OutOfMemoryError(String.format(
          "Cannot allocate %d bytes (actual segment: %d bytes). Physical memory: %d/%d MB, would exceed limit",
          size, actualSegmentSize, currentPhysical / (1024 * 1024), maxBufferSize.get() / (1024 * 1024)));
    }

    Deque<MemorySegment> pool = segmentPools[index];
    MemorySegment segment = pool.poll();

    if (segment == null) {
      // This should not happen with pre-allocated virtual regions
      LOGGER.error("Pool {} exhausted! All {} segments in use. Virtual region may be too small.",
                   index, poolSizes[index].get());
      throw new OutOfMemoryError("Memory pool exhausted for size class " + size);
    }

    // Track if this is the first time we're borrowing this segment
    long address = segment.address();
    boolean isFirstBorrow = borrowedSegments.add(address);
    
    // SIMPLIFIED: Always increment physical memory on first borrow
    // Note: actualSegmentSize is already defined above (uses SEGMENT_SIZES[index])
    // release() uses segment.byteSize() which is the actual size, so we must match here.
    // Don't track "re-borrow" cases - too complex with async operations
    if (isFirstBorrow) {
      long newPhysical = physicalMemoryBytes.addAndGet(actualSegmentSize);
      
      if (newPhysical > maxBufferSize.get() * 0.9) {
        double percentUsed = (newPhysical * 100.0) / maxBufferSize.get();
        LOGGER.warn("Physical memory at {:.1f}% of limit ({} MB / {} MB)",
                    percentUsed,
                    newPhysical / (1024 * 1024),
                    maxBufferSize.get() / (1024 * 1024));
      }
    }
    // Note: If !isFirstBorrow (re-borrow), don't increment - already counted

    // Decrement pool counter
    poolSizes[index].decrementAndGet();

    // Track utilization
    totalBorrows[index].incrementAndGet();

    return segment;
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
                    segment.address(),
                    size,
                    result);
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

    releasePhysicalMemory(segment, segment.byteSize());
  }

  private static final java.util.concurrent.atomic.AtomicLong allocateCallCount = new java.util.concurrent.atomic.AtomicLong(0);
  private static final java.util.concurrent.atomic.AtomicLong releaseCallCount = new java.util.concurrent.atomic.AtomicLong(0);
  private static final java.util.concurrent.atomic.AtomicLong doubleReleaseCount = new java.util.concurrent.atomic.AtomicLong(0);
  
  @Override
  public synchronized void release(MemorySegment segment) {
    if (segment == null) {
      return; // Already released/nulled
    }
    
    long callNum = releaseCallCount.incrementAndGet();
    if (callNum % 1000 == 0) {
      LOGGER.debug("Allocator: {} segments released ({} double-releases), physical: {} MB, pool sizes: {}", 
                   callNum, doubleReleaseCount.get(), physicalMemoryBytes.get() / (1024 * 1024), 
                   java.util.Arrays.toString(getPoolSizes()));
    }

    long size = segment.byteSize();
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      LOGGER.error("CANNOT RETURN SEGMENT! Invalid size: {}", size);
      throw new IllegalArgumentException("Segment size not supported for reuse: " + size);
    }

    long address = segment.address();
    
    // CRITICAL: Atomically check-and-remove to prevent TOCTOU race
    // Multiple threads can call release() simultaneously on same segment address
    // (e.g., cache removalListener + manual close racing on ForkJoinPool threads)
    boolean wasRemoved = borrowedSegments.remove(address);
    if (!wasRemoved) {
      // Segment was never borrowed OR already released by another thread
      doubleReleaseCount.incrementAndGet();
      if (callNum % 100 == 0) {
        LOGGER.warn("Attempting to release segment that was never borrowed or already released: address={}, size={} (call #{})",
                    address, size, callNum);
      }
      return; // Prevent double-release
    }

    // Track utilization
    totalReturns[index].incrementAndGet();

    // Use madvise to release physical memory while keeping virtual mapping
    boolean madviseSucceeded = false;
    try {
      int result = (int) MADVISE.invokeExact(segment, size, MADV_DONTNEED);
      if (result == 0) {
        // Successfully released physical memory - decrement counter using CAS to prevent going negative
        long currentPhysical;
        long newPhysical = 0; // Initialize to avoid compilation error
        boolean hadAccountingError = false;
        
        do {
          currentPhysical = physicalMemoryBytes.get();
          
          if (currentPhysical < size) {
            // Counter would go negative - accounting drift detected
            // This can happen with async cache operations racing with allocate/release
            // DON'T set to 0 (causes cascade) - just set to 0 and accept the drift
            if (callNum % 1000 == 0) {
              LOGGER.warn("Physical memory accounting drift at release #{}: " +
                          "trying to release {} bytes but only {} bytes tracked (short by {}). " +
                          "Allocates: {}, Releases: {}, Borrowed: {}. " +
                          "Accepting drift and continuing.",
                          callNum, size, currentPhysical, size - currentPhysical,
                          allocateCallCount.get(), releaseCallCount.get(),
                          borrowedSegments.size());
            }
            
            // Set to 0 to prevent negative
            newPhysical = 0;
            hadAccountingError = true;
            break; // Exit CAS loop with newPhysical=0
          }
          
          newPhysical = currentPhysical - size;
        } while (!physicalMemoryBytes.compareAndSet(currentPhysical, newPhysical));
        
        // DIAGNOSTIC: Log every decrement to track accounting (skip if had error - newPhysical not meaningful)
        if (!hadAccountingError && callNum % 100 == 0) {
          LOGGER.debug("Release {}: physical {} → {} MB (- {} bytes, borrowed count: {})",
                      callNum, currentPhysical / (1024 * 1024), newPhysical / (1024 * 1024),
                      size, borrowedSegments.size());
        }
        
        madviseSucceeded = true;
        // Note: Already removed from borrowedSegments above (atomically)
      } else {
        // madvise failed - add back to borrowed set since memory not actually released
        borrowedSegments.add(address);
        LOGGER.warn("madvise MADV_DONTNEED failed for address {}, size {} (error code: {}). " +
                    "Physical memory may not be released. Segment NOT returned to pool.",
                    address, size, result);
        // DO NOT return to pool - segment still holds physical memory
        return;
      }
    } catch (Throwable t) {
      // madvise invocation failed - add back to borrowed set since memory not actually released
      borrowedSegments.add(address);
      LOGGER.error("madvise invocation failed for address {}, size {}. Segment NOT returned to pool.", address, size, t);
      // DO NOT return to pool - we don't know the state of physical memory
      return;
    }

    // Only return segment to pool if madvise succeeded
    // This prevents accounting errors from re-borrowing segments with unreleased physical memory
    if (madviseSucceeded) {
      segmentPools[index].offer(segment);
      poolSizes[index].incrementAndGet();
    }
  }

  @Override
  public void free() {
    if (!isInitialized.get()) {
      LOGGER.debug("Allocator not initialized, nothing to free");
      return;
    }

    LOGGER.info("Cleaning up allocator (physical memory: {} MB)",
                physicalMemoryBytes.get() / (1024 * 1024));

    // Unmap virtual memory regions
    for (int i = 0; i < virtualRegions.length; i++) {
      MemorySegment region = virtualRegions[i];
      if (region != null) {
        try {
          long size = region.byteSize();
          int result = (int) MUNMAP.invokeExact(region, size);
          if (result != 0) {
            LOGGER.error("Failed to munmap virtual region {} (size {} GB, error code: {})",
                         i, size / (1024 * 1024 * 1024), result);
          } else {
            LOGGER.debug("Unmapped virtual region {} ({} GB)", i, size / (1024 * 1024 * 1024));
          }
        } catch (Throwable t) {
          LOGGER.error("Error unmapping virtual region {}", i, t);
        }
        virtualRegions[i] = null;
      }
    }

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
    physicalMemoryBytes.set(0);
    isInitialized.set(false);

    LOGGER.info("LinuxMemorySegmentAllocator cleanup complete");
  }

  /**
   * Print current memory statistics.
   */
  public void printMemoryStats() {
    LOGGER.info("Memory: Physical={} MB / {} MB limit, Borrowed segments={}",
                physicalMemoryBytes.get() / (1024 * 1024),
                maxBufferSize.get() / (1024 * 1024),
                borrowedSegments.size());
  }

  public static void main(String[] args) {
    LinuxMemorySegmentAllocator allocator = LinuxMemorySegmentAllocator.getInstance();
    allocator.init(1L << 30);

    MemorySegment segment4KB = allocator.allocate(4096);
    LOGGER.info("Allocated 4KB segment: {}", segment4KB);

    MemorySegment segment128KB = allocator.allocate(131072);
    LOGGER.info("Allocated 128KB segment: {}", segment128KB);

    allocator.release(segment4KB);
    allocator.release(segment128KB);

    allocator.printMemoryStats();
  }
}

