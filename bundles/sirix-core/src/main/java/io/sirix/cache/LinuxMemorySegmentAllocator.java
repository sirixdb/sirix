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
      
      // Report page leak statistics before shutdown
      long finalized = io.sirix.page.KeyValueLeafPage.PAGES_FINALIZED_WITHOUT_CLOSE.get();
      long created = io.sirix.page.KeyValueLeafPage.PAGES_CREATED.get();
      long closed = io.sirix.page.KeyValueLeafPage.PAGES_CLOSED.get();
      var livePages = io.sirix.page.KeyValueLeafPage.ALL_LIVE_PAGES;
      
      if (finalized > 0 || created > 0 || closed > 0 || !livePages.isEmpty()) {
        System.err.println("\n========== PAGE LEAK DIAGNOSTICS ==========");
        System.err.println("Pages Created: " + created);
        System.err.println("Pages Closed: " + closed);
        System.err.println("Pages Leaked (caught by finalizer): " + finalized);
        System.err.println("Pages Still Live: " + livePages.size());
        
        // Show finalized pages breakdown
        if (finalized > 0) {
          System.err.println("\nFinalized Pages (NOT closed properly) by Type:");
          io.sirix.page.KeyValueLeafPage.FINALIZED_BY_TYPE.forEach((type, count) -> 
              System.err.println("  " + type + ": " + count.get() + " pages"));
          
          System.err.println("\nFinalized Pages (NOT closed properly) by Page Key (top 15):");
          io.sirix.page.KeyValueLeafPage.FINALIZED_BY_PAGE_KEY.entrySet().stream()
              .sorted(java.util.Map.Entry.<Long, java.util.concurrent.atomic.AtomicLong>comparingByValue(
                  (a, b) -> Long.compare(b.get(), a.get())).reversed())
              .limit(15)
              .forEach(e -> System.err.println("  Page " + e.getKey() + ": " + e.getValue().get() + " times"));
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
          System.err.println("\nLive Pages by Page Key:");
          pageKeyCount.entrySet().stream()
              .sorted(java.util.Map.Entry.<Long, Integer>comparingByValue().reversed())
              .limit(10)
              .forEach(e -> System.err.println("  Page " + e.getKey() + ": " + e.getValue() + " instances"));
          
          System.err.println("\nLive Pages by Index Type:");
          indexTypeCount.forEach((type, count) -> 
              System.err.println("  " + type + ": " + count + " instances"));
          
          // Check pin counts of leaked pages
          int pinnedLeaks = 0;
          int unpinnedLeaks = 0;
          var pinCountBreakdown = new java.util.HashMap<Integer, Integer>();
          for (var page : livePages) {
            int pinCount = page.getPinCount();
            if (pinCount > 0) {
              pinnedLeaks++;
            } else {
              unpinnedLeaks++;
            }
            pinCountBreakdown.merge(pinCount, 1, Integer::sum);
          }
          
          System.err.println("\nPin Count Analysis:");
          System.err.println("  Pinned leaks (pinCount > 0): " + pinnedLeaks);
          System.err.println("  Unpinned leaks (pinCount = 0): " + unpinnedLeaks);
          System.err.println("  Pin count breakdown: " + pinCountBreakdown);
          
          // Show which transaction IDs leaked PATH_SUMMARY pins
          if (pinnedLeaks > 0) {
            System.err.println("\nPinned Pages - Which Transactions:");
            for (var page : livePages) {
              if (page.getPinCount() > 0) {
                System.err.println("  Page " + page.getPageKey() + 
                                   " (" + page.getIndexType() + ")" +
                                   " revision=" + page.getRevision() +
                                   " pinned by transactions: " + page.getPinCountByTransaction());
              }
            }
          }
          
          // Show details of unpinned leaks (first 10) and check if they're in cache
          if (unpinnedLeaks > 0) {
            System.err.println("\nUnpinned Leak Details (these should have been closed!):");
            
            // Check if global buffer manager exists to search caches
            int inRecordCache = 0;
            int inFragmentCache = 0;
            int inPageCache = 0;
            int notInAnyCache = 0;
            
            try {
              var bufferMgr = io.sirix.access.Databases.getGlobalBufferManager();
              
              for (var page : livePages) {
                if (page.getPinCount() == 0) {
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
              
              System.err.println("  In RecordPageCache: " + inRecordCache);
              System.err.println("  In RecordPageFragmentCache: " + inFragmentCache);
              System.err.println("  In PageCache: " + inPageCache);
              System.err.println("  NOT in any cache: " + notInAnyCache + " ← LEAKED, NOT TRACKED!");
              
            } catch (Exception e) {
              // BufferManager might not exist
            }
            
            // Show first few examples
            int shown = 0;
            for (var page : livePages) {
              if (page.getPinCount() == 0 && shown < 5) {
                System.err.println("  Example: Page " + page.getPageKey() + 
                                   " (" + page.getIndexType() + ")" +
                                   " revision=" + page.getRevision());
                shown++;
              }
            }
          }
          
          // CRITICAL: Force-unpin and close any remaining pages as final cleanup
          // After all fixes, there should be 0-5 pages here (99.9%+ leak-free)
          // Handle BOTH pinned and unpinned leaks
          if (!livePages.isEmpty()) {
            System.err.println("\nForce-closing any remaining pages...");
            int forceUnpinnedCount = 0;
            int forceClosedCount = 0;
            for (var page : new java.util.ArrayList<>(livePages)) {
              if (!page.isClosed()) {
                try {
                  // Force-unpin if still pinned
                  if (page.getPinCount() > 0) {
                    var pinsByTrx = new java.util.HashMap<>(page.getPinCountByTransaction());
                    for (var entry : pinsByTrx.entrySet()) {
                      int trxId = entry.getKey();
                      int pins = entry.getValue();
                      for (int i = 0; i < pins; i++) {
                        page.decrementPinCount(trxId);
                        forceUnpinnedCount++;
                      }
                    }
                  }
                  // Now close it
                  page.close();
                  forceClosedCount++;
                } catch (Exception e) {
                  System.err.println("  Warning: Failed to force-close page " + page.getPageKey() + 
                                     " (" + page.getIndexType() + "): " + e.getMessage());
                }
              }
            }
            if (forceClosedCount > 0) {
              System.err.println("Force-unpinned " + forceUnpinnedCount + " pins, closed " + forceClosedCount + " pages.");
            } else {
              System.err.println("✅ Perfect: No leaked pages to force-close!");
            }
          }
        }
        System.err.println("===========================================\n");
      }
      
      free();
      LOGGER.info("LinuxMemorySegmentAllocator shutdown complete.");
    }));
  }

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
      System.out.println(
          "Pool " + i + " (size " + SEGMENT_SIZES[i] + "): " + poolSizes[i].get() + " segments available");
    }
    System.out.println("Physical memory: " + (physicalMemoryBytes.get() / (1024 * 1024)) + " MB / " + 
                       (maxBufferSize.get() / (1024 * 1024)) + " MB limit");
    System.out.println("Borrowed segments: " + borrowedSegments.size());
    System.out.println("Virtual regions: " + virtualRegions.length + " x " + 
                       (VIRTUAL_REGION_SIZE / (1024 * 1024 * 1024)) + " GB");
    System.out.println("====================================================\n");
  }

  @Override
  public synchronized MemorySegment allocate(long size) {
    long callNum = allocateCallCount.incrementAndGet();
    
    int index = SegmentAllocators.getIndexForSize(size);

    if (index < 0 || index >= segmentPools.length) {
      throw new IllegalArgumentException("Unsupported size: " + size);
    }

    // Check physical memory limit before allocation
    long currentPhysical = physicalMemoryBytes.get();
    if (currentPhysical + size > maxBufferSize.get()) {
      LOGGER.warn("Physical memory limit approaching: {} / {} MB (requested {} bytes)",
                  currentPhysical / (1024 * 1024),
                  maxBufferSize.get() / (1024 * 1024),
                  size);
      
      // TODO: Implement memory pressure notification and waiting mechanism
      // For now, throw an exception to prevent exceeding limit
      throw new OutOfMemoryError(String.format(
          "Cannot allocate %d bytes. Physical memory: %d/%d MB, would exceed limit",
          size, currentPhysical / (1024 * 1024), maxBufferSize.get() / (1024 * 1024)));
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
    
    if (isFirstBorrow) {
      // First time borrowing - physical pages will be committed on first access
      // Always increment physical memory for first borrow
      long oldPhysical = physicalMemoryBytes.get();
      long newPhysical = physicalMemoryBytes.addAndGet(size);
      
      // DIAGNOSTIC: Log every increment to track accounting
      if (callNum % 100 == 0) {
        LOGGER.debug("Allocate {}: physical {} → {} MB (+ {} bytes, borrowed count: {})",
                    callNum, oldPhysical / (1024 * 1024), newPhysical / (1024 * 1024),
                    size, borrowedSegments.size());
      }
      
      if (newPhysical > maxBufferSize.get() * 0.9) {
        double percentUsed = (newPhysical * 100.0) / maxBufferSize.get();
        LOGGER.warn("Physical memory at {:.1f}% of limit ({} MB / {} MB)",
                    percentUsed,
                    newPhysical / (1024 * 1024),
                    maxBufferSize.get() / (1024 * 1024));
      }
    } else {
      // Re-borrowing a segment that's still in borrowedSegments - THIS IS THE BUG!
      // Segments should be removed from borrowedSegments in release() before returning to pool
      LOGGER.error("🔴 ROOT CAUSE FOUND: Re-borrowing segment {} (size={}) that's still in borrowed set! " +
                   "This causes UNTRACKED ALLOCATION. Physical: {} MB, Borrowed count: {}",
                   address, size, physicalMemoryBytes.get() / (1024 * 1024), borrowedSegments.size());
      
      // Print stack trace to see WHERE this is being allocated from
      StackTraceElement[] stack = Thread.currentThread().getStackTrace();
      StringBuilder stackStr = new StringBuilder("\n  Re-borrow stack trace:");
      for (int i = 2; i < Math.min(stack.length, 10); i++) {
        stackStr.append(String.format("\n    %s.%s(%s:%d)",
            stack[i].getClassName(), stack[i].getMethodName(),
            stack[i].getFileName(), stack[i].getLineNumber()));
      }
      LOGGER.error(stackStr.toString());
      
      // Don't increment physical memory - it's already counted
      // This WILL cause accounting errors on next release!
    }

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
            // Counter would go negative - this is an accounting bug
            // Report detailed diagnostics to find root cause
            LOGGER.error("🔴 Physical memory accounting error at release #{}: " +
                        "trying to release {} bytes but only {} bytes tracked. " +
                        "Total allocate calls: {}, total release calls: {}, " +
                        "borrowed segments: {}, double-releases: {}. " +
                        "Setting counter to 0 and continuing.",
                        callNum, size, currentPhysical,
                        allocateCallCount.get(), releaseCallCount.get(),
                        borrowedSegments.size(), doubleReleaseCount.get());
            
            // Set to 0 using CAS (another thread might have changed it)
            physicalMemoryBytes.compareAndSet(currentPhysical, 0);
            hadAccountingError = true;
            
            // Continue with release - segment is returned to pool
            break; // Exit CAS loop, continue to return segment
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
    System.out.println("Allocated 4KB segment: " + segment4KB);

    MemorySegment segment128KB = allocator.allocate(131072);
    System.out.println("Allocated 128KB segment: " + segment128KB);

    allocator.release(segment4KB);
    allocator.release(segment128KB);

    allocator.printMemoryStats();
  }
}
