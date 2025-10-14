package io.sirix.cache;

import io.sirix.access.ResourceConfiguration;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.RecordSerializer;
import io.sirix.page.KeyValueLeafPage;
import io.sirix.page.PageReference;
import io.sirix.utils.OS;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.*;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * A thread-safe pool for KeyValueLeafPage instances that manages memory segments efficiently.
 * This pool eliminates the unnecessary distinction between pages with and without DeweyIDs,
 * as the KeyValueLeafPage class already handles both cases internally.
 */
public final class KeyValueLeafPagePool {

  private static final Logger LOGGER = LoggerFactory.getLogger(KeyValueLeafPagePool.class);
  private static final int[] PAGE_SIZES = MemorySegmentAllocator.SEGMENT_SIZES;
  private static final int BASE_SEGMENT_COUNT = 100;

  /**
   * Pool state enumeration.
   */
  private enum PoolState {
    UNINITIALIZED, INITIALIZED, SHUTDOWN
  }

  // Single pool array indexed by size - one pool per page size
  private final Deque<KeyValueLeafPage>[] pools;

  private final AtomicReference<PoolState> state = new AtomicReference<>(PoolState.UNINITIALIZED);

  // Thread-safe metrics
  private final AtomicLong totalBorrowedPages = new AtomicLong(0);
  private final AtomicLong totalReturnedPages = new AtomicLong(0);
  private final AtomicLong poolMisses = new AtomicLong(0);
  private final AtomicLong poolHits = new AtomicLong(0);

  // Lock for initialization to prevent race conditions
  private final Semaphore initializationLock = new Semaphore(1);

  // Configurable allocator - volatile for safe publication
  private volatile MemorySegmentAllocator segmentAllocator =
      OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() : LinuxMemorySegmentAllocator.getInstance();

  private static final KeyValueLeafPagePool INSTANCE = new KeyValueLeafPagePool();

  @SuppressWarnings("unchecked")
  private KeyValueLeafPagePool() {
    this.pools = new Deque[PAGE_SIZES.length];
    initializePoolCollections();
  }

  /**
   * Initialize pool collections with thread-safe deques.
   */
  private void initializePoolCollections() {
    for (int i = 0; i < pools.length; i++) {
      pools[i] = new ConcurrentLinkedDeque<>();
    }
  }

  public static KeyValueLeafPagePool getInstance() {
    return INSTANCE;
  }

  // Thread-safe setter with validation
  void setSegmentAllocator(@NonNull MemorySegmentAllocator segmentAllocator) {
    requireNonNull(segmentAllocator, "MemorySegmentAllocator cannot be null");
    this.segmentAllocator = segmentAllocator;
  }

  public MemorySegmentAllocator getSegmentAllocator() {
    return segmentAllocator;
  }

  public void init(long maxBufferSize) {
    try {
      initializationLock.acquire();
      try {
        PoolState currentState = state.get();

        // Allow reinitialization after shutdown
        if (currentState == PoolState.SHUTDOWN) {
          LOGGER.info("Reinitializing KeyValueLeafPagePool after shutdown");
          // Reset metrics
          totalBorrowedPages.set(0);
          totalReturnedPages.set(0);
          poolMisses.set(0);
          poolHits.set(0);
          // Clear and reinitialize pools
          clearAllPagePools();
          initializePoolCollections();
          // Reset state to UNINITIALIZED so we can proceed with initialization
          state.set(PoolState.UNINITIALIZED);
          currentState = PoolState.UNINITIALIZED;
        }

        if (currentState == PoolState.UNINITIALIZED) {
          LOGGER.info("Initializing KeyValueLeafPagePool with max buffer size: {} bytes", maxBufferSize);
          initializeSegmentAllocator(maxBufferSize);
          // NOTE: We no longer pre-allocate pages since we create fresh pages on demand
          // The MemorySegmentAllocator handles segment pooling internally
          state.set(PoolState.INITIALIZED);
          LOGGER.info("KeyValueLeafPagePool initialization completed successfully");
        }
      } finally {
        initializationLock.release();
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException("Interrupted during pool initialization", e);
    }
  }

  private void initializeSegmentAllocator(long maxBufferSize) {
    try {
      segmentAllocator.init(maxBufferSize);
    } catch (Exception e) {
      LOGGER.error("Failed to initialize segment allocator", e);
      throw new RuntimeException("Pool initialization failed", e);
    }
  }

  /**
   * Cleanup all resources managed by the pool.
   */
  public void free() {
    if (state.compareAndSet(PoolState.INITIALIZED, PoolState.SHUTDOWN)) {
      try {
        LOGGER.info("Shutting down KeyValueLeafPagePool");
        clearAllPagePools();
        freeSegmentAllocator();
        LOGGER.info("KeyValueLeafPagePool shutdown completed");
      } catch (Exception e) {
        LOGGER.error("Error during pool shutdown", e);
      }
    }
  }

  /**
   * Clear all page pools and properly clean up pages.
   * Return segments from any pre-allocated pages back to the allocator.
   */
  private void clearAllPagePools() {
    for (Deque<KeyValueLeafPage> pool : pools) {
      if (pool != null) {
        // Return segments from all pages before clearing
        while (!pool.isEmpty()) {
          KeyValueLeafPage page = pool.poll();
          if (page != null) {
            page.returnSegmentsToAllocator();
          }
        }
        pool.clear();
      }
    }
  }

  /**
   * Free the underlying memory segment allocator.
   */
  private void freeSegmentAllocator() {
    try {
      if (segmentAllocator != null) {
        segmentAllocator.free();
      }
    } catch (Exception e) {
      LOGGER.warn("Error freeing segment allocator", e);
    }
  }


  private void assertNotShutdown() {
    if (state.get() == PoolState.SHUTDOWN) {
      throw new IllegalStateException("KeyValueLeafPagePool is shutdown");
    }
  }

  private void ensureInitialized() {
    if (state.get() != PoolState.INITIALIZED) {
      throw new IllegalStateException("KeyValueLeafPagePool is not initialized. Call init() first.");
    }
  }

  private int validateAndGetSizeIndex(int size) {
    for (int i = 0; i < PAGE_SIZES.length; i++) {
      if (size <= PAGE_SIZES[i]) {
        return i;
      }
    }
    throw new IllegalArgumentException(
        "Unsupported page size: " + size + ". Maximum supported size: " + PAGE_SIZES[PAGE_SIZES.length - 1]);
  }

  // Public API methods with enhanced validation and error handling
  public KeyValueLeafPage borrowPage(final long recordPageKey, final int revisionNumber, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final boolean areDeweyIDsStored,
      final RecordSerializer recordPersister, final Map<Long, PageReference> references, final int slotMemorySize,
      final int deweyIdMemorySize, final int lastSlotIndex, final int lastDeweyIdIndex) {

    requireNonNull(indexType, "IndexType cannot be null");
    requireNonNull(resourceConfig, "ResourceConfiguration cannot be null");
    requireNonNull(recordPersister, "RecordSerializer cannot be null");
    checkArgument(recordPageKey >= 0, "Record page key must be non-negative");
    checkArgument(revisionNumber >= 0, "Revision number must be non-negative");
    checkArgument(slotMemorySize > 0, "Slot memory size must be positive");

    // Auto-initialize if needed
    if (state.get() == PoolState.UNINITIALIZED) {
      LOGGER.warn("KeyValueLeafPagePool not initialized, auto-initializing with default size");
      init(1L << 30);
    }
    assertNotShutdown();
    ensureInitialized();

    // Validate size is supported
    validateAndGetSizeIndex(slotMemorySize);
    totalBorrowedPages.incrementAndGet();

    // NEW APPROACH: Don't pool page objects, only pool MemorySegments via the allocator
    // Always create fresh page objects to avoid state contamination issues
    // The allocator will handle pooling/reuse of the actual memory segments
    DiagnosticLogger.log("borrowPage: recordPageKey=" + recordPageKey + ", slotSize=" + slotMemorySize + 
                        ", deweyIdSize=" + deweyIdMemorySize + ", indexType=" + indexType);
    MemorySegment slotSegment = segmentAllocator.allocate(slotMemorySize);
    MemorySegment deweyIdSegment = deweyIdMemorySize > 0 ? segmentAllocator.allocate(deweyIdMemorySize) : slotSegment;
    return new KeyValueLeafPage(recordPageKey, revisionNumber, indexType, resourceConfig, areDeweyIDsStored,
                               recordPersister, references != null ? references : new HashMap<>(),
                               slotSegment, deweyIdSegment, lastSlotIndex, lastDeweyIdIndex);
  }

  public KeyValueLeafPage borrowPage(int size, final @NonNegative long recordPageKey, final IndexType indexType,
      final ResourceConfiguration resourceConfig, final int revisionNumber) {

    requireNonNull(indexType, "IndexType cannot be null");
    requireNonNull(resourceConfig, "ResourceConfiguration cannot be null");
    checkArgument(recordPageKey >= 0, "Record page key must be non-negative");
    checkArgument(revisionNumber >= 0, "Revision number must be non-negative");
    checkArgument(size > 0, "Size must be positive");

    // Auto-initialize if needed
    if (state.get() == PoolState.UNINITIALIZED) {
      LOGGER.warn("KeyValueLeafPagePool not initialized, auto-initializing with default size");
      init(1L << 30);
    }
    assertNotShutdown();
    ensureInitialized();

    // Validate size is supported
    validateAndGetSizeIndex(size);
    totalBorrowedPages.incrementAndGet();

    // NEW APPROACH: Don't pool page objects, only pool MemorySegments via the allocator
    // Always create fresh page objects to avoid state contamination issues
    // The allocator will handle pooling/reuse of the actual memory segments
    DiagnosticLogger.log("borrowPage(2): recordPageKey=" + recordPageKey + ", size=" + size + ", indexType=" + indexType);
    MemorySegment slotSegment = segmentAllocator.allocate(size);
    MemorySegment deweyIdSegment = segmentAllocator.allocate(size);
    return new KeyValueLeafPage(recordPageKey, indexType, resourceConfig, revisionNumber,
                               slotSegment, deweyIdSegment);
  }

  /**
   * Return memory segments from a page back to the allocator for reuse.
   * NEW APPROACH: We don't pool page objects anymore, just return the segments.
   * This method is safe to call multiple times on the same page (idempotent).
   */
  public void returnPage(@Nullable KeyValueLeafPage page) {
    if (page == null) {
      DiagnosticLogger.log("KeyValueLeafPagePool.returnPage() called with null page");
      return;
    }

    DiagnosticLogger.log("KeyValueLeafPagePool.returnPage() called for page " + page.getPageKey());

    if (state.get() == PoolState.SHUTDOWN) {
      LOGGER.debug("Pool is shutdown, discarding returned page");
      DiagnosticLogger.log("Pool is shutdown, discarding page " + page.getPageKey());
      return;
    }

    try {
      long count = totalReturnedPages.incrementAndGet();
      DiagnosticLogger.log("Total returned pages now: " + count);

      // Use the page's idempotent segment return method to avoid double-free
      page.returnSegmentsToAllocator();

    } catch (Exception e) {
      LOGGER.error("Error returning memory segments to allocator", e);
      DiagnosticLogger.error("ERROR in returnPage for page " + page.getPageKey() + ": " + e.getMessage(), e);
    }
  }

  /**
   * Thread-safe statistics collection.
   */
  public Map<String, Map<Integer, Integer>> getPoolStatistics() {
    Map<String, Map<Integer, Integer>> stats = new HashMap<>();
    Map<Integer, Integer> poolSizes = new HashMap<>();

    for (int i = 0; i < pools.length; i++) {
      Deque<KeyValueLeafPage> pool = pools[i];
      if (pool != null) {
        poolSizes.put(PAGE_SIZES[i], pool.size());
      }
    }

    stats.put("poolSizes", poolSizes);
    return stats;
  }

  /**
   * Comprehensive thread-safe metrics.
   */
  public Map<String, Object> getDetailedStatistics() {
    Map<String, Object> stats = new HashMap<>();
    stats.put("totalBorrowedPages", totalBorrowedPages.get());
    stats.put("totalReturnedPages", totalReturnedPages.get());
    stats.put("poolHits", poolHits.get());
    stats.put("poolMisses", poolMisses.get());

    PoolState currentState = state.get();
    stats.put("isInitialized", currentState == PoolState.INITIALIZED);
    stats.put("isShutdown", currentState == PoolState.SHUTDOWN);
    stats.put("state", currentState.name());

    // Calculate hit ratio
    long hits = poolHits.get();
    long total = hits + poolMisses.get();
    double hitRatio = total > 0 ? (double) hits / total : 0.0;
    stats.put("hitRatio", hitRatio);

    // Add pool sizes
    Map<Integer, Integer> poolSizes = new HashMap<>();
    int totalPages = 0;
    for (int i = 0; i < pools.length; i++) {
      Deque<KeyValueLeafPage> pool = pools[i];
      if (pool != null) {
        int size = pool.size();
        poolSizes.put(PAGE_SIZES[i], size);
        totalPages += size;
      }
    }
    stats.put("poolSizes", poolSizes);
    stats.put("totalPages", totalPages);

    return stats;
  }

  public boolean isInitialized() {
    return state.get() == PoolState.INITIALIZED;
  }

  public boolean isShutdown() {
    return state.get() == PoolState.SHUTDOWN;
  }
}