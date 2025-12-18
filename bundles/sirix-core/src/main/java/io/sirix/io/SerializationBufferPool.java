package io.sirix.io;

import io.sirix.cache.LinuxMemorySegmentAllocator;
import io.sirix.cache.MemorySegmentAllocator;
import io.sirix.cache.WindowsMemorySegmentAllocator;
import io.sirix.node.PooledGrowingSegment;
import io.sirix.utils.OS;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.foreign.MemorySegment;
import java.util.ArrayDeque;

/**
 * Striped buffer pool for parallel serialization (ScyllaDB pattern).
 * Reduces contention by partitioning buffers across CPU cores.
 * 
 * <p>This pool is designed for use during parallel page serialization in
 * {@code NodeStorageEngineWriter.parallelSerializationOfKeyValuePages()}.
 * It eliminates the overhead of creating new Arena.ofAuto() instances
 * for each page serialized.</p>
 * 
 * <p>The pool uses striping to reduce lock contention: each CPU core
 * has its own deque of buffers, so threads rarely contend for the same lock.</p>
 * 
 * <p>Thread safety: This class is thread-safe. Each stripe's deque is accessed
 * under its own lock, and the stripe assignment is based on thread ID.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class SerializationBufferPool {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(SerializationBufferPool.class);
    
    private static final int STRIPE_COUNT = Runtime.getRuntime().availableProcessors();
    private static final int BUFFERS_PER_STRIPE = 2;
    
    /** Default buffer size: 128KB - sufficient for most pages */
    private static final int DEFAULT_BUFFER_SIZE = 128 * 1024;
    
    // Striped queues - each stripe accessed by subset of threads
    private final ArrayDeque<PooledGrowingSegment>[] stripes;
    
    // Locks for each stripe
    private final Object[] stripeLocks;
    
    // Use page allocator for underlying segments
    private static final MemorySegmentAllocator ALLOCATOR = 
        OS.isWindows() ? WindowsMemorySegmentAllocator.getInstance() 
                       : LinuxMemorySegmentAllocator.getInstance();
    
    private final int bufferSize;
    
    /** Singleton instance for page serialization */
    public static final SerializationBufferPool INSTANCE = new SerializationBufferPool(DEFAULT_BUFFER_SIZE);
    
    /**
     * Create a new SerializationBufferPool.
     * 
     * @param bufferSize the size of each buffer in bytes
     */
    @SuppressWarnings("unchecked")
    public SerializationBufferPool(int bufferSize) {
        this.bufferSize = bufferSize;
        this.stripes = new ArrayDeque[STRIPE_COUNT];
        this.stripeLocks = new Object[STRIPE_COUNT];
        
        for (int i = 0; i < STRIPE_COUNT; i++) {
            stripes[i] = new ArrayDeque<>(BUFFERS_PER_STRIPE);
            stripeLocks[i] = new Object();
            
            // Pre-allocate buffers
            for (int j = 0; j < BUFFERS_PER_STRIPE; j++) {
                MemorySegment segment = ALLOCATOR.allocate(bufferSize);
                stripes[i].add(new PooledGrowingSegment(segment));
            }
        }
        
        LOGGER.debug("SerializationBufferPool initialized: {} stripes × {} buffers = {} total, {}KB each",
                     STRIPE_COUNT, BUFFERS_PER_STRIPE, STRIPE_COUNT * BUFFERS_PER_STRIPE, bufferSize / 1024);
    }
    
    /**
     * Acquire a buffer from the pool.
     * 
     * <p>If the thread's stripe has available buffers, one is returned immediately.
     * If the stripe is exhausted, a temporary buffer is allocated (not from pool).</p>
     * 
     * @return a PooledGrowingSegment ready for writing
     */
    public PooledGrowingSegment acquire() {
        int stripe = getStripeIndex();
        
        synchronized (stripeLocks[stripe]) {
            PooledGrowingSegment seg = stripes[stripe].pollFirst();
            if (seg != null) {
                return seg;
            }
        }
        
        // All stripes exhausted - create temporary (rare case under normal load)
        LOGGER.trace("Stripe {} exhausted, allocating temporary buffer", stripe);
        return new PooledGrowingSegment(ALLOCATOR.allocate(bufferSize));
    }
    
    /**
     * Release a buffer back to the pool.
     * 
     * <p>The buffer is reset (clearing any overflow arena) before being returned.
     * If the stripe is full, the underlying segment is returned to the allocator.</p>
     * 
     * @param seg the segment to release
     */
    public void release(PooledGrowingSegment seg) {
        if (seg == null) {
            return;
        }
        
        // Reset clears overflow arena and restores original buffer pointer
        seg.reset();
        
        int stripe = getStripeIndex();
        
        synchronized (stripeLocks[stripe]) {
            if (stripes[stripe].size() < BUFFERS_PER_STRIPE) {
                stripes[stripe].addLast(seg);
                return;
            }
        }
        
        // Pool full - return underlying segment to allocator to prevent leak
        LOGGER.trace("Pool full, releasing segment to allocator");
        ALLOCATOR.release(seg.getUnderlyingSegment());
    }
    
    /**
     * Get the stripe index for the current thread.
     * Uses thread ID for consistent assignment.
     */
    private static int getStripeIndex() {
        return (int) (Thread.currentThread().threadId() % STRIPE_COUNT);
    }
    
    /**
     * Get statistics about the pool.
     * 
     * @return a string describing pool state
     */
    public String getStats() {
        int[] sizes = new int[STRIPE_COUNT];
        int total = 0;
        for (int i = 0; i < STRIPE_COUNT; i++) {
            synchronized (stripeLocks[i]) {
                sizes[i] = stripes[i].size();
                total += sizes[i];
            }
        }
        return String.format("SerializationBufferPool: %d/%d buffers available across %d stripes",
                            total, STRIPE_COUNT * BUFFERS_PER_STRIPE, STRIPE_COUNT);
    }
    
    /**
     * Get the buffer size used by this pool.
     * 
     * @return the buffer size in bytes
     */
    public int getBufferSize() {
        return bufferSize;
    }
    
    /**
     * Get the total number of buffers currently in the pool.
     * 
     * @return the count of available buffers
     */
    public int getAvailableBufferCount() {
        int total = 0;
        for (int i = 0; i < STRIPE_COUNT; i++) {
            synchronized (stripeLocks[i]) {
                total += stripes[i].size();
            }
        }
        return total;
    }
}




