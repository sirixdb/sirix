package io.sirix.access.trx;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.ArrayList;
import java.util.List;

/**
 * A transaction-scoped arena for efficient memory allocation during node creation.
 * 
 * <p>This class implements a bump allocator pattern (similar to RocksDB's Arena
 * and DuckDB's ArenaAllocator) where memory is allocated from large blocks
 * with O(1) allocation cost. All memory is freed in bulk when the arena is closed.</p>
 * 
 * <p>Key properties:</p>
 * <ul>
 *   <li>Uses Arena.ofConfined() - only accessed from transaction thread</li>
 *   <li>Bump allocation - O(1) time, no fragmentation within blocks</li>
 *   <li>8-byte alignment for all allocations</li>
 *   <li>reset() allows reusing blocks without reallocating (for auto-commit windows)</li>
 *   <li>close() deterministically frees all memory</li>
 * </ul>
 * 
 * <p>Thread safety: This class is NOT thread-safe. It is designed to be used
 * by a single transaction thread. The underlying Arena.ofConfined() enforces this.</p>
 * 
 * @author Johannes Lichtenberger
 */
public final class TransactionArena implements AutoCloseable {
    
    /** Default block size: 1MB */
    private static final int DEFAULT_BLOCK_SIZE = 1024 * 1024;
    
    /** Alignment for allocations (8 bytes for longs) */
    private static final long ALIGNMENT = 8;
    
    /** The confined arena that owns all memory */
    private final Arena arena;
    
    /** Block size to use for new allocations */
    private final int blockSize;
    
    /** Current block being allocated from */
    private MemorySegment currentBlock;
    
    /** Current offset within the current block */
    private long offset;
    
    /** All allocated blocks (for reuse on reset) */
    private final List<MemorySegment> blocks;
    
    /** Index of current block in blocks list */
    private int currentBlockIndex;
    
    /** Whether the arena is closed */
    private boolean closed;
    
    /**
     * Create a new TransactionArena with default block size (1MB).
     */
    public TransactionArena() {
        this(DEFAULT_BLOCK_SIZE);
    }
    
    /**
     * Create a new TransactionArena with specified block size.
     * 
     * @param blockSize the size of each memory block
     */
    public TransactionArena(int blockSize) {
        this.arena = Arena.ofConfined();
        this.blockSize = blockSize;
        this.blocks = new ArrayList<>();
        this.currentBlockIndex = -1;
        this.closed = false;
    }
    
    /**
     * Allocate a segment of the specified size.
     * 
     * <p>The returned segment is 8-byte aligned and valid until the arena is closed.</p>
     * 
     * @param size the number of bytes to allocate
     * @return a MemorySegment of at least the requested size
     * @throws IllegalStateException if the arena is closed
     */
    public MemorySegment allocate(long size) {
        if (closed) {
            throw new IllegalStateException("TransactionArena is closed");
        }
        
        // Align size to 8 bytes
        long alignedSize = (size + ALIGNMENT - 1) & ~(ALIGNMENT - 1);
        
        // Check if current block has enough space
        if (currentBlock == null || offset + alignedSize > currentBlock.byteSize()) {
            allocateNewBlock(Math.max(blockSize, alignedSize));
        }
        
        // Bump allocation - O(1)
        MemorySegment result = currentBlock.asSlice(offset, size);
        offset += alignedSize;
        return result;
    }
    
    /**
     * Allocate a new block (or reuse an existing one if available).
     */
    private void allocateNewBlock(long minSize) {
        currentBlockIndex++;
        
        if (currentBlockIndex < blocks.size()) {
            // Reuse existing block
            currentBlock = blocks.get(currentBlockIndex);
            if (currentBlock.byteSize() < minSize) {
                // Block too small, allocate new one
                currentBlock = arena.allocate(minSize, ALIGNMENT);
                blocks.set(currentBlockIndex, currentBlock);
            }
        } else {
            // Allocate new block
            currentBlock = arena.allocate(minSize, ALIGNMENT);
            blocks.add(currentBlock);
        }
        offset = 0;
    }
    
    /**
     * Reset the arena for reuse. All previously allocated segments become invalid.
     * 
     * <p>This is more efficient than creating a new arena because it reuses
     * already-allocated blocks. Useful for auto-commit windows where the
     * transaction continues after committing.</p>
     */
    public void reset() {
        if (closed) {
            throw new IllegalStateException("TransactionArena is closed");
        }
        
        // Reset to first block
        currentBlockIndex = -1;
        if (!blocks.isEmpty()) {
            currentBlockIndex = 0;
            currentBlock = blocks.get(0);
            offset = 0;
        } else {
            currentBlock = null;
            offset = 0;
        }
    }
    
    /**
     * Get the total amount of memory allocated by this arena.
     * 
     * @return total allocated bytes
     */
    public long getTotalAllocated() {
        long total = 0;
        for (MemorySegment block : blocks) {
            total += block.byteSize();
        }
        return total;
    }
    
    /**
     * Get the number of blocks allocated.
     * 
     * @return number of blocks
     */
    public int getBlockCount() {
        return blocks.size();
    }
    
    /**
     * Check if this arena is closed.
     * 
     * @return true if closed
     */
    public boolean isClosed() {
        return closed;
    }
    
    /**
     * Close the arena and release all memory.
     * 
     * <p>After calling close(), all MemorySegments allocated from this arena
     * become invalid and must not be accessed.</p>
     */
    @Override
    public void close() {
        if (!closed) {
            closed = true;
            arena.close(); // Frees all blocks deterministically
            blocks.clear();
            currentBlock = null;
        }
    }
}



