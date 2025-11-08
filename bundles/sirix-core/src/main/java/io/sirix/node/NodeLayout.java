package io.sirix.node;

import io.sirix.access.ResourceConfiguration;
import io.sirix.access.trx.node.HashType;

import java.lang.foreign.MemoryLayout;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.VarHandle;
import java.util.Optional;

/**
 * MemoryLayout-based approach for accessing serialized node data.
 * This replaces the OffsetTable approach with structured memory layouts
 * that provide better type safety and cleaner field access patterns.
 * <p>
 * The layout handles the common structure of serialized nodes:
 * 1. Optional hash field (8 bytes)
 * 2. NodeDelegate fields (variable-length parent offset + 2 ints)
 * 3. StructNodeDelegate fields (variable-length sibling/child offsets + optional counts)
 * 4. Node-specific data (varies by node type)
 */
public final class NodeLayout {
    
    // Basic layouts for fixed-size fields
    private static final MemoryLayout HASH_LAYOUT = ValueLayout.JAVA_LONG.withName("hash");
    private static final MemoryLayout REVISION_LAYOUT = ValueLayout.JAVA_INT.withName("previousRevision");
    private static final MemoryLayout LAST_MODIFIED_LAYOUT = ValueLayout.JAVA_INT.withName("lastModifiedRevision");
    
    // VarHandles for direct field access
    private static final VarHandle HASH_HANDLE = HASH_LAYOUT.varHandle();
    private static final VarHandle REVISION_HANDLE = REVISION_LAYOUT.varHandle();
    private static final VarHandle LAST_MODIFIED_HANDLE = LAST_MODIFIED_LAYOUT.varHandle();
    
    // Layout definitions for different node components
    public static final class ObjectNodeLayout {
        
        private final boolean hasHash;
        private final boolean storeChildCount;
        private final boolean storeDescendantCount;
        
        // Calculated offsets for major field groups
        private final long hashOffset;
        private final long nodeDelegateOffset;
        private final long structDelegateOffset;
        
        public ObjectNodeLayout(ResourceConfiguration config) {
            this.hasHash = config.hashType != HashType.NONE;
            this.storeChildCount = config.storeChildCount();
            this.storeDescendantCount = config.hashType != HashType.NONE;
            
            // Calculate offsets based on configuration
            long offset = 0;
            
            if (hasHash) {
                this.hashOffset = offset;
                offset += HASH_LAYOUT.byteSize();
            } else {
                this.hashOffset = -1; // No hash field
            }
            
            this.nodeDelegateOffset = offset;
            // We'll need to calculate struct delegate offset dynamically
            this.structDelegateOffset = -1; // Will be calculated when reading
        }
        
        /**
         * Read hash value if present
         */
        public Optional<Long> readHash(MemorySegment segment) {
            if (!hasHash) {
                return Optional.empty();
            }
            long hash = (long) HASH_HANDLE.get(segment, hashOffset);
            return Optional.of(hash);
        }
        
        /**
         * Read NodeDelegate revision fields at given offset
         */
        public RevisionData readRevisions(MemorySegment segment, long offset) {
            int previousRevision = (int) REVISION_HANDLE.get(segment, offset);
            int lastModifiedRevision = (int) LAST_MODIFIED_HANDLE.get(segment, offset + Integer.BYTES);
            return new RevisionData(previousRevision, lastModifiedRevision);
        }
        
        /**
         * Get the starting offset for NodeDelegate fields
         */
        public long getNodeDelegateOffset() {
            return nodeDelegateOffset;
        }
        
        /**
         * Calculate StructNodeDelegate offset based on NodeDelegate size
         */
        public long calculateStructDelegateOffset(long nodeDelegateStartOffset, int parentKeyOffsetSize) {
            // NodeDelegate = varlong(parentKeyOffset) + int(previousRevision) + int(lastModifiedRevision)
            return nodeDelegateStartOffset + parentKeyOffsetSize + Integer.BYTES + Integer.BYTES;
        }
        
        public boolean hasHash() {
            return hasHash;
        }
        
        public boolean storeChildCount() {
            return storeChildCount;
        }
        
        public boolean storeDescendantCount() {
            return storeDescendantCount;
        }
    }
    
    /**
     * Data structure for revision information
     */
    public record RevisionData(int previousRevision, int lastModifiedRevision) {}
    
    /**
     * Factory methods for creating layouts for different node types
     */
    public static ObjectNodeLayout forObjectNode(ResourceConfiguration config) {
        return new ObjectNodeLayout(config);
    }
    
    
    // Additional layout types can be added for ArrayNode, etc.
    // public static ArrayNodeLayout forArrayNode(ResourceConfiguration config) { ... }
}