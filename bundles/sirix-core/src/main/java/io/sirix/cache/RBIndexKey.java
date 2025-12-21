package io.sirix.cache;

import io.sirix.index.IndexType;

/**
 * Composite cache key for RedBlackTree index nodes to support global BufferManager.
 * Includes database ID and resource ID to uniquely identify index nodes
 * across all databases and resources.
 *
 * @param databaseId the unique database ID
 * @param resourceId the unique resource ID within the database
 * @param nodeKey the node key
 * @param revisionNumber the revision number
 * @param indexType the index type
 * @param indexNumber the index number
 */
public record RBIndexKey(long databaseId, long resourceId, long nodeKey, int revisionNumber, IndexType indexType, int indexNumber) {
}
