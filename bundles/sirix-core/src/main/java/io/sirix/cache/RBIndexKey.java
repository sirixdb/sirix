package io.sirix.cache;

import io.sirix.index.IndexType;

/**
 * Composite cache key for RedBlackTree index nodes to support global BufferManager. Includes
 * database ID and resource ID to uniquely identify index nodes across all databases and resources.
 * <p>
 * This is an immutable class with equals/hashCode that also supports {@link RBIndexKeyLookup} for
 * zero-allocation cache lookups during tree traversal.
 * </p>
 *
 * @author Johannes Lichtenberger
 */
public final class RBIndexKey {

  private final long databaseId;
  private final long resourceId;
  private final long nodeKey;
  private final int revisionNumber;
  private final IndexType indexType;
  private final int indexNumber;

  /**
   * Create a new cache key.
   *
   * @param databaseId the unique database ID
   * @param resourceId the unique resource ID within the database
   * @param nodeKey the node key
   * @param revisionNumber the revision number
   * @param indexType the index type
   * @param indexNumber the index number
   */
  public RBIndexKey(long databaseId, long resourceId, long nodeKey, int revisionNumber, IndexType indexType,
      int indexNumber) {
    this.databaseId = databaseId;
    this.resourceId = resourceId;
    this.nodeKey = nodeKey;
    this.revisionNumber = revisionNumber;
    this.indexType = indexType;
    this.indexNumber = indexNumber;
  }

  public long databaseId() {
    return databaseId;
  }

  public long resourceId() {
    return resourceId;
  }

  public long nodeKey() {
    return nodeKey;
  }

  public int revisionNumber() {
    return revisionNumber;
  }

  public IndexType indexType() {
    return indexType;
  }

  public int indexNumber() {
    return indexNumber;
  }

  /**
   * Hash code compatible with {@link RBIndexKeyLookup}.
   */
  @Override
  public int hashCode() {
    int result = Long.hashCode(databaseId);
    result = 31 * result + Long.hashCode(resourceId);
    result = 31 * result + Long.hashCode(nodeKey);
    result = 31 * result + revisionNumber;
    result = 31 * result + (indexType != null
        ? indexType.hashCode()
        : 0);
    result = 31 * result + indexNumber;
    return result;
  }

  /**
   * Equals implementation that supports both {@link RBIndexKey} and {@link RBIndexKeyLookup} for
   * zero-allocation cache lookups.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof RBIndexKey other) {
      return databaseId == other.databaseId && resourceId == other.resourceId && nodeKey == other.nodeKey
          && revisionNumber == other.revisionNumber && indexType == other.indexType && indexNumber == other.indexNumber;
    }
    if (obj instanceof RBIndexKeyLookup other) {
      return databaseId == other.databaseId() && resourceId == other.resourceId() && nodeKey == other.nodeKey()
          && revisionNumber == other.revisionNumber() && indexType == other.indexType()
          && indexNumber == other.indexNumber();
    }
    return false;
  }

  @Override
  public String toString() {
    return "RBIndexKey[" + "databaseId=" + databaseId + ", resourceId=" + resourceId + ", nodeKey=" + nodeKey
        + ", revisionNumber=" + revisionNumber + ", indexType=" + indexType + ", indexNumber=" + indexNumber + ']';
  }
}
