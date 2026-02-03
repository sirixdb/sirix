package io.sirix.cache;

import io.sirix.index.IndexType;

/**
 * Mutable lookup key for RedBlackTree index cache lookups.
 * <p>
 * This class is designed to be reused via ThreadLocal to avoid allocation
 * on every cache lookup during tree traversal. It has hashCode/equals
 * compatible with the immutable {@link RBIndexKey} record.
 * </p>
 * <p>
 * <b>IMPORTANT:</b> This class must NOT be used as a key for cache insertions
 * (put operations). Only the immutable {@link RBIndexKey} record should be
 * used for insertions to ensure cache integrity.
 * </p>
 *
 * @author Johannes Lichtenberger
 * @see RBIndexKey
 */
public final class RBIndexKeyLookup {

  private long databaseId;
  private long resourceId;
  private long nodeKey;
  private int revisionNumber;
  private IndexType indexType;
  private int indexNumber;

  /**
   * Create a new lookup key with default values.
   */
  public RBIndexKeyLookup() {
    // Default constructor - fields will be set via setAll()
  }

  /**
   * Set all fields at once for reuse.
   *
   * @param databaseId     the database ID
   * @param resourceId     the resource ID
   * @param nodeKey        the node key
   * @param revisionNumber the revision number
   * @param indexType      the index type
   * @param indexNumber    the index number
   * @return this instance for method chaining
   */
  public RBIndexKeyLookup setAll(long databaseId, long resourceId, long nodeKey,
                                  int revisionNumber, IndexType indexType, int indexNumber) {
    this.databaseId = databaseId;
    this.resourceId = resourceId;
    this.nodeKey = nodeKey;
    this.revisionNumber = revisionNumber;
    this.indexType = indexType;
    this.indexNumber = indexNumber;
    return this;
  }

  /**
   * Set only the node key (for tree traversal where other fields are constant).
   *
   * @param nodeKey the node key
   * @return this instance for method chaining
   */
  public RBIndexKeyLookup setNodeKey(long nodeKey) {
    this.nodeKey = nodeKey;
    return this;
  }

  // Getters for all fields (needed for equals/hashCode compatibility)

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
   * Hash code compatible with {@link RBIndexKey} record.
   * This follows the same algorithm as the auto-generated record hashCode.
   */
  @Override
  public int hashCode() {
    int result = Long.hashCode(databaseId);
    result = 31 * result + Long.hashCode(resourceId);
    result = 31 * result + Long.hashCode(nodeKey);
    result = 31 * result + revisionNumber;
    result = 31 * result + (indexType != null ? indexType.hashCode() : 0);
    result = 31 * result + indexNumber;
    return result;
  }

  /**
   * Equals implementation compatible with {@link RBIndexKey} record.
   * Supports equality checks with both RBIndexKeyLookup and RBIndexKey.
   */
  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj instanceof RBIndexKeyLookup other) {
      return databaseId == other.databaseId
          && resourceId == other.resourceId
          && nodeKey == other.nodeKey
          && revisionNumber == other.revisionNumber
          && indexType == other.indexType
          && indexNumber == other.indexNumber;
    }
    if (obj instanceof RBIndexKey other) {
      return databaseId == other.databaseId()
          && resourceId == other.resourceId()
          && nodeKey == other.nodeKey()
          && revisionNumber == other.revisionNumber()
          && indexType == other.indexType()
          && indexNumber == other.indexNumber();
    }
    return false;
  }

  @Override
  public String toString() {
    return "RBIndexKeyLookup[" +
        "databaseId=" + databaseId +
        ", resourceId=" + resourceId +
        ", nodeKey=" + nodeKey +
        ", revisionNumber=" + revisionNumber +
        ", indexType=" + indexType +
        ", indexNumber=" + indexNumber +
        ']';
  }
}
