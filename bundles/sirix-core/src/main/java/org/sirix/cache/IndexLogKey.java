package org.sirix.cache;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.sirix.index.IndexType;

/**
 * Index log key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndexLogKey {
  /** Unique number. */
  private final int index;

  /** Record page key. */
  private final long recordPageKey;

  /** The kind of index. */
  private final IndexType indexType;

  /** The revision number. */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param recordPageKey the record page key
   * @param index the index number
   * @param indexType the index type
   * @param revisionNumber the revision number
   */
  public IndexLogKey(final IndexType indexType, final long recordPageKey,
      final @NonNegative int index, final @NonNegative int revisionNumber) {
    this.recordPageKey = recordPageKey;
    this.index = index;
    this.indexType = indexType;
    this.revisionNumber = revisionNumber;
  }

  public long getRecordPageKey() {
    return recordPageKey;
  }

  public int getIndex() {
    return index;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(recordPageKey, index, indexType, revisionNumber);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndexLogKey) {
      final IndexLogKey other = (IndexLogKey) obj;
      return recordPageKey == other.recordPageKey && index == other.index
          && indexType == other.indexType && revisionNumber == other.revisionNumber;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("recordPageKey", recordPageKey)
                      .add("index", index)
                      .add("indexType", indexType)
                      .add("revisionNumber", revisionNumber)
                      .toString();
  }
}
