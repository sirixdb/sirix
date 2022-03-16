package org.sirix.cache;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import org.sirix.index.IndexType;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Index log key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndexLogKey {
  /** Unique number. */
  private final int indexNumber;

  /** Record page key. */
  private final long recordPageKey;

  /** The kind of index. */
  private final IndexType indexType;

  /** The revision number. */
  private final int revisionNumber;

  /**
   * Constructor.
   *
   * @param indexType the indexNumber type
   * @param recordPageKey the record page key
   * @param indexNumber the index number
   * @param revisionNumber the revision number
   */
  public IndexLogKey(final IndexType indexType, final long recordPageKey,
      final @NonNegative int indexNumber, final @NonNegative int revisionNumber) {
    this.recordPageKey = recordPageKey;
    this.indexNumber = indexNumber;
    this.indexType = indexType;
    this.revisionNumber = revisionNumber;
  }

  public long getRecordPageKey() {
    return recordPageKey;
  }

  public int getIndexNumber() {
    return indexNumber;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(recordPageKey, indexNumber, indexType, revisionNumber);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndexLogKey) {
      final IndexLogKey other = (IndexLogKey) obj;
      return recordPageKey == other.recordPageKey && indexNumber == other.indexNumber
          && indexType == other.indexType && revisionNumber == other.revisionNumber;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("recordPageKey", recordPageKey)
                      .add("index", indexNumber)
                      .add("indexType", indexType)
                      .add("revisionNumber", revisionNumber)
                      .toString();
  }
}
