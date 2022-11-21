package org.sirix.cache;

import org.checkerframework.checker.index.qual.NonNegative;
import org.sirix.index.IndexType;

import java.util.Objects;

/**
 * Index log key.
 *
 * @author Johannes Lichtenberger
 */
public final class IndexLogKey {
  private final IndexType indexType;
  private final long recordPageKey;
  private final @NonNegative int indexNumber;
  private final @NonNegative int revisionNumber;

  public IndexLogKey(IndexType indexType, long recordPageKey, @NonNegative int indexNumber,
      @NonNegative int revisionNumber) {
    this.indexType = indexType;
    this.recordPageKey = recordPageKey;
    this.indexNumber = indexNumber;
    this.revisionNumber = revisionNumber;
  }

  private int hash;

  @Override
  public int hashCode() {
    if (hash == 0) {
      hash = Objects.hash(indexType, recordPageKey, indexNumber, revisionNumber);
    }
    return hash;
  }

  public long getRecordPageKey() {
    return recordPageKey;
  }

  public IndexType getIndexType() {
    return indexType;
  }

  public int getIndexNumber() {
    return indexNumber;
  }

  public int getRevisionNumber() {
    return revisionNumber;
  }

  public IndexType indexType() {
    return indexType;
  }

  public long recordPageKey() {
    return recordPageKey;
  }

  public @NonNegative int indexNumber() {
    return indexNumber;
  }

  public @NonNegative int revisionNumber() {
    return revisionNumber;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this)
      return true;
    if (obj == null || obj.getClass() != this.getClass())
      return false;
    var that = (IndexLogKey) obj;
    return this.indexType == that.indexType && this.recordPageKey == that.recordPageKey
        && this.indexNumber == that.indexNumber && this.revisionNumber == that.revisionNumber;
  }

  @Override
  public String toString() {
    return "IndexLogKey[" + "indexType=" + indexType + ", " + "recordPageKey=" + recordPageKey + ", " + "indexNumber="
        + indexNumber + ", " + "revisionNumber=" + revisionNumber + ']';
  }

}