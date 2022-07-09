package org.sirix.cache;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.Nullable;

import org.sirix.index.IndexType;
import com.google.common.base.MoreObjects;

import java.util.Objects;

/**
 * Index log key.
 *
 * @author Johannes Lichtenberger
 */
public record IndexLogKey(IndexType indexType, long recordPageKey, @NonNegative int indexNumber,
                          @NonNegative int revisionNumber) {
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
}

