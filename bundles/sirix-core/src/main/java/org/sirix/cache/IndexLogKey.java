package org.sirix.cache;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import org.sirix.page.PageKind;
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
  private final int mIndex;

  /** Record page key. */
  private final long mRecordPageKey;

  /** The kind of index. */
  private final PageKind mPageKind;

  /** The revision number. */
  private final int mRevisionNumber;

  /**
   * Constructor.
   *
   * @param recordPageKey the record page key
   * @param index the index number
   * @param pageKind the page kind (kind of the index)
   * @param revisionNumber the revision number
   */
  public IndexLogKey(final PageKind pageKind, final long recordPageKey,
      final @Nonnegative int index, final @Nonnegative int revisionNumber) {
    mRecordPageKey = recordPageKey;
    mIndex = index;
    mPageKind = pageKind;
    mRevisionNumber = revisionNumber;
  }

  public long getRecordPageKey() {
    return mRecordPageKey;
  }

  public int getIndex() {
    return mIndex;
  }

  public PageKind getIndexType() {
    return mPageKind;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mRecordPageKey, mIndex, mPageKind, mRevisionNumber);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndexLogKey) {
      final IndexLogKey other = (IndexLogKey) obj;
      return mRecordPageKey == other.mRecordPageKey && mIndex == other.mIndex
          && mPageKind == other.mPageKind && mRevisionNumber == other.mRevisionNumber;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("recordPageKey", mRecordPageKey)
                      .add("index", mIndex)
                      .add("pageKind", mPageKind)
                      .add("revisionNumber", mRevisionNumber)
                      .toString();
  }
}
