package org.sirix.cache;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.sirix.page.PageKind;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;

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

  private final PageKind mPageKind;

  /**
   * Constructor.
   *
   * @param recordPageKey the record page key
   * @param index the index number
   * @param pageKind the page kind (kind of the index)
   */
  public IndexLogKey(final PageKind pageKind, final long recordPageKey,
      final @Nonnegative int index) {
    mRecordPageKey = recordPageKey;
    mIndex = index;
    mPageKind = pageKind;
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
    return Objects.hashCode(mRecordPageKey, mIndex, mPageKind);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndexLogKey) {
      final IndexLogKey other = (IndexLogKey) obj;
      return mRecordPageKey == other.mRecordPageKey && mIndex == other.mIndex
          && mPageKind == other.mPageKind;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("recordPageKey", mRecordPageKey)
                      .add("index", mIndex)
                      .add("pageKind", mPageKind)
                      .toString();
  }
}
