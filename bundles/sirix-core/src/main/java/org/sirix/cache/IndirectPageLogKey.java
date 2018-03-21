package org.sirix.cache;

import javax.annotation.Nullable;
import org.sirix.page.PageKind;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

/**
 * Log key.
 *
 * @author Johannes Lichtenberger
 *
 */
public final class IndirectPageLogKey {
  private int mLevel;
  private int mOffset;
  private PageKind mPageKind;
  private int mIndex;
  private long mPageKey;

  public IndirectPageLogKey(final PageKind pageKind, final int index, final int level,
      final int offset, final long pageKey) {
    assert level >= -1;
    assert offset >= -1;
    assert pageKind != null;
    mPageKind = pageKind;
    mIndex = index;
    mLevel = level;
    mOffset = offset;
    mPageKey = pageKey;
  }

  public int getLevel() {
    return mLevel;
  }

  public int getOffset() {
    return mOffset;
  }

  public PageKind getPageKind() {
    return mPageKind;
  }

  public int getIndex() {
    return mIndex;
  }

  public long getPageKey() {
    return mPageKey;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(mPageKind, mIndex, mLevel, mOffset, mPageKey);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndirectPageLogKey) {
      final IndirectPageLogKey other = (IndirectPageLogKey) obj;
      return mPageKind == other.mPageKind && mIndex == other.mIndex && mLevel == other.mLevel
          && mOffset == other.mOffset && mPageKey == other.mPageKey;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("pageKind", mPageKind).add("index", mIndex)
        .add("level", mLevel).add("offset", mOffset).add("pageKey", mPageKey).toString();
  }
}
