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
  private final int index;

  /** Record page key. */
  private final long recordPageKey;

  /** The kind of index. */
  private final PageKind pageKind;

  /** The revision number. */
  private final int revisionNumber;

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
    this.recordPageKey = recordPageKey;
    this.index = index;
    this.pageKind = pageKind;
    this.revisionNumber = revisionNumber;
  }

  public long getRecordPageKey() {
    return recordPageKey;
  }

  public int getIndex() {
    return index;
  }

  public PageKind getIndexType() {
    return pageKind;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(recordPageKey, index, pageKind, revisionNumber);
  }

  @Override
  public boolean equals(final @Nullable Object obj) {
    if (obj instanceof IndexLogKey) {
      final IndexLogKey other = (IndexLogKey) obj;
      return recordPageKey == other.recordPageKey && index == other.index
          && pageKind == other.pageKind && revisionNumber == other.revisionNumber;
    }
    return false;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
                      .add("recordPageKey", recordPageKey)
                      .add("index", index)
                      .add("pageKind", pageKind)
                      .add("revisionNumber", revisionNumber)
                      .toString();
  }
}
