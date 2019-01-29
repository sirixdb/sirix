package org.sirix.page;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.page.delegates.PageDelegate;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;
import com.google.common.base.MoreObjects;

/**
 * Page to hold references to a value summary.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathPage extends AbstractForwardingPage {

  /** {@link PageDelegate} instance. */
  private final PageDelegate mDelegate;

  /** Maximum node keys. */
  private final Map<Integer, Long> mMaxNodeKeys;

  /** Current maximum levels of indirect pages in the tree. */
  private final Map<Integer, Integer> mCurrentMaxLevelsOfIndirectPages;

  /**
   * Constructor.
   */
  public PathPage() {
    mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR);
    mMaxNodeKeys = new HashMap<>();
    mCurrentMaxLevelsOfIndirectPages = new HashMap<>();
  }

  /**
   * Get indirect page reference.
   *
   * @param offset the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(int offset) {
    return getReference(offset);
  }

  /**
   * Read meta page.
   *
   * @param in input bytes to read from
   * @throws IOException if the page couldn't be deserialized
   */
  protected PathPage(final DataInput in, final SerializationType type) throws IOException {
    mDelegate = new PageDelegate(PageConstants.MAX_INDEX_NR, in, type);
    final int size = in.readInt();
    mMaxNodeKeys = new HashMap<>(size);
    for (int i = 0; i < size; i++) {
      mMaxNodeKeys.put(i, in.readLong());
    }
    final int currentMaxLevelOfIndirectPages = in.readInt();
    mCurrentMaxLevelsOfIndirectPages = new HashMap<>(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      mCurrentMaxLevelsOfIndirectPages.put(i, in.readByte() & 0xFF);
    }
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("mDelegate", mDelegate).toString();
  }

  @Override
  protected Page delegate() {
    return mDelegate;
  }

  /**
   * Initialize path index tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createPathIndexTree(final PageReadOnlyTrx pageReadTrx, final int index,
      final TransactionIntentLog log) {
    final PageReference reference = getReference(index);
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(reference, PageKind.PATHPAGE, index, pageReadTrx, log);
      if (mMaxNodeKeys.get(index) == null) {
        mMaxNodeKeys.put(index, 0l);
      } else {
        mMaxNodeKeys.put(index, mMaxNodeKeys.get(index).longValue() + 1);
      }
      if (mCurrentMaxLevelsOfIndirectPages.get(index) == null) {
        mCurrentMaxLevelsOfIndirectPages.put(index, 1);
      } else {
        mCurrentMaxLevelsOfIndirectPages.put(
            index, mCurrentMaxLevelsOfIndirectPages.get(index) + 1);
      }
    }
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    super.serialize(out, type);
    final int size = mMaxNodeKeys.size();
    out.writeInt(size);
    for (int i = 0; i < size; i++) {
      out.writeLong(mMaxNodeKeys.get(i));
    }
    final int currentMaxLevelOfIndirectPages = mMaxNodeKeys.size();
    out.writeInt(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      out.writeByte(mCurrentMaxLevelsOfIndirectPages.get(i));
    }
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return mCurrentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return mCurrentMaxLevelsOfIndirectPages.merge(
        index, 1, (previousValue, value) -> previousValue + value);
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNo the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNo) {
    return mMaxNodeKeys.get(indexNo);
  }

  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = mMaxNodeKeys.get(indexNo).longValue() + 1;
    mMaxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }
}
