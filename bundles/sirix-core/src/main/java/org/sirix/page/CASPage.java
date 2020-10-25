package org.sirix.page;

import com.google.common.base.MoreObjects;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Page to hold references to a content and value summary.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class CASPage extends AbstractForwardingPage {

  /** The references page instance. */
  private Page delegate;

  /** Maximum node keys. */
  private final Map<Integer, Long> maxNodeKeys;

  /** Current maximum levels of indirect pages in the tree. */
  private final Map<Integer, Integer> currentMaxLevelsOfIndirectPages;

  /**
   * Constructor.
   */
  public CASPage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new HashMap<>();
    currentMaxLevelsOfIndirectPages = new HashMap<>();
  }

  /**
   * Read meta page.
   *
   * @param in input bytes to read from
   */
  protected CASPage(final DataInput in, final SerializationType type) throws IOException {
    delegate = PageUtils.createDelegate(in, type);
    final int maxNodeKeySize = in.readInt();
    maxNodeKeys = new HashMap<>(maxNodeKeySize);
    for (int i = 0; i < maxNodeKeySize; i++) {
      maxNodeKeys.put(i, in.readLong());
    }
    final int currentMaxLevelOfIndirectPages = in.readInt();
    currentMaxLevelsOfIndirectPages = new HashMap<>(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      currentMaxLevelsOfIndirectPages.put(i, in.readByte() & 0xFF);
    }
  }

  @Override
  public boolean setOrCreateReference(int offset, PageReference pageReference) {
    delegate = PageUtils.setReference(delegate, offset, pageReference);

    return false;
  }

  /**
   * Get indirect page reference.
   *
   * @param index the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(int index) {
    return getOrCreateReference(index);
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("mDelegate", delegate).toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize CAS index tree.
   *
   * @param pageReadTrx {@link PageReadOnlyTrx} instance
   * @param index the index number
   * @param log the transaction intent log
   */
  public void createCASIndexTree(final PageReadOnlyTrx pageReadTrx, final int index,
      final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
        && reference.getLogKey() == Constants.NULL_ID_INT
        && reference.getPersistentLogKey() == Constants.NULL_ID_LONG) {
      PageUtils.createTree(reference, IndexType.CAS, pageReadTrx, log);
      if (maxNodeKeys.get(index) == null) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
    }
  }

  @Override
  public void serialize(final DataOutput out, final SerializationType type) throws IOException {
    if (delegate instanceof ReferencesPage4) {
      out.writeByte(0);
    } else if (delegate instanceof BitmapReferencesPage) {
      out.writeByte(1);
    }
    super.serialize(out, type);
    final int maxNodeKeySize = maxNodeKeys.size();
    out.writeInt(maxNodeKeySize);
    for (int i = 0; i < maxNodeKeySize; i++) {
      out.writeLong(maxNodeKeys.get(i));
    }
    final int currentMaxLevelOfIndirectPages = maxNodeKeys.size();
    out.writeInt(currentMaxLevelOfIndirectPages);
    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
      out.writeByte(currentMaxLevelsOfIndirectPages.get(i));
    }
  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }

  public int incrementAndGetCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.merge(
        index, 1, Integer::sum);
  }

  /**
   * Get the maximum node key of the specified index by its index number.
   *
   * @param indexNo the index number
   * @return the maximum node key stored
   */
  public long getMaxNodeKey(final int indexNo) {
    return maxNodeKeys.get(indexNo);
  }

  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = maxNodeKeys.get(indexNo) + 1;
    maxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }
}
