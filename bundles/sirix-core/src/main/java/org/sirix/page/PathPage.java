package org.sirix.page;

import com.google.common.base.MoreObjects;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.ints.Int2LongOpenHashMap;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.access.DatabaseType;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.cache.TransactionIntentLog;
import org.sirix.index.IndexType;
import org.sirix.page.delegates.BitmapReferencesPage;
import org.sirix.page.delegates.ReferencesPage4;
import org.sirix.page.interfaces.Page;
import org.sirix.settings.Constants;

/**
 * Page to hold references to a value summary.
 *
 * @author Johannes Lichtenberger, University of Konstanz
 *
 */
public final class PathPage extends AbstractForwardingPage {

  /** The references page instance. */
  private Page delegate;

  /** Maximum node keys. */
  private final Int2LongMap maxNodeKeys;

  /** Current maximum levels of indirect pages in the tree. */
  private final Int2IntMap currentMaxLevelsOfIndirectPages;

  /**
   * Constructor.
   */
  public PathPage() {
    delegate = new ReferencesPage4();
    maxNodeKeys = new Int2LongOpenHashMap();
    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap();
  }

  /**
   * Get indirect page reference.
   *
   * @param offset the offset of the indirect page, that is the index number
   * @return indirect page reference
   */
  public PageReference getIndirectPageReference(int offset) {
    return getOrCreateReference(offset);
  }

  //**
//   * Read meta page.
//   *
//   * @param in input bytes to read from
//   */
//  public PathPage(final Bytes<?> in, final SerializationType type) {
//    delegate = PageUtils.createDelegate(in, type);
//    final int size = in.readInt();
//    maxNodeKeys = new Int2LongOpenHashMap((int) Math.ceil(size / 0.75));
//    for (int i = 0; i < size; i++) {
//      maxNodeKeys.put(i, in.readLong());
//    }
//    final int currentMaxLevelOfIndirectPages = in.readInt();
//    currentMaxLevelsOfIndirectPages = new Int2IntOpenHashMap((int) Math.ceil(currentMaxLevelOfIndirectPages / 0.75));
//    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
//      currentMaxLevelsOfIndirectPages.put(i, in.readByte() & 0xFF);
//    }
//  }
  /**
   * Read meta page.
   *
   * @param delegate The references page instance.
   * @param  maxNodeKeys Maximum node keys.
   * @param currentMaxLevelsOfIndirectPages Current maximum levels of indirect pages in the tree.
   */
  public PathPage(final Page delegate, final Int2LongMap maxNodeKeys, final Int2IntMap currentMaxLevelsOfIndirectPages){
    this.delegate = delegate;
    this.maxNodeKeys = maxNodeKeys;
    this.currentMaxLevelsOfIndirectPages = currentMaxLevelsOfIndirectPages;
  }

  @Override
  public @NonNull String toString() {
    return MoreObjects.toStringHelper(this).add("delegate", delegate).toString();
  }

  @Override
  protected Page delegate() {
    return delegate;
  }

  /**
   * Initialize path index tree.
   *
   * @param databaseType The type of database.
   * @param pageReadTrx  {@link PageReadOnlyTrx} instance
   * @param index        the index number
   * @param log          the transaction intent log
   */
  public void createPathIndexTree(final DatabaseType databaseType,
                                  final PageReadOnlyTrx pageReadTrx,
                                  final int index,
                                  final TransactionIntentLog log) {
    PageReference reference = getOrCreateReference(index);
    if (reference == null) {
      delegate = new BitmapReferencesPage(Constants.INP_REFERENCE_COUNT, (ReferencesPage4) delegate());
      reference = delegate.getOrCreateReference(index);
    }
    if (reference.getPage() == null && reference.getKey() == Constants.NULL_ID_LONG
            && reference.getLogKey() == Constants.NULL_ID_INT) {
      PageUtils.createTree(databaseType, reference, IndexType.PATH, pageReadTrx, log);
      if (maxNodeKeys.get(index) == 0L) {
        maxNodeKeys.put(index, 0L);
      } else {
        maxNodeKeys.put(index, maxNodeKeys.get(index) + 1);
      }
      currentMaxLevelsOfIndirectPages.merge(index, 1, Integer::sum);
    }
  }

//  @Override
//  public void serialize(final PageReadOnlyTrx pageReadOnlyTrx, final Bytes<ByteBuffer> out, final SerializationType type) {
//    if (delegate instanceof ReferencesPage4) {
//      out.writeByte((byte) 0);
//    } else if (delegate instanceof BitmapReferencesPage) {
//      out.writeByte((byte) 1);
//    }
//    super.serialize(pageReadOnlyTrx, out, type);
//    final int size = maxNodeKeys.size();
//    out.writeInt(size);
//    for (int i = 0; i < size; i++) {
//      out.writeLong(maxNodeKeys.get(i));
//    }
//    final int currentMaxLevelOfIndirectPages = maxNodeKeys.size();
//    out.writeInt(currentMaxLevelOfIndirectPages);
//    for (int i = 0; i < currentMaxLevelOfIndirectPages; i++) {
//      out.writeByte((byte) currentMaxLevelsOfIndirectPages.get(i));
//    }
//  }

  public int getCurrentMaxLevelOfIndirectPages(int index) {
    return currentMaxLevelsOfIndirectPages.get(index);
  }
  /**
   * Get the size of CurrentMaxLevelOfIndirectPages to Serialize
   * @return int Size of CurrentMaxLevelOfIndirectPages
   */
  public int getCurrentMaxLevelOfIndirectPagesSize(){
    return currentMaxLevelsOfIndirectPages.size();
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

  /**
   * Get the size of MaxNodeKey to Serialize
   * @return int Size of MaxNodeKey
   */
  public int getMaxNodeKeySize(){
    return maxNodeKeys.size();
  }

  public long incrementAndGetMaxNodeKey(final int indexNo) {
    final long newMaxNodeKey = maxNodeKeys.get(indexNo) + 1;
    maxNodeKeys.put(indexNo, newMaxNodeKey);
    return newMaxNodeKey;
  }
}
