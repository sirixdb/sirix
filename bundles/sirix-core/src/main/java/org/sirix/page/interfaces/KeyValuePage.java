package org.sirix.page.interfaces;

import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.index.IndexType;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageReference;

import java.util.Map.Entry;
import java.util.Set;

/**
 * Key/Value page.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface KeyValuePage<V extends DataRecord> extends Page  {

  /**
   * All available records.
   *
   * @return all records
   */
  <I extends Iterable<V>> I values();

  /**
   * All slots.
   * @return all slots
   */
  byte[][] slots();

  /**
   * All deweyIds.
   * @return all deweyIDs
   */
  byte[][] deweyIds();


  /**
   * Get the unique page record identifier.
   *
   * @return page record key/identifier
   */
  long getPageKey();

  /**
   * Get value with the specified key.
   *
   * @param pageReadOnlyTrx the page read only transaction
   * @param key the key
   * @return value with given key, or {@code null} if not present
   */
  V getValue(@NonNull PageReadOnlyTrx pageReadOnlyTrx, long key);

  byte[] getSlot(int slotNumber);

  byte[] getDeweyId(int offset);

  /**
   * Store or overwrite a single entry. The implementation must make sure if the key must be
   * permitted, the record or none.
   *
   * @param record record to store
   */
  void setRecord(@NonNull V record);

  V[] records();

  V getRecord(long key);

  Set<Entry<Long, PageReference>> referenceEntrySet();

  /**
   * Store or overwrite a single reference associated with a key for overlong entries. That is
   * entries which are larger than a predefined threshold are written to OverflowPages and thus are
   * just referenced and not deserialized during the deserialization of a page.
   *
   * @param key key to store
   * @param reference reference to store
   */
  void setPageReference(long key, @NonNull PageReference reference);

  PageReference getPageReference(long key);

  void setSlot(byte[] recordData, int offset);

  void setDeweyId(byte[] deweyId, int offset);

  /**
   * Create a new instance.
   *
   * @param recordPageKey the record page key
   * @param indexType the index type
   * @param pageReadTrx transaction to read pages
   * @return a new {@link KeyValuePage} instance
   */
  <C extends KeyValuePage<V>> C newInstance(@NonNegative long recordPageKey,
      @NonNull IndexType indexType, @NonNull PageReadOnlyTrx pageReadTrx);

  <C extends KeyValuePage<V>> C copy();

  /**
   * Get the index type.
   *
   * @return index type
   */
  IndexType getIndexType();

  /**
   * Get the number of entries/slots/page references filled.
   *
   * @return number of entries/slots/page references filled
   */
  int size();

  int getRevision();
}
