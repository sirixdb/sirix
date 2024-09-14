package io.sirix.page.interfaces;

import io.sirix.api.PageReadOnlyTrx;
import io.sirix.index.IndexType;
import io.sirix.node.interfaces.DataRecord;
import io.sirix.page.PageReference;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;

import java.lang.foreign.MemorySegment;
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
   *
   * @return all slots
   */
  MemorySegment slots();

  /**
   * All deweyIds.
   *
   * @return all deweyIDs
   */
  MemorySegment deweyIds();

  void incrementPinCount();

  void decrementPinCount();

  int getPinCount();

  /**
   * Get the unique page record identifier.
   *
   * @return page record key/identifier
   */
  long getPageKey();

  void setSlot(MemorySegment data, int slotNumber);

  int getUsedDeweyIdSize();

  int getUsedSlotsSize();

  byte[] getSlotAsByteArray(int slotNumber);

  byte[] getDeweyIdAsByteArray(int offset);

  MemorySegment getDeweyId(int offset);

  /**
   * Store or overwrite a single entry. The implementation must make sure if the key must be
   * permitted, the record or none.
   *
   * @param record record to store
   */
  void setRecord(@NonNull V record);

  V[] records();

  V getRecord(int offset);

  Set<Entry<Long, PageReference>> referenceEntrySet();

  boolean isClosed();

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

  MemorySegment getSlot(int slotNumber);

  void setDeweyId(byte[] deweyId, int offset);

  void setDeweyId(MemorySegment deweyId, int offset);

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
