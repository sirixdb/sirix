package org.sirix.page.interfaces;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.index.IndexType;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;

/**
 * Key/Value page.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface KeyValuePage<V extends DataRecord> extends Page {

  /**
   * All available records.
   *
   * @return all records
   */
  <I extends Iterable<V>> I values();

  /**
   * Get the unique page record identifier.
   *
   * @return page record key/identifier
   */
  long getPageKey();

  /**
   * Get value with the specified key.
   *
   * @param key the key
   * @return value with given key, or {@code null} if not present
   */
  V getValue(long key);

  /**
   * Store or overwrite a single entry. The implementation must make sure if the key must be
   * permitted, the value or none.
   *
   * @param value value to store
   */
  void setRecord(@NonNull V value);

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
   * Get the {@link PageReadOnlyTrx}.
   *
   * @return page reading transaction
   */
  PageReadOnlyTrx getPageReadOnlyTrx();

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

//  /**
//   * Get the optional {@link PageReference}s pointing to the previous versions / page fragments of the page
//   *
//   * @return optional {@link PageReference} pointing to the previous versions / page fragments of the page
//   */
//  List<PageFragmentKey> getPreviousReferenceKeys();

  int getRevision();
}
