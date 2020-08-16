package org.sirix.page.interfaces;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.sirix.api.PageReadOnlyTrx;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;

/**
 * Key/Value page.
 *
 * @author Johannes Lichtenberger
 *
 */
public interface KeyValuePage<K extends Comparable<? super K>, V extends DataRecord> extends Page {
  /**
   * Entry set of all nodes in the page. Changes to the set are reflected in the internal data
   * structure
   *
   * @return an entry set
   */
  Set<Entry<K, V>> entrySet();

  /**
   * All available records.
   *
   * @return all records
   */
  Collection<V> values();

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
  V getValue(K key);

  /**
   * Store or overwrite a single entry. The implementation must make sure if the key must be
   * permitted, the value or none.
   *
   * @param key key to store
   * @param value value to store
   */
  void setRecord(K key, @Nonnull V value);

  Set<Entry<K, PageReference>> referenceEntrySet();

  /**
   * Store or overwrite a single reference associated with a key for overlong entries. That is
   * entries which are larger than a predefined threshold are written to OverflowPages and thus are
   * just referenced and not deserialized during the deserialization of a page.
   *
   * @param key key to store
   * @param reference reference to store
   */
  void setPageReference(K key, @Nonnull PageReference reference);

  PageReference getPageReference(K key);

  /**
   * Create a new instance.
   *
   * @param recordPageKey the record page key
   * @param pageKind the kind of page (in which subtree it is (NODEPAGE, PATHSUMMARYPAGE,
   *        TEXTVALUEPAGE, ATTRIBUTEVALUEPAGE))
   * @param pageReadTrx transaction to read pages
   * @return a new {@link KeyValuePage} instance
   */
  <C extends KeyValuePage<K, V>> C newInstance(@Nonnegative long recordPageKey,
      @Nonnull PageKind pageKind, @Nonnull PageReadOnlyTrx pageReadTrx);

  /**
   * Get the {@link PageReadOnlyTrx}.
   *
   * @return page reading transaction
   */
  PageReadOnlyTrx getPageReadOnlyTrx();

  /**
   * Get the page kind.
   *
   * @return page kind
   */
  PageKind getPageKind();

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
