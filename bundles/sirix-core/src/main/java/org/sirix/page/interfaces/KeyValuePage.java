package org.sirix.page.interfaces;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;

/**
 * Key/Value page.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface KeyValuePage<K extends Comparable<? super K>, V extends Record>
		extends Page {
	/**
	 * Entry set of all nodes in the page. Changes to the set are reflected in the
	 * internal data structure
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
	 * @param key
	 *          the key
	 * @return value with given key, or {@code null} if not present
	 */
	V getValue(@Nonnull K key);

	/**
	 * Set the payload of the key/value page.
	 * 
	 * @param key
	 *          the key
	 * @param value
	 *          the value in bytes
	 */
	void setSlot(@Nonnull K key, @Nonnull byte[] value);

	/**
	 * Get the slot value, or {@code null} if not present.
	 * 
	 * @param key
	 *          the key
	 * @return byte array with the data
	 */
	byte[] getSlotValue(@Nonnull K key);

	Set<Entry<K, byte[]>> slotEntrySet();

	/**
	 * Store or overwrite a single entry. The implementation must make sure if the
	 * key must be permitted, the value or none.
	 * 
	 * @param key
	 *          key to store
	 * @param value
	 *          value to store
	 */
	void setEntry(@Nonnull K key, @Nonnull V value);
	
	Set<Entry<K, PageReference>> referenceEntrySet();

	/**
	 * Store or overwrite a single reference associated with a key for overlong entries.
	 * That is entries which are larger than a predefined threshold are written to OverflowPages
	 * and thus are just referenced and not deserialized during the deserialization of a page.
	 * 
	 * @param key
	 *          key to store
	 * @param reference
	 *          reference to store
	 */
	void setPageReference(@Nonnull K key, @Nonnull PageReference reference);
	
	PageReference getPageReference(@Nonnull K key);

	/**
	 * Create a new instance.
	 * 
	 * @param recordPageKey
	 *          the record page key
	 * @param pageKind
	 *          the kind of page (in which subtree it is (NODEPAGE,
	 *          PATHSUMMARYPAGE, TEXTVALUEPAGE, ATTRIBUTEVALUEPAGE))
	 * @param pageReadTrx
	 *          transaction to read pages
	 * @return a new {@link KeyValuePage} instance
	 */
	<C extends KeyValuePage<K, V>> C newInstance(@Nonnegative long recordPageKey,
			@Nonnull PageKind pageKind, @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Get the {@link PageReadTrx}.
	 * 
	 * @return page reading transaction
	 */
	PageReadTrx getPageReadTrx();

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
}
