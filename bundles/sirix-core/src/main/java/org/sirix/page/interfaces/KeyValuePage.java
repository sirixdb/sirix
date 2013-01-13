package org.sirix.page.interfaces;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;

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
	V getValue(K key);

	/**
	 * Store or overwrite a single entry. The implementation must make sure if the
	 * key must be permitted, the value or none.
	 * 
	 * @param key
	 *          key to store
	 * @param value
	 *          value to store
	 */
	void setEntry(K key, V value);

	/**
	 * Create a new instance.
	 * 
	 * @param recordPageKey
	 *          the record page key
	 * @param pageKind
	 *          the kind of page (in which subtree it is (NODEPAGE,
	 *          PATHSUMMARYPAGE, TEXTVALUEPAGE, ATTRIBUTEVALUEPAGE))
	 * @param revision
	 *          the revision
	 * @param pageReadTrx
	 *          transaction to read pages
	 * @return a new {@link KeyValuePage} instance
	 */
	<C extends KeyValuePage<K, V>> C newInstance(@Nonnegative long recordPageKey,
			@Nonnull PageKind pageKind, @Nonnegative int revision,
			@Nonnull PageReadTrx pageReadTrx);

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
	 * Set the revision the page belongs to if the page is updated.
	 * 
	 * @return the page instance
	 */
	KeyValuePage<K, V> setRevision(@Nonnegative int revision);
}
