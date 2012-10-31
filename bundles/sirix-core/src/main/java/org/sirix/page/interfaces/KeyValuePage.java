package org.sirix.page.interfaces;

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.api.PageReadTrx;
import org.sirix.node.interfaces.Record;

/**
 * Record page.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface KeyValuePage<K, V extends Record> extends Page {
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
	long getRecordPageKey();

	/**
	 * Get record with the specified key.
	 * 
	 * @param key
	 *          record key
	 * @return record with given key, or {@code null} if not present
	 * @throws IllegalArgumentException
	 *           if {@code key < 0}
	 */
	Record getRecord(@Nonnegative K key);

	/**
	 * Store or overwrite a single record.
	 * 
	 * @param record
	 *          record to store
	 */
	void setRecord(@Nonnull V record);

	/**
	 * Create a new instance.
	 * 
	 * @param recordPageKey
	 *          the record page key
	 * @param revision
	 *          the revision
	 * @param pageReadTrx
	 *          transaction to read pages
	 * @return a new {@link KeyValuePage} instance
	 */
	<C extends KeyValuePage<K, V>> C newInstance(long recordPageKey,
			@Nonnegative int revision, @Nonnull PageReadTrx pageReadTrx);

	/**
	 * Get the {@link PageReadTrx}.
	 * 
	 * @return page reading transaction
	 */
	PageReadTrx getPageReadTrx();
}
