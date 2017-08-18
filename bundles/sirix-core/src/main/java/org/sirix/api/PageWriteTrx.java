package org.sirix.api;

import java.util.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.Restore;
import org.sirix.cache.IndirectPageLogKey;
import org.sirix.cache.RecordPageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;
import org.sirix.page.interfaces.KeyValuePage;
import org.sirix.page.interfaces.Page;

/**
 * Interface for writing pages to disk and to create in-memory records.
 *
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageWriteTrx<K extends Comparable<? super K>, V extends Record, S extends KeyValuePage<K, V>>
		extends PageReadTrx {

	/**
	 * Truncate resource to given revision.
	 *
	 * @param revision the given revision
	 * @return this page writer instance
	 */
	PageWriteTrx<K, V, S> truncateTo(int revision);

	/**
	 * Put a page into the cache.
	 *
	 * @param key the unique logKey in a subtree
	 * @param page the page to put into the cache
	 */
	void putPageIntoCache(IndirectPageLogKey key, @Nonnull Page page);

	/**
	 * Put a pageContainer into the key/value page cache.
	 *
	 * @param pageKind the kind of page
	 * @param key the unique pageKey in a subtree
	 * @param index the index number or {@code -1} for regular key/value pages
	 * @param pageContainer the pageContainer to put into the cache
	 */
	void putPageIntoKeyValueCache(PageKind pageKind, @Nonnegative long key, int index,
			@Nonnull RecordPageContainer<UnorderedKeyValuePage> pageContainer);

	/**
	 * Create fresh key/value (value must be a record) and prepare key/value-tuple for modifications
	 * (CoW). The record might be a node, in this case the key is the node.
	 *
	 * @param key optional key associated with the record to add (otherwise the record nodeKey is
	 *        used)
	 * @param value value to add (usually a node)
	 * @param pageKind kind of subtree the page belongs to
	 * @param index the index number
	 * @param keyValuePage optional keyValue page
	 * @return unmodified record for convenience
	 * @throws SirixIOException if an I/O error occurs
	 * @throws NullPointerException if {@code record} or {@code page} is {@code null}
	 */
	V createEntry(K key, @Nonnull V value, @Nonnull PageKind pageKind, int index,
			@Nonnull Optional<S> keyValuePage) throws SirixIOException;

	/**
	 * Prepare an entry for modification. This is getting the entry from the (persistence) layer,
	 * storing the page in the cache and setting up the entry for upcoming modification. The key of
	 * the entry might be the node key and the value the node itself.
	 *
	 * @param key key of the entry to be modified
	 * @param pageKind the kind of subtree (for instance regular data pages or the kind of index
	 *        pages)
	 * @param index the index number
	 * @param keyValuePage optional key/value page
	 * @return instance of the class implementing the {@link Record} instance
	 * @throws SirixIOException if an I/O-error occurs
	 * @throws IllegalArgumentException if {@code recordKey < 0}
	 * @throws NullPointerException if {@code page} is {@code null}
	 */
	V prepareEntryForModification(@Nonnegative K key, @Nonnull PageKind pageKind, int index,
			@Nonnull Optional<S> keyValuePage);

	/**
	 * Remove an entry from the storage.
	 *
	 * @param key entry key from entry to be removed
	 * @param pageKind denoting the kind of page (that is the subtree root kind)
	 * @param index the index number
	 * @throws SirixIOException if the removal fails
	 * @throws IllegalArgumentException if {@code recordKey < 0}
	 * @throws NullPointerException if {@code pageKind} or {@code keyValuePage} is {@code null}
	 */
	void removeEntry(K key, @Nonnull PageKind pageKind, int index, @Nonnull Optional<S> keyValuePage);

	/**
	 * Creating a namekey for a given name.
	 *
	 * @param name for which the key should be created
	 * @param kind kind of node
	 * @return an int, representing the namekey
	 * @throws SirixIOException if something odd happens while storing the new key
	 * @throws NullPointerException if {@code name} or {@code kind} is {@code null}
	 */
	int createNameKey(String name, @Nonnull Kind kind);

	/**
	 * Commit the transaction, that is persist changes if any and create a new revision.
	 *
	 * @throws SirixException if Sirix fails to commit
	 * @throws NullPointerException if {@code multipleWriteTrx} is {@code null}
	 */
	UberPage commit();

	/**
	 * Committing a {@link XdmNodeWriteTrx}. This method is recursively invoked by all
	 * {@link PageReference}s.
	 *
	 * @param reference to be commited
	 * @throws SirixException if the write fails
	 * @throws NullPointerException if {@code reference} is {@code null}
	 */
	void commit(PageReference reference);

	/**
	 * Determines if this page write trx must restore a previous failed trx.
	 *
	 * @param restore determines if this page write trx must restore a previous failed trx
	 * @throws NullPointerException if {@code restore} is {@code null}
	 */
	void restore(Restore restore);

	/**
	 * Get inlying {@link PageReadTrx}.
	 *
	 * @return the {@link PageReadTrx} reference
	 */
	PageReadTrx getPageReadTrx();
}
