package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.MultipleWriteTrx;
import org.sirix.access.Restore;
import org.sirix.cache.RecordPageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;
import org.sirix.page.UnorderedKeyValuePage;

/**
 * Interface for writing pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageWriteTrx<K extends Comparable<? super K>, V extends Record>
		extends PageReadTrx {

	/**
	 * Create fresh key/value (value must be a record) and prepare key/value-tuple
	 * for modifications (CoW). The record might be a node, in this case the key
	 * is the node.
	 * 
	 * @param key
	 *          optional key associated with the record to add (otherwise the
	 *          record nodeKey is used)
	 * @param value
	 *          value to add (usually a node)
	 * @return unmodified record for convenience
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 * @throws NullPointerException
	 *           if {@code record} or {@code page} is {@code null}
	 */
	V createEntry(@Nonnull K key, @Nonnull V value, @Nonnull PageKind page)
			throws SirixIOException;

	/**
	 * Prepare an entry for modification. This is getting the entry from the
	 * (persistence) layer, storing the page in the cache and setting up the entry
	 * for upcoming modification. The key of the entry might be the node key and
	 * the value the node itself.
	 * 
	 * @param key
	 *          key of the entry to be modified
	 * @return instance of the class implementing the {@link Record} instance
	 * @throws SirixIOException
	 *           if an I/O-error occurs
	 * @throws IllegalArgumentException
	 *           if {@code recordKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	V prepareEntryForModification(@Nonnegative K key, @Nonnull PageKind page)
			throws SirixIOException;

	/**
	 * Finishing the entry modification. That is storing the entry including the
	 * page in the cache.
	 * 
	 * @param key
	 *          key from entry which is modified
	 * @param pageKind
	 *          denoting the kind of page (that is the subtree root kind)
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	void finishEntryModification(@Nonnull K key, @Nonnull PageKind pageKind);

	/**
	 * Remove an entry from the storage.
	 * 
	 * @param key
	 *          entry key from entry to be removed
	 * @param pageKind
	 *          denoting the kind of page (that is the subtree root kind)
	 * @throws SirixIOException
	 *           if the removal fails
	 * @throws IllegalArgumentException
	 *           if {@code recordKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	void removeEntry(@Nonnull K key, @Nonnull PageKind pageKind)
			throws SirixIOException;

	/**
	 * Creating a namekey for a given name.
	 * 
	 * @param name
	 *          for which the key should be created
	 * @param kind
	 *          kind of node
	 * @return an int, representing the namekey
	 * @throws SirixIOException
	 *           if something odd happens while storing the new key
	 * @throws NullPointerException
	 *           if {@code name} or {@code kind} is {@code null}
	 */
	int createNameKey(@Nonnull String name, @Nonnull Kind kind)
			throws SirixIOException;

	/**
	 * Commit the transaction, that is persist changes if any and create a new
	 * revision.
	 * 
	 * @throws SirixException
	 *           if Sirix fails to commit
	 * @throws NullPointerException
	 *           if {@code multipleWriteTrx} is {@code null}
	 */
	UberPage commit(@Nonnull MultipleWriteTrx multipleWriteTrx)
			throws SirixException;

	/**
	 * Update log.
	 * 
	 * @param nodePageCont
	 *          {@link RecordPageContainer} reference to synchronize
	 * @param page
	 *          type of page
	 * @throws NullPointerException
	 *           if {@code nodePageCont} or {@code page} is {@code null}
	 */
	void updateDataContainer(
			@Nonnull RecordPageContainer<UnorderedKeyValuePage> nodePageCont,
			@Nonnull PageKind page);

	/**
	 * Committing a {@link NodeWriteTrx}. This method is recursively invoked by
	 * all {@link PageReference}s.
	 * 
	 * @param reference
	 *          to be commited
	 * @throws SirixException
	 *           if the write fails
	 * @throws NullPointerException
	 *           if {@code reference} is {@code null}
	 */
	void commit(@Nonnull PageReference reference) throws SirixException;

	/**
	 * Determines if this page write trx must restore a previous failed trx.
	 * 
	 * @param restore
	 *          determines if this page write trx must restore a previous failed
	 *          trx
	 * @throws NullPointerException
	 *           if {@code restore} is {@code null}
	 */
	void restore(@Nonnull Restore restore);

	/**
	 * Get {@link PageReadTrx}.
	 * 
	 * @return the {@link PageReadTrx} reference
	 */
	PageReadTrx getPageReadTrx();
}
