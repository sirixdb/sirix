package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.cache.RecordPageContainer;
import org.sirix.cache.TransactionLogPageCache;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Record;
import org.sirix.page.PageKind;
import org.sirix.page.RecordPageImpl;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.Page;

import com.google.common.base.Optional;

/**
 * Interface for reading pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageReadTrx extends AutoCloseable {

	/**
	 * Get the session this transaction is bound to.
	 * 
	 * @return session instance
	 */
	Session getSession();

	/**
	 * Get a node from persistent storage.
	 * 
	 * @param key
	 *          the unique node-ID
	 * @param page
	 *          the page from which to fetch the node
	 * @return an {@link Optional} reference usually containing the node reference
	 * @throws SirixIOException
	 *           if an I/O error occured
	 */
	Optional<? extends Record> getNode(final @Nonnegative long key,
			final @Nonnull PageKind page) throws SirixIOException;

	/**
	 * Current reference to actual rev-root page.
	 * 
	 * @return the current revision root page
	 * 
	 * @throws SirixIOException
	 *           if something odd happens within the creation process.
	 */
	RevisionRootPage getActualRevisionRootPage() throws SirixIOException;

	/**
	 * Getting the name corresponding to the given key.
	 * 
	 * @param nameKey
	 *          name key for the term to search
	 * @param kind
	 *          kind of node
	 * @return the name
	 * @throws NullPointerException
	 *           if {@code pKind} is {@code null}
	 */
	String getName(int nameKey, @Nonnull Kind kind);

	/**
	 * Get the number of references for a name.
	 * 
	 * @param nameKey
	 *          name key for the term to search
	 * @param kind
	 *          node type
	 * @return the number of references for a given keyy.
	 */
	int getNameCount(int nameKey, @Nonnull Kind kind);

	/**
	 * Getting the raw name related to the name key and the node kind.
	 * 
	 * @param nameKey
	 *          name key for the term to search
	 * @param kind
	 *          kind of node
	 * @return a byte array containing the raw name
	 * @throws NullPointerException
	 *           if {@code kind} is {@code null}
	 */
	byte[] getRawName(int nameKey, @Nonnull Kind kind);

	/**
	 * Close transaction.
	 * 
	 * @throws SirixIOException
	 *           if something weird happened in the storage
	 */
	@Override
	void close() throws SirixIOException;

	/**
	 * Get a node from the page layer.
	 * 
	 * @param key
	 *          {@code nodeKey} of node
	 * @return {@code the node} or {@code null} if it's not available
	 * @throws SirixIOException
	 *           if can't read nodePage
	 * @throws NullPointerException
	 *           if {@code pPage} is {@code null}
	 * @throws IllegalArgumentException
	 *           if {@code pKey} is negative
	 */
	RecordPageContainer<Long, RecordPageImpl> getNodeFromPage(@Nonnegative long key,
			@Nonnull PageKind page) throws SirixIOException;

	/**
	 * Get the {@link UberPage}.
	 * 
	 * @return {@link UberPage} reference
	 */
	UberPage getUberPage();

	/** Determines if transaction is closed or not. */
	boolean isClosed();

	/**
	 * Get the revision number associated with the transaction.
	 * 
	 * @return the revision number
	 */
	int getRevisionNumber();

	/**
	 * Get page from cache.
	 * 
	 * @param key
	 *          key of persistent storage
	 * @return page instance
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	Page getFromPageCache(@Nonnegative long key) throws SirixIOException;

	/**
	 * Clear the caches.
	 */
	void clearCaches();

	/**
	 * Put content from page cache into persistent storage.
	 * 
	 * @param pageLog
	 *          persistent page log
	 */
	void putPageCache(@Nonnull TransactionLogPageCache pageLog);
}
