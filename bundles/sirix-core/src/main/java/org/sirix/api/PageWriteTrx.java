package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.MultipleWriteTrx;
import org.sirix.access.Restore;
import org.sirix.cache.NodePageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.PageKind;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Interface for reading pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface PageWriteTrx extends PageReadTrx {

	/**
	 * Get {@link UberPage}.
	 * 
	 * @return the {@link UberPage} reference
	 */
	UberPage getUberPage();

	/**
	 * Create fresh node and prepare node nodePageReference for modifications
	 * (COW).
	 * 
	 * @param node
	 *          node to add
	 * @return unmodified node for convenience
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 * @throws NullPointerException
	 *           if {@code node} or {@code page} is {@code null}
	 */
	NodeBase createNode(@Nonnull NodeBase node, @Nonnull PageKind page)
			throws SirixIOException;

	/**
	 * Prepare a node for modification. This is getting the node from the
	 * (persistence) layer, storing the page in the cache and setting up the node
	 * for upcoming modification. Note that this only occurs for {@link Node}s.
	 * 
	 * @param nodeKey
	 *          key of the node to be modified
	 * @return an {@link Node} instance
	 * @throws SirixIOException
	 *           if an I/O-error occurs
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	NodeBase prepareNodeForModification(@Nonnegative long nodeKey,
			@Nonnull PageKind page) throws SirixIOException;

	/**
	 * Finishing the node modification. That is storing the node including the
	 * page in the cache.
	 * 
	 * @param nodeKey
	 *          node key from node to be removed
	 * @param page
	 *          denoting the kind of node page
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	void finishNodeModification(@Nonnull long nodeKey, @Nonnull PageKind page);

	/**
	 * Removing a node from the storage.
	 * 
	 * @param nodeKey
	 *          node key from node to be removed
	 * @param page
	 *          denoting the kind of node page
	 * @throws SirixIOException
	 *           if the removal fails
	 * @throws IllegalArgumentException
	 *           if {@code nodeKey < 0}
	 * @throws NullPointerException
	 *           if {@code page} is {@code null}
	 */
	void removeNode(@Nonnull long nodeKey, @Nonnull PageKind page)
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
	 * 					 if {@code multipleWriteTrx} is {@code null}
	 */
	UberPage commit(@Nonnull MultipleWriteTrx multipleWriteTrx)
			throws SirixException;

	/**
	 * Update log.
	 * 
	 * @param nodePageCont
	 *          {@link NodePageContainer} reference to synchronize
	 * @param page
	 *          type of page
	 * @throws NullPointerException
	 * 					if {@code nodePageCont} or {@code page} is {@code null}
	 */
	void updateDateContainer(@Nonnull NodePageContainer nodePageCont,
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
	 * 					 if {@code reference} is {@code null}
	 */
	void commit(@Nonnull PageReference reference) throws SirixException;

	/**
	 * Determines if this page write trx must restore a previous failed trx.
	 * 
	 * @param restore
	 *          determines if this page write trx must restore a previous failed
	 *          trx
	 * @throws NullPointerException
	 * 				  if {@code restore} is {@code null}
	 */
	void restore(@Nonnull Restore restore);
}
