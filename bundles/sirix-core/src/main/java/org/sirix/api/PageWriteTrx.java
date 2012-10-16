package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.MultipleWriteTrx;
import org.sirix.access.Restore;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.node.Kind;
import org.sirix.node.interfaces.Node;
import org.sirix.node.interfaces.NodeBase;
import org.sirix.page.EPage;
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
	 * @param pNode
	 *          node to add
	 * @return unmodified node for convenience
	 * @throws SirixIOException
	 *           if an I/O error occurs
	 */
	NodeBase createNode(@Nonnull NodeBase pNode, @Nonnull EPage pPage)
			throws SirixIOException;

	/**
	 * Prepare a node for modification. This is getting the node from the
	 * (persistence) layer, storing the page in the cache and setting up the node
	 * for upcoming modification. Note that this only occurs for {@link Node}s.
	 * 
	 * @param pNodeKey
	 *          key of the node to be modified
	 * @return an {@link Node} instance
	 * @throws SirixIOException
	 *           if an I/O-error occurs
	 */
	NodeBase prepareNodeForModification(@Nonnegative long pNodeKey,
			@Nonnull EPage pPage) throws SirixIOException;

	/**
	 * Finishing the node modification. That is storing the node including the
	 * page in the cache.
	 * 
	 * @param pNodeKey
	 *          node key from node to be removed
	 * @param pPage
	 * 					denoting the kind of node page
	 */
	void finishNodeModification(@Nonnull long pNodeKey, @Nonnull EPage pPage);

	/**
	 * Removing a node from the storage.
	 * 
	 * @param pNodeKey
	 *          node key from node to be removed
	 * @param pPage
	 * 					denoting the kind of node page
	 * @throws SirixIOException
	 *           if the removal fails
	 */
	void removeNode(@Nonnull long pNodeKey, @Nonnull EPage pPage)
			throws SirixIOException;

	/**
	 * Creating a namekey for a given name.
	 * 
	 * @param pName
	 *          for which the key should be created
	 * @param pKind
	 *          kind of node
	 * @return an int, representing the namekey
	 * @throws SirixIOException
	 *           if something odd happens while storing the new key
	 */
	int createNameKey(@Nonnull String pName, @Nonnull Kind pKind)
			throws SirixIOException;

	/**
	 * Commit the transaction, that is persist changes if any and create a new
	 * revision.
	 */
	UberPage commit(@Nonnull MultipleWriteTrx pMultipleWriteTrx)
			throws SirixException;

	/**
	 * Update log.
	 * 
	 * @param pNodePageCont
	 *          {@link PageContainer} reference to synchronize
	 * @param pPage
	 *          type of page
	 */
	void updateDateContainer(@Nonnull PageContainer pNodePageCont,
			@Nonnull EPage pPage);

	/**
	 * Committing a {@link NodeWriteTrx}. This method is recursively invoked by
	 * all {@link PageReference}s.
	 * 
	 * @param reference
	 *          to be commited
	 * @throws SirixThreadedException
	 * @throws SirixException
	 *           if the write fails
	 */
	void commit(@Nonnull PageReference pReference) throws SirixException;

	/**
	 * Determines if this page write trx must restore a previous failed trx.
	 * 
	 * @param pRestore
	 *          determines if this page write trx must restore a previous failed
	 *          trx
	 */
	void restore(@Nonnull Restore pRestore);
}
