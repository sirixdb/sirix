package org.sirix.api;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.access.EMultipleWriteTrx;
import org.sirix.access.ERestore;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.exception.SirixThreadedException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INode;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.PageReference;
import org.sirix.page.UberPage;

/**
 * Interface for reading pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface IPageWriteTrx extends IPageReadTrx {

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
	INodeBase createNode(@Nonnull INodeBase pNode, @Nonnull EPage pPage)
			throws SirixIOException;

	/**
	 * Prepare a node for modification. This is getting the node from the
	 * (persistence) layer, storing the page in the cache and setting up the node
	 * for upcoming modification. Note that this only occurs for {@link INode}s.
	 * 
	 * @param pNodeKey
	 *          key of the node to be modified
	 * @return an {@link INode} instance
	 * @throws SirixIOException
	 *           if an I/O-error occurs
	 */
	INodeBase prepareNodeForModification(@Nonnegative long pNodeKey,
			@Nonnull EPage pPage) throws SirixIOException;

	/**
	 * Finishing the node modification. That is storing the node including the
	 * page in the cache.
	 * 
	 * @param pNode
	 *          the node to be modified
	 */
	void finishNodeModification(@Nonnull INodeBase pNode, @Nonnull EPage pPage);

	/**
	 * Removing a node from the storage.
	 * 
	 * @param pNode
	 *          node to be removed
	 * @throws SirixIOException
	 *           if the removal fails
	 */
	void removeNode(@Nonnull INode pNode, @Nonnull EPage pPage)
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
	int createNameKey(@Nonnull String pName, @Nonnull EKind pKind)
			throws SirixIOException;

	/**
	 * Commit the transaction, that is persist changes if any and create a new
	 * revision.
	 */
	UberPage commit(@Nonnull EMultipleWriteTrx pMultipleWriteTrx)
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
	 * Committing a {@link INodeWriteTrx}. This method is recursively invoked by
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
	void restore(@Nonnull ERestore pRestore);

	/**
	 * Determines if a new log directory has been created.
	 * 
	 * @return {@code true} if yes, otherwise {@code false} which means that a
	 *         crash occured and the log must be re-applied
	 */
	boolean isCreated();

	/** Flush content of page cache to persistent storage for write-ahead log. */
	void flushToPersistentCache();
}
