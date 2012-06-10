package org.treetank.api;

import org.treetank.cache.NodePageContainer;
import org.treetank.exception.AbsTTException;
import org.treetank.exception.TTIOException;
import org.treetank.exception.TTThreadedException;
import org.treetank.node.ENode;
import org.treetank.node.interfaces.INode;
import org.treetank.page.PageReference;
import org.treetank.page.UberPage;

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
   * @throws TTIOException
   *           if an I/O error occurs
   */
  <T extends INode> T createNode(T pNode) throws TTIOException;

  /**
   * Prepare a node for modification. This is getting the node from the
   * (persistence) layer, storing the page in the cache and setting up the
   * node for upcoming modification. Note that this only occurs for {@link INode}s.
   * 
   * @param pNodeKey
   *          key of the node to be modified
   * @return an {@link INode} instance
   * @throws TTIOException
   *           if an I/O-error occurs
   */
  INode prepareNodeForModification(long pNodeKey) throws TTIOException;

  /**
   * Finishing the node modification. That is storing the node including the
   * page in the cache.
   * 
   * @param pNode
   *          the node to be modified
   */
  <T extends INode> void finishNodeModification(T pNode);

  /**
   * Removing a node from the storage.
   * 
   * @param pNode
   *          node to be removed
   * @throws TTIOException
   *           if the removal fails
   */
  <T extends INode> void removeNode(T pNode) throws TTIOException;

  /**
   * Creating a namekey for a given name.
   * 
   * @param pName
   *          for which the key should be created
   * @param pKind
   *          kind of node
   * @return an int, representing the namekey
   * @throws TTIOException
   *           if something odd happens while storing the new key
   */
  int createNameKey(String pName, ENode pKind) throws TTIOException;

  /** Commit the transaction, that is persist changes if any and create a new revision. */
  UberPage commit() throws AbsTTException;

  /**
   * Update log.
   * 
   * @param pNodePageCont
   *          {@link NodePageContainer} reference to synchronize
   */
  void updateDateContainer(NodePageContainer pNodePageCont);

  /**
   * Committing a {@link INodeWriteTrx}. This method is recursively invoked by all {@link PageReference}s.
   * 
   * @param reference
   *          to be commited
   * @throws TTThreadedException
   * @throws AbsTTException
   *           if the write fails
   */
  void commit(PageReference pReference) throws AbsTTException;
}
