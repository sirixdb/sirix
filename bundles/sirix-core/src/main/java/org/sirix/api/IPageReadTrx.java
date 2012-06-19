package org.sirix.api;

import com.google.common.base.Optional;
import org.sirix.cache.PageContainer;
import org.sirix.exception.TTIOException;
import org.sirix.node.ENode;
import org.sirix.node.interfaces.INode;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;

/**
 * Interface for reading pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface IPageReadTrx extends AutoCloseable {

  Optional<INode> getNode(final long pKey) throws TTIOException;

  /**
   * Current reference to actual rev-root page.
   * 
   * @return the current revision root page
   * 
   * @throws TTIOException
   *           if something odd happens within the creation process.
   */
  RevisionRootPage getActualRevisionRootPage() throws TTIOException;

  /**
   * Getting the name corresponding to the given key.
   * 
   * @param pKey
   *          for the term searched
   * @param pKind
   *          kind of node
   * @return the name
   */
  String getName(int pKey, ENode pKind);

  /**
   * Getting the raw name related to the name key and the node kind.
   * 
   * @param pKey
   *          for the raw name searched
   * @param pKind
   *          kind of node
   * @return a byte array containing the raw name
   */
  byte[] getRawName(int pKey, ENode pKind);

  /**
   * Close transaction.
   * 
   * @throws TTIOException
   *           if something weird happened in the storage
   */
  @Override
  void close() throws TTIOException;

  /**
   * Get a node from the page layer.
   * 
   * @param key
   *          {@code nodeKey} of node
   * @return {@code the node} or {@code null} if it's not available
   * @throws TTIOException
   *           if can't read nodePage
   */
  PageContainer getNodeFromPage(long key) throws TTIOException;

  /**
   * Get the {@link UberPage}.
   * 
   * @return {@link UberPage} reference
   */
  UberPage getUberPage();
}
