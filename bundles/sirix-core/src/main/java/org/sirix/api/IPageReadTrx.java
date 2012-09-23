package org.sirix.api;

import com.google.common.base.Optional;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

import org.sirix.cache.BerkeleyPersistencePageCache;
import org.sirix.cache.PageContainer;
import org.sirix.exception.SirixIOException;
import org.sirix.node.EKind;
import org.sirix.node.interfaces.INodeBase;
import org.sirix.page.EPage;
import org.sirix.page.RevisionRootPage;
import org.sirix.page.UberPage;
import org.sirix.page.interfaces.IPage;

/**
 * Interface for reading pages.
 * 
 * @author Sebastian Graf, University of Konstanz
 * @author Johannes Lichtenberger, University of Konstanz
 */
public interface IPageReadTrx extends AutoCloseable {

  /**
   * Get the session this transaction is bound to.
   * 
   * @return session instance
   */
  ISession getSession();
  
  /**
   * Get a node from persistent storage.
   * 
   * @param pKey
   *          the unique node-ID
   * @param pPage
   *          the page from which to fetch the node
   * @return an {@link Optional} reference usually containing the node reference
   * @throws SirixIOException
   *           if an I/O error occured
   */
  Optional<? extends INodeBase> getNode(@Nonnegative final long pKey,
    @Nonnull final EPage pPage) throws SirixIOException;

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
   * @param pNameKey
   *          name key for the term to search
   * @param pKind
   *          kind of node
   * @return the name
   * @throws NullPointerException
   *           if {@code pKind} is {@code null}
   */
  String getName(int pNameKey, @Nonnull EKind pKind);

  /**
   * Get the number of references for a name.
   * 
   * @param pNameKey
   *          name key for the term to search
   * @param pKind
   *          node type
   * @return the number of references for a given keyy.
   */
  int getNameCount(int pNameKey, @Nonnull EKind pKind);

  /**
   * Getting the raw name related to the name key and the node kind.
   * 
   * @param pNameKey
   *          name key for the term to search
   * @param pKind
   *          kind of node
   * @return a byte array containing the raw name
   * @throws NullPointerException
   *           if {@code pKind} is {@code null}
   */
  byte[] getRawName(int pNameKey, @Nonnull EKind pKind);

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
   * @param pKey
   *          {@code nodeKey} of node
   * @return {@code the node} or {@code null} if it's not available
   * @throws SirixIOException
   *           if can't read nodePage
   * @throws NullPointerException
   *           if {@code pPage} is {@code null}
   * @throws IllegalArgumentException
   *           if {@code pKey} is negative
   */
  PageContainer getNodeFromPage(@Nonnegative long pKey, @Nonnull EPage pPage)
    throws SirixIOException;

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
   * @param pKey
   *          key of persistent storage
   * @return page instance
   * @throws SirixIOException
   *           if an I/O error occurs
   */
  IPage getFromPageCache(@Nonnegative long pKey) throws SirixIOException;

  /**
   * Put content from page cache into persistent storage.
   * 
   * @param pPageLog
   *            persistent page log
   */
  void putPageCache(@Nonnull BerkeleyPersistencePageCache pPageLog);
}
