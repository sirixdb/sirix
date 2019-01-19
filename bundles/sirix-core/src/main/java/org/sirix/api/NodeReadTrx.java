package org.sirix.api;

import org.brackit.xquery.atomic.QNm;
import org.sirix.access.trx.node.CommitCredentials;
import org.sirix.access.trx.node.Move;
import org.sirix.api.xdm.XdmNodeReadTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.node.Kind;


public interface NodeReadTrx extends AutoCloseable {

  /**
   * Get ID of reader.
   *
   * @return ID of reader
   */
  long getId();

  /**
   * Get the revision number of this transaction.
   *
   * @return immutable revision number of this IReadTransaction
   */
  int getRevisionNumber();

  /**
   * UNIX-style timestamp of the commit of the revision.
   *
   * @throws SirixIOException if can't get timestamp
   */
  long getRevisionTimestamp();

  /**
   * Getting the maximum nodekey available in this revision.
   *
   * @return the maximum nodekey
   */
  long getMaxNodeKey();

  /**
   * Close shared read transaction and immediately release all resources.
   *
   * This is an idempotent operation and does nothing if the transaction is already closed.
   *
   * @throws SirixException if can't close {@link XdmNodeReadTrx}
   */
  @Override
  void close();

  /**
   * Get the node key of the currently selected node.
   *
   * @return node key of the currently selected node
   */
  long getNodeKey();


  /**
   * Get the {@link ResourceManager} this instance is bound to.
   *
   * @return session instance
   */
  ResourceManager<? extends NodeReadTrx, ? extends NodeWriteTrx> getResourceManager();

  /**
   * Get the commit credentials.
   *
   * @return The commit credentials.
   */
  CommitCredentials getCommitCredentials();

  Move<? extends NodeCursor> moveTo(long key);

  PageReadTrx getPageTrx();

  long getPathNodeKey();

  /**
   * Get key for given name. This is used for efficient name testing.
   *
   * @param name name, i.e., local part, URI, or prefix
   * @return internal key assigned to given name
   */
  int keyForName(String name);

  /**
   * Get name for key. This is used for efficient key testing.
   *
   * @param key key, i.e., local part key, URI key, or prefix key.
   * @return String containing name for given key
   */
  String nameForKey(int key);

  long getDescendantCount();

  long getChildCount();

  Kind getPathKind();

  boolean isDocumentRoot();

  boolean isClosed();

  QNm getName();

  boolean hasChildren();
}
