package org.sirix.api;

import org.sirix.access.User;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathSummaryReader;
import org.sirix.node.interfaces.DataRecord;
import org.sirix.page.UnorderedKeyValuePage;

import javax.annotation.Nonnegative;
import java.util.Optional;

public interface NodeTrx extends NodeReadOnlyTrx, AutoCloseable {

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all.
   *
   * @throws SirixException if this revision couldn't be commited
   * @return NodeTrx return current instance
   */
  NodeTrx commit();

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all. The author assignes a commit message.
   *
   * @param commitMessage message of the commit
   * @throws SirixException if this revision couldn't be committed
   * @return NodeTrx return current instance
   */
  NodeTrx commit(String commitMessage);

  /**
   * Rollback all modifications of the exclusive write transaction.
   *
   * @throws SirixException if the changes in this revision couldn't be rollbacked
   * @return NodeTrx return current instance
   */
  NodeTrx rollback();

  /**
   * Reverting all changes to the revision defined. This command has to be finalized with a commit. A
   * revert is always bound to a {@link XmlNodeReadOnlyTrx#moveToDocumentRoot()}.
   *
   * @param revision revert to the revision
   * @return NodeTrx return current instance
   */
  NodeTrx revertTo(@Nonnegative int revision);

  /**
   * Add pre commit hook.
   *
   * @param hook pre commit hook
   * @return NodeTrx return current instance
   */
  NodeTrx addPreCommitHook(PreCommitHook hook);

  /**
   * Add a post commit hook.
   *
   * @param hook post commit hook
   * @return NodeTrx return current instance
   */
  NodeTrx addPostCommitHook(PostCommitHook hook);

  /**
   * Truncate to a revision.
   *
   * @param revision the revision to truncate to
   * @return NodeTrx return current instance
   */
  NodeTrx truncateTo(int revision);

  /**
   * Get the {@link PathSummaryReader} associated with the current write transaction -- might be
   * {@code null} if no path summary index is used.
   *
   * @return {@link PathSummaryReader} instance
   */
  PathSummaryReader getPathSummary();

  /**
   * Closing current WriteTransaction.
   *
   * @throws SirixIOException if write transaction couldn't be closed
   */
  @Override
  void close();

  PageTrx<Long, DataRecord, UnorderedKeyValuePage> getPageWtx();

  /**
   * Get the user who committed the revision you reverted to, if available.
   *
   * @return the user who committed the revision you reverted to, if available
   */
  Optional<User> getUserOfRevisionToRepresent();

  NodeTrx remove();
}
