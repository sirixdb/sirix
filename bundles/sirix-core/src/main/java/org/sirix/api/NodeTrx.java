package org.sirix.api;

import org.sirix.access.User;
import org.sirix.api.xml.XmlNodeReadOnlyTrx;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;
import org.sirix.index.path.summary.PathSummaryReader;

import javax.annotation.Nonnegative;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Optional;

public interface NodeTrx extends NodeReadOnlyTrx, AutoCloseable {

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all.
   *
   * @return NodeTrx return current instance
   * @throws SirixException if this revision couldn't be commited
   */
  default NodeTrx commit() {
    return commit(null);
  }

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all. The author assignes a commit message.
   *
   * @param commitMessage message of the commit
   * @return NodeTrx return current instance
   * @throws SirixException if this revision couldn't be committed
   */
  default NodeTrx commit(@Nullable String commitMessage) {
    return commit(commitMessage, null);
  }

  NodeTrx commit(@Nullable String commitMessage, @Nullable Instant commitTimeStamp);

  /**
   * Rollback all modifications of the exclusive write transaction.
   *
   * @return NodeTrx return current instance
   * @throws SirixException if the changes in this revision couldn't be rollbacked
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

  /**
   * Get the page read-write transaction.
   *
   * @return the page read-write transaction used for reading pages from the index structure(s)
   */
  PageTrx getPageWtx();

  /**
   * Get the user who committed the revision you reverted to, if available.
   *
   * @return the user who committed the revision you reverted to, if available
   */
  Optional<User> getUserOfRevisionToRepresent();

  /**
   * Remove a node (with it's subtree if it has a subtree).
   *
   * @return this instance
   */
  NodeTrx remove();
}
