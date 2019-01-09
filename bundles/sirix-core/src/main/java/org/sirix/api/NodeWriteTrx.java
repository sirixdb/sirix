package org.sirix.api;

import javax.annotation.Nonnegative;
import org.sirix.exception.SirixException;
import org.sirix.exception.SirixIOException;

public interface NodeWriteTrx extends AutoCloseable {

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all.
   *
   * @throws SirixException if this revision couldn't be commited
   */
  NodeWriteTrx commit();

  /**
   * Commit all modifications of the exclusive write transaction. Even commit if there are no
   * modification at all. The author assignes a commit message.
   *
   * @param commitMessage message of the commit
   * @throws SirixException if this revision couldn't be commited
   */
  NodeWriteTrx commit(String commitMessage);

  /**
   * Rollback all modifications of the exclusive write transaction.
   *
   * @throws SirixException if the changes in this revision couldn't be rollbacked
   */
  NodeWriteTrx rollback();

  /**
   * Reverting all changes to the revision defined. This command has to be finalized with a commit. A
   * revert is always bound to a {@link XdmNodeReadTrx#moveToDocumentRoot()}.
   *
   * @param revision revert to the revision
   */
  NodeWriteTrx revertTo(@Nonnegative int revision);

  /**
   * Add pre commit hook.
   *
   * @param hook pre commit hook
   */
  NodeWriteTrx addPreCommitHook(PreCommitHook hook);

  /**
   * Add a post commit hook.
   *
   * @param hook post commit hook
   */
  NodeWriteTrx addPostCommitHook(PostCommitHook hook);

  /**
   * Truncate to a revision.
   *
   * @param revision the revision to truncate to
   * @return
   */
  NodeWriteTrx truncateTo(int revision);

  /**
   * Closing current WriteTransaction.
   *
   * @throws SirixIOException if write transaction couldn't be closed
   */
  @Override
  void close();
}
