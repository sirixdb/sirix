package org.sirix.api;

import org.sirix.api.xdm.XdmNodeReadTrx;

/**
 * Pre commit hook.
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionalInterface
public interface PreCommitHook {
  /**
   * Pre commit hook. Called before a revision is commited.
   *
   * @param rtx Sirix {@link XdmNodeReadTrx}
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadTrx rtx);
}
