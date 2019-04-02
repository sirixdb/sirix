package org.sirix.api;

import org.sirix.api.xdm.XdmNodeReadOnlyTrx;

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
   * @param rtx Sirix {@link XdmNodeReadOnlyTrx}
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadOnlyTrx rtx);
}
