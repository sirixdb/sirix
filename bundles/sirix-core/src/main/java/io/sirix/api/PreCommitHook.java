package io.sirix.api;

import io.sirix.api.xml.XmlNodeReadOnlyTrx;

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
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadOnlyTrx rtx);
}
