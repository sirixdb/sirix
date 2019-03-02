package org.sirix.api;

import org.sirix.api.xml.XmlNodeReadOnlyTrx;

/**
 * Post commit hook.
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionalInterface
public interface PostCommitHook {

  /**
   * Post commit hook. Called after a revision has been commited.
   *
   * @param rtx Sirix {@link XmlNodeReadOnlyTrx}
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void postCommit(NodeReadOnlyTrx rtx);
}
