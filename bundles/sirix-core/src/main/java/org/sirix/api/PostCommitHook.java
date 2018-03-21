package org.sirix.api;

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
   * @param rtx Sirix {@link XdmNodeReadTrx}
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void postCommit(final XdmNodeReadTrx rtx);
}
