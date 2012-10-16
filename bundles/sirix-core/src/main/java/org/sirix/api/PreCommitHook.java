package org.sirix.api;

/**
 * Pre commit hook.
 * 
 * @author Johannes Lichtenberger
 * 
 */
public interface PreCommitHook {
	/**
	 * Pre commit hook. Called before a revision is commited.
	 * 
	 * @param rtx
	 * 				Sirix {@link NodeReadTrx}
	 * @throws NullPointerException
	 * 				if {@code rtx} is {@code null}
	 */
  void preCommit(final NodeReadTrx rtx);
}
