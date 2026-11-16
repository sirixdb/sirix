package io.sirix.api;

/**
 * Pre commit hook.
 *
 * @author Johannes Lichtenberger
 *
 */
@FunctionalInterface
public interface PreCommitHook {
  /**
   * Called before publication; an exception vetoes the commit. The callback lifecycle restrictions in
   * {@link PostCommitHook#postCommit(NodeReadOnlyTrx)} also apply here and in revision listeners.
   * Ordinary read-only work remains allowed.
   *
   * @param rtx borrowed Sirix transaction view
   * @throws NullPointerException if {@code rtx} is {@code null}
   */
  void preCommit(NodeReadOnlyTrx rtx);
}
