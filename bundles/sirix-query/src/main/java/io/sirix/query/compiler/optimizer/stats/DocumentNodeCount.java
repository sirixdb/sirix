package io.sirix.query.compiler.optimizer.stats;

import io.sirix.api.NodeReadOnlyTrx;

/**
 * How many nodes a resource holds, estimated without depending on its hash setting.
 *
 * <p>
 * A container record only stores a descendant count when the resource maintains node hashes
 * ({@code NodeLayout.storeDescendantCount} is gated on {@code hashType != NONE}), and node hashes
 * are off by default since 1.0. So {@code getDescendantCount()} answers {@code 0} on a default
 * resource, and a planner heuristic reading it would conclude "tiny document" for every resource —
 * declining index routes, parallel axes and cost-based annotation on exactly the large resources
 * they exist for.
 *
 * <p>
 * The node-key frontier is maintained unconditionally and is an upper bound on the live node count
 * (it also counts keys of removed nodes). It provides a constant-work fallback for size heuristics
 * and sampling strides, avoiding a collapse of every estimate to the smallest case. Unlike the
 * stored count, it may overestimate after deletions or for a non-root cursor.
 *
 * <p>
 * Size-gating decisions use this constant-work estimate without traversing descendants.
 */
public final class DocumentNodeCount {

  private DocumentNodeCount() {
    throw new AssertionError("may not be instantiated");
  }

  /**
   * An estimate of the node count behind {@code rtx}: the stored descendant count where the resource
   * keeps one, otherwise the resource's node-key frontier.
   *
   * @param rtx positioned read-only transaction
   * @return a positive estimate, or {@code 0} for an empty resource
   */
  public static long estimate(final NodeReadOnlyTrx rtx) {
    final long descendants = rtx.getDescendantCount();
    return descendants > 0
        ? descendants
        : rtx.getMaxNodeKey();
  }
}
