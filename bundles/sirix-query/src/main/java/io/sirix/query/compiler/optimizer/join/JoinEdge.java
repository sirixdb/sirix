package io.sirix.query.compiler.optimizer.join;

/**
 * An edge in the join graph connecting two base relations.
 *
 * @param rel1        index of the first relation (0-based)
 * @param rel2        index of the second relation (0-based)
 * @param selectivity estimated join selectivity in (0, 1]
 */
public record JoinEdge(int rel1, int rel2, double selectivity) {

  public JoinEdge {
    if (rel1 < 0 || rel2 < 0 || rel1 == rel2) {
      throw new IllegalArgumentException("Invalid relation indices: " + rel1 + ", " + rel2);
    }
    if (selectivity <= 0.0 || selectivity > 1.0) {
      throw new IllegalArgumentException("Selectivity must be in (0, 1]: " + selectivity);
    }
  }

  /**
   * Returns true if this edge connects the two disjoint relation sets.
   */
  public boolean connects(long set1, long set2) {
    return ((set1 >>> rel1 & 1) == 1 && (set2 >>> rel2 & 1) == 1)
        || ((set1 >>> rel2 & 1) == 1 && (set2 >>> rel1 & 1) == 1);
  }
}
