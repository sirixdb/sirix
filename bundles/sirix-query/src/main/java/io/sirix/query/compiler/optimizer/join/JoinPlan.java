package io.sirix.query.compiler.optimizer.join;

/**
 * Represents a node in a join plan tree — either a base relation (leaf)
 * or a binary join of two sub-plans.
 *
 * <p>Immutable after construction. The {@code relationSet} bitmask
 * identifies which base relations are covered by this plan.</p>
 */
public final class JoinPlan {

  private final long relationSet;
  private final double cost;
  private final long cardinality;
  private final JoinPlan left;
  private final JoinPlan right;
  private final int baseRelation;

  private JoinPlan(long relationSet, double cost, long cardinality,
                   JoinPlan left, JoinPlan right, int baseRelation) {
    this.relationSet = relationSet;
    this.cost = cost;
    this.cardinality = cardinality;
    this.left = left;
    this.right = right;
    this.baseRelation = baseRelation;
  }

  /**
   * Create a plan for a single base relation (leaf node).
   *
   * @param index       relation index (0-based)
   * @param cardinality estimated tuple count
   * @param cost        estimated access cost
   */
  public static JoinPlan baseRelation(int index, long cardinality, double cost) {
    return new JoinPlan(1L << index, cost, cardinality, null, null, index);
  }

  /**
   * Create a join plan combining two sub-plans.
   *
   * @param left        left sub-plan
   * @param right       right sub-plan
   * @param joinCost    incremental cost of performing this join
   * @param cardinality estimated output cardinality of the join
   */
  public static JoinPlan join(JoinPlan left, JoinPlan right,
                              double joinCost, long cardinality) {
    final long set = left.relationSet | right.relationSet;
    final double totalCost = left.cost + right.cost + joinCost;
    return new JoinPlan(set, totalCost, cardinality, left, right, -1);
  }

  public long relationSet() {
    return relationSet;
  }

  public double cost() {
    return cost;
  }

  public long cardinality() {
    return cardinality;
  }

  public JoinPlan left() {
    return left;
  }

  public JoinPlan right() {
    return right;
  }

  public int baseRelation() {
    return baseRelation;
  }

  public boolean isBaseRelation() {
    return left == null;
  }

  @Override
  public String toString() {
    if (isBaseRelation()) {
      return "R" + baseRelation + "[" + cardinality + "]";
    }
    return "(" + left + " ⋈ " + right + ")[cost=" + String.format("%.1f", cost)
        + ",card=" + cardinality + "]";
  }
}
