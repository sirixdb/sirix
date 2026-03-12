package io.sirix.query.compiler.optimizer.mesh;

import io.brackit.query.compiler.AST;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

/**
 * Groups semantically equivalent query execution plans (QEPs).
 *
 * <p>Each equivalence class stores alternative physical plans that produce
 * the same logical result but differ in execution strategy (e.g., index scan
 * vs sequential scan, different join orders). The class tracks the best
 * (lowest-cost) alternative for efficient plan selection.</p>
 *
 * <p>Based on Graefe/DeWitt Exodus optimizer and Weiner et al. Section 4.2.</p>
 */
public final class EquivalenceClass {

  private final int classId;
  private final ObjectArrayList<PlanAlternative> alternatives;
  private int bestIndex;
  private double bestCost;

  public EquivalenceClass(int classId) {
    this.classId = classId;
    this.alternatives = new ObjectArrayList<>(4);
    this.bestIndex = -1;
    this.bestCost = Double.MAX_VALUE;
  }

  /**
   * Add an alternative plan to this equivalence class.
   * Automatically updates the best plan if the new one is cheaper.
   *
   * @param plan the AST subtree
   * @param cost the estimated cost
   */
  public void addAlternative(AST plan, double cost) {
    if (plan == null) {
      throw new IllegalArgumentException("Plan must not be null");
    }
    if (Double.isNaN(cost) || cost < 0) {
      throw new IllegalArgumentException("Cost must be non-negative and finite: " + cost);
    }
    alternatives.add(new PlanAlternative(plan, cost));
    if (cost < bestCost) {
      bestCost = cost;
      bestIndex = alternatives.size() - 1;
    }
  }

  /**
   * Get the lowest-cost plan from this equivalence class.
   *
   * @return the best AST alternative, or null if empty
   */
  public AST getBestPlan() {
    return bestIndex >= 0 ? alternatives.get(bestIndex).plan() : null;
  }

  /**
   * Get the cost of the best alternative.
   *
   * @return the lowest cost, or Double.MAX_VALUE if empty
   */
  public double getBestCost() {
    return bestCost;
  }

  /**
   * @return number of alternatives in this class
   */
  public int size() {
    return alternatives.size();
  }

  /**
   * @return the equivalence class identifier
   */
  public int classId() {
    return classId;
  }

  /**
   * Get an alternative by index.
   *
   * @param index the alternative index
   * @return the plan alternative
   */
  public PlanAlternative getAlternative(int index) {
    return alternatives.get(index);
  }
}
