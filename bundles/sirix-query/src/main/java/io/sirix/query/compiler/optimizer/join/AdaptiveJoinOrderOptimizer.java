package io.sirix.query.compiler.optimizer.join;

import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Join order optimizer using the DPhyp algorithm from
 * Moerkotte &amp; Neumann, "Dynamic Programming Strikes Back" (2006),
 * with a GOO (Greedy Operator Ordering) fallback for large queries.
 *
 * <p>DPhyp efficiently enumerates <em>connected subgraph complement pairs</em>
 * (csg-cmp-pairs) to find the optimal join ordering. For n relations, it runs
 * in O(n · 3^n) vs O(4^n) for System-R style bottom-up DP.</p>
 *
 * <p>For queries with more than {@link #DPHYP_THRESHOLD} relations, the optimizer
 * falls back to GOO-DP — a greedy algorithm that picks the cheapest connected
 * pair of plans at each step, running in O(n^3) time. GOO does not guarantee
 * global optimality but produces good plans for any practical query size.</p>
 *
 * <h3>Algorithm outline (DPhyp)</h3>
 * <ol>
 *   <li>Initialize single-relation plans in the DP table</li>
 *   <li>For each relation (high to low), enumerate connected subgraphs containing it</li>
 *   <li>For each connected subgraph S1, find complement connected subgraphs S2</li>
 *   <li>For each (S1, S2) pair, compute join cost and keep the cheaper plan for S1∪S2</li>
 * </ol>
 *
 * <h3>Algorithm outline (GOO fallback)</h3>
 * <ol>
 *   <li>Create one plan per base relation</li>
 *   <li>Greedily pick the pair of plans with the lowest join cost that share an edge</li>
 *   <li>Merge them into a single plan and update connectivity</li>
 *   <li>Repeat until one plan remains</li>
 * </ol>
 */
public final class AdaptiveJoinOrderOptimizer {

  private static final Logger LOG = LoggerFactory.getLogger(AdaptiveJoinOrderOptimizer.class);

  /** DPhyp is used for up to this many relations; above this threshold GOO kicks in. */
  private static final int DPHYP_THRESHOLD = 20;

  private final JoinGraph graph;
  private final JsonCostModel costModel;
  private final DpTable dpTable;

  /**
   * @param graph     the join graph with base relation cardinalities and join edges
   * @param costModel cost model for estimating join costs
   */
  public AdaptiveJoinOrderOptimizer(JoinGraph graph, JsonCostModel costModel) {
    this.graph = graph;
    this.costModel = costModel;
    // Size DP table for expected number of entries (2^n in worst case for DPhyp,
    // but cap at 16 to avoid enormous allocations for large n in GOO path)
    final int n = graph.relationCount();
    this.dpTable = new DpTable(1 << Math.min(n, 16));
  }

  /**
   * Find the optimal join ordering for all relations in the graph.
   *
   * <p>Uses DPhyp for n &le; {@link #DPHYP_THRESHOLD}, GOO otherwise.</p>
   *
   * @return the optimal join plan, or null if the graph has no edges
   */
  public JoinPlan optimize() {
    final int n = graph.relationCount();
    if (n > DPHYP_THRESHOLD) {
      LOG.info("Join graph has {} relations (> {}), using GOO greedy fallback instead of DPhyp",
          n, DPHYP_THRESHOLD);
      return greedyOptimize();
    }
    return dphypOptimize();
  }

  /**
   * DPhyp enumeration — the optimal algorithm for small n.
   */
  private JoinPlan dphypOptimize() {
    final int n = graph.relationCount();

    // Phase 1: Initialize base relation plans
    for (int i = 0; i < n; i++) {
      final long cardinality = graph.baseCardinality(i);
      final double cost = graph.baseCost(i);
      dpTable.put(1L << i, JoinPlan.baseRelation(i, cardinality, cost));
    }

    // Phase 2: DPhyp enumeration — process relations from highest to lowest
    for (int i = n - 1; i >= 0; i--) {
      final long vi = 1L << i;
      // Emit pairs for the singleton {v_i}
      emitCsg(vi);
      // Extend {v_i} to larger connected subgraphs
      final long forbidden = vi | ((1L << i) - 1); // {v_i} ∪ {v_0,...,v_{i-1}}
      enumerateCsgRec(vi, forbidden);
    }

    return dpTable.get(graph.fullSet());
  }

  /**
   * GOO (Greedy Operator Ordering) — O(n^3) fallback for large join graphs.
   *
   * <p>Starts with one plan per base relation. At each step, finds the pair
   * of existing plans that are connected by an edge and whose join has the
   * lowest cost, merges them, and repeats until a single plan covers all
   * relations.</p>
   *
   * <p>The algorithm does not guarantee global optimality but produces good
   * plans for any reasonable query size.</p>
   *
   * @return the greedy join plan, or null if the graph is disconnected
   */
  private JoinPlan greedyOptimize() {
    final int n = graph.relationCount();

    // Active plans: plans[i] is null if merged into another plan.
    // relationSets[i] tracks the bitmask of relations covered by plans[i].
    final JoinPlan[] plans = new JoinPlan[n];
    final long[] relationSets = new long[n];
    int activePlanCount = n;

    // Initialize one plan per base relation
    for (int i = 0; i < n; i++) {
      final long cardinality = graph.baseCardinality(i);
      final double cost = graph.baseCost(i);
      plans[i] = JoinPlan.baseRelation(i, cardinality, cost);
      relationSets[i] = 1L << i;
    }

    // Merge plans greedily until only one remains
    while (activePlanCount > 1) {
      int bestI = -1;
      int bestJ = -1;
      double bestCost = Double.MAX_VALUE;

      // Find the cheapest pair of connected plans
      for (int i = 0; i < n; i++) {
        if (plans[i] == null) {
          continue;
        }
        for (int j = i + 1; j < n; j++) {
          if (plans[j] == null) {
            continue;
          }
          // Check if these two plans share a join edge
          if (!graph.hasEdgeBetween(relationSets[i], relationSets[j])) {
            continue;
          }

          final double joinCost = costModel.estimateHashJoinCost(
              plans[i].cardinality(), plans[j].cardinality());
          final double totalCost = plans[i].cost() + plans[j].cost() + joinCost;

          if (totalCost < bestCost) {
            bestCost = totalCost;
            bestI = i;
            bestJ = j;
          }
        }
      }

      if (bestI < 0) {
        // No connected pair found — graph is disconnected
        LOG.warn("GOO fallback: join graph is disconnected, cannot produce a full plan");
        return null;
      }

      // Merge bestI and bestJ into bestI
      final double selectivity = graph.joinSelectivity(relationSets[bestI], relationSets[bestJ]);
      final long joinCard = costModel.estimateJoinCardinality(
          plans[bestI].cardinality(), plans[bestJ].cardinality(), selectivity);
      final double joinCost = costModel.estimateHashJoinCost(
          plans[bestI].cardinality(), plans[bestJ].cardinality());

      plans[bestI] = JoinPlan.join(plans[bestI], plans[bestJ], joinCost, joinCard);
      relationSets[bestI] = relationSets[bestI] | relationSets[bestJ];

      // Deactivate the merged plan
      plans[bestJ] = null;
      relationSets[bestJ] = 0L;
      activePlanCount--;
    }

    // Find the surviving plan
    for (int i = 0; i < n; i++) {
      if (plans[i] != null) {
        // Also put into DP table so dpTable() accessor works consistently
        dpTable.put(relationSets[i], plans[i]);
        return plans[i];
      }
    }
    return null;
  }

  /**
   * Returns the DP table for inspection (e.g., in tests).
   */
  public DpTable dpTable() {
    return dpTable;
  }

  /**
   * For connected subgraph S1, find complement connected subgraphs S2
   * such that (S1, S2) is a valid csg-cmp-pair.
   */
  private void emitCsg(long S1) {
    // Exclusion set: S1 ∪ everything below min(S1)
    final long minBit = Long.lowestOneBit(S1);
    final long forbidden = S1 | (minBit - 1);
    long N = graph.neighborhood(S1) & ~forbidden;

    // Process each neighbor as a potential complement seed (descending order)
    while (N != 0) {
      final long vi = Long.highestOneBit(N);
      // Try (S1, {v_i}) as a csg-cmp-pair
      emitCsgCmp(S1, vi);
      // Extend the complement {v_i} to larger connected subgraphs
      final int idx = Long.numberOfTrailingZeros(vi);
      final long bvi = (1L << idx) - 1; // {v_0,...,v_{idx-1}}
      enumerateCmpRec(S1, vi, forbidden | (N & bvi));
      N &= ~vi;
    }
  }

  /**
   * Extend connected subgraph S by adding neighboring relations,
   * then emit csg-cmp-pairs for each extension.
   * Emit and recurse are fused into a single subset enumeration pass.
   */
  private void enumerateCsgRec(long S, long X) {
    final long N = graph.neighborhood(S) & ~X;
    if (N == 0) {
      return;
    }

    final long extendedX = X | N;
    for (long sub = N; sub != 0; sub = (sub - 1) & N) {
      final long extended = S | sub;
      emitCsg(extended);
      enumerateCsgRec(extended, extendedX);
    }
  }

  /**
   * Extend complement S2 by adding its neighbors, emitting csg-cmp-pairs
   * for each connected extension.
   * Emit and recurse are fused into a single subset enumeration pass.
   */
  private void enumerateCmpRec(long S1, long S2, long X) {
    final long N = graph.neighborhood(S2) & ~(X | S1);
    if (N == 0) {
      return;
    }

    final long extendedX = X | N;
    for (long sub = N; sub != 0; sub = (sub - 1) & N) {
      final long extended = S2 | sub;
      emitCsgCmp(S1, extended);
      enumerateCmpRec(S1, extended, extendedX);
    }
  }

  /**
   * Consider joining S1 and S2. Checks cost before allocating a JoinPlan
   * to avoid garbage on the DPhyp hot path.
   *
   * <p>Note: the hash join cost model is symmetric (cost(a,b) == cost(b,a)),
   * and FP addition is commutative, so there is no need to try both orderings.
   * If an asymmetric cost model is added later, both orderings should be tried.</p>
   */
  private void emitCsgCmp(long S1, long S2) {
    final JoinPlan p1 = dpTable.get(S1);
    final JoinPlan p2 = dpTable.get(S2);
    if (p1 == null || p2 == null) {
      return;
    }

    // Compute join cost first — this is cheap (arithmetic only)
    final double joinCost = costModel.estimateHashJoinCost(
        p1.cardinality(), p2.cardinality());
    final double totalCost = p1.cost() + p2.cost() + joinCost;

    // Early exit: skip selectivity scan and allocation if not cheaper
    final long S = S1 | S2;
    final JoinPlan current = dpTable.get(S);
    if (current != null && totalCost >= current.cost()) {
      return;
    }

    // Only compute selectivity (edge scan) and cardinality when plan wins
    final double selectivity = graph.joinSelectivity(S1, S2);
    final long joinCard = costModel.estimateJoinCardinality(
        p1.cardinality(), p2.cardinality(), selectivity);
    dpTable.put(S, JoinPlan.join(p1, p2, joinCost, joinCard));
  }
}
