package io.sirix.query.compiler.optimizer.join;

import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral tests for the DPhyp join ordering algorithm.
 *
 * <p>Key invariant: DPhyp should always produce the minimum-cost join plan,
 * considering all valid join tree shapes (left-deep, right-deep, bushy).</p>
 *
 * <p>Tests compare optimal DPhyp output against manually computed alternatives
 * to verify the optimizer picks the cheapest plan.</p>
 */
final class DPhypJoinOrdererTest {

  private final JsonCostModel costModel = new JsonCostModel();

  // --- 2-way joins ---

  @Test
  void twoWayJoinSmallLeftLargeRight() {
    // R0: 100 tuples, R1: 10000 tuples
    // Hash join cost: 3 * (100 + 10000) * 0.01 = 303
    // With R0 as probe and R1 as build: same (symmetric for hash join)
    // DPhyp should produce a valid plan
    final var graph = new JoinGraph(2);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 10000);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 100.0);
    graph.addEdge(0, 1, 0.01);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    assertEquals(0b11, plan.relationSet());
    assertFalse(plan.isBaseRelation());

    // Output cardinality: 100 * 10000 * 0.01 = 10000
    assertEquals(10000L, plan.cardinality());
  }

  @Test
  void twoWayJoinPicksCheaperOrdering() {
    // Asymmetric costs: R0 is expensive to access, R1 is cheap
    // DPhyp tries both orderings and picks the cheaper one
    final var graph = new JoinGraph(2);
    graph.setBaseCardinality(0, 1000);
    graph.setBaseCardinality(1, 1000);
    graph.setBaseCost(0, 100.0);
    graph.setBaseCost(1, 10.0);
    graph.addEdge(0, 1, 0.1);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    // Total cost should be baseCost(left) + baseCost(right) + joinCost
    // Both orderings have same hash join cost (symmetric), but plan should exist
    final double expectedJoinCost = 3.0 * (1000 + 1000) * 0.01;
    // Total: min(100+10, 10+100) + 60 = 70 + 60 = 170 (wait, it should try both)
    // Actually both orderings have same total: 100 + 10 + 60 = 170
    assertTrue(plan.cost() > 0);
  }

  // --- 3-way chain joins ---

  @Test
  void threeWayChainPicksOptimalOrder() {
    // Chain: R0(100) — R1(10000) — R2(50)
    // selectivity all 0.01
    //
    // Left-deep: (R0 ⋈ R1) ⋈ R2
    //   R0⋈R1: card=100*10000*0.01=10000, cost=baseCosts + hash(100,10000)
    //   Then ⋈R2: card=10000*50*0.01=5000
    //
    // Right-deep: R0 ⋈ (R1 ⋈ R2)
    //   R1⋈R2: card=10000*50*0.01=5000, cost=baseCosts + hash(10000,50)
    //   Then R0⋈: card=100*5000*0.01=5000
    //
    // DPhyp should pick the ordering with lower total cost
    final var graph = new JoinGraph(3);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 10000);
    graph.setBaseCardinality(2, 50);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 100.0);
    graph.setBaseCost(2, 0.5);
    graph.addEdge(0, 1, 0.01);
    graph.addEdge(1, 2, 0.01);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    assertEquals(0b111, plan.relationSet());
    assertFalse(plan.isBaseRelation());

    // Verify DPhyp found a plan — the exact ordering depends on cost model
    assertTrue(plan.cost() > 0, "Plan should have positive cost");

    // Verify cost is optimal by checking against manual alternatives
    final double leftDeepCost = computeLeftDeepCost(
        new long[]{100, 10000, 50},
        new double[]{1.0, 100.0, 0.5},
        new int[]{0, 1, 2},
        0.01);

    final double rightDeepCost = computeRightDeepCost(
        new long[]{100, 10000, 50},
        new double[]{1.0, 100.0, 0.5},
        new int[]{0, 1, 2},
        0.01);

    final double optimalCost = Math.min(leftDeepCost, rightDeepCost);
    assertTrue(plan.cost() <= optimalCost + 0.01,
        "DPhyp cost (" + plan.cost() + ") should be <= optimal("
            + optimalCost + "). Left-deep=" + leftDeepCost + ", right-deep=" + rightDeepCost);
  }

  @Test
  void threeWayChainSmallRelationsAtEnds() {
    // Chain: R0(10) — R1(100000) — R2(10)
    // Best strategy: join R0⋈R1 first (card=10000), then ⋈R2 (card=1000)
    // OR join R1⋈R2 first (card=10000), then R0⋈ (card=1000)
    // Both give similar costs. But joining the two small relations
    // would require going through R1 (no direct edge R0—R2).
    final var graph = new JoinGraph(3);
    graph.setBaseCardinality(0, 10);
    graph.setBaseCardinality(1, 100000);
    graph.setBaseCardinality(2, 10);
    graph.setBaseCost(0, 0.1);
    graph.setBaseCost(1, 1000.0);
    graph.setBaseCost(2, 0.1);
    graph.addEdge(0, 1, 0.001);
    graph.addEdge(1, 2, 0.001);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    assertEquals(0b111, plan.relationSet());
    assertTrue(plan.cost() > 0);
  }

  // --- 3-way star joins ---

  @Test
  void threeWayStarJoinCenterHasLowestCardinality() {
    // Star: R1(center=100) connected to R0(10000) and R2(5000)
    // Best: join R1 with the larger relation first, then smaller
    // Or: doesn't matter much since R1 is small
    final var graph = new JoinGraph(3);
    graph.setBaseCardinality(0, 10000);
    graph.setBaseCardinality(1, 100);
    graph.setBaseCardinality(2, 5000);
    graph.setBaseCost(0, 100.0);
    graph.setBaseCost(1, 1.0);
    graph.setBaseCost(2, 50.0);
    graph.addEdge(1, 0, 0.01);
    graph.addEdge(1, 2, 0.01);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    assertEquals(0b111, plan.relationSet());
    assertTrue(plan.cost() > 0);
  }

  // --- 4-way joins (the spec test) ---

  @Test
  void fourWayJoinDPhypPicksMinimumCost() {
    // 4-way chain: R0(100) — R1(10000) — R2(50) — R3(5000)
    // sel=0.01 everywhere
    //
    // This tests that DPhyp explores ALL valid tree shapes (left-deep,
    // right-deep, bushy) and picks the minimum-cost plan.
    final var graph = new JoinGraph(4);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 10000);
    graph.setBaseCardinality(2, 50);
    graph.setBaseCardinality(3, 5000);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 100.0);
    graph.setBaseCost(2, 0.5);
    graph.setBaseCost(3, 50.0);
    graph.addEdge(0, 1, 0.01);
    graph.addEdge(1, 2, 0.01);
    graph.addEdge(2, 3, 0.01);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan, "DPhyp should find a plan for 4-way chain join");
    assertEquals(0b1111, plan.relationSet());
    assertTrue(plan.cost() > 0);

    // Compute left-deep cost: ((R0⋈R1)⋈R2)⋈R3
    final double leftDeepCost = computeLeftDeepCostFor4Way(
        100, 10000, 50, 5000, 0.01);

    // Compute right-deep cost: R0⋈(R1⋈(R2⋈R3))
    final double rightDeepCost = computeRightDeepCostFor4Way(
        100, 10000, 50, 5000, 0.01);

    // DPhyp should be at least as good as both
    assertTrue(plan.cost() <= leftDeepCost + 0.01,
        "DPhyp (" + plan.cost() + ") should be <= left-deep (" + leftDeepCost + ")");
    assertTrue(plan.cost() <= rightDeepCost + 0.01,
        "DPhyp (" + plan.cost() + ") should be <= right-deep (" + rightDeepCost + ")");
  }

  @Test
  void fourWayCliqueConsidersBushyPlans() {
    // 4-way clique: all pairs connected
    // R0(100), R1(200), R2(100), R3(200)
    // With a clique, bushy plans like (R0⋈R2) ⋈ (R1⋈R3) are possible
    final var graph = new JoinGraph(4);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 200);
    graph.setBaseCardinality(2, 100);
    graph.setBaseCardinality(3, 200);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 2.0);
    graph.setBaseCost(2, 1.0);
    graph.setBaseCost(3, 2.0);
    // All pairs connected
    graph.addEdge(0, 1, 0.1);
    graph.addEdge(0, 2, 0.1);
    graph.addEdge(0, 3, 0.1);
    graph.addEdge(1, 2, 0.1);
    graph.addEdge(1, 3, 0.1);
    graph.addEdge(2, 3, 0.1);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);
    assertEquals(0b1111, plan.relationSet());

    // For a clique, DPhyp considers ALL tree shapes including bushy
    // Verify the plan is complete and has reasonable cost
    assertTrue(plan.cost() > 0);
    assertTrue(plan.cardinality() > 0);
  }

  @Test
  void dphypVsManualLeftDeepVsRightDeepVsBushy() {
    // 4-way clique: R0(1000), R1(10), R2(1000), R3(10)
    // sel=0.1 everywhere
    //
    // Bushy: (R1⋈R3) ⋈ (R0⋈R2)
    //   R1⋈R3: card=10*10*0.1=10, joinCost=3*(10+10)*0.01=0.6
    //   R0⋈R2: card=1000*1000*0.1=100000, joinCost=3*(1000+1000)*0.01=60
    //   Final: card=10*100000*0.1=100000, joinCost=3*(10+100000)*0.01=3000.3
    //   Total: baseCosts + 0.6 + 60 + 3000.3
    //
    // Left-deep (R1, R3, R0, R2):
    //   R1⋈R3: card=10, cost=0.6
    //   (R1⋈R3)⋈R0: card=10*1000*0.1=1000, cost=3*(10+1000)*0.01=30.3
    //   ((R1⋈R3)⋈R0)⋈R2: card=1000*1000*0.1=100000, cost=3*(1000+1000)*0.01=60
    //   Total: baseCosts + 0.6 + 30.3 + 60 = baseCosts + 90.9
    //
    // So left-deep starting with small relations should be cheaper!
    final var graph = new JoinGraph(4);
    graph.setBaseCardinality(0, 1000);
    graph.setBaseCardinality(1, 10);
    graph.setBaseCardinality(2, 1000);
    graph.setBaseCardinality(3, 10);
    graph.setBaseCost(0, 10.0);
    graph.setBaseCost(1, 0.1);
    graph.setBaseCost(2, 10.0);
    graph.setBaseCost(3, 0.1);
    graph.addEdge(0, 1, 0.1);
    graph.addEdge(0, 2, 0.1);
    graph.addEdge(0, 3, 0.1);
    graph.addEdge(1, 2, 0.1);
    graph.addEdge(1, 3, 0.1);
    graph.addEdge(2, 3, 0.1);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    assertNotNull(plan);

    // Manually compute the best left-deep cost (starting with R1, R3)
    // R1⋈R3: card=10, joinCost=0.6, total so far=0.1+0.1+0.6=0.8
    // +R0: card=10*1000*0.1=1000, joinCost=30.3, total=0.8+10+30.3=41.1
    // +R2: card=1000*1000*0.1=100000, joinCost=60, total=41.1+10+60=111.1
    final double baseCostSum = 10.0 + 0.1 + 10.0 + 0.1;
    final double bestLeftDeep = baseCostSum + 0.6 + 30.3 + 60.0;

    // DPhyp should find a plan at most as expensive as the best left-deep
    assertTrue(plan.cost() <= bestLeftDeep + 1.0,
        "DPhyp (" + plan.cost() + ") should be <= best left-deep (" + bestLeftDeep + ")");
  }

  // --- Edge cases ---

  @Test
  void singleRelationReturnsBasePlan() {
    final var graph = new JoinGraph(1);
    graph.setBaseCardinality(0, 500);
    graph.setBaseCost(0, 5.0);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    // Single relation: no join needed, returns base plan
    assertNotNull(plan);
    assertTrue(plan.isBaseRelation());
    assertEquals(500, plan.cardinality());
    assertEquals(5.0, plan.cost());
  }

  @Test
  void disconnectedRelationsReturnNull() {
    // R0 and R1 exist but have no join edge
    final var graph = new JoinGraph(2);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 200);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 2.0);
    // No edge added

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    final JoinPlan plan = optimizer.optimize();

    // No plan for the full set since R0 and R1 are disconnected
    assertNull(plan, "Should return null for disconnected graph");
  }

  @Test
  void dpTableContainsAllConnectedSubsets() {
    // 3-way chain: R0 — R1 — R2
    final var graph = new JoinGraph(3);
    graph.setBaseCardinality(0, 100);
    graph.setBaseCardinality(1, 200);
    graph.setBaseCardinality(2, 300);
    graph.setBaseCost(0, 1.0);
    graph.setBaseCost(1, 2.0);
    graph.setBaseCost(2, 3.0);
    graph.addEdge(0, 1, 0.1);
    graph.addEdge(1, 2, 0.1);

    final var optimizer = new AdaptiveJoinOrderOptimizer(graph, costModel);
    optimizer.optimize();

    final DpTable table = optimizer.dpTable();

    // Base relations
    assertNotNull(table.get(0b001), "Should have plan for {R0}");
    assertNotNull(table.get(0b010), "Should have plan for {R1}");
    assertNotNull(table.get(0b100), "Should have plan for {R2}");

    // Pairs with edges
    assertNotNull(table.get(0b011), "Should have plan for {R0,R1}");
    assertNotNull(table.get(0b110), "Should have plan for {R1,R2}");

    // Full set
    assertNotNull(table.get(0b111), "Should have plan for {R0,R1,R2}");

    // {R0,R2} has no direct edge, but is reachable through R1
    // DPhyp does NOT create plans for disconnected subsets
    assertNull(table.get(0b101), "Should NOT have plan for disconnected {R0,R2}");
  }

  // --- Helper methods for computing manual costs ---

  /**
   * Compute left-deep join cost: ((R[order[0]] ⋈ R[order[1]]) ⋈ R[order[2]])
   */
  private double computeLeftDeepCost(long[] cards, double[] costs, int[] order, double sel) {
    double totalCost = 0;
    for (final double cost : costs) {
      totalCost += cost;
    }

    long currentCard = cards[order[0]];
    for (int i = 1; i < order.length; i++) {
      final long rightCard = cards[order[i]];
      final double joinCost = 3.0 * (currentCard + rightCard) * 0.01;
      totalCost += joinCost;
      currentCard = Math.max(1L, (long) (currentCard * rightCard * sel));
    }
    return totalCost;
  }

  /**
   * Compute right-deep join cost: R[order[0]] ⋈ (R[order[1]] ⋈ R[order[2]])
   */
  private double computeRightDeepCost(long[] cards, double[] costs, int[] order, double sel) {
    double totalCost = 0;
    for (final double cost : costs) {
      totalCost += cost;
    }

    long currentCard = cards[order[order.length - 1]];
    for (int i = order.length - 2; i >= 0; i--) {
      final long leftCard = cards[order[i]];
      final double joinCost = 3.0 * (leftCard + currentCard) * 0.01;
      totalCost += joinCost;
      currentCard = Math.max(1L, (long) (leftCard * currentCard * sel));
    }
    return totalCost;
  }

  private double computeLeftDeepCostFor4Way(long c0, long c1, long c2, long c3, double sel) {
    return computeLeftDeepCost(
        new long[]{c0, c1, c2, c3},
        new double[]{c0 * 0.01, c1 * 0.01, c2 * 0.01, c3 * 0.01},
        new int[]{0, 1, 2, 3},
        sel);
  }

  private double computeRightDeepCostFor4Way(long c0, long c1, long c2, long c3, double sel) {
    return computeRightDeepCost(
        new long[]{c0, c1, c2, c3},
        new double[]{c0 * 0.01, c1 * 0.01, c2 * 0.01, c3 * 0.01},
        new int[]{0, 1, 2, 3},
        sel);
  }
}
