package io.sirix.query.compiler.optimizer.join;

import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for join cost estimation methods added to {@link JsonCostModel}.
 */
final class JoinCostModelTest {

  private final JsonCostModel costModel = new JsonCostModel();

  @Test
  void hashJoinCostIsSymmetric() {
    // Hash join cost should be the same regardless of build/probe assignment
    // because cost = 3 * (left + right) * CPU_PER_TUPLE
    final double cost12 = costModel.estimateHashJoinCost(100, 1000);
    final double cost21 = costModel.estimateHashJoinCost(1000, 100);
    assertEquals(cost12, cost21, 1e-9,
        "Hash join cost should be symmetric (same total I/O)");
  }

  @Test
  void hashJoinCostScalesLinearly() {
    final double small = costModel.estimateHashJoinCost(100, 100);
    final double large = costModel.estimateHashJoinCost(1000, 1000);
    // 10x cardinality should give 10x cost
    assertEquals(large / small, 10.0, 0.01);
  }

  @Test
  void nestedLoopJoinCostIsQuadratic() {
    final double cost = costModel.estimateNestedLoopJoinCost(100, 200);
    // 100 * 200 * 0.01 = 200
    assertEquals(200.0, cost, 1e-9);
  }

  @Test
  void nestedLoopIsMoreExpensiveThanHashJoin() {
    final long left = 1000;
    final long right = 1000;

    final double hashCost = costModel.estimateHashJoinCost(left, right);
    final double nlCost = costModel.estimateNestedLoopJoinCost(left, right);

    assertTrue(hashCost < nlCost,
        "Hash join (" + hashCost + ") should be cheaper than nested-loop ("
            + nlCost + ") for equal-size inputs");
  }

  @Test
  void joinCardinalityWithSelectivity() {
    final long card = costModel.estimateJoinCardinality(1000, 500, 0.01);
    // 1000 * 500 * 0.01 = 5000
    assertEquals(5000L, card);
  }

  @Test
  void joinCardinalityNeverBelowOne() {
    final long card = costModel.estimateJoinCardinality(1, 1, 0.001);
    // 1 * 1 * 0.001 = 0.001 → floor to 1
    assertEquals(1L, card);
  }

  @Test
  void joinCardinalityWithDefaultSelectivity() {
    final long card = costModel.estimateJoinCardinality(1000, 500);
    // 1000 * 500 * 0.1 (default) = 50000
    assertEquals(50000L, card);
  }

  @Test
  void hashJoinCostFormulaMatchesExpected() {
    // 3 * (leftCard + rightCard) * CPU_PER_TUPLE
    // 3 * (100 + 200) * 0.01 = 9.0
    final double cost = costModel.estimateHashJoinCost(100, 200);
    assertEquals(9.0, cost, 1e-9);
  }
}
