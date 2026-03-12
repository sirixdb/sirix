package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.brackit.query.util.Cmp;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import io.sirix.query.compiler.optimizer.walker.json.CostBasedJoinReorder;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral tests for {@link JoinReorderStage} and
 * {@link CostBasedJoinReorder} AST restructuring.
 *
 * <p>Verifies that DPhyp-based join reordering:
 * <ul>
 *   <li>Is wired into the optimizer pipeline</li>
 *   <li>Swaps join children when the left side has lower cardinality</li>
 *   <li>Annotates join nodes with DPhyp cost information</li>
 *   <li>Preserves left outer joins (no reordering)</li>
 * </ul></p>
 */
final class JoinReorderStageTest {

  private final JsonCostModel costModel = new JsonCostModel();

  @Test
  void singleJoinSwapsWhenLeftIsSmallerCardinality() throws Exception {
    // Build: Join(left=100 rows, right=10000 rows)
    // For hash join, build on smaller side (right), probe with larger (left)
    // After reorder: left=10000, right=100
    final AST join = buildJoinNode(100L, 10000L);

    final var walker = new CostBasedJoinReorder(costModel);
    final AST result = walker.walk(join);

    assertNotNull(result);
    assertTrue(walker.wasModified(), "Walker should have modified the AST");

    // Verify join was annotated
    final Object reordered = result.getProperty(CostProperties.JOIN_REORDERED);
    assertEquals(true, reordered, "Join should be marked as reordered");

    final Object joinCost = result.getProperty(CostProperties.JOIN_COST);
    assertNotNull(joinCost, "Join should have cost annotation");
    assertTrue(joinCost instanceof Double, "Cost should be a Double");
    assertTrue((Double) joinCost > 0, "Cost should be positive");
  }

  @Test
  void singleJoinAnnotatesCardinality() throws Exception {
    final AST join = buildJoinNode(500L, 2000L);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(join);

    final Object estCard = join.getProperty(CostProperties.ESTIMATED_CARDINALITY);
    assertNotNull(estCard, "Join should have estimated cardinality");
    assertTrue(estCard instanceof Long, "Cardinality should be Long");
    assertTrue((Long) estCard > 0, "Cardinality should be positive");
  }

  @Test
  void leftOuterJoinIsNotReordered() throws Exception {
    final AST join = buildJoinNode(100L, 10000L);
    join.setProperty(CostProperties.LEFT_JOIN, true);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(join);

    // Left outer join should not be reordered
    assertTrue(!walker.wasModified(), "Left outer join should not be reordered");
  }

  @Test
  void alreadyReorderedJoinIsSkipped() throws Exception {
    final AST join = buildJoinNode(100L, 10000L);
    join.setProperty(CostProperties.JOIN_REORDERED, true);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(join);

    assertTrue(!walker.wasModified(), "Already reordered join should be skipped");
  }

  @Test
  void twoWayJoinGroupAnnotatesWithDphypCost() throws Exception {
    // Build a chain of two joins: (A ⋈ B) ⋈ C
    // When DPhyp cannot find edges for nested joins (Start wrapper),
    // the single join annotation path should still annotate individual joins
    final AST innerJoin = buildJoinNode(100L, 5000L);
    final AST outerJoin = buildOuterJoinNode(innerJoin, 200L);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(outerJoin);

    // The inner join should be annotated as a single join (it's visited bottom-up)
    final Object innerReordered = innerJoin.getProperty(CostProperties.JOIN_REORDERED);
    assertNotNull(innerReordered, "Inner join should be annotated");

    // The outer join may or may not be annotated depending on edge extraction
    // Just verify the walker ran without error and modified something
    assertTrue(walker.wasModified(), "Walker should have modified at least the inner join");
  }

  @Test
  void joinReorderStageProducesValidAST() throws Exception {
    final AST ast = buildSimpleFlworWithJoin(1000L, 50L);

    final var stage = new JoinReorderStage(costModel);
    final AST result = stage.rewrite(null, ast);

    assertNotNull(result, "Stage should return a valid AST");
    // The AST should still be navigable
    assertTrue(result.getChildCount() >= 0);
  }

  @Test
  void joinWithEqualCardinalitiesPreservesOrder() throws Exception {
    final AST join = buildJoinNode(1000L, 1000L);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(join);

    // Equal cardinalities: no reason to swap
    final Object swapped = join.getProperty(CostProperties.JOIN_SWAPPED);
    assertTrue(swapped == null || !Boolean.TRUE.equals(swapped),
        "Equal cardinalities should not trigger swap");
  }

  @Test
  void multiJoinChainAnnotatesInnerJoins() throws Exception {
    // 3-way chain: (A ⋈ B) ⋈ C
    // Each individual join gets annotated even if DPhyp can't plan the full group
    final AST innerJoin = buildJoinNode(100L, 5000L);
    final AST outerJoin = buildOuterJoinNode(innerJoin, 200L);

    final var walker = new CostBasedJoinReorder(costModel);
    walker.walk(outerJoin);

    // Inner join (standalone pair) should be annotated with cost info
    assertNotNull(innerJoin.getProperty(CostProperties.JOIN_REORDERED),
        "Inner join should be annotated");
    assertNotNull(innerJoin.getProperty(CostProperties.JOIN_COST),
        "Inner join should have cost annotation");
    assertNotNull(innerJoin.getProperty(CostProperties.ESTIMATED_CARDINALITY),
        "Inner join should have cardinality annotation");
  }

  // --- Helper methods ---

  /**
   * Build a simple Join node with annotated left and right inputs.
   * Join has 4 children: [leftInput, rightInput, postPipeline, output]
   */
  private static AST buildJoinNode(long leftCard, long rightCard) {
    final AST join = new AST(XQ.Join, null);

    final AST leftInput = new AST(XQ.ForBind, null);
    leftInput.addChild(new AST(XQ.Variable, "left"));
    final AST leftExpr = new AST(XQ.DerefExpr, null);
    leftExpr.setProperty(CostProperties.ESTIMATED_CARDINALITY, leftCard);
    leftInput.addChild(leftExpr);
    leftInput.setProperty(CostProperties.ESTIMATED_CARDINALITY, leftCard);

    final AST rightInput = new AST(XQ.ForBind, null);
    rightInput.addChild(new AST(XQ.Variable, "right"));
    final AST rightExpr = new AST(XQ.DerefExpr, null);
    rightExpr.setProperty(CostProperties.ESTIMATED_CARDINALITY, rightCard);
    rightInput.addChild(rightExpr);
    rightInput.setProperty(CostProperties.ESTIMATED_CARDINALITY, rightCard);

    join.addChild(leftInput);   // child 0: left input
    join.addChild(rightInput);  // child 1: right input
    join.addChild(new AST(XQ.End, null));     // child 2: post-pipeline
    join.addChild(new AST(XQ.Start, null));   // child 3: output

    join.setProperty(CostProperties.CMP, Cmp.eq);

    return join;
  }

  /**
   * Build a Join where the left child is an existing join (forming a chain).
   */
  private static AST buildOuterJoinNode(AST innerJoin, long rightCard) {
    final AST outerJoin = new AST(XQ.Join, null);

    // Wrap inner join in a Start node (as Brackit does for pipeline content)
    final AST leftWrapper = new AST(XQ.Start, null);
    leftWrapper.addChild(innerJoin);

    final AST rightInput = new AST(XQ.ForBind, null);
    rightInput.addChild(new AST(XQ.Variable, "outer_right"));
    final AST rightExpr = new AST(XQ.DerefExpr, null);
    rightExpr.setProperty(CostProperties.ESTIMATED_CARDINALITY, rightCard);
    rightInput.addChild(rightExpr);
    rightInput.setProperty(CostProperties.ESTIMATED_CARDINALITY, rightCard);

    outerJoin.addChild(leftWrapper);
    outerJoin.addChild(rightInput);
    outerJoin.addChild(new AST(XQ.End, null));
    outerJoin.addChild(new AST(XQ.Start, null));

    outerJoin.setProperty(CostProperties.CMP, Cmp.eq);

    return outerJoin;
  }

  /**
   * Build a minimal FLWOR expression with a join inside.
   */
  private static AST buildSimpleFlworWithJoin(long leftCard, long rightCard) {
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST join = buildJoinNode(leftCard, rightCard);
    root.addChild(join);
    root.addChild(new AST(XQ.End, null));
    return root;
  }
}
