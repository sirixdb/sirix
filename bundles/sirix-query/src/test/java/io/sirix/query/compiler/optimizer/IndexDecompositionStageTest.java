package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link IndexDecompositionStage} (integration of Rules 5-6).
 */
final class IndexDecompositionStageTest {

  @Test
  void stageAppliesRule5() throws Exception {
    final AST ast = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    final AST result = stage.rewrite(null, ast);

    final AST join = findJoin(result);
    assertNotNull(join);
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
  }

  @Test
  void stageIsNoopWithoutIndexedInputs() throws Exception {
    final AST ast = buildAstWithPlainJoin();

    final var stage = new IndexDecompositionStage();
    final AST result = stage.rewrite(null, ast);

    final AST join = findJoin(result);
    assertNotNull(join);
    assertNull(join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
  }

  @Test
  void stageAppliesRule6ForIntersection() throws Exception {
    final AST ast = buildAstWithDualIndexedJoin();

    final var stage = new IndexDecompositionStage();
    final AST result = stage.rewrite(null, ast);

    final AST join = findJoin(result);
    assertNotNull(join);
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_6,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_INTERSECT),
        "Rule 6 should mark intersection decomposition");
    // Both sides' index info should be present
    assertNotNull(join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID));
    assertNotNull(join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID_RIGHT));
  }

  @Test
  void rule5AnnotatesCorrectIndexInfo() throws Exception {
    final AST ast = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    final AST result = stage.rewrite(null, ast);

    final AST join = findJoin(result);
    assertNotNull(join);
    assertEquals(5, join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID),
        "Rule 5 should annotate the index ID from the indexed side");
    assertEquals("PATH", join.getProperty(CostProperties.DECOMPOSITION_INDEX_TYPE),
        "Rule 5 should annotate the index type from the indexed side");
  }

  @Test
  void leftOuterJoinsAreSkipped() throws Exception {
    final AST ast = buildAstWithLeftOuterJoin();

    final var stage = new IndexDecompositionStage();
    final AST result = stage.rewrite(null, ast);

    final AST join = findJoin(result);
    assertNotNull(join);
    assertNull(join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE),
        "Left outer joins should be skipped by decomposition");
  }

  @Test
  void alreadyDecomposedJoinsAreSkipped() throws Exception {
    final AST ast = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    // First pass
    stage.rewrite(null, ast);
    final AST join = findJoin(ast);
    assertNotNull(join);
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));

    // Second pass — should not re-decompose
    final AST result = stage.rewrite(null, ast);
    final AST join2 = findJoin(result);
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join2.getProperty(CostProperties.DECOMPOSITION_TYPE),
        "Type should remain Rule 5 (not re-decomposed)");
  }

  // --- Restructuring tests ---

  @Test
  void rule5SwapsIndexedSideToLeft() throws Exception {
    // Build join with index on RIGHT side only
    final var root = new AST(XQ.Start, null);
    final var join = new AST(XQ.Join, null);

    // Left: plain (no index)
    final var leftStart = new AST(XQ.Start, null);
    final var leftBind = new AST(XQ.ForBind, null);
    leftBind.addChild(new AST(XQ.Variable, "plain"));
    leftStart.addChild(leftBind);
    join.addChild(leftStart);

    // Right: indexed
    final var rightStart = new AST(XQ.Start, null);
    final var rightBind = new AST(XQ.ForBind, null);
    rightBind.addChild(new AST(XQ.Variable, "indexed"));
    rightBind.setProperty(CostProperties.PREFER_INDEX, true);
    rightBind.setProperty(CostProperties.INDEX_ID, 5);
    rightBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    rightStart.addChild(rightBind);
    join.addChild(rightStart);

    join.addChild(new AST(XQ.Start, null));
    join.addChild(new AST(XQ.Str, "output"));
    root.addChild(join);

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);

    final AST resultJoin = findJoin(root);
    assertNotNull(resultJoin);
    assertEquals(true, resultJoin.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED),
        "Should be marked as restructured");

    // After swap, child 0 should contain the indexed ForBind
    final AST newLeft = resultJoin.getChild(0);
    assertNotNull(newLeft);
    // The ForBind with "indexed" variable should now be on the left
    final AST leftForBind = findForBind(newLeft);
    assertNotNull(leftForBind, "Should find ForBind in left subtree after swap");
    assertEquals("indexed", leftForBind.getChild(0).getStringValue(),
        "Indexed side should be swapped to child 0 (left/driving)");
  }

  @Test
  void rule5KeepsIndexedSideOnLeftWhenAlreadyLeft() throws Exception {
    // Index already on left — no swap needed
    final AST root = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);

    final AST join = findJoin(root);
    assertNotNull(join);
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED),
        "Should still be marked as restructured (even without swap)");

    // Left side should still have the indexed ForBind
    final AST leftForBind = findForBind(join.getChild(0));
    assertNotNull(leftForBind);
    assertEquals(true, leftForBind.getProperty(CostProperties.PREFER_INDEX),
        "Indexed side should have PREFER_INDEX=true");
  }

  @Test
  void rule5SetsPreferIndexOnIndexedSide() throws Exception {
    final AST root = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);

    final AST join = findJoin(root);
    assertNotNull(join);

    // Find the ForBind with INDEX_ID=5 in child 0
    final AST indexedBind = findForBind(join.getChild(0));
    assertNotNull(indexedBind);
    assertEquals(true, indexedBind.getProperty(CostProperties.PREFER_INDEX),
        "PREFER_INDEX should be true on the indexed side after restructuring");
  }

  @Test
  void rule6MarksIntersectionJoin() throws Exception {
    final AST root = buildAstWithDualIndexedJoin();

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);

    final AST join = findJoin(root);
    assertNotNull(join);
    assertEquals(true, join.getProperty(CostProperties.INTERSECTION_JOIN),
        "Rule 6 should mark the join as an intersection join");
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED));
  }

  @Test
  void rule6SetsPreferIndexOnBothSides() throws Exception {
    final AST root = buildAstWithDualIndexedJoin();

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);

    final AST join = findJoin(root);
    assertNotNull(join);

    // Both sides should have PREFER_INDEX=true
    final AST leftBind = findForBind(join.getChild(0));
    final AST rightBind = findForBind(join.getChild(1));
    assertNotNull(leftBind);
    assertNotNull(rightBind);
    assertEquals(true, leftBind.getProperty(CostProperties.PREFER_INDEX),
        "Left side of intersection should have PREFER_INDEX=true");
    assertEquals(true, rightBind.getProperty(CostProperties.PREFER_INDEX),
        "Right side of intersection should have PREFER_INDEX=true");
  }

  @Test
  void restructuringIsIdempotent() throws Exception {
    final AST root = buildAstWithIndexedJoin();

    final var stage = new IndexDecompositionStage();
    stage.rewrite(null, root);
    final AST join1 = findJoin(root);
    assertEquals(true, join1.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED));

    // Second pass should not re-restructure
    stage.rewrite(null, root);
    final AST join2 = findJoin(root);
    assertEquals(true, join2.getProperty(CostProperties.DECOMPOSITION_RESTRUCTURED),
        "Should still be marked as restructured after second pass");
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join2.getProperty(CostProperties.DECOMPOSITION_TYPE),
        "Decomposition type should not change after second pass");
  }

  // --- Helpers ---

  private static AST findForBind(AST node) {
    if (node == null) return null;
    if (node.getType() == XQ.ForBind) return node;
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST found = findForBind(node.getChild(i));
      if (found != null) return found;
    }
    return null;
  }


  private static AST buildAstWithIndexedJoin() {
    final var root = new AST(XQ.Start, null);
    final var join = new AST(XQ.Join, null);

    final var leftStart = new AST(XQ.Start, null);
    final var leftBind = new AST(XQ.ForBind, null);
    leftBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftBind.setProperty(CostProperties.INDEX_ID, 5);
    leftBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftBind);
    join.addChild(leftStart);

    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    root.addChild(join);
    return root;
  }

  private static AST buildAstWithDualIndexedJoin() {
    final var root = new AST(XQ.Start, null);
    final var join = new AST(XQ.Join, null);

    // Left side: indexed with PATH index (id=5)
    final var leftStart = new AST(XQ.Start, null);
    final var leftBind = new AST(XQ.ForBind, null);
    leftBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftBind.setProperty(CostProperties.INDEX_ID, 5);
    leftBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftBind);
    join.addChild(leftStart);

    // Right side: indexed with CAS index (id=8) — different index
    final var rightStart = new AST(XQ.Start, null);
    final var rightBind = new AST(XQ.ForBind, null);
    rightBind.setProperty(CostProperties.PREFER_INDEX, true);
    rightBind.setProperty(CostProperties.INDEX_ID, 8);
    rightBind.setProperty(CostProperties.INDEX_TYPE, "CAS");
    rightStart.addChild(rightBind);
    join.addChild(rightStart);

    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    root.addChild(join);
    return root;
  }

  private static AST buildAstWithLeftOuterJoin() {
    final var root = new AST(XQ.Start, null);
    final var join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.LEFT_JOIN, true); // Mark as left outer join

    final var leftStart = new AST(XQ.Start, null);
    final var leftBind = new AST(XQ.ForBind, null);
    leftBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftBind.setProperty(CostProperties.INDEX_ID, 5);
    leftBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftBind);
    join.addChild(leftStart);

    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    root.addChild(join);
    return root;
  }

  private static AST buildAstWithPlainJoin() {
    final var root = new AST(XQ.Start, null);
    final var join = new AST(XQ.Join, null);

    final var leftStart = new AST(XQ.Start, null);
    leftStart.addChild(new AST(XQ.End, null));
    join.addChild(leftStart);

    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    root.addChild(join);
    return root;
  }

  private static AST findJoin(AST node) {
    if (node == null) {
      return null;
    }
    if (node.getType() == XQ.Join) {
      return node;
    }
    for (int i = 0; i < node.getChildCount(); i++) {
      final AST found = findJoin(node.getChild(i));
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
