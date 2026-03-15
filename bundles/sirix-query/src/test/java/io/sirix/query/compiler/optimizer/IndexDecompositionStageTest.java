package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

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

  // --- Helpers ---

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
