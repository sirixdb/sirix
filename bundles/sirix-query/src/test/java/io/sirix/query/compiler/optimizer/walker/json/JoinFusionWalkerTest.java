package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JoinFusionWalker} (Rule 1: Join Fusion).
 */
final class JoinFusionWalkerTest {

  @Test
  void fuseAdjacentJoinsOverDerefChain() {
    // Outer join with inner join as child, connected via DerefExpr
    // Join₂ → Start → Join₁ → Start → ForBind → DerefExpr
    final AST innerJoin = buildJoinWithDerefInput();
    final AST outerJoin = buildJoinWithNestedJoin(innerJoin);

    final var walker = new JoinFusionWalker();
    walker.walk(outerJoin);

    assertTrue(walker.wasModified());
    assertEquals(true, outerJoin.getProperty(CostProperties.JOIN_FUSED));
    assertEquals(true, innerJoin.getProperty(CostProperties.JOIN_FUSED));

    // Both should share the same group ID
    assertEquals(outerJoin.getProperty(CostProperties.JOIN_FUSION_GROUP_ID),
        innerJoin.getProperty(CostProperties.JOIN_FUSION_GROUP_ID));

    // Step count should be 2 (two joins in the chain)
    assertEquals(2, outerJoin.getProperty(CostProperties.JOIN_FUSION_STEP_COUNT));
  }

  @Test
  void skipSingleJoinWithoutNestedJoin() {
    // A single join with no nested join — nothing to fuse
    final AST join = buildSimpleJoin();

    final var walker = new JoinFusionWalker();
    walker.walk(join);

    assertNull(join.getProperty(CostProperties.JOIN_FUSED),
        "Single join should not be marked as fused");
  }

  @Test
  void skipLeftOuterJoin() {
    final AST innerJoin = buildJoinWithDerefInput();
    final AST outerJoin = buildJoinWithNestedJoin(innerJoin);
    outerJoin.setProperty(CostProperties.LEFT_JOIN, true);

    final var walker = new JoinFusionWalker();
    walker.walk(outerJoin);

    assertNull(outerJoin.getProperty(CostProperties.JOIN_FUSED),
        "Left outer joins must not be fused");
  }

  @Test
  void skipAlreadyFusedJoin() {
    final AST innerJoin = buildJoinWithDerefInput();
    final AST outerJoin = buildJoinWithNestedJoin(innerJoin);
    outerJoin.setProperty(CostProperties.JOIN_FUSED, true);

    final var walker = new JoinFusionWalker();
    walker.walk(outerJoin);

    // Should not modify — already fused
    assertNull(outerJoin.getProperty(CostProperties.JOIN_FUSION_GROUP_ID),
        "Already fused join should not be re-annotated");
  }

  @Test
  void fuseThreeWayJoinChain() {
    // Three nested joins: Join₃ → Join₂ → Join₁
    final AST join1 = buildJoinWithDerefInput();
    final AST join2 = buildJoinWithNestedJoin(join1);
    final AST join3 = buildJoinWithNestedJoin(join2);

    final var walker = new JoinFusionWalker();
    walker.walk(join3);

    assertTrue(walker.wasModified());
    assertEquals(true, join3.getProperty(CostProperties.JOIN_FUSED));
    assertEquals(true, join2.getProperty(CostProperties.JOIN_FUSED));
    assertEquals(true, join1.getProperty(CostProperties.JOIN_FUSED));

    // All three should share the same group ID
    final Object groupId = join3.getProperty(CostProperties.JOIN_FUSION_GROUP_ID);
    assertEquals(groupId, join2.getProperty(CostProperties.JOIN_FUSION_GROUP_ID));
    assertEquals(groupId, join1.getProperty(CostProperties.JOIN_FUSION_GROUP_ID));

    // Step count should be 3
    assertEquals(3, join3.getProperty(CostProperties.JOIN_FUSION_STEP_COUNT));
  }

  @Test
  void skipJoinWithoutDerefConnection() {
    // Two nested joins but without DerefExpr connection (e.g., LetBind only)
    final AST innerJoin = buildSimpleJoin();
    final AST outerJoin = new AST(XQ.Join, null);

    // Left input: Start with nested join but NO DerefExpr
    final var leftStart = new AST(XQ.Start, null);
    final var letBind = new AST(XQ.LetBind, null);
    letBind.addChild(new AST(XQ.Variable, "x"));
    letBind.addChild(new AST(XQ.Str, "literal")); // No DerefExpr
    leftStart.addChild(letBind);
    leftStart.addChild(innerJoin);
    outerJoin.addChild(leftStart);

    // Right input
    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    outerJoin.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    outerJoin.addChild(postStart);
    outerJoin.addChild(new AST(XQ.Str, "output"));

    final var walker = new JoinFusionWalker();
    walker.walk(outerJoin);

    assertNull(outerJoin.getProperty(CostProperties.JOIN_FUSED),
        "Should not fuse when no structural DerefExpr connection exists");
  }

  @Test
  void skipNonJoinNodes() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));

    final var walker = new JoinFusionWalker();
    walker.walk(forBind);

    assertNull(forBind.getProperty(CostProperties.JOIN_FUSED));
  }

  @Test
  void fuseWithArrayAccessConnection() {
    // Joins connected via ArrayAccess (child-index navigation)
    final AST innerJoin = buildJoinWithArrayAccessInput();
    final AST outerJoin = buildJoinWithNestedJoin(innerJoin);

    final var walker = new JoinFusionWalker();
    walker.walk(outerJoin);

    assertTrue(walker.wasModified());
    assertEquals(true, outerJoin.getProperty(CostProperties.JOIN_FUSED));
  }

  // --- Helper methods ---

  private static AST buildSimpleJoin() {
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
    return join;
  }

  private static AST buildJoinWithDerefInput() {
    final var join = new AST(XQ.Join, null);

    // Left input: Start → ForBind → DerefExpr chain
    final var leftStart = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "v"));
    final var deref = new AST(XQ.DerefExpr, null);
    deref.addChild(new AST(XQ.VariableRef, "$doc"));
    deref.addChild(new AST(XQ.Str, "field"));
    forBind.addChild(deref);
    leftStart.addChild(forBind);
    join.addChild(leftStart);

    // Right input
    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }

  private static AST buildJoinWithArrayAccessInput() {
    final var join = new AST(XQ.Join, null);

    // Left input: Start → ForBind → ArrayAccess
    final var leftStart = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "v"));
    final var arrayAccess = new AST(XQ.ArrayAccess, null);
    arrayAccess.addChild(new AST(XQ.VariableRef, "$arr"));
    forBind.addChild(arrayAccess);
    leftStart.addChild(forBind);
    join.addChild(leftStart);

    // Right input
    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }

  private static AST buildJoinWithNestedJoin(AST innerJoin) {
    final var outerJoin = new AST(XQ.Join, null);

    // Left input: Start → ForBind(DerefExpr) → innerJoin
    final var leftStart = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "v_outer"));
    final var deref = new AST(XQ.DerefExpr, null);
    deref.addChild(new AST(XQ.VariableRef, "$prev"));
    deref.addChild(new AST(XQ.Str, "next_field"));
    forBind.addChild(deref);
    leftStart.addChild(forBind);
    leftStart.addChild(innerJoin);
    outerJoin.addChild(leftStart);

    // Right input
    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    outerJoin.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    outerJoin.addChild(postStart);
    outerJoin.addChild(new AST(XQ.Str, "output"));

    return outerJoin;
  }
}
