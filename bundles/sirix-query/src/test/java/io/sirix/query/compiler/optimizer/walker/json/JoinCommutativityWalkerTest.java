package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JoinCommutativityWalker} (Rule 4: Join Commutativity).
 */
final class JoinCommutativityWalkerTest {

  @Test
  void swapWhenLeftIsSmallerThanRight() {
    // left: card=100, right: card=10000
    // For hash join, smaller should be the build side (right)
    // So left(100) should become right(build), right(10000) should become left(probe)
    final var join = buildJoinWithCardinalities(100L, 10000L);

    // Remember original children
    final AST originalLeft = join.getChild(0);
    final AST originalRight = join.getChild(1);

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.JOIN_SWAPPED));
    // After swap: left should be the original right, right should be the original left
    assertEquals(originalRight, join.getChild(0));
    assertEquals(originalLeft, join.getChild(1));
  }

  @Test
  void noSwapWhenRightIsSmaller() {
    // left: card=10000, right: card=100
    // Right is already the smaller side — no swap needed
    final var join = buildJoinWithCardinalities(10000L, 100L);
    final AST originalLeft = join.getChild(0);
    final AST originalRight = join.getChild(1);

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    // Should annotate but not swap
    assertNull(join.getProperty(CostProperties.JOIN_SWAPPED));
    assertEquals(originalLeft, join.getChild(0));
    assertEquals(originalRight, join.getChild(1));
  }

  @Test
  void noSwapForLeftOuterJoin() {
    final var join = buildJoinWithCardinalities(100L, 10000L);
    join.setProperty(CostProperties.LEFT_JOIN, true);

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    assertNull(join.getProperty(CostProperties.JOIN_SWAPPED),
        "Left outer joins must not be reordered");
  }

  @Test
  void skipAlreadySwappedJoin() {
    final var join = buildJoinWithCardinalities(100L, 10000L);
    join.setProperty(CostProperties.JOIN_SWAPPED, true);

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    // Should not modify again
    // (wasModified is false because it was already swapped)
  }

  @Test
  void annotatesCardinalitiesEvenWithoutSwap() {
    final var join = buildJoinWithCardinalities(5000L, 100L);

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    assertEquals(5000L, join.getProperty(CostProperties.JOIN_LEFT_CARD));
    assertEquals(100L, join.getProperty(CostProperties.JOIN_RIGHT_CARD));
  }

  @Test
  void skipNonJoinNodes() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));

    final var walker = new JoinCommutativityWalker();
    walker.walk(forBind);

    assertNull(forBind.getProperty(CostProperties.JOIN_SWAPPED));
  }

  @Test
  void skipJoinWithMissingCardinalities() {
    // Join with no cardinality annotations at all
    final var join = buildSimpleJoin();

    final var walker = new JoinCommutativityWalker();
    walker.walk(join);

    assertNull(join.getProperty(CostProperties.JOIN_SWAPPED),
        "Should skip when cardinalities are unknown");
  }

  private static AST buildJoinWithCardinalities(long leftCard, long rightCard) {
    final var join = new AST(XQ.Join, null);

    // Left input with cardinality annotation
    final var leftStart = new AST(XQ.Start, null);
    final var leftForBind = new AST(XQ.ForBind, null);
    leftForBind.setProperty(CostProperties.ESTIMATED_CARDINALITY, leftCard);
    leftStart.addChild(leftForBind);
    join.addChild(leftStart);

    // Right input with cardinality annotation
    final var rightStart = new AST(XQ.Start, null);
    final var rightForBind = new AST(XQ.ForBind, null);
    rightForBind.setProperty(CostProperties.ESTIMATED_CARDINALITY, rightCard);
    rightStart.addChild(rightForBind);
    join.addChild(rightStart);

    // Post pipeline
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);

    // Output
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }

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
}
