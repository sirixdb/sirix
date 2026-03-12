package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link JoinDecompositionWalker} (Rules 5-6: Index-Aware Join Decomposition).
 */
final class JoinDecompositionWalkerTest {

  @Test
  void rule5DecomposesWhenOneInputHasIndex() {
    // Join with left input having an index, right input without
    final AST join = buildJoinWithIndexOnLeft();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
    assertEquals(42, join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID));
    assertEquals("PATH", join.getProperty(CostProperties.DECOMPOSITION_INDEX_TYPE));
  }

  @Test
  void rule5DecomposesWhenRightInputHasIndex() {
    // Join with right input having an index, left input without
    final AST join = buildJoinWithIndexOnRight();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
    assertEquals(7, join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID));
    assertEquals("CAS", join.getProperty(CostProperties.DECOMPOSITION_INDEX_TYPE));
  }

  @Test
  void rule6DecomposesWhenBothInputsHaveDifferentIndexes() {
    // Join with both inputs having different indexes — overlap
    final AST join = buildJoinWithBothIndexes();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_6,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_INTERSECT));

    // Should have right index info as well
    assertEquals(7, join.getProperty(CostProperties.DECOMPOSITION_INDEX_ID_RIGHT));
    assertEquals("CAS", join.getProperty(CostProperties.DECOMPOSITION_INDEX_TYPE_RIGHT));
  }

  @Test
  void skipJoinWithNoIndexAnnotations() {
    final AST join = buildSimpleJoin();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertNull(join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE),
        "Should not decompose when no indexes are available");
  }

  @Test
  void skipLeftOuterJoin() {
    final AST join = buildJoinWithIndexOnLeft();
    join.setProperty(CostProperties.LEFT_JOIN, true);

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertNull(join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE),
        "Left outer joins must not be decomposed");
  }

  @Test
  void skipAlreadyDecomposedJoin() {
    final AST join = buildJoinWithIndexOnLeft();
    join.setProperty(CostProperties.DECOMPOSITION_APPLICABLE, true);

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    // Should not re-decompose — DECOMPOSITION_TYPE should not be set
    // (since the join was pre-marked, visit() returns early)
    assertNull(join.getProperty(CostProperties.DECOMPOSITION_TYPE));
  }

  @Test
  void skipNonJoinNodes() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));

    final var walker = new JoinDecompositionWalker();
    walker.walk(forBind);

    assertNull(forBind.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
  }

  @Test
  void indexAnnotationFoundInNestedChild() {
    // Index annotation is not on the direct child but nested inside ForBind
    final AST join = buildJoinWithNestedIndexAnnotation();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.DECOMPOSITION_APPLICABLE));
    assertEquals(CostProperties.DECOMPOSITION_RULE_5,
        join.getProperty(CostProperties.DECOMPOSITION_TYPE));
  }

  @Test
  void sameIndexOnBothSidesDoesNotTriggerRule6() {
    // Both inputs have the SAME index → neither Rule 5 nor Rule 6 applies
    // (Rule 6 requires DIFFERENT indexes)
    final AST join = buildJoinWithSameIndexBothSides();

    final var walker = new JoinDecompositionWalker();
    walker.walk(join);

    // Rule 6 requires different indexes, so intersect should NOT be set.
    // Rule 5 requires exactly one side with an index. Since both have the same, no rule applies.
    assertNull(join.getProperty(CostProperties.DECOMPOSITION_INTERSECT),
        "Same index on both sides should not trigger intersection");
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

  private static AST buildJoinWithIndexOnLeft() {
    final var join = new AST(XQ.Join, null);

    // Left input: annotated with index preference
    final var leftStart = new AST(XQ.Start, null);
    final var leftForBind = new AST(XQ.ForBind, null);
    leftForBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftForBind.setProperty(CostProperties.INDEX_ID, 42);
    leftForBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftForBind);
    join.addChild(leftStart);

    // Right input: no index
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

  private static AST buildJoinWithIndexOnRight() {
    final var join = new AST(XQ.Join, null);

    // Left input: no index
    final var leftStart = new AST(XQ.Start, null);
    leftStart.addChild(new AST(XQ.End, null));
    join.addChild(leftStart);

    // Right input: annotated with CAS index
    final var rightStart = new AST(XQ.Start, null);
    final var rightForBind = new AST(XQ.ForBind, null);
    rightForBind.setProperty(CostProperties.PREFER_INDEX, true);
    rightForBind.setProperty(CostProperties.INDEX_ID, 7);
    rightForBind.setProperty(CostProperties.INDEX_TYPE, "CAS");
    rightStart.addChild(rightForBind);
    join.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }

  private static AST buildJoinWithBothIndexes() {
    final var join = new AST(XQ.Join, null);

    // Left input: PATH index (id=42)
    final var leftStart = new AST(XQ.Start, null);
    final var leftForBind = new AST(XQ.ForBind, null);
    leftForBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftForBind.setProperty(CostProperties.INDEX_ID, 42);
    leftForBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftForBind);
    join.addChild(leftStart);

    // Right input: CAS index (id=7)
    final var rightStart = new AST(XQ.Start, null);
    final var rightForBind = new AST(XQ.ForBind, null);
    rightForBind.setProperty(CostProperties.PREFER_INDEX, true);
    rightForBind.setProperty(CostProperties.INDEX_ID, 7);
    rightForBind.setProperty(CostProperties.INDEX_TYPE, "CAS");
    rightStart.addChild(rightForBind);
    join.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }

  private static AST buildJoinWithNestedIndexAnnotation() {
    final var join = new AST(XQ.Join, null);

    // Left input: nested annotation (Start → ForBind → DerefExpr[indexed])
    final var leftStart = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var deref = new AST(XQ.DerefExpr, null);
    deref.setProperty(CostProperties.PREFER_INDEX, true);
    deref.setProperty(CostProperties.INDEX_ID, 99);
    deref.setProperty(CostProperties.INDEX_TYPE, "NAME");
    forBind.addChild(deref);
    leftStart.addChild(forBind);
    join.addChild(leftStart);

    // Right input: no index
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

  private static AST buildJoinWithSameIndexBothSides() {
    final var join = new AST(XQ.Join, null);

    // Left input: PATH index (id=42)
    final var leftStart = new AST(XQ.Start, null);
    final var leftForBind = new AST(XQ.ForBind, null);
    leftForBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftForBind.setProperty(CostProperties.INDEX_ID, 42);
    leftForBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    leftStart.addChild(leftForBind);
    join.addChild(leftStart);

    // Right input: same PATH index (id=42)
    final var rightStart = new AST(XQ.Start, null);
    final var rightForBind = new AST(XQ.ForBind, null);
    rightForBind.setProperty(CostProperties.PREFER_INDEX, true);
    rightForBind.setProperty(CostProperties.INDEX_ID, 42);
    rightForBind.setProperty(CostProperties.INDEX_TYPE, "PATH");
    rightStart.addChild(rightForBind);
    join.addChild(rightStart);

    // Post pipeline + output
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }
}
