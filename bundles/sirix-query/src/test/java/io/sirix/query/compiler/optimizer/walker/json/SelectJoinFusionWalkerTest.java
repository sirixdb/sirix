package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SelectJoinFusionWalker} (Rule 2: Select-Join Fusion).
 */
final class SelectJoinFusionWalkerTest {

  @Test
  void fuseSelectionWithJoin() {
    // Selection(ComparisonExpr) → Join
    final var join = buildSimpleJoin();
    final var predicate = new AST(XQ.ComparisonExpr, null);
    predicate.addChild(new AST(XQ.Str, "ValueCompEQ"));
    predicate.addChild(new AST(XQ.DerefExpr, null));
    predicate.addChild(new AST(XQ.Str, "value"));

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(predicate);
    selection.addChild(join);

    final var walker = new SelectJoinFusionWalker();
    walker.walk(selection);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.JOIN_PREDICATE_PUSHED));
    assertEquals(true, selection.getProperty(CostProperties.JOIN_PREDICATE_PUSHED));
    assertEquals(true, join.getProperty(CostProperties.FUSED_HAS_PUSHDOWN));
  }

  @Test
  void fuseSelectionWithAndPredicate() {
    final var join = buildSimpleJoin();
    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(new AST(XQ.ComparisonExpr, null));
    andExpr.addChild(new AST(XQ.ComparisonExpr, null));

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(andExpr);
    selection.addChild(join);

    final var walker = new SelectJoinFusionWalker();
    walker.walk(selection);

    assertTrue(walker.wasModified());
    assertEquals(true, join.getProperty(CostProperties.FUSED_HAS_PUSHDOWN));
  }

  @Test
  void skipSelectionWithoutJoin() {
    // Selection over ForBind (not a Join) — should not fuse
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(new AST(XQ.ComparisonExpr, null));
    selection.addChild(forBind);

    final var walker = new SelectJoinFusionWalker();
    walker.walk(selection);

    assertNull(forBind.getProperty(CostProperties.JOIN_PREDICATE_PUSHED));
  }

  @Test
  void skipAlreadyFusedJoin() {
    final var join = buildSimpleJoin();
    join.setProperty(CostProperties.JOIN_PREDICATE_PUSHED, true);

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(new AST(XQ.ComparisonExpr, null));
    selection.addChild(join);

    final var walker = new SelectJoinFusionWalker();
    walker.walk(selection);

    // Should not re-fuse
    assertNull(selection.getProperty(CostProperties.JOIN_PREDICATE_PUSHED),
        "Should not annotate selection when join already fused");
  }

  @Test
  void skipNonSelectionNodes() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));

    final var walker = new SelectJoinFusionWalker();
    walker.walk(forBind);

    assertNull(forBind.getProperty(CostProperties.JOIN_PREDICATE_PUSHED));
  }

  private static AST buildSimpleJoin() {
    final var join = new AST(XQ.Join, null);

    // Left input
    final var leftStart = new AST(XQ.Start, null);
    leftStart.addChild(new AST(XQ.End, null));
    join.addChild(leftStart);

    // Right input
    final var rightStart = new AST(XQ.Start, null);
    rightStart.addChild(new AST(XQ.End, null));
    join.addChild(rightStart);

    // Post pipeline
    final var postStart = new AST(XQ.Start, null);
    postStart.addChild(new AST(XQ.End, null));
    join.addChild(postStart);

    // Output
    join.addChild(new AST(XQ.Str, "output"));

    return join;
  }
}
