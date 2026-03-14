package io.sirix.query.compiler.vectorized;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VectorizedPipelineDetector} — AST analysis to detect
 * vectorizable scan-filter-project pipelines.
 */
final class VectorizedPipelineDetectorTest {

  @Test
  void detectSimpleScanFilterPattern() {
    // for $x in collection[] where $x.value > 100 return $x.value
    // AST: Start → [ForBind(DerefExpr), Selection(GeneralCompGT(DerefExpr, Int))]
    final var start = new AST(XQ.Start, null);
    final var forBind = buildForBindWithDeref();
    start.addChild(forBind);
    final var selection = buildSelectionWithComparison(XQ.GeneralCompGT);
    start.addChild(selection);
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertTrue(detector.analyze(start));
    assertNotNull(forBind.getProperty(VectorizedPipelineDetector.VECTORIZABLE));
    assertEquals(1, forBind.getProperty(VectorizedPipelineDetector.VECTORIZABLE_PREDICATE_COUNT));
  }

  @Test
  void detectWithValueComparisonOperators() {
    // eq, ne, lt, le, gt, ge all supported
    for (final int compOp : new int[]{
        XQ.ValueCompEQ, XQ.ValueCompNE, XQ.ValueCompLT,
        XQ.ValueCompLE, XQ.ValueCompGT, XQ.ValueCompGE,
        XQ.GeneralCompEQ, XQ.GeneralCompNE, XQ.GeneralCompLT,
        XQ.GeneralCompLE, XQ.GeneralCompGT, XQ.GeneralCompGE}) {
      final var start = new AST(XQ.Start, null);
      start.addChild(buildForBindWithDeref());
      start.addChild(buildSelectionWithComparison(compOp));
      start.addChild(new AST(XQ.End, null));

      final var detector = new VectorizedPipelineDetector();
      assertTrue(detector.analyze(start),
          "Should detect with comparison operator type " + compOp);
    }
  }

  @Test
  void detectConjunctivePredicates() {
    // where $x.a > 10 and $x.b < 50
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());

    final var selection = new AST(XQ.Selection, null);
    final var andOp = new AST(XQ.AndExpr, null);
    andOp.addChild(buildComparison(XQ.GeneralCompGT));
    andOp.addChild(buildComparison(XQ.GeneralCompLT));
    selection.addChild(andOp);
    start.addChild(selection);
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertTrue(detector.analyze(start));

    final AST forBind = start.getChild(0);
    assertEquals(2, forBind.getProperty(VectorizedPipelineDetector.VECTORIZABLE_PREDICATE_COUNT));
  }

  @Test
  void rejectJoinAsPipelineBreaker() {
    // Join in the subtree prevents vectorization
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());
    final var selection = buildSelectionWithComparison(XQ.GeneralCompGT);
    start.addChild(selection);
    start.addChild(new AST(XQ.Join, null));  // breaker
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectGroupByAsPipelineBreaker() {
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());
    final var selection = buildSelectionWithComparison(XQ.GeneralCompGT);
    start.addChild(selection);
    start.addChild(new AST(XQ.GroupBy, null));  // breaker
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectOrderByAsPipelineBreaker() {
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());
    final var selection = buildSelectionWithComparison(XQ.GeneralCompGT);
    start.addChild(selection);
    start.addChild(new AST(XQ.OrderBy, null));  // breaker
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectNoSelectionChild() {
    // No where clause → not vectorizable
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectSelectionWithNonVectorizablePredicate() {
    // where some_function($x) — not a simple field comparison
    final var start = new AST(XQ.Start, null);
    start.addChild(buildForBindWithDeref());

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(new AST(XQ.FunctionCall, null));  // not vectorizable
    start.addChild(selection);
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectForBindWithoutDerefBinding() {
    // for $x in join_result — not a simple collection iteration
    final var start = new AST(XQ.Start, null);

    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, null));
    forBind.addChild(new AST(XQ.Join, null));  // not simple iteration
    start.addChild(forBind);

    final var selection = buildSelectionWithComparison(XQ.GeneralCompGT);
    start.addChild(selection);
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(start));
  }

  @Test
  void rejectNonStartNode() {
    // ForBind at root level (not wrapped in Start) → not detected
    final var forBind = buildForBindWithDeref();

    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(forBind));
    assertNull(forBind.getProperty(VectorizedPipelineDetector.VECTORIZABLE));
  }

  @Test
  void nullInputReturnsFalse() {
    final var detector = new VectorizedPipelineDetector();
    assertFalse(detector.analyze(null));
  }

  @Test
  void nestedStartsDetectedRecursively() {
    // Outer non-vectorizable Start wrapping an inner vectorizable Start
    final var outer = new AST(XQ.Start, null);
    outer.addChild(new AST(XQ.LetBind, null));

    final var inner = new AST(XQ.Start, null);
    inner.addChild(buildForBindWithDeref());
    inner.addChild(buildSelectionWithComparison(XQ.GeneralCompGT));
    inner.addChild(new AST(XQ.End, null));
    outer.addChild(inner);

    final var detector = new VectorizedPipelineDetector();
    assertTrue(detector.analyze(outer));
  }

  @Test
  void forBindWithArrayAccessAccepted() {
    final var start = new AST(XQ.Start, null);

    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, null));
    forBind.addChild(new AST(XQ.ArrayAccess, null));  // array access is simple
    start.addChild(forBind);

    start.addChild(buildSelectionWithComparison(XQ.GeneralCompGT));
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertTrue(detector.analyze(start));
  }

  @Test
  void forBindWithVariableRefAccepted() {
    final var start = new AST(XQ.Start, null);

    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, null));
    forBind.addChild(new AST(XQ.VariableRef, null));  // var ref is simple
    start.addChild(forBind);

    start.addChild(buildSelectionWithComparison(XQ.GeneralCompGT));
    start.addChild(new AST(XQ.End, null));

    final var detector = new VectorizedPipelineDetector();
    assertTrue(detector.analyze(start));
  }

  // --- Helpers ---

  private static AST buildForBindWithDeref() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, null));
    forBind.addChild(new AST(XQ.DerefExpr, null));
    return forBind;
  }

  private static AST buildSelectionWithComparison(int compOp) {
    final var selection = new AST(XQ.Selection, null);
    selection.addChild(buildComparison(compOp));
    return selection;
  }

  private static AST buildComparison(int compOp) {
    final var cmp = new AST(compOp, null);
    cmp.addChild(new AST(XQ.DerefExpr, null));   // field access
    cmp.addChild(new AST(XQ.Int, null));          // constant
    return cmp;
  }
}
