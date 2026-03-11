package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link CardinalityEstimator} — verifies pipeline cardinality
 * propagation through ForBind, LetBind, Selection, FilterExpr, and GroupBy.
 */
final class CardinalityEstimatorTest {

  private final SelectivityEstimator selEstimator = new SelectivityEstimator();
  private final CardinalityEstimator cardEstimator = new CardinalityEstimator(selEstimator);

  @Test
  void forBindMultipliesCardinality() {
    // ForBind with a binding expression annotated with pathCardinality=100
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));    // variable declaration
    final var bindingExpr = new AST(XQ.DerefExpr, null);
    bindingExpr.setProperty(CostProperties.PATH_CARDINALITY, 100L);
    forBind.addChild(bindingExpr);                    // binding expression

    final long card = cardEstimator.estimatePipelineCardinality(forBind);
    assertEquals(100L, card, "ForBind should use pathCardinality from binding expr");
  }

  @Test
  void letBindPreservesCardinality() {
    // LetBind wrapping a ForBind
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 50L);
    forBind.addChild(bindExpr);

    final var letBind = new AST(XQ.LetBind, null);
    letBind.addChild(new AST(XQ.Variable, "y"));
    letBind.addChild(new AST(XQ.DerefExpr, null)); // let binding expr
    letBind.addChild(forBind);                       // pipeline continuation

    final long card = cardEstimator.estimatePipelineCardinality(letBind);
    assertEquals(50L, card, "LetBind should preserve input cardinality (not multiply)");
  }

  @Test
  void selectionReducesCardinality() {
    // Selection with equality predicate (0.01 selectivity) over a source of 1000
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 1000L);
    forBind.addChild(bindExpr);

    final var predicate = makeComparison("ValueCompEQ");
    final var selection = new AST(XQ.Selection, null);
    selection.addChild(predicate);   // predicate
    selection.addChild(forBind);     // pipeline input

    final long card = cardEstimator.estimatePipelineCardinality(selection);
    // 1000 * 0.01 = 10
    assertEquals(10L, card, "Selection should apply selectivity: 1000 * 0.01 = 10");
  }

  @Test
  void filterExprAppliesPredicateSelectivity() {
    // FilterExpr: source[predicate]
    final var source = new AST(XQ.ForBind, null);
    source.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 500L);
    source.addChild(bindExpr);

    final var predicate = new AST(XQ.Predicate, null);
    predicate.addChild(makeComparison("ValueCompGT")); // 0.33 selectivity

    final var filterExpr = new AST(XQ.FilterExpr, null);
    filterExpr.addChild(source);
    filterExpr.addChild(predicate);

    final long card = cardEstimator.estimatePipelineCardinality(filterExpr);
    // 500 * 0.33 = 165
    assertEquals(165L, card, "FilterExpr should apply predicate selectivity");
  }

  @Test
  void groupByUsesSquareRootHeuristic() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 10000L);
    forBind.addChild(bindExpr);

    final var groupBy = new AST(XQ.GroupBy, null);
    groupBy.addChild(forBind); // pipeline input

    final long card = cardEstimator.estimatePipelineCardinality(groupBy);
    // √10000 = 100
    assertEquals(100L, card, "GroupBy should use √(input) heuristic");
  }

  @Test
  void cardinalityNeverBelowOne() {
    // Selection with very low selectivity on small input
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 2L);
    forBind.addChild(bindExpr);

    final var predicate = makeComparison("ValueCompEQ"); // 0.01
    final var selection = new AST(XQ.Selection, null);
    selection.addChild(predicate);
    selection.addChild(forBind);

    final long card = cardEstimator.estimatePipelineCardinality(selection);
    // 2 * 0.01 = 0.02 → floor to 1
    assertTrue(card >= 1, "Cardinality should never be below 1, got " + card);
  }

  @Test
  void annotateCardinalitiesSetsProperty() {
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 200L);
    forBind.addChild(bindExpr);

    cardEstimator.annotateCardinalities(forBind);

    final Object annotated = forBind.getProperty(CostProperties.ESTIMATED_CARDINALITY);
    assertTrue(annotated instanceof Long, "Should annotate with Long cardinality");
    assertEquals(200L, annotated, "Should annotate ForBind with its cardinality");
  }

  private static AST makeComparison(String operator) {
    final var comp = new AST(XQ.ComparisonExpr, null);
    comp.addChild(new AST(XQ.Str, operator));
    comp.addChild(new AST(XQ.DerefExpr, null));
    comp.addChild(new AST(XQ.Str, "value"));
    return comp;
  }
}
