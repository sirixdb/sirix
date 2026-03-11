package io.sirix.query.compiler.optimizer.stats;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Integration tests for the Milestone 2 cost-based pipeline:
 * SelectivityEstimator + CardinalityEstimator working together
 * to produce realistic cardinality estimates for multi-predicate queries.
 */
final class CostBasedPipelineIntegrationTest {

  private final SelectivityEstimator selEstimator = new SelectivityEstimator();
  private final CardinalityEstimator cardEstimator = new CardinalityEstimator(selEstimator);

  @Test
  void multiPredicateQueryProducesRealisticEstimate() {
    // Simulates: for $x in $doc.items[] where $x.category eq "books" and $x.price > 40
    // Source cardinality: 10000 items
    // EQ selectivity: 0.01, GT selectivity: 0.33
    // AND backoff: 0.01 * 0.33^(1/2) ≈ 0.00574
    // Expected: 10000 * 0.00574 ≈ 57

    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 10000L);
    forBind.addChild(bindExpr);

    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ")); // category eq "books"
    andExpr.addChild(makeComparison("ValueCompGT")); // price > 40

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(andExpr);   // predicate
    selection.addChild(forBind);   // pipeline input

    final long card = cardEstimator.estimatePipelineCardinality(selection);

    // Expected: 10000 * (0.01 * sqrt(0.33)) ≈ 57
    final double expectedSel = 0.01 * Math.sqrt(0.33);
    final long expected = Math.max(1L, (long) (10000 * expectedSel));

    assertEquals(expected, card,
        "Multi-predicate AND should use exponential backoff, not naive multiplication");

    // Verify it's higher than naive (10000 * 0.01 * 0.33 = 33)
    assertTrue(card > 33,
        "Exponential backoff (" + card + ") should be higher than naive (33)");
  }

  @Test
  void singlePredicateMatchesExpectedSelectivity() {
    // for $x in 500_items where $x.status eq "active"
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 500L);
    forBind.addChild(bindExpr);

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(makeComparison("ValueCompEQ"));
    selection.addChild(forBind);

    final long card = cardEstimator.estimatePipelineCardinality(selection);
    // 500 * 0.01 = 5
    assertEquals(5L, card, "Single EQ predicate: 500 * 0.01 = 5");
  }

  @Test
  void orPredicateProducesHigherCardinalityThanAnd() {
    // OR(EQ, GT) → 0.01 + 0.33 - 0.0033 ≈ 0.337
    // AND(EQ, GT) → 0.01 * 0.33^(1/2) ≈ 0.00574
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 10000L);
    forBind.addChild(bindExpr);

    // OR selection
    final var orExpr = new AST(XQ.OrExpr, null);
    orExpr.addChild(makeComparison("ValueCompEQ"));
    orExpr.addChild(makeComparison("ValueCompGT"));

    final var orSelection = new AST(XQ.Selection, null);
    orSelection.addChild(orExpr);
    orSelection.addChild(forBind);

    final long orCard = cardEstimator.estimatePipelineCardinality(orSelection);

    // AND selection (rebuild ForBind since AST parents get modified)
    final var forBind2 = new AST(XQ.ForBind, null);
    forBind2.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr2 = new AST(XQ.DerefExpr, null);
    bindExpr2.setProperty(CostProperties.PATH_CARDINALITY, 10000L);
    forBind2.addChild(bindExpr2);

    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ"));
    andExpr.addChild(makeComparison("ValueCompGT"));

    final var andSelection = new AST(XQ.Selection, null);
    andSelection.addChild(andExpr);
    andSelection.addChild(forBind2);

    final long andCard = cardEstimator.estimatePipelineCardinality(andSelection);

    assertTrue(orCard > andCard,
        "OR cardinality (" + orCard + ") should be much higher than AND cardinality (" + andCard + ")");
  }

  @Test
  void pipelineWithLetBindAndSelection() {
    // for $x in 1000_items let $y := $x.name where $x.status eq "active"
    // Pipeline: Selection(LetBind(ForBind))
    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 1000L);
    forBind.addChild(bindExpr);

    final var letBind = new AST(XQ.LetBind, null);
    letBind.addChild(new AST(XQ.Variable, "y"));
    letBind.addChild(new AST(XQ.DerefExpr, null));
    letBind.addChild(forBind);

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(makeComparison("ValueCompEQ"));
    selection.addChild(letBind);

    final long card = cardEstimator.estimatePipelineCardinality(selection);
    // 1000 (ForBind) * 1 (LetBind preserves) * 0.01 (Selection) = 10
    assertEquals(10L, card,
        "Pipeline: 1000 items → let (preserves) → where EQ (0.01) = 10");
  }

  @Test
  void costModelCanCompareIndexVsScanWithEstimatedCardinality() {
    // End-to-end: use cardinality estimate to decide index vs scan
    // 100K items, AND(EQ, GT) → ~574 estimated results
    // CAS index on the field → index scan cost vs sequential scan cost

    final var forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final var bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 100000L);
    forBind.addChild(bindExpr);

    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompEQ"));
    andExpr.addChild(makeComparison("ValueCompGT"));

    final var selection = new AST(XQ.Selection, null);
    selection.addChild(andExpr);
    selection.addChild(forBind);

    final long estimatedCard = cardEstimator.estimatePipelineCardinality(selection);

    // Use estimated cardinality in cost model
    final var costModel = new JsonCostModel();
    final double seqCost = costModel.estimateSequentialScanCost(100000L);
    final double idxCost = costModel.estimateIndexScanCost(estimatedCard);

    assertTrue(costModel.isIndexScanCheaper(idxCost, seqCost),
        "Index scan (cost=" + idxCost + ") should be cheaper than sequential scan (cost="
            + seqCost + ") for estimated " + estimatedCard + " out of 100K nodes");
  }

  private static AST makeComparison(String operator) {
    final var comp = new AST(XQ.ComparisonExpr, null);
    comp.addChild(new AST(XQ.Str, operator));
    comp.addChild(new AST(XQ.DerefExpr, null));
    comp.addChild(new AST(XQ.Str, "value"));
    return comp;
  }
}
