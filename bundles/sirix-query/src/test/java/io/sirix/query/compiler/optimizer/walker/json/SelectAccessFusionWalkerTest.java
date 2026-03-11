package io.sirix.query.compiler.optimizer.walker.json;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Unit tests for {@link SelectAccessFusionWalker} (Rule 3: Select-Access Fusion).
 * Verifies that predicates from FilterExpr are annotated onto access expressions.
 */
final class SelectAccessFusionWalkerTest {

  @Test
  void fuseSingleEqualityPredicate() {
    // Build: FilterExpr(DerefExpr, Predicate(ComparisonExpr(EQ, DerefExpr("category"), "books")))
    final var filterExpr = buildFilterExpr("ValueCompEQ", "category");

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    assertTrue(walker.wasModified(), "Walker should report modification");
    assertEquals(1, filterExpr.getProperty("fusedPredicate.count"));
    assertEquals("ValueCompEQ", filterExpr.getProperty("fusedPredicate.operator"));
    assertEquals("category", filterExpr.getProperty("fusedPredicate.fieldName"));

    // Access expression should also be annotated
    final var accessExpr = filterExpr.getChild(0);
    assertEquals(true, accessExpr.getProperty("fusedPredicate.hasPredicatePushdown"));
  }

  @Test
  void fuseRangePredicateOnPrice() {
    final var filterExpr = buildFilterExpr("ValueCompGT", "price");

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    assertTrue(walker.wasModified());
    assertEquals("ValueCompGT", filterExpr.getProperty("fusedPredicate.operator"));
    assertEquals("price", filterExpr.getProperty("fusedPredicate.fieldName"));
  }

  @Test
  void fuseAndPredicateWithTwoComparisons() {
    // FilterExpr(DerefExpr, Predicate(AndExpr(GT(price), LT(price))))
    final var derefAccess = new AST(XQ.DerefExpr, null);
    derefAccess.addChild(new AST(XQ.FunctionCall, null));
    derefAccess.addChild(new AST(XQ.Str, "items"));

    final var andExpr = new AST(XQ.AndExpr, null);
    andExpr.addChild(makeComparison("ValueCompGT", "price"));
    andExpr.addChild(makeComparison("ValueCompLT", "price"));

    final var predicate = new AST(XQ.Predicate, null);
    predicate.addChild(andExpr);

    final var filterExpr = new AST(XQ.FilterExpr, null);
    filterExpr.addChild(derefAccess);
    filterExpr.addChild(predicate);

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    assertTrue(walker.wasModified());
    assertEquals(2, filterExpr.getProperty("fusedPredicate.count"));
    assertEquals("ValueCompGT", filterExpr.getProperty("fusedPredicate.operator"));
    assertEquals("ValueCompLT", filterExpr.getProperty("fusedPredicate.operator2"));
  }

  @Test
  void skipNonFilterExprNodes() {
    // A plain DerefExpr should not be modified
    final var deref = new AST(XQ.DerefExpr, null);
    deref.addChild(new AST(XQ.FunctionCall, null));
    deref.addChild(new AST(XQ.Str, "field"));

    final var walker = new SelectAccessFusionWalker();
    final var result = walker.walk(deref);

    assertNotNull(result);
    assertNull(deref.getProperty("fusedPredicate.count"));
  }

  @Test
  void skipFilterExprWithoutPredicate() {
    // FilterExpr with child count != 2
    final var filterExpr = new AST(XQ.FilterExpr, null);
    filterExpr.addChild(new AST(XQ.DerefExpr, null));

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    assertNull(filterExpr.getProperty("fusedPredicate.count"),
        "Should not annotate FilterExpr without a Predicate child");
  }

  @Test
  void skipAlreadyFusedFilterExpr() {
    final var filterExpr = buildFilterExpr("ValueCompEQ", "name");
    filterExpr.setProperty("fusedPredicate.count", 1); // already fused

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    // Should NOT re-fuse — count stays at 1 (not overwritten)
    assertEquals(1, filterExpr.getProperty("fusedPredicate.count"));
  }

  @Test
  void arrayAccessAsAccessExpression() {
    // FilterExpr with ArrayAccess as the access expression
    final var arrayAccess = new AST(XQ.ArrayAccess, null);
    arrayAccess.addChild(new AST(XQ.DerefExpr, null));
    arrayAccess.addChild(new AST(XQ.SequenceExpr, null));

    final var predicate = new AST(XQ.Predicate, null);
    predicate.addChild(makeComparison("ValueCompEQ", "status"));

    final var filterExpr = new AST(XQ.FilterExpr, null);
    filterExpr.addChild(arrayAccess);
    filterExpr.addChild(predicate);

    final var walker = new SelectAccessFusionWalker();
    walker.walk(filterExpr);

    assertTrue(walker.wasModified());
    assertEquals("ValueCompEQ", filterExpr.getProperty("fusedPredicate.operator"));
    assertEquals(true, arrayAccess.getProperty("fusedPredicate.hasPredicatePushdown"));
  }

  // --- helpers ---

  private static AST buildFilterExpr(String operator, String fieldName) {
    final var derefAccess = new AST(XQ.DerefExpr, null);
    derefAccess.addChild(new AST(XQ.FunctionCall, null));
    derefAccess.addChild(new AST(XQ.Str, "items"));

    final var predicate = new AST(XQ.Predicate, null);
    predicate.addChild(makeComparison(operator, fieldName));

    final var filterExpr = new AST(XQ.FilterExpr, null);
    filterExpr.addChild(derefAccess);
    filterExpr.addChild(predicate);

    return filterExpr;
  }

  private static AST makeComparison(String operator, String fieldName) {
    final var comp = new AST(XQ.ComparisonExpr, null);
    comp.addChild(new AST(XQ.Str, operator));

    // Left operand: DerefExpr($x, "fieldName")
    final var derefLeft = new AST(XQ.DerefExpr, null);
    derefLeft.addChild(new AST(XQ.ContextItemExpr, null));
    derefLeft.addChild(new AST(XQ.Str, fieldName));
    comp.addChild(derefLeft);

    comp.addChild(new AST(XQ.Str, "someValue"));
    return comp;
  }
}
