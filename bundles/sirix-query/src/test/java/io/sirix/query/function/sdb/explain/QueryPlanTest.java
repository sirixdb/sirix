package io.sirix.query.function.sdb.explain;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link QueryPlan} record — the programmatic plan inspection API.
 */
final class QueryPlanTest {

  @Test
  @DisplayName("estimatedCardinality extracts from root")
  void estimatedCardinalityFromRoot() {
    final var ast = new AST(XQ.ForBind, null);
    ast.setProperty(CostProperties.ESTIMATED_CARDINALITY, 42L);

    final var plan = new QueryPlan(ast, null);

    assertEquals(42L, plan.estimatedCardinality());
  }

  @Test
  @DisplayName("estimatedCardinality searches children")
  void estimatedCardinalityFromChild() {
    final var root = new AST(XQ.FlowrExpr, null);
    final var child = new AST(XQ.ForBind, null);
    child.setProperty(CostProperties.ESTIMATED_CARDINALITY, 100L);
    root.addChild(child);

    final var plan = new QueryPlan(root, null);

    assertEquals(100L, plan.estimatedCardinality());
  }

  @Test
  @DisplayName("estimatedCardinality returns -1 when not annotated")
  void estimatedCardinalityMissing() {
    final var ast = new AST(XQ.FlowrExpr, null);
    final var plan = new QueryPlan(ast, null);

    assertEquals(-1L, plan.estimatedCardinality());
  }

  @Test
  @DisplayName("usesIndex returns true when PREFER_INDEX found in subtree")
  void usesIndexTrue() {
    final var root = new AST(XQ.FlowrExpr, null);
    final var start = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, null);
    forBind.setProperty(CostProperties.PREFER_INDEX, true);
    start.addChild(forBind);
    root.addChild(start);

    final var plan = new QueryPlan(root, null);

    assertTrue(plan.usesIndex());
  }

  @Test
  @DisplayName("usesIndex returns false when no PREFER_INDEX")
  void usesIndexFalse() {
    final var ast = new AST(XQ.FlowrExpr, null);
    final var plan = new QueryPlan(ast, null);

    assertFalse(plan.usesIndex());
  }

  @Test
  @DisplayName("hasClosedGate detects INDEX_GATE_CLOSED")
  void hasClosedGateTrue() {
    final var root = new AST(XQ.FlowrExpr, null);
    final var child = new AST(XQ.DerefExpr, null);
    child.setProperty(CostProperties.INDEX_GATE_CLOSED, true);
    root.addChild(child);

    final var plan = new QueryPlan(root, null);

    assertTrue(plan.hasClosedGate());
  }

  @Test
  @DisplayName("vectorizedRoute returns route from subtree")
  void vectorizedRouteFound() {
    final var root = new AST(XQ.FlowrExpr, null);
    final var vec = new AST(XQExt.VectorizedPipelineExpr,
        XQExt.toName(XQExt.VectorizedPipelineExpr));
    vec.setProperty("vectorized.route", "columnar");
    root.addChild(vec);

    final var plan = new QueryPlan(root, null);

    assertEquals("columnar", plan.vectorizedRoute());
  }

  @Test
  @DisplayName("vectorizedRoute returns null when not present")
  void vectorizedRouteNull() {
    final var ast = new AST(XQ.FlowrExpr, null);
    final var plan = new QueryPlan(ast, null);

    assertNull(plan.vectorizedRoute());
  }

  @Test
  @DisplayName("isJoinReordered detects JOIN_REORDERED in subtree")
  void isJoinReordered() {
    final var root = new AST(XQ.FlowrExpr, null);
    final var join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.JOIN_REORDERED, true);
    root.addChild(join);

    final var plan = new QueryPlan(root, null);

    assertTrue(plan.isJoinReordered());
  }

  @Test
  @DisplayName("toJSON returns non-null structured output")
  void toJSON() {
    final var ast = new AST(XQ.ForBind, "x");
    ast.setProperty(CostProperties.ESTIMATED_CARDINALITY, 1000L);

    final var plan = new QueryPlan(ast, null);
    final String json = plan.toJSON();

    assertNotNull(json);
    assertTrue(json.contains("\"operator\": \"ForBind\""));
    assertTrue(json.contains("\"estimatedCardinality\": 1000"));
  }

  @Test
  @DisplayName("toVerboseJSON includes both parsed and optimized")
  void toVerboseJSON() {
    final var parsed = new AST(XQ.FlowrExpr, null);
    final var optimized = new AST(XQ.FlowrExpr, null);
    optimized.setProperty(CostProperties.ESTIMATED_CARDINALITY, 500L);

    final var plan = new QueryPlan(optimized, parsed);
    final String json = plan.toVerboseJSON();

    assertTrue(json.contains("\"parsed\":"));
    assertTrue(json.contains("\"optimized\":"));
  }

  @Test
  @DisplayName("null AST handled gracefully")
  void nullAST() {
    final var plan = new QueryPlan(null, null);

    assertEquals(-1L, plan.estimatedCardinality());
    assertFalse(plan.usesIndex());
    assertNull(plan.vectorizedRoute());
    assertEquals("null", plan.toJSON());
  }
}
