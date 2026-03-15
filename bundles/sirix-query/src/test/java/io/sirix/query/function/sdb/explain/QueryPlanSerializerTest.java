package io.sirix.query.function.sdb.explain;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.optimizer.VectorizedRoutingStage;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.vectorized.ColumnType;
import io.sirix.query.compiler.vectorized.ComparisonOperator;
import io.sirix.query.compiler.vectorized.VectorizedPredicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for {@link QueryPlanSerializer}.
 */
final class QueryPlanSerializerTest {

  @Test
  @DisplayName("Serialize simple ForBind with cost annotations")
  void serializeForBindWithCost() {
    final var forBind = new AST(XQ.ForBind, "x");
    forBind.setProperty(CostProperties.ESTIMATED_CARDINALITY, 5000L);
    forBind.setProperty(CostProperties.PATH_CARDINALITY, 10000L);
    forBind.setProperty(CostProperties.INDEX_SCAN_COST, 42.5);
    forBind.setProperty(CostProperties.SEQ_SCAN_COST, 1200.0);
    forBind.setProperty(CostProperties.PREFER_INDEX, true);
    forBind.setProperty(CostProperties.INDEX_ID, 3);
    forBind.setProperty(CostProperties.INDEX_TYPE, "CAS");

    final String json = QueryPlanSerializer.serialize(forBind);

    assertNotNull(json);
    // Cost section
    assertTrue(json.contains("\"estimatedCardinality\": 5000"), json);
    assertTrue(json.contains("\"pathCardinality\": 10000"), json);
    assertTrue(json.contains("\"indexScanCost\": 42.5"), json);
    assertTrue(json.contains("\"seqScanCost\": 1200.0"), json);
    // Index section
    assertTrue(json.contains("\"preferIndex\": true"), json);
    assertTrue(json.contains("\"indexId\": 3"), json);
    assertTrue(json.contains("\"indexType\": \"CAS\""), json);
    // Summary
    assertTrue(json.contains("index scan (CAS)"), json);
  }

  @Test
  @DisplayName("Serialize Selection with gate closed")
  void serializeGateClosed() {
    final var selection = new AST(XQ.Selection, null);
    selection.setProperty(CostProperties.ESTIMATED_CARDINALITY, 100L);
    selection.setProperty(CostProperties.PREFER_INDEX, false);
    selection.setProperty(CostProperties.INDEX_GATE_CLOSED, true);

    final String json = QueryPlanSerializer.serialize(selection);

    assertTrue(json.contains("\"gateClosed\": true"), json);
    assertTrue(json.contains("seq scan"), json);
    assertTrue(json.contains("[gate closed]"), json);
  }

  @Test
  @DisplayName("Serialize join with reorder annotations")
  void serializeJoinReordered() {
    final var join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.JOIN_REORDERED, true);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 500L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 10000L);
    join.setProperty(CostProperties.JOIN_COST, 75.0);

    final String json = QueryPlanSerializer.serialize(join);

    assertTrue(json.contains("\"reordered\": true"), json);
    assertTrue(json.contains("\"leftCardinality\": 500"), json);
    assertTrue(json.contains("\"rightCardinality\": 10000"), json);
    assertTrue(json.contains("reordered by DPhyp"), json);
  }

  @Test
  @DisplayName("Resolve XQExt types correctly (not UNKNOWN)")
  void resolveXQExtTypes() {
    assertEquals("IndexExpr",
        QueryPlanSerializer.resolveTypeName(XQExt.IndexExpr));
    assertEquals("VectorizedPipelineExpr",
        QueryPlanSerializer.resolveTypeName(XQExt.VectorizedPipelineExpr));
    assertEquals("MultiStepExpr",
        QueryPlanSerializer.resolveTypeName(XQExt.MultiStepExpr));
    assertEquals("ParentExpr",
        QueryPlanSerializer.resolveTypeName(XQExt.ParentExpr));
  }

  @Test
  @DisplayName("Standard XQ types resolve correctly")
  void resolveStandardTypes() {
    assertEquals("ForBind",
        QueryPlanSerializer.resolveTypeName(XQ.ForBind));
    assertEquals("Selection",
        QueryPlanSerializer.resolveTypeName(XQ.Selection));
    assertEquals("Join",
        QueryPlanSerializer.resolveTypeName(XQ.Join));
  }

  @Test
  @DisplayName("Serialize VectorizedPipelineExpr with predicates")
  void serializeVectorized() {
    final var node = new AST(XQExt.VectorizedPipelineExpr,
        XQExt.toName(XQExt.VectorizedPipelineExpr));
    node.setProperty(VectorizedRoutingStage.VECTORIZED_ROUTE, "columnar");
    node.setProperty(VectorizedRoutingStage.VECTORIZED_PREDICATES, List.of(
        new VectorizedPredicate("price", ComparisonOperator.GT, 50L, ColumnType.INT64),
        new VectorizedPredicate("category", ComparisonOperator.EQ, "books", ColumnType.STRING)
    ));
    node.setProperty("databaseName", "mydb");
    node.setProperty("resourceName", "myres");

    final String json = QueryPlanSerializer.serialize(node);

    assertTrue(json.contains("\"operator\": \"VectorizedPipelineExpr\""), json);
    assertTrue(json.contains("\"route\": \"columnar\""), json);
    assertTrue(json.contains("\"field\": \"price\""), json);
    assertTrue(json.contains("\"op\": \"GT\""), json);
    assertTrue(json.contains("\"field\": \"category\""), json);
    assertTrue(json.contains("\"databaseName\": \"mydb\""), json);
    assertTrue(json.contains("columnar route"), json);
    assertTrue(json.contains("2 predicate(s)"), json);
  }

  @Test
  @DisplayName("Serialize tree with children")
  void serializeTreeWithChildren() {
    final var start = new AST(XQ.Start, null);
    final var forBind = new AST(XQ.ForBind, "x");
    forBind.setProperty(CostProperties.ESTIMATED_CARDINALITY, 1000L);
    final var selection = new AST(XQ.Selection, null);
    selection.setProperty(CostProperties.ESTIMATED_CARDINALITY, 50L);

    start.addChild(forBind);
    start.addChild(selection);

    final String json = QueryPlanSerializer.serialize(start);

    assertTrue(json.contains("\"children\": ["), json);
    assertTrue(json.contains("\"operator\": \"ForBind\""), json);
    assertTrue(json.contains("\"operator\": \"Selection\""), json);
    assertTrue(json.contains("\"estimatedCardinality\": 1000"), json);
    assertTrue(json.contains("\"estimatedCardinality\": 50"), json);
  }

  @Test
  @DisplayName("SerializeBoth includes parsed and optimized")
  void serializeBoth() {
    final var parsed = new AST(XQ.FlowrExpr, null);
    final var optimized = new AST(XQ.FlowrExpr, null);
    optimized.setProperty(CostProperties.ESTIMATED_CARDINALITY, 100L);

    final String json = QueryPlanSerializer.serializeBoth(parsed, optimized);

    assertTrue(json.contains("\"parsed\":"), json);
    assertTrue(json.contains("\"optimized\":"), json);
    assertTrue(json.contains("\"estimatedCardinality\": 100"), json);
  }

  @Test
  @DisplayName("Serialize null AST returns 'null'")
  void serializeNull() {
    assertEquals("null", QueryPlanSerializer.serialize(null));
  }

  @Test
  @DisplayName("Nodes without annotations produce clean JSON")
  void serializeCleanNode() {
    final var node = new AST(XQ.Str, "hello");
    final String json = QueryPlanSerializer.serialize(node);

    assertTrue(json.contains("\"operator\": \"Str\""), json);
    assertTrue(json.contains("\"value\": \"hello\""), json);
    // No cost/index/join/vectorized sections
    assertFalse(json.contains("\"cost\""), json);
    assertFalse(json.contains("\"index\""), json);
    assertFalse(json.contains("\"join\""), json);
    assertFalse(json.contains("\"vectorized\""), json);
  }

  @Test
  @DisplayName("Decomposition section serialized when present")
  void serializeDecomposition() {
    final var node = new AST(XQ.Join, null);
    node.setProperty(CostProperties.DECOMPOSITION_APPLICABLE, true);
    node.setProperty(CostProperties.DECOMPOSITION_TYPE, "single-index");
    node.setProperty(CostProperties.DECOMPOSITION_RULE_5, true);

    final String json = QueryPlanSerializer.serialize(node);

    assertTrue(json.contains("\"decomposition\":"), json);
    assertTrue(json.contains("\"applicable\": true"), json);
    assertTrue(json.contains("\"type\": \"single-index\""), json);
    assertTrue(json.contains("\"rule5\": true"), json);
  }

  @Test
  @DisplayName("Fusion section serialized when present")
  void serializeFusion() {
    final var node = new AST(XQ.Join, null);
    node.setProperty(CostProperties.JOIN_FUSED, true);
    node.setProperty(CostProperties.JOIN_FUSION_GROUP_ID, 7);

    final String json = QueryPlanSerializer.serialize(node);

    assertTrue(json.contains("\"fusion\":"), json);
    assertTrue(json.contains("\"joinFused\": true"), json);
    assertTrue(json.contains("\"groupId\": 7"), json);
  }
}
