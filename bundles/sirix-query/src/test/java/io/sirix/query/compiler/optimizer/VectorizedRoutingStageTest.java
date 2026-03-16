package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.XQExt;
import io.sirix.query.compiler.vectorized.ColumnType;
import io.sirix.query.compiler.vectorized.ComparisonOperator;
import io.sirix.query.compiler.vectorized.VectorizedPipelineDetector;
import io.sirix.query.compiler.vectorized.VectorizedPredicate;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link VectorizedRoutingStage} — predicate extraction,
 * operator mapping, and AST replacement with VectorizedPipelineExpr nodes.
 */
final class VectorizedRoutingStageTest {

  // --- Operator mapping tests ---

  @Test
  @DisplayName("Operator mapping: GeneralCompEQ → EQ")
  void operatorMappingEQ() {
    assertEquals(ComparisonOperator.EQ, VectorizedRoutingStage.mapOperator(XQ.GeneralCompEQ));
    assertEquals(ComparisonOperator.EQ, VectorizedRoutingStage.mapOperator(XQ.ValueCompEQ));
  }

  @Test
  @DisplayName("Operator mapping: GeneralCompNE → NE")
  void operatorMappingNE() {
    assertEquals(ComparisonOperator.NE, VectorizedRoutingStage.mapOperator(XQ.GeneralCompNE));
    assertEquals(ComparisonOperator.NE, VectorizedRoutingStage.mapOperator(XQ.ValueCompNE));
  }

  @Test
  @DisplayName("Operator mapping: GeneralCompLT → LT")
  void operatorMappingLT() {
    assertEquals(ComparisonOperator.LT, VectorizedRoutingStage.mapOperator(XQ.GeneralCompLT));
    assertEquals(ComparisonOperator.LT, VectorizedRoutingStage.mapOperator(XQ.ValueCompLT));
  }

  @Test
  @DisplayName("Operator mapping: GeneralCompLE → LE")
  void operatorMappingLE() {
    assertEquals(ComparisonOperator.LE, VectorizedRoutingStage.mapOperator(XQ.GeneralCompLE));
    assertEquals(ComparisonOperator.LE, VectorizedRoutingStage.mapOperator(XQ.ValueCompLE));
  }

  @Test
  @DisplayName("Operator mapping: GeneralCompGT → GT")
  void operatorMappingGT() {
    assertEquals(ComparisonOperator.GT, VectorizedRoutingStage.mapOperator(XQ.GeneralCompGT));
    assertEquals(ComparisonOperator.GT, VectorizedRoutingStage.mapOperator(XQ.ValueCompGT));
  }

  @Test
  @DisplayName("Operator mapping: GeneralCompGE → GE")
  void operatorMappingGE() {
    assertEquals(ComparisonOperator.GE, VectorizedRoutingStage.mapOperator(XQ.GeneralCompGE));
    assertEquals(ComparisonOperator.GE, VectorizedRoutingStage.mapOperator(XQ.ValueCompGE));
  }

  @Test
  @DisplayName("Operator mapping: unknown type → null")
  void operatorMappingUnknown() {
    assertNull(VectorizedRoutingStage.mapOperator(XQ.AndExpr));
  }

  // --- Predicate extraction tests ---

  @Test
  @DisplayName("Predicate extraction from EQ comparison with Int constant")
  void predicateExtractionIntEQ() {
    // price = 100
    final AST cmp = new AST(XQ.GeneralCompEQ);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "price"));
    final AST constant = new AST(XQ.Int, "100");
    cmp.addChild(field);
    cmp.addChild(constant);

    final List<VectorizedPredicate> predicates = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(cmp, predicates);

    assertEquals(1, predicates.size());
    final VectorizedPredicate pred = predicates.get(0);
    assertEquals("price", pred.fieldName());
    assertEquals(ComparisonOperator.EQ, pred.op());
    assertEquals(100L, pred.constant());
    assertEquals(ColumnType.INT64, pred.type());
  }

  @Test
  @DisplayName("Predicate extraction from NE comparison with Double constant")
  void predicateExtractionDblNE() {
    // value != 3.14
    final AST cmp = new AST(XQ.GeneralCompNE);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "value"));
    final AST constant = new AST(XQ.Dbl, "3.14");
    cmp.addChild(field);
    cmp.addChild(constant);

    final List<VectorizedPredicate> predicates = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(cmp, predicates);

    assertEquals(1, predicates.size());
    assertEquals(ComparisonOperator.NE, predicates.get(0).op());
    assertEquals(3.14, predicates.get(0).constant());
    assertEquals(ColumnType.FLOAT64, predicates.get(0).type());
  }

  @Test
  @DisplayName("Predicate extraction from comparison with Str constant")
  void predicateExtractionStrEQ() {
    // category = "electronics"
    final AST cmp = new AST(XQ.ValueCompEQ);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "category"));
    final AST constant = new AST(XQ.Str, "electronics");
    cmp.addChild(field);
    cmp.addChild(constant);

    final List<VectorizedPredicate> predicates = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(cmp, predicates);

    assertEquals(1, predicates.size());
    assertEquals("category", predicates.get(0).fieldName());
    assertEquals("electronics", predicates.get(0).constant());
    assertEquals(ColumnType.STRING, predicates.get(0).type());
    assertTrue(predicates.get(0).isStringFilterable(),
        "String EQ should be filterable by ColumnarStringFilter");
  }

  @Test
  @DisplayName("AND-connected predicates produce multiple VectorizedPredicate entries")
  void andConnectedPredicates() {
    // price > 10 AND category = "books"
    final AST andExpr = new AST(XQ.AndExpr);

    final AST cmp1 = new AST(XQ.GeneralCompGT);
    final AST field1 = new AST(XQ.DerefExpr);
    field1.addChild(new AST(XQ.Str, "price"));
    cmp1.addChild(field1);
    cmp1.addChild(new AST(XQ.Int, "10"));

    final AST cmp2 = new AST(XQ.GeneralCompEQ);
    final AST field2 = new AST(XQ.DerefExpr);
    field2.addChild(new AST(XQ.Str, "category"));
    cmp2.addChild(field2);
    cmp2.addChild(new AST(XQ.Str, "books"));

    andExpr.addChild(cmp1);
    andExpr.addChild(cmp2);

    final List<VectorizedPredicate> predicates = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(andExpr, predicates);

    assertEquals(2, predicates.size());

    // First predicate: price > 10
    assertEquals("price", predicates.get(0).fieldName());
    assertEquals(ComparisonOperator.GT, predicates.get(0).op());
    assertEquals(10L, predicates.get(0).constant());
    assertEquals(ColumnType.INT64, predicates.get(0).type());

    // Second predicate: category = "books"
    assertEquals("category", predicates.get(1).fieldName());
    assertEquals(ComparisonOperator.EQ, predicates.get(1).op());
    assertEquals("books", predicates.get(1).constant());
    assertEquals(ColumnType.STRING, predicates.get(1).type());
  }

  @Test
  @DisplayName("Non-vectorizable nodes left unchanged")
  void nonVectorizableNodesUnchanged() throws Exception {
    // Start pipeline without VECTORIZABLE annotation
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST start = new AST(XQ.Start);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.FunctionCall, "jn:doc"));
    // ForBind NOT marked as vectorizable
    start.addChild(forBind);

    final AST selection = new AST(XQ.Selection);
    final AST cmp = new AST(XQ.GeneralCompEQ);
    cmp.addChild(new AST(XQ.DerefExpr));
    cmp.addChild(new AST(XQ.Int, "1"));
    selection.addChild(cmp);
    start.addChild(selection);

    start.addChild(new AST(XQ.End));
    root.addChild(start);

    final var stage = new VectorizedRoutingStage();
    stage.rewrite(null, root);

    // Start node should remain unchanged (no VectorizedPipelineExpr replacement)
    assertEquals(XQ.Start, root.getChild(0).getType(),
        "Non-vectorizable Start should remain as-is");
  }

  @Test
  @DisplayName("COLUMNAR_STRING_SCAN_ELIGIBLE → columnar route")
  void columnarRouteForStringEligible() throws Exception {
    // Build a vectorizable pipeline with COLUMNAR_STRING_SCAN_ELIGIBLE
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST start = new AST(XQ.Start);

    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.FunctionCall, "jn:doc"));
    forBind.setProperty(VectorizedPipelineDetector.VECTORIZABLE, true);
    forBind.setProperty(VectorizedPipelineDetector.COLUMNAR_STRING_SCAN_ELIGIBLE, true);
    start.addChild(forBind);

    final AST selection = new AST(XQ.Selection);
    final AST cmp = new AST(XQ.GeneralCompEQ);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "name"));
    cmp.addChild(field);
    cmp.addChild(new AST(XQ.Str, "John"));
    selection.addChild(cmp);
    start.addChild(selection);

    start.addChild(new AST(XQ.End));
    root.addChild(start);

    final var stage = new VectorizedRoutingStage();
    stage.rewrite(null, root);

    // The Start should have been replaced with VectorizedPipelineExpr
    final AST replaced = root.getChild(0);
    assertEquals(XQExt.VectorizedPipelineExpr, replaced.getType(),
        "Start should be replaced with VectorizedPipelineExpr");
    assertEquals("columnar", replaced.getProperty(VectorizedRoutingStage.VECTORIZED_ROUTE));

    @SuppressWarnings("unchecked")
    final var preds = (List<VectorizedPredicate>) replaced.getProperty(VectorizedRoutingStage.VECTORIZED_PREDICATES);
    assertNotNull(preds);
    assertEquals(1, preds.size());
    assertEquals("name", preds.get(0).fieldName());
    assertEquals(ComparisonOperator.EQ, preds.get(0).op());
  }

  @Test
  @DisplayName("Non-columnar vectorizable → simd route")
  void simdRouteForNonColumnar() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST start = new AST(XQ.Start);

    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.FunctionCall, "jn:doc"));
    forBind.setProperty(VectorizedPipelineDetector.VECTORIZABLE, true);
    // NOT setting COLUMNAR_STRING_SCAN_ELIGIBLE → simd route
    start.addChild(forBind);

    final AST selection = new AST(XQ.Selection);
    final AST cmp = new AST(XQ.GeneralCompGT);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "price"));
    cmp.addChild(field);
    cmp.addChild(new AST(XQ.Int, "50"));
    selection.addChild(cmp);
    start.addChild(selection);

    start.addChild(new AST(XQ.End));
    root.addChild(start);

    final var stage = new VectorizedRoutingStage();
    stage.rewrite(null, root);

    final AST replaced = root.getChild(0);
    assertEquals(XQExt.VectorizedPipelineExpr, replaced.getType());
    assertEquals("simd", replaced.getProperty(VectorizedRoutingStage.VECTORIZED_ROUTE));
  }

  @Test
  @DisplayName("String range predicates (LT/GT) produce predicates with STRING type")
  void stringRangePredicates() {
    // name > "A"
    final AST cmp = new AST(XQ.GeneralCompGT);
    final AST field = new AST(XQ.DerefExpr);
    field.addChild(new AST(XQ.Str, "name"));
    cmp.addChild(field);
    cmp.addChild(new AST(XQ.Str, "A"));

    final List<VectorizedPredicate> predicates = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(cmp, predicates);

    assertEquals(1, predicates.size());
    final VectorizedPredicate pred = predicates.get(0);
    assertEquals(ComparisonOperator.GT, pred.op());
    assertEquals(ColumnType.STRING, pred.type());
    assertFalse(pred.isStringFilterable(),
        "String GT should NOT be filterable by ColumnarStringFilter (only EQ/NE)");
  }

  @Test
  @DisplayName("Field name extraction from DerefExpr chains")
  void fieldNameExtraction() {
    // Simple DerefExpr with Str child
    final AST deref = new AST(XQ.DerefExpr);
    deref.addChild(new AST(XQ.Str, "price"));
    assertEquals("price", VectorizedRoutingStage.extractFieldName(deref));

    // Nested DerefExpr: outer contains inner with field name
    final AST outerDeref = new AST(XQ.DerefExpr);
    final AST innerDeref = new AST(XQ.DerefExpr);
    innerDeref.addChild(new AST(XQ.Str, "nested"));
    outerDeref.addChild(new AST(XQ.VariableRef, "x"));
    outerDeref.addChild(innerDeref);
    assertEquals("nested", VectorizedRoutingStage.extractFieldName(outerDeref));
  }

  @Test
  @DisplayName("Constant extraction for Int, Dbl, Str types")
  void constantExtractionTypes() {
    // Int
    final AST intCmp = new AST(XQ.GeneralCompEQ);
    final AST f1 = new AST(XQ.DerefExpr);
    f1.addChild(new AST(XQ.Str, "age"));
    intCmp.addChild(f1);
    intCmp.addChild(new AST(XQ.Int, "25"));

    final List<VectorizedPredicate> preds = new ArrayList<>();
    VectorizedRoutingStage.extractPredicates(intCmp, preds);
    assertEquals(25L, preds.get(0).constant());
    assertEquals(ColumnType.INT64, preds.get(0).type());

    // Double
    preds.clear();
    final AST dblCmp = new AST(XQ.GeneralCompEQ);
    final AST f2 = new AST(XQ.DerefExpr);
    f2.addChild(new AST(XQ.Str, "price"));
    dblCmp.addChild(f2);
    dblCmp.addChild(new AST(XQ.Dbl, "9.99"));
    VectorizedRoutingStage.extractPredicates(dblCmp, preds);
    assertEquals(9.99, preds.get(0).constant());
    assertEquals(ColumnType.FLOAT64, preds.get(0).type());

    // String
    preds.clear();
    final AST strCmp = new AST(XQ.GeneralCompEQ);
    final AST f3 = new AST(XQ.DerefExpr);
    f3.addChild(new AST(XQ.Str, "name"));
    strCmp.addChild(f3);
    strCmp.addChild(new AST(XQ.Str, "hello"));
    VectorizedRoutingStage.extractPredicates(strCmp, preds);
    assertEquals("hello", preds.get(0).constant());
    assertEquals(ColumnType.STRING, preds.get(0).type());
  }
}
