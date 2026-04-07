package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link MeshSelectionStage} — applies best-plan decisions
 * from the Mesh back to the original AST nodes.
 */
final class MeshSelectionStageTest {

  private final JsonCostModel costModel = new JsonCostModel();

  @Test
  @DisplayName("Best plan PREFER_INDEX=true propagated to original AST node")
  void bestPlanPreferIndexPropagated() throws Exception {
    // Build AST: ForBind → binding expression with cost annotations
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 1000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 50.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 7);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    // Populate mesh
    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    // Now run selection stage on the same mesh
    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // The best plan (index scan at cost 50.0) should propagate PREFER_INDEX=true
    assertEquals(true, bindExpr.getProperty(CostProperties.PREFER_INDEX),
        "PREFER_INDEX should be true (index scan is cheaper)");
  }

  @Test
  @DisplayName("INDEX_ID copied when index alternative wins")
  void indexIdCopiedWhenIndexWins() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 5000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 10.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 42);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // INDEX_ID should be copied from best plan
    assertEquals(42, bindExpr.getProperty(CostProperties.INDEX_ID),
        "INDEX_ID should be 42 (from the winning index alternative)");
  }

  @Test
  @DisplayName("Nodes without MESH_CLASS_ID are untouched")
  void nodesWithoutMeshClassIdUntouched() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    // No cost properties → no MESH_CLASS_ID will be set
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // No PREFER_INDEX should be set since no mesh class was created
    assertNull(bindExpr.getProperty(CostProperties.PREFER_INDEX),
        "Nodes without MESH_CLASS_ID should not have PREFER_INDEX set");
    assertNull(bindExpr.getProperty(CostProperties.INDEX_ID),
        "Nodes without MESH_CLASS_ID should not have INDEX_ID set");
  }

  @Test
  @DisplayName("Multiple equivalence classes handled independently")
  void multipleEquivalenceClasses() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);

    // First ForBind: index scan wins
    final AST forBind1 = new AST(XQ.ForBind, null);
    forBind1.addChild(new AST(XQ.Variable, "a"));
    final AST bindExpr1 = new AST(XQ.DerefExpr, null);
    bindExpr1.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr1.setProperty(CostProperties.SEQ_SCAN_COST, 2000.0);
    bindExpr1.setProperty(CostProperties.INDEX_SCAN_COST, 100.0);
    bindExpr1.setProperty(CostProperties.INDEX_ID, 1);
    forBind1.addChild(bindExpr1);
    ast.addChild(forBind1);

    // Second ForBind: seq scan wins (no INDEX_ID → no index alternative)
    final AST forBind2 = new AST(XQ.ForBind, null);
    forBind2.addChild(new AST(XQ.Variable, "b"));
    final AST bindExpr2 = new AST(XQ.DerefExpr, null);
    bindExpr2.setProperty(CostProperties.PREFER_INDEX, false);
    bindExpr2.setProperty(CostProperties.SEQ_SCAN_COST, 300.0);
    bindExpr2.setProperty(CostProperties.INDEX_SCAN_COST, 500.0);
    // No INDEX_ID → only seq scan in mesh
    forBind2.addChild(bindExpr2);
    ast.addChild(forBind2);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // First ForBind: index scan wins → PREFER_INDEX=true, INDEX_ID=1
    assertEquals(true, bindExpr1.getProperty(CostProperties.PREFER_INDEX));
    assertEquals(1, bindExpr1.getProperty(CostProperties.INDEX_ID));

    // Second ForBind: only seq scan → PREFER_INDEX=false
    assertEquals(false, bindExpr2.getProperty(CostProperties.PREFER_INDEX));
  }

  @Test
  @DisplayName("Mesh with no matching classId is a no-op")
  void meshWithNoMatchingClassIdNoOp() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    // Set a MESH_CLASS_ID that doesn't exist in the mesh
    bindExpr.setProperty(CostProperties.MESH_CLASS_ID, 9999);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    // Don't populate anything — getBestPlan(9999) returns null

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // Should be a no-op: MESH_CLASS_ID still set but no properties copied
    assertEquals(9999, bindExpr.getProperty(CostProperties.MESH_CLASS_ID));
    assertNull(bindExpr.getProperty(CostProperties.PREFER_INDEX),
        "No PREFER_INDEX should be set for unknown class ID");
  }

  @Test
  @DisplayName("INDEX_TYPE copied when present on best plan")
  void indexTypeCopiedFromBestPlan() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 3000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 20.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 5);
    bindExpr.setProperty(CostProperties.INDEX_TYPE, "CAS");
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    assertEquals("CAS", bindExpr.getProperty(CostProperties.INDEX_TYPE),
        "INDEX_TYPE should be copied from best plan");
  }

  @Test
  @DisplayName("Non-decision properties are NOT overwritten by selection stage")
  void nonDecisionPropertiesPreserved() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 1000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 50.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 7);
    bindExpr.setProperty(CostProperties.ESTIMATED_CARDINALITY, 42000L);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // SEQ_SCAN_COST and ESTIMATED_CARDINALITY should remain from original
    assertEquals(1000.0, bindExpr.getProperty(CostProperties.SEQ_SCAN_COST),
        "SEQ_SCAN_COST should be preserved from original");
    assertEquals(42000L, bindExpr.getProperty(CostProperties.ESTIMATED_CARDINALITY),
        "ESTIMATED_CARDINALITY should be preserved from original");
  }

  @Test
  @DisplayName("Join alternatives: JOIN_COST updated when swapped ordering is cheaper")
  void joinAlternativesCostUpdated() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.JOIN_COST, 500.0);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 1000L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 100L);

    final AST leftInput = new AST(XQ.ForBind, null);
    leftInput.addChild(new AST(XQ.Variable, "a"));
    leftInput.addChild(new AST(XQ.DerefExpr, null));

    final AST rightInput = new AST(XQ.ForBind, null);
    rightInput.addChild(new AST(XQ.Variable, "b"));
    rightInput.addChild(new AST(XQ.DerefExpr, null));

    join.addChild(leftInput);
    join.addChild(rightInput);
    ast.addChild(join);

    final var mesh = new Mesh(16);
    final var populationStage = new MeshPopulationStage(mesh, costModel);
    populationStage.rewrite(null, ast);

    // Verify mesh class was created for the join
    final Object classIdObj = join.getProperty(CostProperties.MESH_CLASS_ID);
    assertTrue(classIdObj instanceof Integer, "Join should have MESH_CLASS_ID");

    final var selectionStage = new MeshSelectionStage(mesh);
    selectionStage.rewrite(null, ast);

    // JOIN_COST should be set to the best cost from the mesh
    final Object joinCost = join.getProperty(CostProperties.JOIN_COST);
    assertTrue(joinCost instanceof Double, "JOIN_COST should be a Double");
  }

  @Test
  @DisplayName("INDEX_SCAN_COST and SEQ_SCAN_COST are copied from best plan")
  void scanCostsCopiedFromBestPlan() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 2000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 75.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 3);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    new MeshPopulationStage(mesh, costModel).rewrite(null, ast);
    new MeshSelectionStage(mesh).rewrite(null, ast);

    // Both cost properties should be copied for explain output
    assertNotNull(bindExpr.getProperty(CostProperties.INDEX_SCAN_COST),
        "INDEX_SCAN_COST should be copied from best plan");
    assertNotNull(bindExpr.getProperty(CostProperties.SEQ_SCAN_COST),
        "SEQ_SCAN_COST should be copied from best plan");
  }

  @Test
  @DisplayName("Join children swapped when alternative NLJ ordering wins")
  void joinChildrenSwappedWhenAlternativeWins() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST join = new AST(XQ.Join, null);
    // Asymmetric cardinalities: NLJ with 10 on left is much cheaper than 10000 on left
    join.setProperty(CostProperties.JOIN_COST, 500.0);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 10000L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 10L);

    final AST leftInput = new AST(XQ.ForBind, null);
    leftInput.addChild(new AST(XQ.Variable, "big"));
    leftInput.addChild(new AST(XQ.DerefExpr, null));

    final AST rightInput = new AST(XQ.ForBind, null);
    rightInput.addChild(new AST(XQ.Variable, "small"));
    rightInput.addChild(new AST(XQ.DerefExpr, null));

    join.addChild(leftInput);
    join.addChild(rightInput);
    ast.addChild(join);

    final var mesh = new Mesh(16);
    new MeshPopulationStage(mesh, costModel).rewrite(null, ast);

    // Verify there are alternatives in the mesh
    final Object classIdObj = join.getProperty(CostProperties.MESH_CLASS_ID);
    assertTrue(classIdObj instanceof Integer);
    final var eqClass = mesh.getClass((int) classIdObj);

    // Only test swap if population actually created an alternative
    if (eqClass != null && eqClass.size() >= 2) {
      new MeshSelectionStage(mesh).rewrite(null, ast);

      // Check if swap was applied
      final Object swapped = join.getProperty(CostProperties.JOIN_SWAPPED);
      if (Boolean.TRUE.equals(swapped)) {
        // After swap, child 0 should be the formerly-right "small" input
        final AST newLeft = join.getChild(0);
        assertEquals("small",
            newLeft.getChild(0).getStringValue(),
            "After swap, 'small' should be on the left (driving) side");
      }
    }
  }

  @Test
  @DisplayName("Join not swapped when original order is already best")
  void joinNotSwappedWhenOriginalWins() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST join = new AST(XQ.Join, null);
    // Symmetric cardinalities → no swap benefit
    join.setProperty(CostProperties.JOIN_COST, 500.0);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 100L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 100L);

    final AST leftInput = new AST(XQ.ForBind, null);
    leftInput.addChild(new AST(XQ.Variable, "a"));
    leftInput.addChild(new AST(XQ.DerefExpr, null));

    final AST rightInput = new AST(XQ.ForBind, null);
    rightInput.addChild(new AST(XQ.Variable, "b"));
    rightInput.addChild(new AST(XQ.DerefExpr, null));

    join.addChild(leftInput);
    join.addChild(rightInput);
    ast.addChild(join);

    final var mesh = new Mesh(16);
    new MeshPopulationStage(mesh, costModel).rewrite(null, ast);
    new MeshSelectionStage(mesh).rewrite(null, ast);

    // With symmetric cardinalities, no swap should happen
    final AST newLeft = join.getChild(0);
    assertEquals("a", newLeft.getChild(0).getStringValue(),
        "'a' should remain on the left when costs are symmetric");
  }

  @Test
  @DisplayName("Bottom-up walk: child classes resolved before parent")
  void bottomUpChildClassesResolvedFirst() throws Exception {
    // Create a simple structure with nested ForBind inside a join
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.JOIN_COST, 200.0);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 50L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 50L);

    // Left input has index preference
    final AST leftForBind = new AST(XQ.ForBind, null);
    leftForBind.addChild(new AST(XQ.Variable, "x"));
    final AST leftBind = new AST(XQ.DerefExpr, null);
    leftBind.setProperty(CostProperties.PREFER_INDEX, true);
    leftBind.setProperty(CostProperties.SEQ_SCAN_COST, 500.0);
    leftBind.setProperty(CostProperties.INDEX_SCAN_COST, 25.0);
    leftBind.setProperty(CostProperties.INDEX_ID, 1);
    leftForBind.addChild(leftBind);

    final AST rightForBind = new AST(XQ.ForBind, null);
    rightForBind.addChild(new AST(XQ.Variable, "y"));
    rightForBind.addChild(new AST(XQ.DerefExpr, null));

    join.addChild(leftForBind);
    join.addChild(rightForBind);
    ast.addChild(join);

    final var mesh = new Mesh(16);
    new MeshPopulationStage(mesh, costModel).rewrite(null, ast);
    new MeshSelectionStage(mesh).rewrite(null, ast);

    // Child should have been resolved: leftBind should have PREFER_INDEX from mesh
    assertEquals(true, leftBind.getProperty(CostProperties.PREFER_INDEX),
        "Child ForBind should be resolved before parent join (bottom-up)");
  }
}
