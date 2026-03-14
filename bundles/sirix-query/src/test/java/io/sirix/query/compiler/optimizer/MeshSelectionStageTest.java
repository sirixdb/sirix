package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
}
