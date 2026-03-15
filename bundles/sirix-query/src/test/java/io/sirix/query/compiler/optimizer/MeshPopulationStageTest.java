package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.mesh.Mesh;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.compiler.optimizer.stats.JsonCostModel;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral tests for {@link MeshPopulationStage} — wiring the Mesh
 * search space into the optimizer pipeline.
 *
 * <p>Verifies:
 * <ul>
 *   <li>Access alternatives (index scan vs seq scan) are added to the Mesh</li>
 *   <li>Join alternatives with different orderings are added</li>
 *   <li>The Mesh selects the cheapest alternative correctly</li>
 *   <li>Child class references link parent and child equivalence classes</li>
 * </ul></p>
 */
final class MeshPopulationStageTest {

  private final JsonCostModel costModel = new JsonCostModel();

  @Test
  void accessAlternativesPopulatedInMesh() throws Exception {
    // ForBind with PREFER_INDEX=true and both cost annotations
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

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    // Mesh should have at least one equivalence class
    assertTrue(mesh.classCount() >= 1,
        "Mesh should have equivalence classes for access alternatives");

    // The class should contain 2 alternatives: seq scan and index scan
    final Object classIdObj = bindExpr.getProperty(CostProperties.MESH_CLASS_ID);
    assertNotNull(classIdObj, "Binding expression should have a mesh class ID");
    assertTrue(classIdObj instanceof Integer);
    final int classId = (Integer) classIdObj;

    final var eqClass = mesh.getClass(classId);
    assertNotNull(eqClass);
    assertEquals(2, eqClass.size(),
        "Should have 2 alternatives: seq scan and index scan");

    // Best alternative should be the cheaper one (index scan at 50.0)
    assertEquals(50.0, eqClass.getBestCost(), 0.01,
        "Best cost should be the index scan cost (50.0)");
  }

  @Test
  void accessAlternativesWithNoIndexOnlyHasSeqScan() throws Exception {
    // ForBind with PREFER_INDEX=false (no index available)
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, false);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 500.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 600.0);
    // No INDEX_ID → no index alternative
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    final Object classIdObj = bindExpr.getProperty(CostProperties.MESH_CLASS_ID);
    assertNotNull(classIdObj);
    final int classId = (Integer) classIdObj;

    final var eqClass = mesh.getClass(classId);
    assertNotNull(eqClass);
    assertEquals(1, eqClass.size(),
        "Should have only 1 alternative (seq scan) when no index available");
    assertEquals(500.0, eqClass.getBestCost(), 0.01);
  }

  @Test
  void joinAlternativesPopulatedInMesh() throws Exception {
    // Join with cost annotations
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST join = new AST(XQ.Join, null);
    join.setProperty(CostProperties.JOIN_COST, 150.0);
    join.setProperty(CostProperties.JOIN_LEFT_CARD, 1000L);
    join.setProperty(CostProperties.JOIN_RIGHT_CARD, 500L);

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
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    final Object classIdObj = join.getProperty(CostProperties.MESH_CLASS_ID);
    assertNotNull(classIdObj, "Join should have a mesh class ID");
    final int classId = (Integer) classIdObj;

    final var eqClass = mesh.getClass(classId);
    assertNotNull(eqClass);
    assertTrue(eqClass.size() >= 1,
        "Join should have at least one alternative in mesh");
    assertEquals(150.0, eqClass.getBestCost(), 0.01);
  }

  @Test
  void meshClearedBetweenRewrites() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 1000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 50.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 1);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);

    // First rewrite
    stage.rewrite(null, ast);
    final int countAfterFirst = mesh.classCount();
    assertTrue(countAfterFirst > 0);

    // Second rewrite — should clear and repopulate
    stage.rewrite(null, ast);
    assertEquals(countAfterFirst, mesh.classCount(),
        "Mesh should be cleared and repopulated (same count for same AST)");
  }

  @Test
  void noAnnotationsProducesEmptyMesh() throws Exception {
    // AST with no cost annotations
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.DerefExpr, null));
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    assertEquals(0, mesh.classCount(),
        "No cost annotations should produce empty mesh");
  }

  @Test
  void nWayJoinChildClassesPopulated() throws Exception {
    // Build a 3-way join: Join(Join(A, B), C) — both sub-joins should get mesh classes
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST outerJoin = new AST(XQ.Join, null);
    outerJoin.setProperty(CostProperties.JOIN_COST, 300.0);
    outerJoin.setProperty(CostProperties.JOIN_LEFT_CARD, 5000L);
    outerJoin.setProperty(CostProperties.JOIN_RIGHT_CARD, 200L);

    // Inner join (left child of outer)
    final AST innerJoin = new AST(XQ.Join, null);
    innerJoin.setProperty(CostProperties.JOIN_COST, 100.0);
    innerJoin.setProperty(CostProperties.JOIN_LEFT_CARD, 1000L);
    innerJoin.setProperty(CostProperties.JOIN_RIGHT_CARD, 500L);

    final AST inputA = new AST(XQ.ForBind, null);
    inputA.addChild(new AST(XQ.Variable, "a"));
    inputA.addChild(new AST(XQ.DerefExpr, null));
    innerJoin.addChild(inputA);

    final AST inputB = new AST(XQ.ForBind, null);
    inputB.addChild(new AST(XQ.Variable, "b"));
    inputB.addChild(new AST(XQ.DerefExpr, null));
    innerJoin.addChild(inputB);

    outerJoin.addChild(innerJoin);

    final AST inputC = new AST(XQ.ForBind, null);
    inputC.addChild(new AST(XQ.Variable, "c"));
    inputC.addChild(new AST(XQ.DerefExpr, null));
    outerJoin.addChild(inputC);

    ast.addChild(outerJoin);

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    // Both joins should have mesh class IDs
    assertNotNull(outerJoin.getProperty(CostProperties.MESH_CLASS_ID),
        "Outer join should have MESH_CLASS_ID");
    assertNotNull(innerJoin.getProperty(CostProperties.MESH_CLASS_ID),
        "Inner join should have MESH_CLASS_ID");

    // They should be different equivalence classes
    final int outerClassId = (Integer) outerJoin.getProperty(CostProperties.MESH_CLASS_ID);
    final int innerClassId = (Integer) innerJoin.getProperty(CostProperties.MESH_CLASS_ID);
    assertTrue(outerClassId != innerClassId,
        "Outer and inner joins should have different mesh class IDs");

    // Mesh should have at least 2 classes for the joins
    assertTrue(mesh.classCount() >= 2,
        "Mesh should have >= 2 classes for n-way join: " + mesh.classCount());
  }

  @Test
  void meshBestPlanSelectsCheapestAlternative() throws Exception {
    // ForBind where index scan is much cheaper
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    bindExpr.setProperty(CostProperties.SEQ_SCAN_COST, 5000.0);
    bindExpr.setProperty(CostProperties.INDEX_SCAN_COST, 10.0);
    bindExpr.setProperty(CostProperties.INDEX_ID, 3);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var mesh = new Mesh(16);
    final var stage = new MeshPopulationStage(mesh, costModel);
    stage.rewrite(null, ast);

    final int classId = (Integer) bindExpr.getProperty(CostProperties.MESH_CLASS_ID);
    final AST bestPlan = mesh.getBestPlan(classId);
    assertNotNull(bestPlan);

    // Best plan should be the index scan (cheapest)
    assertEquals(10.0, mesh.getBestCost(classId), 0.01);
    assertEquals(true, bestPlan.getProperty(CostProperties.PREFER_INDEX),
        "Best plan should prefer index (cheaper)");
  }
}
