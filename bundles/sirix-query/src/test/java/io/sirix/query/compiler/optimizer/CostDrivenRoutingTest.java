package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Behavioral tests for {@link CostDrivenRoutingStage} — cost-driven
 * execution routing that gates index matching based on cost annotations.
 *
 * <p>Verifies:
 * <ul>
 *   <li>PREFER_INDEX=false propagates INDEX_GATE_CLOSED to descendants</li>
 *   <li>PREFER_INDEX=true leaves the gate open (default)</li>
 *   <li>Unrelated subtrees are not affected</li>
 *   <li>Multiple ForBinds are handled independently</li>
 * </ul></p>
 */
final class CostDrivenRoutingTest {

  @Test
  void preferIndexFalseClosesGateOnDescendants() throws Exception {
    // ForBind with PREFER_INDEX=false
    // All descendants of the binding expression should have INDEX_GATE_CLOSED=true
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, false);
    final AST innerDeref = new AST(XQ.DerefExpr, null);
    bindExpr.addChild(innerDeref);
    forBind.addChild(bindExpr);
    root.addChild(forBind);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertTrue(CostProperties.isIndexGateClosed(bindExpr),
        "Binding expression should have INDEX_GATE_CLOSED");
    assertTrue(CostProperties.isIndexGateClosed(innerDeref),
        "Inner descendant should have INDEX_GATE_CLOSED");
  }

  @Test
  void preferIndexTrueLeavesGateOpen() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, true);
    final AST innerDeref = new AST(XQ.DerefExpr, null);
    bindExpr.addChild(innerDeref);
    forBind.addChild(bindExpr);
    root.addChild(forBind);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertFalse(CostProperties.isIndexGateClosed(bindExpr),
        "PREFER_INDEX=true should not close the gate");
    assertFalse(CostProperties.isIndexGateClosed(innerDeref),
        "Descendants should not be gated when PREFER_INDEX=true");
  }

  @Test
  void noAnnotationLeavesGateOpen() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    // No PREFER_INDEX annotation
    forBind.addChild(bindExpr);
    root.addChild(forBind);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertNull(bindExpr.getProperty(CostProperties.INDEX_GATE_CLOSED),
        "No annotation should leave gate property unset");
  }

  @Test
  void multipleForBindsHandledIndependently() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);

    // First ForBind: PREFER_INDEX=false
    final AST forBind1 = new AST(XQ.ForBind, null);
    forBind1.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr1 = new AST(XQ.DerefExpr, null);
    bindExpr1.setProperty(CostProperties.PREFER_INDEX, false);
    forBind1.addChild(bindExpr1);

    // Second ForBind: PREFER_INDEX=true
    final AST forBind2 = new AST(XQ.ForBind, null);
    forBind2.addChild(new AST(XQ.Variable, "y"));
    final AST bindExpr2 = new AST(XQ.DerefExpr, null);
    bindExpr2.setProperty(CostProperties.PREFER_INDEX, true);
    forBind2.addChild(bindExpr2);

    root.addChild(forBind1);
    root.addChild(forBind2);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertTrue(CostProperties.isIndexGateClosed(bindExpr1),
        "First binding (false) should be gated");
    assertFalse(CostProperties.isIndexGateClosed(bindExpr2),
        "Second binding (true) should not be gated");
  }

  @Test
  void letBindAlsoHandled() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST letBind = new AST(XQ.LetBind, null);
    letBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    bindExpr.setProperty(CostProperties.PREFER_INDEX, false);
    letBind.addChild(bindExpr);
    root.addChild(letBind);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertTrue(CostProperties.isIndexGateClosed(bindExpr),
        "LetBind should also propagate gate closed signal");
  }

  @Test
  void deepDescendantsAreGated() throws Exception {
    // Deep tree: ForBind → DerefExpr → ArrayAccess → DerefExpr → FunctionCall
    final AST root = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));

    final AST level1 = new AST(XQ.DerefExpr, null);
    level1.setProperty(CostProperties.PREFER_INDEX, false);
    final AST level2 = new AST(XQ.ArrayAccess, null);
    final AST level3 = new AST(XQ.DerefExpr, null);
    final AST level4 = new AST(XQ.FunctionCall, null);

    level3.addChild(level4);
    level2.addChild(level3);
    level1.addChild(level2);
    forBind.addChild(level1);
    root.addChild(forBind);

    final var stage = new CostDrivenRoutingStage();
    stage.rewrite(null, root);

    assertTrue(CostProperties.isIndexGateClosed(level1));
    assertTrue(CostProperties.isIndexGateClosed(level2));
    assertTrue(CostProperties.isIndexGateClosed(level3));
    assertTrue(CostProperties.isIndexGateClosed(level4));
  }

  @Test
  void isIndexGateClosedUtilityMethod() {
    final AST node = new AST(XQ.DerefExpr, null);

    // Not set → not closed
    assertFalse(CostProperties.isIndexGateClosed(node));

    // Set to true → closed
    node.setProperty(CostProperties.INDEX_GATE_CLOSED, true);
    assertTrue(CostProperties.isIndexGateClosed(node));

    // Set to false → not closed
    node.setProperty(CostProperties.INDEX_GATE_CLOSED, false);
    assertFalse(CostProperties.isIndexGateClosed(node));
  }

  @Test
  void stageReturnsOriginalAST() throws Exception {
    final AST root = new AST(XQ.FlowrExpr, null);
    root.addChild(new AST(XQ.ForBind, null));

    final var stage = new CostDrivenRoutingStage();
    final AST result = stage.rewrite(null, root);

    assertEquals(root, result, "Stage should return the same AST reference");
  }
}
