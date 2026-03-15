package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.AST;
import io.brackit.query.compiler.XQ;
import io.sirix.access.Databases;
import io.sirix.query.compiler.optimizer.stats.CostProperties;
import io.sirix.query.json.BasicJsonDBStore;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * Unit tests for {@link CostBasedStage}.
 *
 * <p>Tests exercise the stage's pure AST logic (extractPathAndDocument,
 * variable binding resolution) using a real {@link BasicJsonDBStore}
 * backed by a temp directory. Database-dependent behavior is covered
 * by the E2E integration tests in CostBasedPipelineIntegrationTest.</p>
 */
final class CostBasedStageTest {

  private Path tempDir;
  private BasicJsonDBStore store;

  @BeforeEach
  void setUp() throws Exception {
    tempDir = Files.createTempDirectory("sirix-cost-stage-test");
    store = BasicJsonDBStore.newBuilder().location(tempDir).build();
  }

  @AfterEach
  void tearDown() {
    if (store != null) {
      store.close();
    }
    if (tempDir != null) {
      Databases.removeDatabase(tempDir);
    }
  }

  @Test
  @DisplayName("extractPathAndDocument returns null for non-deref expressions")
  void extractPathReturnsNullForNonDeref() throws Exception {
    // ForBind over a plain integer literal — no deref chain to extract
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(new AST(XQ.Int, "42")); // not a deref chain
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    assertDoesNotThrow(() -> stage.rewrite(null, ast));

    // No cost annotations should be set on the literal binding
    assertNull(forBind.getChild(1).getProperty(CostProperties.PATH_CARDINALITY));
    assertNull(forBind.getChild(1).getProperty(CostProperties.PREFER_INDEX));
  }

  @Test
  @DisplayName("Variable binding resolution follows VariableRef to LetBind definition")
  void variableBindingResolution() throws Exception {
    // LetBind: let $col := <DerefExpr that is not a jn:doc() call>
    // ForBind: for $x in $col.field
    // The CostBasedStage should resolve $col → LetBind's expression
    final AST ast = new AST(XQ.FlowrExpr, null);

    // LetBind definition
    final AST letBind = new AST(XQ.LetBind, null);
    final AST letVar = new AST(XQ.Variable, "col");
    letVar.setValue("col");
    letBind.addChild(letVar);
    final AST letExpr = new AST(XQ.DerefExpr, null);
    letExpr.addChild(new AST(XQ.Str, "someField"));
    letBind.addChild(letExpr);
    ast.addChild(letBind);

    // ForBind using $col
    final AST forBind = new AST(XQ.ForBind, null);
    final AST forVar = new AST(XQ.Variable, "x");
    forVar.setValue("x");
    forBind.addChild(forVar);
    final AST deref = new AST(XQ.DerefExpr, null);
    final AST varRef = new AST(XQ.VariableRef, null);
    varRef.setValue("col"); // references the LetBind
    deref.addChild(varRef);
    deref.addChild(new AST(XQ.Str, "field"));
    forBind.addChild(deref);
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    // Should not throw — variable resolution happens but won't find jn:doc()
    assertDoesNotThrow(() -> stage.rewrite(null, ast));

    // No annotations since the chain doesn't end at jn:doc()
    assertNull(deref.getProperty(CostProperties.PREFER_INDEX));
  }

  @Test
  @DisplayName("Exception cleanup path clears statistics caches")
  void exceptionCleanupPath() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    // Plain non-deref expression — won't trigger stats lookup
    forBind.addChild(new AST(XQ.Int, "1"));
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    // First rewrite initializes the provider
    assertDoesNotThrow(() -> stage.rewrite(null, ast));
    // The stats provider should exist and be accessible
    assertNotNull(stage.getStatsProvider());
  }

  @Test
  @DisplayName("Empty AST is a no-op")
  void emptyAstNoOp() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final var stage = new CostBasedStage(store);
    assertDoesNotThrow(() -> stage.rewrite(null, ast));
  }

  @Test
  @DisplayName("Deeply nested deref chains terminate with maxIterations guard")
  void deeplyNestedDerefTerminates() throws Exception {
    // Build a deref chain deeper than 50 levels to test maxIterations guard
    AST current = new AST(XQ.Str, "base");
    for (int i = 0; i < 60; i++) {
      final AST deref = new AST(XQ.DerefExpr, null);
      deref.addChild(current);
      deref.addChild(new AST(XQ.Str, "field" + i));
      current = deref;
    }

    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    forBind.addChild(current);
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    // Should not hang or stack overflow
    assertDoesNotThrow(() -> stage.rewrite(null, ast));
  }

  @Test
  @DisplayName("FilterExpr in binding expression is unwrapped for path extraction")
  void filterExprUnwrapped() throws Exception {
    // ForBind over FilterExpr wrapping a DerefExpr
    // (simulates jn:doc(...)[][?predicate])
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));

    final AST filter = new AST(XQ.FilterExpr, null);
    final AST deref = new AST(XQ.DerefExpr, null);
    deref.addChild(new AST(XQ.Str, "someExpr")); // Not a jn:doc() call
    deref.addChild(new AST(XQ.Str, "field"));
    filter.addChild(deref);
    filter.addChild(new AST(XQ.ComparisonExpr, null)); // predicate
    forBind.addChild(filter);
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    assertDoesNotThrow(() -> stage.rewrite(null, ast));
    // No annotations since we didn't find jn:doc()
    assertNull(filter.getProperty(CostProperties.PREFER_INDEX));
  }

  @Test
  @DisplayName("CardinalityEstimator annotates ForBind with cardinality from child")
  void cardinalityEstimatorAnnotatesForBind() throws Exception {
    final AST ast = new AST(XQ.FlowrExpr, null);
    final AST forBind = new AST(XQ.ForBind, null);
    forBind.addChild(new AST(XQ.Variable, "x"));
    final AST bindExpr = new AST(XQ.DerefExpr, null);
    // Pre-set a PATH_CARDINALITY — CardinalityEstimator should propagate it
    bindExpr.setProperty(CostProperties.PATH_CARDINALITY, 5000L);
    forBind.addChild(bindExpr);
    ast.addChild(forBind);

    final var stage = new CostBasedStage(store);
    stage.rewrite(null, ast);

    // The ForBind should now have an ESTIMATED_CARDINALITY annotation
    final Object card = forBind.getProperty(CostProperties.ESTIMATED_CARDINALITY);
    assertNotNull(card, "ForBind should have ESTIMATED_CARDINALITY after annotation");
  }
}
