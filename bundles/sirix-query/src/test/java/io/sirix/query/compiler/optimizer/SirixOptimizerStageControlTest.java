package io.sirix.query.compiler.optimizer;

import io.brackit.query.compiler.optimizer.Stage;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link SirixOptimizer}'s stage disable/enable mechanism.
 */
final class SirixOptimizerStageControlTest {

  @Test
  @DisplayName("All stages are enabled by default")
  void allStagesEnabledByDefault() {
    final var optimizer = new SirixOptimizer(null, null, null);

    assertTrue(optimizer.isStageEnabled(JqgmRewriteStage.class));
    assertTrue(optimizer.isStageEnabled(CostBasedStage.class));
    assertTrue(optimizer.isStageEnabled(JoinReorderStage.class));
    assertTrue(optimizer.isStageEnabled(MeshPopulationStage.class));
    assertTrue(optimizer.isStageEnabled(MeshSelectionStage.class));
    assertTrue(optimizer.isStageEnabled(IndexDecompositionStage.class));
    assertTrue(optimizer.isStageEnabled(CostDrivenRoutingStage.class));
    assertTrue(optimizer.isStageEnabled(VectorizedDetectionStage.class));
    assertTrue(optimizer.isStageEnabled(VectorizedRoutingStage.class));
  }

  @Test
  @DisplayName("disableStage marks stage as disabled")
  void disableStageWorks() {
    final var optimizer = new SirixOptimizer(null, null, null);

    optimizer.disableStage(CostBasedStage.class);

    assertFalse(optimizer.isStageEnabled(CostBasedStage.class));
    // Other stages should still be enabled
    assertTrue(optimizer.isStageEnabled(JqgmRewriteStage.class));
  }

  @Test
  @DisplayName("enableStage re-enables a disabled stage")
  void enableStageWorks() {
    final var optimizer = new SirixOptimizer(null, null, null);

    optimizer.disableStage(VectorizedRoutingStage.class);
    assertFalse(optimizer.isStageEnabled(VectorizedRoutingStage.class));

    optimizer.enableStage(VectorizedRoutingStage.class);
    assertTrue(optimizer.isStageEnabled(VectorizedRoutingStage.class));
  }

  @Test
  @DisplayName("Multiple stages can be disabled independently")
  void multipleStagesDisabled() {
    final var optimizer = new SirixOptimizer(null, null, null);

    optimizer.disableStage(MeshPopulationStage.class);
    optimizer.disableStage(MeshSelectionStage.class);
    optimizer.disableStage(VectorizedDetectionStage.class);

    assertFalse(optimizer.isStageEnabled(MeshPopulationStage.class));
    assertFalse(optimizer.isStageEnabled(MeshSelectionStage.class));
    assertFalse(optimizer.isStageEnabled(VectorizedDetectionStage.class));
    // Others still enabled
    assertTrue(optimizer.isStageEnabled(CostBasedStage.class));
    assertTrue(optimizer.isStageEnabled(JoinReorderStage.class));
  }

  @Test
  @DisplayName("Enabling a never-disabled stage is a no-op")
  void enableNeverDisabledIsNoOp() {
    final var optimizer = new SirixOptimizer(null, null, null);
    // Should not throw
    optimizer.enableStage(CostBasedStage.class);
    assertTrue(optimizer.isStageEnabled(CostBasedStage.class));
  }

  @Test
  @DisplayName("Disabling the same stage twice is idempotent")
  void disableTwiceIdempotent() {
    final var optimizer = new SirixOptimizer(null, null, null);
    optimizer.disableStage(JoinReorderStage.class);
    optimizer.disableStage(JoinReorderStage.class);
    assertFalse(optimizer.isStageEnabled(JoinReorderStage.class));

    optimizer.enableStage(JoinReorderStage.class);
    assertTrue(optimizer.isStageEnabled(JoinReorderStage.class));
  }

  @Test
  @DisplayName("Stage count includes parent and sirix stages")
  void stageCountCorrect() {
    final var optimizer = new SirixOptimizer(null, null, null);
    // TopDownOptimizer parent adds ~7 stages, SirixOptimizer adds 10 more
    assertTrue(optimizer.getStageCount() >= 10,
        "Should have at least 10 optimization stages (parent + sirix): " + optimizer.getStageCount());
  }
}
