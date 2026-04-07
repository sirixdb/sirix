package io.sirix.query.compiler.optimizer;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for {@link CardinalityTracker} — adaptive re-optimization via
 * cardinality drift detection.
 */
final class CardinalityTrackerTest {

  private final PlanCache planCache = new PlanCache();
  private final CardinalityTracker tracker = CardinalityTracker.getInstance();

  @BeforeEach
  void setUp() {
    tracker.clear();
    planCache.clear();
  }

  @AfterEach
  void tearDown() {
    tracker.clear();
  }

  @Test
  void accurateEstimateDoesNotInvalidate() {
    // Cache a plan
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query1@v0", ast);

    // Record accurate execution: estimated 100, actual 95 → ratio < 10x
    tracker.record("query1@v0", 100, 95, planCache);
    tracker.record("query1@v0", 100, 95, planCache);
    tracker.record("query1@v0", 100, 95, planCache);

    // Plan should still be cached
    assertNotNull(planCache.get("query1@v0"),
        "Accurate estimates should not invalidate cached plan");
    assertEquals(0, tracker.trackedKeyCount(),
        "Accurate estimates should not track drift state");
  }

  @Test
  void singleMismatchDoesNotInvalidate() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query2@v0", ast);

    // One 10x mismatch — not enough to invalidate
    tracker.record("query2@v0", 100, 5000, planCache);

    assertNotNull(planCache.get("query2@v0"),
        "Single mismatch should not invalidate (damping)");
    assertEquals(1, tracker.trackedKeyCount(),
        "Should track drift state after first mismatch");
  }

  @Test
  void consecutiveMismatchesInvalidatePlan() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query3@v0", ast);

    // 3 consecutive 10x+ mismatches
    tracker.record("query3@v0", 100, 5000, planCache);
    tracker.record("query3@v0", 100, 5000, planCache);
    assertNotNull(planCache.get("query3@v0"),
        "Plan should still exist after 2 mismatches");

    tracker.record("query3@v0", 100, 5000, planCache);
    assertNull(planCache.get("query3@v0"),
        "Plan should be invalidated after 3 consecutive mismatches");
    assertEquals(0, tracker.trackedKeyCount(),
        "Drift state should be cleared after invalidation");
  }

  @Test
  void accurateExecutionResetsConsecutiveCount() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query4@v0", ast);

    // 2 mismatches, then 1 accurate → resets counter
    tracker.record("query4@v0", 100, 5000, planCache);
    tracker.record("query4@v0", 100, 5000, planCache);
    tracker.record("query4@v0", 100, 100, planCache); // accurate — resets

    // 2 more mismatches — not enough (counter was reset)
    tracker.record("query4@v0", 100, 5000, planCache);
    tracker.record("query4@v0", 100, 5000, planCache);

    assertNotNull(planCache.get("query4@v0"),
        "Plan should not be invalidated — accurate execution reset the counter");
  }

  @Test
  void underestimationDetected() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query5@v0", ast);

    // Estimated 10, actual 1000 → 100x underestimation
    for (int i = 0; i < CardinalityTracker.DRIFT_CONSECUTIVE_THRESHOLD; i++) {
      tracker.record("query5@v0", 10, 1000, planCache);
    }

    assertNull(planCache.get("query5@v0"),
        "Underestimation should also trigger invalidation");
  }

  @Test
  void overestimationDetected() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query6@v0", ast);

    // Estimated 10000, actual 5 → 2000x overestimation
    for (int i = 0; i < CardinalityTracker.DRIFT_CONSECUTIVE_THRESHOLD; i++) {
      tracker.record("query6@v0", 10000, 5, planCache);
    }

    assertNull(planCache.get("query6@v0"),
        "Overestimation should also trigger invalidation");
  }

  @Test
  void invalidEstimatesIgnored() {
    final var ast = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("query7@v0", ast);

    // Invalid inputs: -1 estimated, null cacheKey
    tracker.record("query7@v0", -1, 100, planCache);
    tracker.record(null, 100, 100, planCache);
    tracker.record("query7@v0", 0, 100, planCache);

    assertNotNull(planCache.get("query7@v0"),
        "Invalid inputs should be silently ignored");
    assertEquals(0, tracker.trackedKeyCount());
  }

  @Test
  void independentQueriesTrackedSeparately() {
    final var ast1 = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    final var ast2 = new io.brackit.query.compiler.AST(io.brackit.query.compiler.XQ.FlowrExpr, null);
    planCache.put("queryA@v0", ast1);
    planCache.put("queryB@v0", ast2);

    // queryA gets 3 mismatches → invalidated
    for (int i = 0; i < 3; i++) {
      tracker.record("queryA@v0", 10, 5000, planCache);
    }

    // queryB only gets 1 mismatch → still cached
    tracker.record("queryB@v0", 10, 5000, planCache);

    assertNull(planCache.get("queryA@v0"), "queryA should be invalidated");
    assertNotNull(planCache.get("queryB@v0"), "queryB should still be cached");
  }
}
