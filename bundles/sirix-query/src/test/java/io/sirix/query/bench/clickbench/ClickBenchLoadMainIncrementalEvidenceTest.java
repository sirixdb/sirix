/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClickBenchLoadMainIncrementalEvidenceTest {

  @Test
  void reportsPerLoadDeltasAndAllowsCanonicalFrontierSplices() {
    final ClickBenchLoadMain.HotMutationCounters before =
        new ClickBenchLoadMain.HotMutationCounters(17L, 23L, 5L, 7L, 11L);
    final ClickBenchLoadMain.HotMutationCounters after =
        new ClickBenchLoadMain.HotMutationCounters(20L, 23L, 5L, 7L, 12L);

    final ClickBenchLoadMain.HotMutationDeltas deltas = before.deltasTo(after);

    assertEquals("# HOT_INCREMENTAL_DELTAS COMPLETE_STRUCTURAL_FRONTIER_SPLICE=3 STRUCTURAL_VALIDATION_FAILURE=0 "
        + "STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE=0 MUTATION_TRAVERSAL_REFUSED=0 "
        + "STRUCTURAL_VALIDATION_OVERSIZE_SKIPPED=1", deltas.logLine());
    assertDoesNotThrow(deltas::requireHealthyIncrementalMutations,
        "a canonical frontier splice and a bounded diagnostic skip are not mutation failures");
  }

  @Test
  void rejectsPublishedStructuralValidationFailure() {
    final ClickBenchLoadMain.HotMutationCounters before =
        new ClickBenchLoadMain.HotMutationCounters(100L, 40L, 7L, 3L, 9L);
    final ClickBenchLoadMain.HotMutationCounters after =
        new ClickBenchLoadMain.HotMutationCounters(100L, 41L, 7L, 3L, 9L);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> before.deltasTo(after).requireHealthyIncrementalMutations());

    assertTrue(failure.getMessage().contains("STRUCTURAL_VALIDATION_FAILURE=1"), failure::getMessage);
  }

  @Test
  void rejectsPropagationPreflightFailureAndTraversalRefusal() {
    final ClickBenchLoadMain.HotMutationCounters before =
        new ClickBenchLoadMain.HotMutationCounters(100L, 40L, 7L, 3L, 9L);
    final ClickBenchLoadMain.HotMutationCounters after =
        new ClickBenchLoadMain.HotMutationCounters(101L, 40L, 8L, 4L, 9L);

    final IllegalStateException failure =
        assertThrows(IllegalStateException.class, () -> before.deltasTo(after).requireHealthyIncrementalMutations());

    assertTrue(failure.getMessage().contains("STRUCTURAL_PROPAGATION_PREFLIGHT_FAILURE=1"), failure::getMessage);
    assertTrue(failure.getMessage().contains("MUTATION_TRAVERSAL_REFUSED=1"), failure::getMessage);
  }

  @Test
  void rejectsADecreasingProcessCounterInsteadOfPublishingFalseEvidence() {
    final ClickBenchLoadMain.HotMutationCounters before =
        new ClickBenchLoadMain.HotMutationCounters(100L, 40L, 7L, 3L, 9L);
    final ClickBenchLoadMain.HotMutationCounters after =
        new ClickBenchLoadMain.HotMutationCounters(99L, 40L, 7L, 3L, 9L);

    final IllegalStateException failure = assertThrows(IllegalStateException.class, () -> before.deltasTo(after));

    assertTrue(failure.getMessage().contains("COMPLETE_STRUCTURAL_FRONTIER_SPLICE decreased"), failure::getMessage);
  }
}
