/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.bench.clickbench;

import org.junit.jupiter.api.Test;

import java.util.EnumSet;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

final class ClickBenchRunMainServingEvidenceTest {

  @Test
  void deltaNamesEveryOutcomeCounterThatMoved() {
    final ClickBenchRunMain.ServingRoute[] routes = ClickBenchRunMain.ServingRoute.values();
    final long[] before = new long[routes.length];
    final long[] after = new long[routes.length];
    after[ClickBenchRunMain.ServingRoute.PROJECTION_COUNT_DISTINCT.ordinal()] = 1L;
    after[ClickBenchRunMain.ServingRoute.GROUP_AGGREGATE.ordinal()] = 3L;

    final EnumSet<ClickBenchRunMain.ServingRoute> delta = ClickBenchRunMain.servingDelta(before, after);

    assertEquals(EnumSet.of(ClickBenchRunMain.ServingRoute.PROJECTION_COUNT_DISTINCT,
        ClickBenchRunMain.ServingRoute.GROUP_AGGREGATE), delta);
    assertEquals("route=projection-count-distinct+group-aggregate", ClickBenchRunMain.formatServingRoutes(true, delta));
  }

  @Test
  void malformedOrNonMonotonicSnapshotsFailClosed() {
    final int width = ClickBenchRunMain.ServingRoute.values().length;
    assertThrows(IllegalArgumentException.class,
        () -> ClickBenchRunMain.servingDelta(new long[width - 1], new long[width]));

    final long[] before = new long[width];
    final long[] after = new long[width];
    before[ClickBenchRunMain.ServingRoute.PREDICATE_COUNT.ordinal()] = 2L;
    after[ClickBenchRunMain.ServingRoute.PREDICATE_COUNT.ordinal()] = 1L;
    assertThrows(IllegalStateException.class, () -> ClickBenchRunMain.servingDelta(before, after));
  }

  @Test
  void q0RequiresStoredCardinalityAndEveryOtherQueryRequiresVectorizedEvidence() {
    final Set<ClickBenchRunMain.ServingRoute> none = EnumSet.noneOf(ClickBenchRunMain.ServingRoute.class);
    final Set<ClickBenchRunMain.ServingRoute> vectorized = EnumSet.of(ClickBenchRunMain.ServingRoute.PREDICATE_COUNT);
    final Set<ClickBenchRunMain.ServingRoute> structural =
        EnumSet.of(ClickBenchRunMain.ServingRoute.STRUCTURAL_ARRAY_SIZE);

    for (final ClickBenchQueries.Query query : ClickBenchQueries.all()) {
      assertNotNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
          query.index(), true, none), () -> "q" + query.index() + " accepted a silent generic decline");
      if (query.index() == 0) {
        assertNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            query.index(), true, structural), "q0 rejected stored-cardinality evidence");
        assertNotNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            query.index(), true, vectorized), "q0 accepted an unrelated vectorized route");
      } else {
        assertNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            query.index(), true, vectorized), () -> "q" + query.index() + " rejected vectorized evidence");
        assertNotNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            query.index(), true, structural), () -> "q" + query.index() + " accepted only structural evidence");
      }
      assertNotNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
          query.index(), false, query.index() == 0
              ? structural
              : vectorized),
          () -> "q" + query.index() + " accepted an incomplete run");
    }
    assertEquals(43, ClickBenchQueries.all().size());
  }

  @Test
  void genericProofRejectsVectorizedRoutesButAllowsTheStructuralRewrite() {
    final Set<ClickBenchRunMain.ServingRoute> none = EnumSet.noneOf(ClickBenchRunMain.ServingRoute.class);
    final Set<ClickBenchRunMain.ServingRoute> served = EnumSet.of(ClickBenchRunMain.ServingRoute.SORTED_SCAN);
    final Set<ClickBenchRunMain.ServingRoute> structural =
        EnumSet.of(ClickBenchRunMain.ServingRoute.STRUCTURAL_ARRAY_SIZE);

    assertNull(ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_GENERIC, 12, true, none));
    assertNotNull(
        ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_GENERIC, 12, true, served));
    assertNull(
        ClickBenchRunMain.servingProofFailure(ClickBenchRunMain.ServingProof.REQUIRE_GENERIC, 0, true, structural));
    assertEquals("route=generic", ClickBenchRunMain.formatServingRoutes(false, none));
    assertEquals("route=structural-array-size", ClickBenchRunMain.formatServingRoutes(false, structural));
  }

  @Test
  void oneServedTryCannotMaskALaterGenericDecline() {
    final Set<ClickBenchRunMain.ServingRoute> served = EnumSet.of(ClickBenchRunMain.ServingRoute.GROUP_AGGREGATE);
    final Set<ClickBenchRunMain.ServingRoute> none = EnumSet.noneOf(ClickBenchRunMain.ServingRoute.class);

    assertNull(
        ClickBenchRunMain.servingProofFailureForTry(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED, 18, 0, served));
    final String failure =
        ClickBenchRunMain.servingProofFailureForTry(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED, 18, 1, none);
    assertNotNull(failure, "try 1 evidence must not certify a route-less try 2");
    assertTrue(failure.endsWith("on try 2"));
    assertThrows(IllegalArgumentException.class,
        () -> ClickBenchRunMain.servingProofFailureForTry(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED, 18, -1,
            served));
  }

  @Test
  void proofModeMustAgreeWithTheAutoVectorizeSwitchAndNumberedSuite() {
    ClickBenchRunMain.validateServingProofConfiguration(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED, true, false);
    ClickBenchRunMain.validateServingProofConfiguration(ClickBenchRunMain.ServingProof.REQUIRE_GENERIC, false, false);

    assertThrows(IllegalArgumentException.class,
        () -> ClickBenchRunMain.validateServingProofConfiguration(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            false, false));
    assertThrows(IllegalArgumentException.class,
        () -> ClickBenchRunMain.validateServingProofConfiguration(ClickBenchRunMain.ServingProof.REQUIRE_GENERIC, true,
            false));
    assertThrows(IllegalArgumentException.class,
        () -> ClickBenchRunMain.validateServingProofConfiguration(ClickBenchRunMain.ServingProof.REQUIRE_VECTORIZED,
            true, true));
  }

  @Test
  void routeLabelsAreStableAndUniqueForMachineReadableEvidence() {
    final Set<String> labels = new HashSet<>();
    for (final ClickBenchRunMain.ServingRoute route : ClickBenchRunMain.ServingRoute.values()) {
      assertTrue(labels.add(route.label()), () -> "duplicate route label " + route.label());
    }
    assertTrue(labels.contains("projection-count-distinct"),
        "Q4/Q5 need an outcome counter distinct from a projection lookup or attempt");
    assertTrue(labels.contains("structural-array-size"),
        "Q0 needs successful stored-cardinality evidence distinct from a compile-time rewrite attempt");
  }
}
