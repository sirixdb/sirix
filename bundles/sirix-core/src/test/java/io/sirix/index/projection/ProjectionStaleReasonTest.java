/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexMetadata.StaleReason;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The durable stale-reason code: it must survive a serialize/parse round trip, stay compatible with
 * tombstones written before reasons existed, and never turn an unknown future reason into a parse
 * failure.
 *
 * <p>
 * The reason exists because "stale" and "corrupt" are different claims and the difference was being
 * lost (tasks #45, #50), and because #52 needs the same slot to say "still wanted, known
 * incomplete" rather than "retired". Both are wire values now, so the encoding is what has to be
 * pinned — a renumbering later would be a format migration.
 * </p>
 */
final class ProjectionStaleReasonTest {

  @Test
  void everyReasonSurvivesARoundTrip() {
    for (final StaleReason reason : StaleReason.values()) {
      final byte[] wire = ProjectionIndexMetadata.staleTombstone(reason).serialize();
      final ProjectionIndexMetadata parsed = ProjectionIndexMetadata.parse(wire);
      assertTrue(parsed.isStale(), reason + " must still read as stale");
      assertEquals(reason, parsed.staleReason(), reason + " must survive the round trip");
    }
  }

  /**
   * The bits carrying the reason were always zero before this existed, so an old tombstone must parse
   * as UNSPECIFIED rather than as anything else — that is what makes the change additive and lets it
   * skip a version bump.
   */
  @Test
  void aReasonlessTombstoneReadsAsUnspecified() {
    final ProjectionIndexMetadata parsed =
        ProjectionIndexMetadata.parse(ProjectionIndexMetadata.staleTombstone().serialize());
    assertTrue(parsed.isStale());
    assertEquals(StaleReason.UNSPECIFIED, parsed.staleReason());
  }

  /**
   * Wire values are append-only: pin the ordinals so a reorder fails here rather than in the field.
   */
  @Test
  void ordinalsArePinnedBecauseTheyAreWireValues() {
    assertEquals(0, StaleReason.UNSPECIFIED.ordinal());
    assertEquals(1, StaleReason.GLOBAL_DICTIONARY_NOT_MAINTAINABLE.ordinal());
    assertEquals(2, StaleReason.MAINTENANCE_FAILED.ordinal());
    assertEquals(3, StaleReason.KIND_INCONSISTENT_STORE.ordinal());
    assertEquals(4, StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED.ordinal());
    assertEquals(5, StaleReason.REBUILD_PENDING.ordinal());
    assertTrue(StaleReason.values().length <= 8, "only three flag bits carry the reason");
  }

  /**
   * Every reason must name a remedy someone can run. "Rebuild required" in the abstract makes the
   * operator rediscover what the writer already knew.
   */
  @Test
  void everyReasonNamesARunnableRemedy() {
    for (final StaleReason reason : StaleReason.values()) {
      final String remedy = reason.remedy();
      assertTrue(remedy.contains("jn:create-projection-index"),
          reason + " must name the actual call, not describe it: " + remedy);
    }
    // The budget breach has a second, cheaper remedy than rebuilding blind, and should say so.
    final String budget = StaleReason.GLOBAL_DICTIONARY_BUDGET_EXCEEDED.remedy();
    assertTrue(budget.contains("hint"), "the budget breach should mention the row-count hint: " + budget);
    assertNotEquals(StaleReason.MAINTENANCE_FAILED.remedy(), budget,
        "a breach and a failure are different situations and should not give identical advice");
  }

  @Test
  void aNullReasonIsRejectedRatherThanStoredAsZero() {
    assertThrows(NullPointerException.class, () -> ProjectionIndexMetadata.staleTombstone(null));
  }
}
