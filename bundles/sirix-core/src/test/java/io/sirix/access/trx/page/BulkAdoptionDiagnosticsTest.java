/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.access.trx.page;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

/** The encoding diagnostic is a cumulative record count, independent of carrier-counter resets. */
final class BulkAdoptionDiagnosticsTest {
  @Test
  void pendingStringCountsAccumulateAndSurviveCarrierCounterResets() {
    final long before = BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization();
    BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization(3);
    assertEquals(before + 3, BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization());
    BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization(0);
    assertEquals(before + 3, BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization());
    BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization(5);
    assertEquals(before + 8, BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization());
    BulkAdoptionDiagnostics.reset();
    assertEquals(before + 8, BulkAdoptionDiagnostics.pendingStringsEncodedBeforeMaterialization());
  }
}
