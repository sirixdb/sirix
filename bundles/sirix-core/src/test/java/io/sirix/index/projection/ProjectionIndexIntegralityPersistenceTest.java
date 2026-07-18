/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Persistence of NUMERIC_LONG integrality provenance in the projection leaf
 * presence tail (flag bit1). Historically the "did this column ever see a
 * truncated non-integral value?" flags lived only in
 * {@link ProjectionIndexBuilder} memory, so a close/re-open (hydrating
 * leaves from HOT storage) lost them — {@code numericColumnIsIntegral}
 * returned {@code false} and every sum/avg/min/max aggregate silently fell
 * back from the projection to a full storage scan. The flags now persist in
 * each leaf's tail; the registry handle re-derives them from the bytes when
 * the installer cannot supply builder-tracked flags.
 *
 * <p>Fail-closed invariants under test:
 * <ul>
 * <li>leaves round-trip the per-column non-integral bit;</li>
 * <li>tail-less payloads are rejected by {@code deserialize} as corrupt —
 * provenance is never fabricated;</li>
 * <li>a single malformed leaf poisons the whole probe (it might hide a
 * truncated value).</li>
 * </ul>
 */
public final class ProjectionIndexIntegralityPersistenceTest {

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG
  };

  private static final String[] FIELD_NAMES = {"age", "active", "amount"};

  /**
   * Build a leaf of {@code rows} dense rows; when {@code col2NonIntegral},
   * every fifth row's second numeric column is marked as truncated from a
   * non-integral number (the value slot itself always carries a long — the
   * flag records provenance, not representation).
   */
  private static ProjectionIndexLeafPage leaf(final int rows, final boolean col2NonIntegral) {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = {true, true, true};
    final boolean[] unrep = new boolean[3];
    final boolean[] nonIntegral = new boolean[3];
    for (int i = 0; i < rows; i++) {
      longs[0] = 18 + (i % 48);
      bools[1] = i % 2 == 0;
      longs[2] = i * 10L;
      nonIntegral[2] = col2NonIntegral && i % 5 == 0;
      assertTrue(page.appendRow(1000 + i, longs, bools, strings, present, unrep, nonIntegral));
    }
    return page;
  }

  private static int intLE(final byte[] b, final int off) {
    return (b[off] & 0xFF) | ((b[off + 1] & 0xFF) << 8) | ((b[off + 2] & 0xFF) << 16) | ((b[off + 3] & 0xFF) << 24);
  }

  /** Strip the mandatory presence tail — a corrupt/truncated payload. */
  private static byte[] stripTail(final byte[] payload) {
    final int tailLen = intLE(payload, payload.length - 8);
    return Arrays.copyOf(payload, payload.length - 8 - tailLen);
  }

  // ==================== leaf round-trip ====================

  @Test
  void freshLeafRoundTripsIntegralColumns() {
    final byte[] payload = leaf(100, false).serialize();
    assertEquals(ProjectionIndexLeafPage.PRESENCE_TAIL_MAGIC, intLE(payload, payload.length - 4));

    final ProjectionIndexLeafPage back = ProjectionIndexLeafPage.deserialize(payload);
    for (int c = 0; c < KINDS.length; c++) {
      assertFalse(back.columnNumericNonIntegral(c), "column " + c + " must stay provably integral");
    }
  }

  @Test
  void nonIntegralFlagSurvivesRoundTrip() {
    final byte[] payload = leaf(100, true).serialize();
    final ProjectionIndexLeafPage back = ProjectionIndexLeafPage.deserialize(payload);
    assertFalse(back.columnNumericNonIntegral(0));
    assertTrue(back.columnNumericNonIntegral(2));
    // Re-serialization preserves the flag bytes exactly.
    assertArrayEquals(payload, back.serialize());
  }

  @Test
  void tailLessPayloadIsRejected() {
    final byte[] truncated = stripTail(leaf(64, true).serialize());
    assertThrows(IllegalStateException.class, () -> ProjectionIndexLeafPage.deserialize(truncated));
  }

  // ==================== byte-level probe ====================

  @Test
  void probeRecoversFlagsFromPersistedLeaves() {
    final List<byte[]> leaves = List.of(leaf(100, false).serialize(), leaf(80, true).serialize());
    final boolean[] nonIntegral = ProjectionIndexByteScan.probeNumericNonIntegral(leaves);
    assertArrayEquals(new boolean[] {false, false, true}, nonIntegral,
        "flags must OR across leaves");
  }

  @Test
  void probeFailsClosedWhenAnyLeafIsMalformed() {
    final byte[] tailed = leaf(100, false).serialize();
    assertNull(ProjectionIndexByteScan.probeNumericNonIntegral(List.of(tailed, stripTail(tailed))));
    assertNull(ProjectionIndexByteScan.probeNumericNonIntegral(List.of(stripTail(tailed))));
  }

  @Test
  void probeOfEmptyLeafListIsZeroColumns() {
    assertEquals(0, ProjectionIndexByteScan.probeNumericNonIntegral(List.of()).length);
  }

  // ==================== registry handle (close/re-open path) ====================

  @Test
  void handleWithoutInstallerFlagsRederivesIntegralityFromPersistedBytes() {
    // Simulates the hydrate-after-reopen path: leaves come back from HOT
    // storage, the builder (and its flags) are long gone.
    final List<byte[]> leaves = List.of(leaf(100, false).serialize(), leaf(80, true).serialize());
    final ProjectionIndexRegistry.Handle handle =
        new ProjectionIndexRegistry.Handle(FIELD_NAMES, leaves);
    assertTrue(handle.numericColumnIsIntegral(0), "age column must stay aggregate-servable after re-open");
    assertFalse(handle.numericColumnIsIntegral(2), "amount column saw truncated values");
    assertTrue(handle.numericColumnKnownNonIntegral(2));
    assertFalse(handle.numericColumnKnownNonIntegral(0));
  }

  @Test
  void handleFailsClosedOnMalformedLeaves() {
    final byte[] tailed = leaf(100, false).serialize();
    final ProjectionIndexRegistry.Handle handle =
        new ProjectionIndexRegistry.Handle(FIELD_NAMES, List.of(tailed, stripTail(tailed)));
    assertFalse(handle.numericColumnIsIntegral(0), "a malformed leaf must resolve the handle to UNKNOWN");
    assertFalse(handle.numericColumnKnownNonIntegral(0));
  }

  @Test
  void installerProvidedFlagsStillWin() {
    // Builder-tracked flags say non-integral even though the (synthetic)
    // leaves look clean — explicit evidence must not be overridden by probing.
    final List<byte[]> leaves = List.of(leaf(100, false).serialize());
    final ProjectionIndexRegistry.Handle handle =
        new ProjectionIndexRegistry.Handle(FIELD_NAMES, leaves, new boolean[] {false, false, true});
    assertTrue(handle.numericColumnIsIntegral(0));
    assertFalse(handle.numericColumnIsIntegral(2));
  }
}
