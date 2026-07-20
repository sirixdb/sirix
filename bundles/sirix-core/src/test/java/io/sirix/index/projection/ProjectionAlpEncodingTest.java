/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * ALP encoding for double BODY streams (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §11-6):
 * bit-exact round-trips through the width-65 escape, verbatim exception carriage, strict
 * profitability (never fatter than plain FOR), determinism (the descriptor-hash no-op
 * contract), and loud corrupt-payload rejection.
 */
final class ProjectionAlpEncodingTest {

  /** Encode transform-domain cells through the double encoder, decode via the shared entry. */
  private static long[] roundTrip(final long[] cells, final int rowCount) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    ProjectionIndexLeafCodec.encodeForBitPackedDouble(out, cells, rowCount);
    final ProjectionIndexLeafCodec.Cursor in = new ProjectionIndexLeafCodec.Cursor(out.toByteArray(), 0);
    return ProjectionIndexLeafCodec.decodeForBitPackedColumn(in, rowCount);
  }

  private static byte[] encodedBytes(final long[] cells, final int rowCount) {
    final ByteArrayOutputStream out = new ByteArrayOutputStream();
    ProjectionIndexLeafCodec.encodeForBitPackedDouble(out, cells, rowCount);
    return out.toByteArray();
  }

  private static long[] transformOf(final double[] values) {
    final long[] cells = new long[values.length];
    for (int i = 0; i < values.length; i++) {
      cells[i] = ProjectionDoubleEncoding.encode(values[i]);
    }
    return cells;
  }

  /** Price-like tenths — the classic ALP win (tenths are NOT exact binary fractions). */
  private static double[] tenths(final int n) {
    final double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = BigDecimal.valueOf(i - n / 2, 1).doubleValue(); // (i - n/2) / 10.0, exact decimal
    }
    return values;
  }

  @Test
  void decimalVectorRoundTripsAndShrinks() {
    final int n = 1024;
    final long[] cells = transformOf(tenths(n));

    final byte[] alp = encodedBytes(cells, n);
    assertEquals(ProjectionAlpEncoding.WIDTH_ESCAPE_ALP, alp[8] & 0xFF,
        "tenths must take the ALP escape");
    assertArrayEquals(cells, roundTrip(cells, n), "ALP must round-trip bit-exactly");

    final ByteArrayOutputStream plain = new ByteArrayOutputStream();
    ProjectionIndexLeafCodec.encodeForBitPacked(plain, cells, n);
    assertTrue(alp.length < plain.size(),
        "ALP (" + alp.length + " B) must beat plain FOR (" + plain.size() + " B) on decimals");
  }

  @Test
  void binaryFractionsFallBackToPlainFor() {
    final int n = 512;
    final double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = Math.PI * (i + 1); // no decimal scale round-trips π multiples
    }
    final long[] cells = transformOf(values);
    final byte[] bytes = encodedBytes(cells, n);
    assertTrue((bytes[8] & 0xFF) <= 64, "π multiples must fall back to plain FOR");
    assertArrayEquals(cells, roundTrip(cells, n));
  }

  @Test
  void fewExceptionsAreCarriedVerbatim() {
    final int n = 1024;
    final double[] values = tenths(n);
    for (int i = 0; i < n; i += 100) {
      values[i] = Math.PI * (i + 1); // sprinkle non-decimals below the 1/8 exception cap
    }
    final long[] cells = transformOf(values);
    final byte[] bytes = encodedBytes(cells, n);
    assertEquals(ProjectionAlpEncoding.WIDTH_ESCAPE_ALP, bytes[8] & 0xFF,
        "mostly-decimal data must still take ALP");
    assertArrayEquals(cells, roundTrip(cells, n), "exceptions must round-trip bit-exactly");
  }

  @Test
  void hugeMagnitudesRejectAlp() {
    final int n = 256;
    final double[] values = new double[n];
    for (int i = 0; i < n; i++) {
      values[i] = 1.0e300 * (i + 1);
    }
    final long[] cells = transformOf(values);
    // 1e300·k is scaled beyond 2^53 for every (e, f) with e ≥ f, so every pair overflows
    // the digits grid — the guard must reject rather than mis-round.
    assertNull(ProjectionAlpEncoding.tryEncode(cells, n, Integer.MAX_VALUE));
    assertArrayEquals(cells, roundTrip(cells, n), "fallback must stay bit-exact");
  }

  @Test
  void negativeZeroLandsInExceptions() {
    // -0.0: digits = round(-0.0 · scale) = 0, and 0 · descale = +0.0 ≠ -0.0 bitwise —
    // the verify step must route it to the exception list, not silently flip the sign.
    final int n = 64;
    final double[] values = tenths(n);
    values[7] = -0.0;
    final long[] cells = transformOf(values);
    assertEquals(ProjectionAlpEncoding.WIDTH_ESCAPE_ALP, encodedBytes(cells, n)[8] & 0xFF,
        "the vector must take ALP, or the exception-path claim below is tested vacuously");
    final long[] decoded = roundTrip(cells, n);
    assertArrayEquals(cells, decoded);
    assertEquals(ProjectionDoubleEncoding.encode(-0.0), decoded[7], "-0.0 must survive bitwise");
  }

  @Test
  void encodingIsDeterministic() {
    final int n = 1024;
    final long[] cells = transformOf(tenths(n));
    assertArrayEquals(encodedBytes(cells, n), encodedBytes(cells, n),
        "identical cells must produce identical bytes (descriptor-hash no-op contract)");
  }

  @Test
  void corruptPayloadsRejectLoudly() {
    final int n = 16;
    final long[] cells = transformOf(tenths(n));
    final byte[] good = encodedBytes(cells, n);
    assertEquals(ProjectionAlpEncoding.WIDTH_ESCAPE_ALP, good[8] & 0xFF);

    final byte[] badScale = good.clone();
    badScale[9] = 19; // e > 18
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexLeafCodec.decodeForBitPackedColumn(
            new ProjectionIndexLeafCodec.Cursor(badScale, 0), n));

    final byte[] badCount = good.clone();
    badCount[11] = (byte) 0xFF; // exceptionCount low byte → > rowCount
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexLeafCodec.decodeForBitPackedColumn(
            new ProjectionIndexLeafCodec.Cursor(badCount, 0), n));

    final byte[] reserved = good.clone();
    reserved[8] = 66; // reserved escape past ALP
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexLeafCodec.decodeForBitPackedColumn(
            new ProjectionIndexLeafCodec.Cursor(reserved, 0), n));

    final byte[] nestedEscape = good.clone();
    nestedEscape[23] = 65; // digits-stream forWidth byte — an escape INSIDE ALP is corruption
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexLeafCodec.decodeForBitPackedColumn(
            new ProjectionIndexLeafCodec.Cursor(nestedEscape, 0), n),
        "nested escapes must reject, never recurse into a second ALP header");

    assertNotNull(roundTrip(cells, n), "untouched payload still decodes");
  }
}
