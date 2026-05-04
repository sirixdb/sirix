/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.compiler.vectorized;

import io.sirix.page.pax.StringRegion;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.foreign.MemorySegment;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * End-to-end tests for the iter#23 QuestDB-style StringRegion fast path inside
 * {@link ColumnarStringFilter}. Builds an in-memory StringRegion + a parallel
 * uncompressed page payload, threads them through {@link ColumnBatch}, and
 * asserts that the filter's selection-vector output matches the byte-equality
 * baseline regardless of whether the literal hits or misses any tag dict.
 */
@DisplayName("ColumnarStringFilter / StringRegion fast path")
final class ColumnarStringFilterDictIdTest {

  private static byte[] bytes(final String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  /** Row layout: each value's UTF-8 bytes packed back-to-back, no padding. */
  private static byte[] packPage(final String[] values) {
    int total = 0;
    for (final String v : values) total += bytes(v).length;
    final byte[] out = new byte[total];
    int pos = 0;
    for (final String v : values) {
      final byte[] b = bytes(v);
      System.arraycopy(b, 0, out, pos, b.length);
      pos += b.length;
    }
    return out;
  }

  private static int[] computeOffsets(final String[] values) {
    final int[] offs = new int[values.length];
    int pos = 0;
    for (int i = 0; i < values.length; i++) {
      offs[i] = pos;
      pos += bytes(values[i]).length;
    }
    return offs;
  }

  @Test
  @DisplayName("literal absent in StringRegion → EQ produces empty selection (whole-page short-circuit)")
  void literalAbsentEqShortCircuits() {
    // Build a region whose dict for tag T contains {"Eng","Sales"} only.
    final int tag = 7;
    final String[] values = {"Eng", "Sales", "Eng", "Eng", "Sales"};
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    for (final String v : values) enc.addValue(tag, bytes(v));
    final byte[] regionPayload = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(regionPayload);

    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData),
        /*parsedSymbols=*/null, h, regionPayload);

    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    // "Marketing" never seen on this page → fast-path drops every row for EQ.
    ColumnarStringFilter.filterStringEqual(batch, 0, "Marketing");
    assertEquals(0, batch.selectionCount());
  }

  @Test
  @DisplayName("literal absent in StringRegion → NE keeps every non-null row")
  void literalAbsentNeKeepsAll() {
    final int tag = 7;
    final String[] values = {"Eng", "Sales", "Eng"};
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    for (final String v : values) enc.addValue(tag, bytes(v));
    final byte[] regionPayload = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(regionPayload);

    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData),
        null, h, regionPayload);

    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    ColumnarStringFilter.filterStringNotEqual(batch, 0, "Marketing");
    assertEquals(values.length, batch.selectionCount());
  }

  @Test
  @DisplayName("literal present in StringRegion → falls through to byte-compare path")
  void literalPresentByteCompare() {
    final int tag = 7;
    final String[] values = {"Eng", "Sales", "Eng", "Mkt"};
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    for (final String v : values) enc.addValue(tag, bytes(v));
    final byte[] regionPayload = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(regionPayload);

    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData),
        null, h, regionPayload);
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, "Eng");
    assertEquals(2, batch.selectionCount());
    assertEquals(0, batch.selectedRow(0));
    assertEquals(2, batch.selectedRow(1));
  }

  @Test
  @DisplayName("StringRegion absent → behaviour identical to legacy FSST/byte path")
  void noStringRegionFallsBack() {
    final String[] values = {"Eng", "Sales", "Eng"};
    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    // 2-arg overload: no StringRegion
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData), null);
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    assertTrue(!batch.hasStringRegion(0));

    ColumnarStringFilter.filterStringEqual(batch, 0, "Sales");
    assertEquals(1, batch.selectionCount());
    assertEquals(1, batch.selectedRow(0));
  }

  @Test
  @DisplayName("multi-tag region: literal hits only one tag → result still byte-compares")
  void literalHitsOneTagOnly() {
    final int deptTag = 7, cityTag = 11;
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    // Mix in encode order; the page's row order also mixes them.
    enc.addValue(deptTag, bytes("Eng"));
    enc.addValue(cityTag, bytes("NYC"));
    enc.addValue(deptTag, bytes("Sales"));
    enc.addValue(cityTag, bytes("LA"));
    final byte[] regionPayload = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(regionPayload);

    final String[] values = {"Eng", "NYC", "Sales", "LA"};
    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData),
        null, h, regionPayload);
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    // "NYC" is in the city dict only — the literal IS present in the region.
    ColumnarStringFilter.filterStringEqual(batch, 0, "NYC");
    assertEquals(1, batch.selectionCount());
    assertEquals(1, batch.selectedRow(0));
  }

  @Test
  @DisplayName("region count mismatch → safety guard skips fast-path, byte-compare still works")
  void regionCountMismatchFallsBack() {
    final int tag = 7;
    final StringRegion.Encoder enc = new StringRegion.Encoder();
    enc.addValue(tag, bytes("Eng"));
    enc.addValue(tag, bytes("Sales"));
    final byte[] regionPayload = enc.finish();
    final StringRegion.Header h = new StringRegion.Header().parseInto(regionPayload);
    assertEquals(2, h.count);

    // Batch contributes 3 rows from this page — region only covers 2.
    // Fast path must NOT short-circuit even though "Marketing" is absent.
    // It must fall through to byte-compare.
    final String[] values = {"Eng", "Sales", "Marketing"};
    final byte[] pageData = packPage(values);
    final int[] offs = computeOffsets(values);

    final ColumnBatch batch = new ColumnBatch(8, 1);
    batch.setColumnType(0, ColumnType.DEFERRED_BYTES);
    final int pgIdx = batch.addBackingPage(0, MemorySegment.ofArray(pageData),
        null, h, regionPayload);
    for (int i = 0; i < values.length; i++) {
      batch.setDeferredBytes(0, i, pgIdx, offs[i], bytes(values[i]).length, false);
    }
    batch.setRowCount(values.length);
    batch.seal();

    ColumnarStringFilter.filterStringEqual(batch, 0, "Marketing");
    // Without the safety guard, a buggy fast path would short-circuit to 0; a correct
    // fall-through finds the byte-equal row.
    assertEquals(1, batch.selectionCount());
    assertEquals(2, batch.selectedRow(0));
  }
}
