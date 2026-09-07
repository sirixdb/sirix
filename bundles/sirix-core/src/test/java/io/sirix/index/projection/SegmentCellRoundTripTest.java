/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A packed {@code (segment, id)} cell must survive the page's own serialize/deserialize. Observed
 * at 1M: the builder stamped segment 1 on leaf 867 onward, and every cell read back as segment 0 —
 * real ids resolved against the wrong segment's dictionary, which is a plausible value for another
 * row.
 */
final class SegmentCellRoundTripTest {

  /** Mints 1, 2, 3, … so the test can predict every cell exactly. */
  private static final class CountingEncoder implements GlobalValueDictionaryEncoder {
    private int next = 1;

    @Override
    public int intern(final byte[] source, final int offset, final int length) {
      return next++;
    }

    @Override
    public int intern(final String value) {
      return next++;
    }
  }

  private static ProjectionIndexRowGroupPage leafWithStrings(final int rows) {
    final byte[] kinds = {ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT};
    final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds);
    final long[] longs = new long[1];
    final boolean[] bools = new boolean[1];
    final String[] strings = new String[1];
    final boolean[] present = {true};
    final boolean[] unrep = new boolean[1];
    final boolean[] nonIntegral = new boolean[1];
    for (int row = 0; row < rows; row++) {
      strings[0] = "value-" + row;
      page.appendRow(row + 1L, longs, bools, strings, present, unrep, nonIntegral);
    }
    return page;
  }

  @Test
  @DisplayName("a cell stamped with segment 1 still names segment 1 after a serialize/deserialize")
  void aSegmentStampSurvivesTheRoundTrip() {
    final int rows = 64;
    final int segment = 1;
    final ProjectionIndexRowGroupPage page = leafWithStrings(rows);
    page.convertStringDictColumnToSegment(0, new CountingEncoder(), segment);

    // In memory, before any encoding: the stamp is there.
    final long[] inMemory = page.numericColumn(0);
    assertEquals(segment, ProjectionIndexRowGroupPage.segmentOfCell(inMemory[0]),
        "the conversion itself must stamp the segment");

    final byte[] raw = page.serialize();
    final ProjectionIndexRowGroupPage reread = ProjectionIndexRowGroupPage.deserialize(raw);
    final long[] afterRoundTrip = reread.numericColumn(0);

    for (int row = 0; row < rows; row++) {
      assertEquals(inMemory[row], afterRoundTrip[row], "cell " + row + " changed across the round trip");
      assertEquals(segment, ProjectionIndexRowGroupPage.segmentOfCell(afterRoundTrip[row]),
          "cell " + row + " lost its segment: it would resolve against segment 0's dictionary, which holds a"
              + " different value under the same id");
    }
    assertTrue(reread.columnMin(0) > 0xFFFFFFFFL, "the zone must describe cells, which are above the id range");
  }

  @Test
  @DisplayName("a segment-1 cell survives the COLUMN SEGMENT codec, which is the path a query reads")
  void aSegmentStampSurvivesTheColumnSegmentCodec() {
    // The raw form is not what a query reads: a stored leaf is split into per-column segments and
    // reassembled on demand. That path has its own kind dispatch, and a kind it does not know about
    // is exactly where a lane silently loses its high bits.
    final int rows = 64;
    final int segment = 1;
    final ProjectionIndexRowGroupPage page = leafWithStrings(rows);
    page.convertStringDictColumnToSegment(0, new CountingEncoder(), segment);
    final long[] inMemory = page.numericColumn(0).clone();

    final byte[] raw = page.serialize();
    final ProjectionIndexColumnSegmentCodec.EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(raw);
    RowGroupDescriptor.validate(encoded.descriptor());
    final Map<Integer, byte[]> byId = new HashMap<>();
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      byId.put(encoded.columnSegmentIds()[i], encoded.segments()[i]);
    }
    final byte[] reassembled = ProjectionIndexColumnSegmentCodec.assembleRaw(encoded.descriptor(), byId::get);
    final long[] afterCodec = ProjectionIndexRowGroupPage.deserialize(reassembled).numericColumn(0);

    for (int row = 0; row < rows; row++) {
      assertEquals(segment, ProjectionIndexRowGroupPage.segmentOfCell(afterCodec[row]),
          "cell " + row + " lost its segment in the column-segment codec");
      assertEquals(inMemory[row], afterCodec[row], "cell " + row + " changed in the column-segment codec");
    }
  }
}
