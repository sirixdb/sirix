/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import static io.sirix.index.projection.NumericLaneTestSupport.copyNumericValues;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.index.projection.ProjectionIndexColumnSegmentCodec.EncodedRowGroup;
import io.sirix.index.projection.ProjectionIndexRowGroupCodec.Cursor;
import io.sirix.node.SegmentAccess;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * The segment decoders are twins of the array decoders: for every FOR width, for ALP with and
 * without exceptions, and for every long-lane column kind through the body-slice decoder, the lane
 * written into a slab equals the heap array word for word, and both cursors stop at the same byte.
 */
final class LaneDecoderEquivalenceTest {

  private static final int[] COUNTS = {1, 7, 63, 64, 65, 1000, 2047};

  @Test
  @DisplayName("FOR bit-packed lanes agree for every width 0..64 and every row count")
  void forBitPackedLanesAgreeForEveryWidth() {
    final Random rnd = new Random(1106L);
    try (Arena arena = Arena.ofConfined()) {
      for (int width = 0; width <= 64; width++) {
        for (final int count : COUNTS) {
          final long[] values = new long[count];
          final long mask = width == 64
              ? -1L
              : (1L << width) - 1L;
          final long min = width == 64
              ? Long.MIN_VALUE
              : rnd.nextLong(-1_000_000L, 1_000_000L);
          for (int i = 0; i < count; i++) {
            values[i] = min + (rnd.nextLong() & mask);
          }
          values[0] = min;
          if (count > 1) {
            values[1] = min + mask; // pins the range so the encoder picks exactly this width
          }
          final ByteArrayOutputStream out = new ByteArrayOutputStream();
          ProjectionIndexRowGroupCodec.encodeForBitPacked(out, values, count);
          final byte[] bytes = out.toByteArray();
          if (count > 1) {
            // Packed runs are capped at 56 bits (clampPackWidth); wider ranges take the raw 64-bit path.
            assertEquals(width <= 56
                ? width
                : 64, bytes[8] & 0xFF, "width " + width);
          }
          final long[] heap = new long[count];
          final Cursor heapCursor = new Cursor(bytes, 0);
          ProjectionIndexRowGroupCodec.decodeForBitPackedColumnInto(heapCursor, count, heap);
          final MemorySegment slab = arena.allocate(8L * count + 128, 64);
          slab.fill((byte) 0x5A);
          final Cursor laneCursor = new Cursor(bytes, 0);
          ProjectionIndexRowGroupCodec.decodeForBitPackedColumnInto(laneCursor, count, slab, 64L);
          assertEquals(heapCursor.position(), laneCursor.position(), "width " + width + " count " + count);
          assertLaneEquals(heap, slab, 64L, "width " + width + " count " + count);
          assertEquals(0x5A5A5A5A5A5A5A5AL, SegmentAccess.getLong(slab, 56L), "nothing written before the lane");
          assertEquals(0x5A5A5A5A5A5A5A5AL, SegmentAccess.getLong(slab, 64L + 8L * count), "nor after it");
          if (count > 1) {
            assertArrayEquals(values, heap, "the heap decoder round-trips the encoder");
          }
        }
      }
    }
  }

  @Test
  @DisplayName("ALP lanes agree, with exceptions and at every exponent the encoder picks")
  void alpLanesAgree() {
    final Random rnd = new Random(2211L);
    boolean alpTaken = false;
    try (Arena arena = Arena.ofConfined()) {
      for (int round = 0; round < 40; round++) {
        final int count = COUNTS[round % COUNTS.length];
        final long[] values = new long[count];
        final int scale = 1 + rnd.nextInt(4);
        final double unit = Math.pow(10, -scale);
        for (int i = 0; i < count; i++) {
          final double d = switch (rnd.nextInt(12)) {
            case 0 -> Double.NaN; // exceptions
            case 1 -> 1e300;
            case 2 -> -0.0;
            case 3 -> Double.MIN_VALUE;
            default -> (rnd.nextInt(2_000_000) - 1_000_000L) * unit;
          };
          values[i] = ProjectionDoubleEncoding.encode(d);
        }
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        ProjectionIndexRowGroupCodec.encodeForBitPackedDouble(out, values, count);
        final byte[] bytes = out.toByteArray();
        alpTaken |= (bytes[8] & 0xFF) == ProjectionAlpEncoding.WIDTH_ESCAPE_ALP;
        final long[] heap = new long[count];
        final Cursor heapCursor = new Cursor(bytes, 0);
        ProjectionIndexRowGroupCodec.decodeForBitPackedColumnInto(heapCursor, count, heap);
        assertArrayEquals(values, heap, "round " + round);
        final MemorySegment slab = arena.allocate(8L * count + 64, 64);
        final Cursor laneCursor = new Cursor(bytes, 0);
        ProjectionIndexRowGroupCodec.decodeForBitPackedColumnInto(laneCursor, count, slab, 0L);
        assertEquals(heapCursor.position(), laneCursor.position());
        assertLaneEquals(heap, slab, 0L, "round " + round);
      }
    }
    assertTrue(alpTaken, "the fixture must exercise the ALP escape, not only plain FOR");
  }

  @Test
  @DisplayName("body slices of every long-lane kind agree, with presence gaps, an all-missing and a rowless leaf")
  void bodySlicesAgreeForEveryLongLaneKind() {
    final byte[] kinds =
        {ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG, ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
            ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE};
    final Random rnd = new Random(9L);
    try (Arena arena = Arena.ofConfined()) {
      for (final int rows : new int[] {0, 1, 64, 65, 300, 2048}) {
        for (final int missingOneIn : new int[] {1, 2, 10, Integer.MAX_VALUE}) {
          final ProjectionIndexRowGroupPage page = new ProjectionIndexRowGroupPage(kinds.clone());
          final long[] longs = new long[kinds.length];
          final boolean[] bools = new boolean[kinds.length];
          final String[] strings = new String[kinds.length];
          final boolean[] present = new boolean[kinds.length];
          final boolean[] unrepresentable = new boolean[kinds.length];
          for (int row = 0; row < rows; row++) {
            longs[0] = rnd.nextLong(-5_000L, 5_000L);
            longs[1] = ProjectionDoubleEncoding.encode(rnd.nextInt(100_000) / 100.0);
            longs[2] = 1_700_000_000L + rnd.nextInt(1_000_000);
            longs[3] = 19_000L + rnd.nextInt(1_000);
            for (int c = 0; c < kinds.length; c++) {
              present[c] = missingOneIn == Integer.MAX_VALUE || rnd.nextInt(missingOneIn) != 0;
            }
            page.appendRow(1 + 3L * row, longs, bools, strings, present, unrepresentable);
          }
          final EncodedRowGroup encoded = ProjectionIndexColumnSegmentCodec.encode(page.serialize());
          if (encoded == null) {
            assertEquals(0, rows, "only a rowless leaf may encode to nothing");
            continue;
          }
          for (int col = 0; col < kinds.length; col++) {
            final byte[] body = segment(encoded, ProjectionIndexColumnSegmentCodec.bodyColumnSegmentId(col));
            final ColumnSlice heap = ProjectionIndexColumnSegmentCodec.decodeBodySlice(encoded.descriptor(), body, col);
            final MemorySegment slab = arena.allocate(8L * Math.max(rows, 1) + 64, 64);
            final ColumnSlice lane =
                ProjectionIndexColumnSegmentCodec.decodeBodySliceInto(encoded.descriptor(), body, col, slab, 0L);
            final String at = "rows " + rows + " missingOneIn " + missingOneIn + " col " + col;
            assertEquals(heap.rowCount(), lane.rowCount(), at);
            assertEquals(heap.flags(), lane.flags(), at);
            assertEquals(heap.min(), lane.min(), at);
            assertEquals(heap.max(), lane.max(), at);
            assertArrayEquals(heap.presenceWords(), lane.presenceWords(), at);
            assertNull(lane.numericValues(), at);
            if (rows == 0) {
              assertNull(lane.lane(), "a rowless leaf carries no lane: " + at);
              continue;
            }
            final NumericLane resident = lane.lane();
            assertNotNull(resident, at);
            assertEquals(heap.rowCount(), resident.rows(), at); // the page caps at MAX_ROWS
            final long[] values = heap.numericValues();
            assertNotNull(values, at);
            assertLaneEquals(values, slab, 0L, at);
            assertArrayEquals(values, copyNumericValues(lane), "a test-owned copy holds the lane exactly: " + at);
          }
        }
      }
    }
  }

  private static byte[] segment(final EncodedRowGroup encoded, final int id) {
    for (int i = 0; i < encoded.columnSegmentIds().length; i++) {
      if (encoded.columnSegmentIds()[i] == id) {
        return encoded.segments()[i];
      }
    }
    throw new IllegalStateException("no segment " + id);
  }

  private static void assertLaneEquals(final long[] expected, final MemorySegment slab, final long base,
      final String at) {
    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], SegmentAccess.getLong(slab, base + 8L * i), at + " row " + i);
    }
  }
}
