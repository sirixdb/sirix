/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.ValueDictionaryEntryNode;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;

final class SegmentValueMergeTest {

  @Test
  void mergesDisjointRunsInValueOrder() {
    final String[][] values = new String[7][2_048];
    for (int run = 0; run < values.length; run++) {
      for (int i = 0; i < values[run].length; i++) {
        values[run][i] = "domain-" + run + "/" + String.format(Locale.ROOT, "%05d", i);
      }
    }
    assertMerge(values, false, Integer.MAX_VALUE);
  }

  @Test
  void equalBoundaryValuesKeepOneRankAndTheFirstRunRepresentative() {
    final String[][] values = new String[5][];
    for (int run = 0; run < values.length; run++) {
      final TreeSet<String> mine = new TreeSet<>();
      mine.add("");
      mine.add("same-prefix/" + "z".repeat(70_000)); // a spill can also be the competing head
      for (int block = 0; block < 6; block++) {
        mine.add("block-" + block + "/shared");
        for (int i = 0; i < 200; i++) {
          mine.add("block-" + block + "/run-" + run + "/" + String.format(Locale.ROOT, "%04d", i));
        }
        mine.add("block-" + block + "/\uD800\uDC00");
        mine.add("block-" + block + "/\uE000"); // UTF-16 order differs from UTF-8 byte order
      }
      values[run] = mine.toArray(String[]::new);
    }
    assertMerge(values, false, Integer.MAX_VALUE);
    assertMerge(values, true, 257);
  }

  @Test
  void selectedInterleavedAndClusteredRunsMatchValueSort() {
    final SplittableRandom random = new SplittableRandom(0x57ADEC);
    for (final int count : new int[] {1, 2, 3, 8, 19}) {
      final String[][] values = new String[count][];
      for (int run = 0; run < count; run++) {
        final TreeSet<String> mine = new TreeSet<>();
        for (int i = 0; i < 800; i++) {
          final int number = i < 400
              ? run * 1_000 + i
              : random.nextInt(12_000);
          mine.add("prefix/" + String.format(Locale.ROOT, "%05d", number));
        }
        values[run] = mine.toArray(String[]::new);
      }
      assertMerge(values, false, Integer.MAX_VALUE);
      assertMerge(values, true, 129);
    }
  }

  @Test
  void mixedUtf8WidthsKeepUtf16OrderAcrossInterleavedRuns() {
    final int[] alphabet = {0, 65, 127, 128, 0x7FF, 0x800, 0xD7FF, 0xE000, 0xFFFF, 0x10000, 0x10001, 0x1F642, 0x10FFFF};
    final SplittableRandom random = new SplittableRandom(0x16C011A7);
    final String[][] values = new String[7][];
    for (int run = 0; run < values.length; run++) {
      final TreeSet<String> mine = new TreeSet<>();
      for (int i = 0; i < 1_000; i++) {
        final StringBuilder word = new StringBuilder();
        for (int c = 0; c < 3; c++) {
          word.appendCodePoint(alphabet[random.nextInt(alphabet.length)]);
        }
        mine.add(word.toString());
      }
      values[run] = mine.toArray(String[]::new);
    }
    assertMerge(values, false, Integer.MAX_VALUE);
    assertMerge(values, true, 97);
  }

  private static void assertMerge(final String[][] values, final boolean sparse, final int rangeTarget) {
    final int[] segments = new int[values.length];
    final int[] counts = new int[values.length];
    final long[][] marks = new long[values.length][];
    final TreeMap<String, Long> expected = new TreeMap<>();
    for (int run = 0; run < values.length; run++) {
      Arrays.sort(values[run]);
      segments[run] = run;
      counts[run] = values[run].length;
      marks[run] = new long[(counts[run] >>> 6) + 1];
      for (int mint = 1; mint <= counts[run]; mint++) {
        if (!sparse || mint % 29 == 0) {
          marks[run][mint >>> 6] |= 1L << mint;
          expected.putIfAbsent(values[run][counts[run] - mint], ProjectionIndexRowGroupPage.packSegmentCell(run, mint));
        }
      }
    }
    final SegmentGroupCanonicaliser.CellResolver resolver = new SegmentGroupCanonicaliser.CellResolver() {
      @Override
      public String valueOfCell(final long cell) {
        final int run = ProjectionIndexRowGroupPage.segmentOfCell(cell);
        return values[run][counts[run] - ProjectionIndexRowGroupPage.idOfCell(cell)];
      }

      @Override
      public int positionOfCell(final long cell) {
        return counts[ProjectionIndexRowGroupPage.segmentOfCell(cell)] - ProjectionIndexRowGroupPage.idOfCell(cell) + 1;
      }

      @Override
      public SegmentRunCursor cursorOfSegment(final long cell) {
        final int run = ProjectionIndexRowGroupPage.segmentOfCell(cell);
        return new SegmentRunCursor() {
          @Override
          public void seek(final int position) {
            final byte[] bytes = values[run][position - 1].getBytes(StandardCharsets.UTF_8);
            if (bytes.length > 65_536) {
              spill = new ValueDictionaryEntryNode(position, bytes);
              backing = null;
            } else {
              spill = null;
              backing = bytes;
              offset = 0;
              length = bytes.length;
            }
          }

          @Override
          public int mintAt(final int position) {
            return counts[run] - position + 1; // mints deliberately reverse the value order
          }
        };
      }
    };
    final SegmentValueMerge.Result result = SegmentValueMerge.merge(resolver, segments, marks, counts,
        SegmentGroupCanonicaliser.SERIAL_SEGMENTS, rangeTarget, null);
    assertEquals(expected.size(), result.representatives().length);
    final Map<String, Integer> ranks = new TreeMap<>();
    int rank = 0;
    for (final Map.Entry<String, Long> entry : expected.entrySet()) {
      assertEquals(entry.getValue().longValue(), result.representatives()[rank], "stable value representative");
      ranks.put(entry.getKey(), ++rank);
    }
    int marked = 0;
    for (int run = 0; run < values.length; run++) {
      for (int mint = 1; mint <= counts[run]; mint++) {
        if ((marks[run][mint >>> 6] & 1L << mint) != 0) {
          assertEquals(ranks.get(values[run][counts[run] - mint]).intValue(), result.tables()[run][mint]);
          marked++;
        } else {
          assertEquals(0, result.tables()[run][mint], "unselected values receive no rank");
        }
      }
    }
    assertEquals(marked, Arrays.stream(result.rangeCells()).sum(), "every selected cell appears once");
  }
}
