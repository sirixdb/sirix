/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Sparse-field correctness of the projection leaf format (presence
 * bitmaps + per-column unrepresentable flags) and the presence-aware
 * {@link ProjectionIndexByteScan} kernels.
 *
 * <p>The semantics under test mirror the JSONiq interpreter:
 * <ul>
 * <li>a comparison over a MISSING field is false → row excluded (the
 * historical layout stored defaults, so {@code x < 40} matched missing
 * rows via the phantom {@code 0});</li>
 * <li>group-by routes missing keys to a dedicated missing bucket instead
 * of the {@code ""} default group;</li>
 * <li>aggregates skip missing rows (the historical sum counted phantom
 * zeros into count/min/max);</li>
 * <li>the presence tail is mandatory — tail-less payloads are rejected
 * as corrupt, never misread.</li>
 * </ul>
 */
public final class ProjectionIndexPresenceTest {

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  /**
   * Build a leaf where every third row misses the numeric column, every
   * fourth misses the string column, and the boolean column is dense.
   */
  private static ProjectionIndexLeafPage sparseLeaf(final int rows) {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = new boolean[3];
    final boolean[] unrep = new boolean[3];
    for (int i = 0; i < rows; i++) {
      final boolean numMissing = i % 3 == 0;
      final boolean strMissing = i % 4 == 0;
      longs[0] = numMissing ? 0L : i;
      bools[1] = i % 2 == 0;
      strings[2] = strMissing ? "" : (i % 2 == 0 ? "even" : "odd");
      present[0] = !numMissing;
      present[1] = true;
      present[2] = !strMissing;
      Arrays.fill(unrep, false);
      assertTrue(page.appendRow(1000 + i, longs, bools, strings, present, unrep));
    }
    return page;
  }

  // ==================== format round-trip ====================

  @Test
  void presenceSurvivesSerializeRoundTrip() {
    final ProjectionIndexLeafPage page = sparseLeaf(100);
    final byte[] payload = page.serialize();
    final ProjectionIndexLeafPage back = ProjectionIndexLeafPage.deserialize(payload);
    assertEquals(100, back.getRowCount());
    for (int i = 0; i < 100; i++) {
      final boolean numPresent = (back.presenceColumnBits(0)[i >>> 6] & (1L << (i & 63))) != 0;
      final boolean strPresent = (back.presenceColumnBits(2)[i >>> 6] & (1L << (i & 63))) != 0;
      assertEquals(i % 3 != 0, numPresent, "numeric presence row " + i);
      assertEquals(i % 4 != 0, strPresent, "string presence row " + i);
    }
    assertFalse(back.columnUnrepresentable(0));
    // Round-trip the round-trip: byte-identical re-serialization.
    assertArrayEquals(payload, back.serialize());
  }

  @Test
  void unrepresentableFlagSurvivesRoundTrip() {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final boolean[] present = { true, true, true };
    final boolean[] unrep = { true, false, false };  // e.g. a JSON null in the numeric column
    page.appendRow(1L, new long[] { 0, 0, 0 }, new boolean[3], new String[] { "", "", "x" }, present, unrep);
    final ProjectionIndexLeafPage back = ProjectionIndexLeafPage.deserialize(page.serialize());
    assertTrue(back.columnUnrepresentable(0));
    assertFalse(back.columnUnrepresentable(1));
    assertFalse(back.columnUnrepresentable(2));
    // Present (the field EXISTS, it's just null/complex) — presence bit set.
    assertTrue((back.presenceColumnBits(0)[0] & 1L) != 0);
  }

  @Test
  void tailLessPayloadIsRejectedNotMisread() {
    final ProjectionIndexLeafPage page = sparseLeaf(50);
    final byte[] full = page.serialize();
    // Strip the tail — the mandatory footer is gone, deserialize must reject.
    final int presWords = (50 + 63) >>> 6;
    final int tailSize = KINDS.length + KINDS.length * presWords * 8 + 8;
    final byte[] truncated = Arrays.copyOf(full, full.length - tailSize);
    assertThrows(IllegalStateException.class, () -> ProjectionIndexLeafPage.deserialize(truncated));
    // The byte-level sparse probe fails closed on the same payload.
    final byte[] status = ProjectionIndexByteScan.probeSparseEvidence(List.of(truncated));
    for (final byte st : status) {
      assertEquals(ProjectionIndexByteScan.SPARSE_STATUS_DIRTY, st);
    }
  }

  @Test
  void probeSparseEvidenceFlagsUnrepresentableColumnsOnly() {
    final ProjectionIndexLeafPage clean = sparseLeaf(10);
    final ProjectionIndexLeafPage poisoned = new ProjectionIndexLeafPage(KINDS);
    poisoned.appendRow(1L, new long[3], new boolean[3], new String[] { "", "", "x" },
        new boolean[] { true, true, true }, new boolean[] { false, false, true });
    final byte[] status =
        ProjectionIndexByteScan.probeSparseEvidence(List.of(clean.serialize(), poisoned.serialize()));
    assertEquals(ProjectionIndexByteScan.SPARSE_STATUS_CLEAN, status[0]);
    assertEquals(ProjectionIndexByteScan.SPARSE_STATUS_CLEAN, status[1]);
    assertEquals(ProjectionIndexByteScan.SPARSE_STATUS_DIRTY, status[2]);
  }

  @Test
  void emptyLeafRoundTripsWithTail() {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final byte[] payload = page.serialize();
    final ProjectionIndexLeafPage back = ProjectionIndexLeafPage.deserialize(payload);
    assertEquals(0, back.getRowCount());
    assertArrayEquals(payload, back.serialize());
  }

  // ==================== presence-aware kernels ====================

  @Test
  void predicateOverMissingFieldIsFalse() {
    // 100 rows; rows 0,3,6,... miss the numeric field. Historical layout stored
    // 0 there, so `x < 40` matched every missing row via the phantom zero.
    final byte[] payload = sparseLeaf(100).serialize();
    final var lt40 = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LT, 40L)
    };
    // Present rows are i with i % 3 != 0 and value i: matching i in [1,39] minus multiples of 3.
    long expected = 0;
    for (int i = 0; i < 100; i++) {
      if (i % 3 != 0 && i < 40) expected++;
    }
    assertEquals(expected, ProjectionIndexByteScan.conjunctiveCount(List.of(payload), lt40));

    // GE 0 matches every PRESENT row — and no missing row.
    final var ge0 = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GE, 0L)
    };
    long present = 0;
    for (int i = 0; i < 100; i++) {
      if (i % 3 != 0) present++;
    }
    assertEquals(present, ProjectionIndexByteScan.conjunctiveCount(List.of(payload), ge0));
  }

  @Test
  void stringEqOverMissingFieldIsFalse() {
    // The missing rows intern "" as the default — `eq ""` must NOT match them.
    final byte[] payload = sparseLeaf(100).serialize();
    final var eqEmpty = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, new byte[0])
    };
    assertEquals(0, ProjectionIndexByteScan.conjunctiveCount(List.of(payload), eqEmpty));
  }

  @Test
  void allMissingLeafZoneMapPrunes() {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final boolean[] present = { false, true, false };
    for (int i = 0; i < 10; i++) {
      page.appendRow(i, new long[3], new boolean[] { false, true, false }, new String[] { "", "", "" },
          present, null);
    }
    final byte[] payload = page.serialize();
    // Any numeric predicate over the all-missing column matches nothing —
    // including EQ 0, which would hit every phantom default.
    for (final ProjectionIndexScan.Op op : new ProjectionIndexScan.Op[] {
        ProjectionIndexScan.Op.EQ, ProjectionIndexScan.Op.LE, ProjectionIndexScan.Op.GE }) {
      final var pred = new ProjectionIndexScan.ColumnPredicate[] {
          ProjectionIndexScan.ColumnPredicate.numeric(0, op, 0L)
      };
      assertEquals(0, ProjectionIndexByteScan.conjunctiveCount(List.of(payload), pred), "op " + op);
    }
  }

  @Test
  void groupByCountsMissingBucketSeparately() {
    final byte[] payload = sparseLeaf(100).serialize();
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    final long[] missing = new long[1];
    ProjectionIndexByteScan.conjunctiveCountByGroup(List.of(payload),
        new ProjectionIndexScan.ColumnPredicate[0], 2, out, missing);
    long even = 0, odd = 0, miss = 0;
    for (int i = 0; i < 100; i++) {
      if (i % 4 == 0) miss++;
      else if (i % 2 == 0) even++;
      else odd++;
    }
    assertEquals(miss, missing[0]);
    assertEquals(even, out.getLong("even"));
    assertEquals(odd, out.getLong("odd"));
    assertFalse(out.containsKey(""), "missing rows must not group under the empty-string default");
  }

  @Test
  void multiKeyGroupByEmitsMissingSegment() {
    final byte[] kinds = {
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT,
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
    };
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(kinds);
    // (a, b), (a, missing), (missing, b), (missing, missing)
    page.appendRow(1, new long[2], new boolean[2], new String[] { "a", "b" },
        new boolean[] { true, true }, null);
    page.appendRow(2, new long[2], new boolean[2], new String[] { "a", "" },
        new boolean[] { true, false }, null);
    page.appendRow(3, new long[2], new boolean[2], new String[] { "", "b" },
        new boolean[] { false, true }, null);
    page.appendRow(4, new long[2], new boolean[2], new String[] { "", "" },
        new boolean[] { false, false }, null);
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMulti(List.of(page.serialize()),
        new ProjectionIndexScan.ColumnPredicate[0], new int[] { 0, 1 }, out);
    assertEquals(1L, out.getLong("s1:as1:b"));
    assertEquals(1L, out.getLong("s1:am"));
    assertEquals(1L, out.getLong("ms1:b"));
    assertEquals(1L, out.getLong("mm"));
    assertEquals(4, out.size());
  }

  @Test
  void aggregateSkipsMissingRows() {
    final byte[] payload = sparseLeaf(100).serialize();
    final long[] acc = { 0, 0, Long.MAX_VALUE, Long.MIN_VALUE };
    ProjectionIndexByteScan.conjunctiveAggregateNumeric(List.of(payload),
        new ProjectionIndexScan.ColumnPredicate[0], 0, acc);
    long count = 0, sum = 0, min = Long.MAX_VALUE, max = Long.MIN_VALUE;
    for (int i = 0; i < 100; i++) {
      if (i % 3 == 0) continue;  // missing
      count++;
      sum += i;
      min = Math.min(min, i);
      max = Math.max(max, i);
    }
    assertEquals(count, acc[0], "missing rows must not inflate the count");
    assertEquals(sum, acc[1]);
    assertEquals(min, acc[2], "phantom zeros must not poison min");
    assertEquals(max, acc[3]);
  }

  @Test
  void materializingScanAgreesWithByteScanOnSparseLeaves() {
    // The materializing reference scan must apply the same presence
    // semantics as the zero-copy byte scan.
    final byte[] payload = sparseLeaf(100).serialize();
    final var lt40 = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LT, 40L)
    };
    assertEquals(ProjectionIndexByteScan.conjunctiveCount(List.of(payload), lt40),
        ProjectionIndexScan.conjunctiveCount(List.of(payload), lt40));
    final var eqEmpty = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, new byte[0])
    };
    assertEquals(0, ProjectionIndexScan.conjunctiveCount(List.of(payload), eqEmpty));
  }

  @Test
  void legacyDenseCallersUnchangedOnDenseData() {
    // A fully-present page behaves identically through old and new paths.
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    for (int i = 0; i < 64; i++) {
      page.appendRow(i, new long[] { i, 0, 0 }, new boolean[] { false, i % 2 == 0, false },
          new String[] { "", "", i % 2 == 0 ? "x" : "y" });
    }
    final byte[] payload = page.serialize();
    final var gt10 = new ProjectionIndexScan.ColumnPredicate[] {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 10L)
    };
    assertEquals(53, ProjectionIndexByteScan.conjunctiveCount(List.of(payload), gt10));
    final byte[] status = ProjectionIndexByteScan.probeSparseEvidence(List.of(payload));
    for (final byte st : status) {
      assertEquals(ProjectionIndexByteScan.SPARSE_STATUS_CLEAN, st);
    }
  }
}
