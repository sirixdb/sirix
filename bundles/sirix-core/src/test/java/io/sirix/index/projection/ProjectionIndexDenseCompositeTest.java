/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Parity and fail-closed behavior of the dense composite group-by kernel
 * ({@link ProjectionIndexByteScan#conjunctiveCountByGroupMultiDense}) and the
 * dictionary-union count-distinct kernel
 * ({@link ProjectionIndexByteScan#distinctPresentStrings}) against the
 * composite hashmap kernel as ground truth — including missing fields
 * (presence bits), per-leaf fallback on out-of-canon dictionary values, and
 * the phantom-{@code ""} disambiguation that missing rows force on the
 * distinct kernel.
 */
public final class ProjectionIndexDenseCompositeTest {

  private static final byte[] KINDS = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  private static final String[] DEPTS = {"Eng", "Sales", "Mkt"};
  private static final String[] CITIES = {"NYC", "LA"};

  /**
   * Build a leaf of {@code rows} rows over (age, dept, city): dept missing on
   * every fifth row, city missing on every seventh, values round-robin from
   * the pools (offset by {@code seed} so different leaves favor different
   * dictionary orders).
   */
  private static ProjectionIndexLeafPage leaf(final int rows, final int seed, final String[] depts) {
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final long[] longs = new long[3];
    final boolean[] bools = new boolean[3];
    final String[] strings = new String[3];
    final boolean[] present = new boolean[3];
    final boolean[] unrep = new boolean[3];
    for (int i = 0; i < rows; i++) {
      final boolean deptMissing = i % 5 == 0;
      final boolean cityMissing = i % 7 == 0;
      longs[0] = 18 + ((seed + i) % 48);
      strings[1] = deptMissing ? "" : depts[(seed + i) % depts.length];
      strings[2] = cityMissing ? "" : CITIES[(seed + i) % CITIES.length];
      present[0] = true;
      present[1] = !deptMissing;
      present[2] = !cityMissing;
      Arrays.fill(unrep, false);
      assertTrue(page.appendRow(1000L * (seed + 1) + i, longs, bools, strings, present, unrep));
    }
    return page;
  }

  private static List<byte[]> leaves(final String[]... deptsPerLeaf) {
    final List<byte[]> out = new ArrayList<>(deptsPerLeaf.length);
    for (int i = 0; i < deptsPerLeaf.length; i++) {
      out.add(leaf(100 + i * 30, i, deptsPerLeaf[i]).serialize());
    }
    return out;
  }

  /** Decode the dense counts array into composite keys exactly as the executor does. */
  private static Object2LongOpenHashMap<String> decode(final long[] counts, final byte[][][] canon) {
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    final int m = canon.length;
    final int[] ids = new int[m];
    for (int cell = 0; cell < counts.length; cell++) {
      if (counts[cell] == 0L) continue;
      int rem = cell;
      for (int g = m - 1; g >= 0; g--) {
        final int radix = canon[g].length + 1;
        ids[g] = rem % radix;
        rem /= radix;
      }
      final StringBuilder kb = new StringBuilder();
      for (int g = 0; g < m; g++) {
        if (ids[g] == canon[g].length) {
          kb.append('m');
        } else {
          final String v = new String(canon[g][ids[g]], StandardCharsets.UTF_8);
          kb.append('s').append(v.length()).append(':').append(v);
        }
      }
      out.addTo(kb.toString(), counts[cell]);
    }
    return out;
  }

  private static int cellCount(final byte[][][] canon) {
    int n = 1;
    for (final byte[][] dict : canon) {
      n *= dict.length + 1;
    }
    return n;
  }

  // ==================== dense composite parity ====================

  @Test
  void denseCompositeMatchesHashmapKernel() {
    final List<byte[]> payloads = leaves(DEPTS, DEPTS, DEPTS);
    final int[] cols = {1, 2};
    final ProjectionIndexScan.ColumnPredicate[] noPreds = new ProjectionIndexScan.ColumnPredicate[0];

    final Object2LongOpenHashMap<String> reference = new Object2LongOpenHashMap<>();
    reference.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMulti(payloads, noPreds, cols, reference);

    final byte[][][] canon = {
        ProjectionIndexByteScan.probeCanonicalDict(payloads, 1, payloads.size(), 256),
        ProjectionIndexByteScan.probeCanonicalDict(payloads, 2, payloads.size(), 256)
    };
    final long[] counts = new long[cellCount(canon)];
    final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
    fallback.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMultiDense(payloads, noPreds, cols, canon, counts, fallback);
    assertEquals(0, fallback.size(), "all dict values are canonical — no fallback expected");

    assertEquals(reference, decode(counts, canon));
  }

  @Test
  void denseCompositeMatchesHashmapKernelUnderPredicate() {
    final List<byte[]> payloads = leaves(DEPTS, DEPTS);
    final int[] cols = {1, 2};
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L)
    };

    final Object2LongOpenHashMap<String> reference = new Object2LongOpenHashMap<>();
    reference.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMulti(payloads, preds, cols, reference);

    final byte[][][] canon = {
        ProjectionIndexByteScan.probeCanonicalDict(payloads, 1, payloads.size(), 256),
        ProjectionIndexByteScan.probeCanonicalDict(payloads, 2, payloads.size(), 256)
    };
    final long[] counts = new long[cellCount(canon)];
    final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
    fallback.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMultiDense(payloads, preds, cols, canon, counts, fallback);
    assertEquals(0, fallback.size());

    assertEquals(reference, decode(counts, canon));
  }

  @Test
  void denseCompositeFallsBackPerLeafOnOutOfCanonValue() {
    // Canonical dicts probed from the FIRST leaf only; the second leaf
    // introduces "Legal", which is out-of-canon → that leaf must be counted
    // through the fallback hashmap, and dense+fallback must equal reference.
    final List<byte[]> payloads = leaves(DEPTS, new String[] {"Eng", "Sales", "Legal"});
    final int[] cols = {1, 2};
    final ProjectionIndexScan.ColumnPredicate[] noPreds = new ProjectionIndexScan.ColumnPredicate[0];

    final Object2LongOpenHashMap<String> reference = new Object2LongOpenHashMap<>();
    reference.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMulti(payloads, noPreds, cols, reference);

    final byte[][][] canon = {
        ProjectionIndexByteScan.probeCanonicalDict(payloads.subList(0, 1), 1, 1, 256),
        ProjectionIndexByteScan.probeCanonicalDict(payloads.subList(0, 1), 2, 1, 256)
    };
    final long[] counts = new long[cellCount(canon)];
    final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
    fallback.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupMultiDense(payloads, noPreds, cols, canon, counts, fallback);
    assertTrue(fallback.size() > 0, "second leaf must route through the fallback kernel");

    final Object2LongOpenHashMap<String> combined = decode(counts, canon);
    fallback.object2LongEntrySet().fastForEach(e -> combined.addTo(e.getKey(), e.getLongValue()));
    assertEquals(reference, combined);
  }

  // ==================== dictionary-union count-distinct ====================

  @Test
  void distinctExcludesPhantomEmptyFromMissingRows() {
    // Every leaf has missing dept rows (interning the "" default), but no
    // PRESENT row carries "" — the phantom must not count.
    final List<byte[]> payloads = leaves(DEPTS, DEPTS, new String[] {"Eng", "Legal", "HR"});
    final ArrayList<byte[]> distinct = ProjectionIndexByteScan.distinctPresentStrings(payloads, 1, 1024);
    // Union: Eng, Sales, Mkt, Legal, HR — and NOT "".
    assertEquals(5, distinct.size());
    for (final byte[] v : distinct) {
      assertTrue(v.length > 0, "phantom empty value must be excluded");
    }
  }

  @Test
  void distinctIncludesRealEmptyValue() {
    // A leaf where "" is a REAL present dept value on rows that are present.
    final ProjectionIndexLeafPage page = new ProjectionIndexLeafPage(KINDS);
    final boolean[] present = {true, true, true};
    final boolean[] unrep = new boolean[3];
    page.appendRow(1L, new long[] {30, 0, 0}, new boolean[3], new String[] {"", "", "NYC"}, present, unrep);
    page.appendRow(2L, new long[] {31, 0, 0}, new boolean[3], new String[] {"", "Eng", "LA"}, present, unrep);
    final List<byte[]> payloads = List.of(page.serialize());
    final ArrayList<byte[]> distinct = ProjectionIndexByteScan.distinctPresentStrings(payloads, 1, 1024);
    // "" (present on row 1) and "Eng".
    assertEquals(2, distinct.size());
  }

  @Test
  void distinctMatchesGroupCountingKernel() {
    final List<byte[]> payloads = leaves(DEPTS, new String[] {"Legal", "HR", "Eng"});
    final ArrayList<byte[]> distinct = ProjectionIndexByteScan.distinctPresentStrings(payloads, 1, 1024);

    final Object2LongOpenHashMap<String> groups = new Object2LongOpenHashMap<>();
    groups.defaultReturnValue(0L);
    final long[] missing = new long[1];
    ProjectionIndexByteScan.conjunctiveCountByGroup(payloads,
        new ProjectionIndexScan.ColumnPredicate[0], 1, groups, missing);
    assertEquals(groups.size(), distinct.size(), "dict union must equal the group-counting distinct set");
  }

  @Test
  void distinctFailsClosedOnCardinalityAndMalformedLeaves() {
    final List<byte[]> payloads = leaves(DEPTS);
    assertNull(ProjectionIndexByteScan.distinctPresentStrings(payloads, 1, 2),
        "cardinality above the limit must bail");
    // Tail-less (malformed) leaf — no presence info, must fail closed.
    final byte[] full = payloads.get(0);
    final int tailLen = (full[full.length - 9] & 0xFF) | ((full[full.length - 8] & 0xFF) << 8)
        | ((full[full.length - 7] & 0xFF) << 16) | ((full[full.length - 6] & 0xFF) << 24);
    final byte[] truncated = Arrays.copyOf(full, full.length - 9 - tailLen);
    assertNull(ProjectionIndexByteScan.distinctPresentStrings(List.of(truncated), 1, 1024));
  }
}
