/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Randomized parity for the gap-1 MULTI-KEY kernels against naive row-wise oracles built
 * from the same appended rows:
 *
 * <ul>
 *   <li>{@link ProjectionIndexByteScan#conjunctiveAggregateByGroupMulti}: composite-key
 *       accumulators ({@code [rows, firstSeen, per-agg count/sum/min/max]}) and
 *       first-appearance emission order must match a per-row {@code LinkedHashMap} fold,
 *       including {@code null} components for missing group cells and the empty-string
 *       vs missing distinction.</li>
 *   <li>{@link ProjectionIndexByteScan#collectMatchingSortTuples}: row-major tuple/key
 *       collection in document order, with rows missing ANY sort column routed to the
 *       missing list.</li>
 * </ul>
 *
 * Predicate semantics mirror the reviewed single-key kernels: a predicate matches only a
 * PRESENT cell.
 */
final class ProjectionMultiKeyKernelParityTest {

  // Columns: 0=agg long, 1=sort long A, 2=sort long B, 3=group string X, 4=group string Y.
  private static final byte[] KINDS = {
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT,
      ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT
  };

  /** One generated row, kept for the oracle. */
  private record Row(long key, long agg, boolean aggPresent, long sortA, boolean sortAPresent,
      long sortB, boolean sortBPresent, String gx, String gy) {
  }

  // "" is a REAL dictionary value and must never collapse into the missing (null) group.
  private static final String[] GX_POOL = {"Eng", "Sales", "", "Ops"};
  private static final String[] GY_POOL = {"Berlin", "", "Munich"};

  private static List<Row> randomRows(final Random rnd, final int count, final long baseKey) {
    final List<Row> rows = new ArrayList<>(count);
    for (int i = 0; i < count; i++) {
      rows.add(new Row(baseKey + i,
          rnd.nextInt(100) - 20, rnd.nextInt(10) != 0,
          rnd.nextInt(5), rnd.nextInt(12) != 0,
          rnd.nextInt(4), rnd.nextInt(12) != 0,
          rnd.nextInt(8) == 0 ? null : GX_POOL[rnd.nextInt(GX_POOL.length)],
          rnd.nextInt(8) == 0 ? null : GY_POOL[rnd.nextInt(GY_POOL.length)]));
    }
    return rows;
  }

  private static byte[] buildLeaf(final List<Row> rows) {
    final ProjectionIndexRowGroupPage p = new ProjectionIndexRowGroupPage(KINDS);
    for (final Row r : rows) {
      final long[] nums = {r.agg, r.sortA, r.sortB, 0L, 0L};
      final boolean[] present = {
          r.aggPresent, r.sortAPresent, r.sortBPresent, r.gx != null, r.gy != null
      };
      final String[] strs = {null, null, null, r.gx == null ? "" : r.gx, r.gy == null ? "" : r.gy};
      assertTrue(p.appendRow(r.key, nums, new boolean[5], strs, present, null));
    }
    return p.serialize();
  }

  /** Predicate matches only present cells with agg value > threshold — oracle twin. */
  private static boolean matches(final Row r, final long threshold) {
    return r.aggPresent && r.agg > threshold;
  }

  @Test
  void multiKeyGroupAggregateParity() {
    for (long seed = 1; seed <= 6; seed++) {
      final Random rnd = new Random(seed);
      final List<List<Row>> perLeaf = new ArrayList<>();
      final List<byte[]> leaves = new ArrayList<>();
      final int rowGroupCount = 1 + rnd.nextInt(4);
      for (int l = 0; l < rowGroupCount; l++) {
        final List<Row> rows = randomRows(rnd, 1 + rnd.nextInt(200), 10_000L * (l + 1));
        perLeaf.add(rows);
        leaves.add(buildLeaf(rows));
      }
      final long threshold = 5;
      final ProjectionIndexScan.ColumnPredicate[] preds = {
          ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, threshold)
      };
      final int[] groupCols = {3, 4};
      final int[] aggCols = {0, 1};

      // Oracle: LinkedHashMap preserves first-appearance order across leaves.
      final Map<List<String>, long[]> expected = new LinkedHashMap<>();
      for (final List<Row> rows : perLeaf) {
        for (final Row r : rows) {
          if (!matches(r, threshold)) {
            continue;
          }
          final long[] acc = expected.computeIfAbsent(Arrays.asList(r.gx, r.gy),
              k -> new long[] {0, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0, 0, Long.MAX_VALUE,
                  Long.MIN_VALUE});
          acc[0]++;
          if (r.aggPresent) {
            acc[1]++;
            acc[2] += r.agg;
            acc[3] = Math.min(acc[3], r.agg);
            acc[4] = Math.max(acc[4], r.agg);
          }
          if (r.sortAPresent) {
            acc[5]++;
            acc[6] += r.sortA;
            acc[7] = Math.min(acc[7], r.sortA);
            acc[8] = Math.max(acc[8], r.sortA);
          }
        }
      }

      final Object2ObjectOpenHashMap<ProjectionIndexByteScan.GroupKey, long[]> out =
          new Object2ObjectOpenHashMap<>();
      ProjectionIndexByteScan.conjunctiveAggregateByGroupMulti(leaves, preds, groupCols, aggCols,
          out, 0);

      assertEquals(expected.size(), out.size(), "group count (seed " + seed + ")");
      // Emission order: kernel groups sorted by first-seen ordinal must equal the
      // oracle's LinkedHashMap insertion order.
      final ArrayList<Map.Entry<ProjectionIndexByteScan.GroupKey, long[]>> ordered =
          new ArrayList<>(out.entrySet());
      ordered.sort((a, b) -> Long.compare(a.getValue()[1], b.getValue()[1]));
      int idx = 0;
      for (final Map.Entry<List<String>, long[]> e : expected.entrySet()) {
        final Map.Entry<ProjectionIndexByteScan.GroupKey, long[]> got = ordered.get(idx++);
        assertEquals(e.getKey().get(0), got.getKey().part(0), "gx (seed " + seed + ")");
        assertEquals(e.getKey().get(1), got.getKey().part(1), "gy (seed " + seed + ")");
        final long[] exp = e.getValue();
        final long[] acc = got.getValue();
        assertEquals(exp[0], acc[0], "rows (seed " + seed + ")");
        assertEquals(exp[1], acc[2], "agg0 count");
        assertEquals(exp[2], acc[3], "agg0 sum");
        if (exp[1] > 0) {
          assertEquals(exp[3], acc[4], "agg0 min");
          assertEquals(exp[4], acc[5], "agg0 max");
        }
        assertEquals(exp[5], acc[6], "agg1 count");
        assertEquals(exp[6], acc[7], "agg1 sum");
        if (exp[5] > 0) {
          assertEquals(exp[7], acc[8], "agg1 min");
          assertEquals(exp[8], acc[9], "agg1 max");
        }
      }
    }
  }

  @Test
  void multiKeySortTupleCollectionParity() {
    for (long seed = 1; seed <= 6; seed++) {
      final Random rnd = new Random(seed);
      final List<List<Row>> perLeaf = new ArrayList<>();
      final List<byte[]> leaves = new ArrayList<>();
      final int rowGroupCount = 1 + rnd.nextInt(4);
      for (int l = 0; l < rowGroupCount; l++) {
        final List<Row> rows = randomRows(rnd, 1 + rnd.nextInt(200), 10_000L * (l + 1));
        perLeaf.add(rows);
        leaves.add(buildLeaf(rows));
      }
      final long threshold = 5;
      final ProjectionIndexScan.ColumnPredicate[] preds = {
          ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, threshold)
      };
      final int[] sortCols = {1, 2};

      final LongArrayList expValues = new LongArrayList();
      final LongArrayList expKeys = new LongArrayList();
      final LongArrayList expMissing = new LongArrayList();
      for (final List<Row> rows : perLeaf) {
        for (final Row r : rows) {
          if (!matches(r, threshold)) {
            continue;
          }
          if (!r.sortAPresent || !r.sortBPresent) {
            expMissing.add(r.key);
            continue;
          }
          expValues.add(r.sortA);
          expValues.add(r.sortB);
          expKeys.add(r.key);
        }
      }

      final LongArrayList values = new LongArrayList();
      final LongArrayList keys = new LongArrayList();
      final LongArrayList missing = new LongArrayList();
      ProjectionIndexByteScan.collectMatchingSortTuples(leaves, preds, sortCols, values, keys,
          missing);

      assertArrayEquals(expValues.toLongArray(), values.toLongArray(),
          "row-major tuple values (seed " + seed + ")");
      assertArrayEquals(expKeys.toLongArray(), keys.toLongArray(), "keys (seed " + seed + ")");
      assertArrayEquals(expMissing.toLongArray(), missing.toLongArray(),
          "missing keys (seed " + seed + ")");
    }
  }
}
