/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Parity tests: every count produced by {@link ProjectionIndexByteScan}
 * must equal the count produced by {@link ProjectionIndexScan} on the
 * same inputs. The two paths differ only in how they read the leaf —
 * one materialises columns, the other reads bytes directly — so they
 * should agree exactly.
 */
final class ProjectionIndexByteScanTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  private static byte[] buildLeaf(final long baseKey, final int rowCount) {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Ops"};
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {40L + i, 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false};
      final String[] strs = {null, null, depts[i % depts.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  @Test
  void countRowsParity() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(1000L, 10));
    leaves.add(buildLeaf(2000L, 5));
    assertEquals(ProjectionIndexScan.countRows(leaves), ProjectionIndexByteScan.countRows(leaves));
  }

  @Test
  void numericGtParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void numericEqParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 42L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanTrueParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanFalseParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, false)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqHitParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqMissParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "NotInDict".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void threeWayAndParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void zoneMapPruneParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 1000L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void multiLeafParity() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 10));
    leaves.add(buildLeaf(1000L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void groupByCountUnfilteredSingleLeaf() {
    // 9 rows cycling through {Eng, Sales, Ops} → each group has 3.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(
        leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, out);
    assertEquals(3L, out.getLong("Eng"));
    assertEquals(3L, out.getLong("Sales"));
    assertEquals(3L, out.getLong("Ops"));
  }

  @Test
  void groupByCountFilteredByNumeric() {
    // age > 42 filter: rows 3..9 match → 7 rows cycling through depts.
    // Row indices matching: i where 40+i > 42 ⇒ i ∈ {3,4,5,6,7,8}.
    // At i=3 dept=Eng, i=4 Sales, i=5 Ops, i=6 Eng, i=7 Sales, i=8 Ops.
    // Eng: 2, Sales: 2, Ops: 2 (6 total — i=9 excluded since rowCount=9).
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 42L)
    };
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, preds, 2, out);
    assertEquals(2L, out.getLong("Eng"));
    assertEquals(2L, out.getLong("Sales"));
    assertEquals(2L, out.getLong("Ops"));
  }

  @Test
  void groupByCountMultiLeafAccumulates() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 9));      // 3/3/3
    leaves.add(buildLeaf(1000L, 6));   // 2/2/2
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(
        leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, out);
    assertEquals(5L, out.getLong("Eng"));
    assertEquals(5L, out.getLong("Sales"));
    assertEquals(5L, out.getLong("Ops"));
  }

  @Test
  void groupByOnNonStringColumnRejected() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 4));
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    // Column 0 is NUMERIC_LONG — should throw.
    assertThrows(IllegalStateException.class,
        () -> ProjectionIndexByteScan.conjunctiveCountByGroup(
            leaves, new ProjectionIndexScan.ColumnPredicate[0], 0, out));
  }

  // ---------------------------------------------------------------------
  // iter#07 range-fusion parity tests.
  //
  // For every BETWEEN op combination, the fused predicate must produce
  // byte-for-byte the same row count as two independent predicates
  // evaluated in conjunction. Tests cover the four op combos plus edge
  // cases (empty range, single-value range, all-match, no-match, mixed
  // with non-numeric predicates).
  // ---------------------------------------------------------------------

  /**
   * Build a wider synthetic leaf so between-tests can hit a variety of
   * numeric values. {@code values[i] = baseKey + i * 3 % 100} — covers
   * 0..99 with a stride that stresses the zone-map boundary.
   */
  private static byte[] buildLeafBetween(final long baseKey, final int rowCount) {
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(KINDS_NUM_BOOL_STR);
    final String[] depts = {"Eng", "Sales", "Ops"};
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {(long) ((i * 17) % 100), 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false};
      final String[] strs = {null, null, depts[i % depts.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  private static long countUnfused(final List<byte[]> leaves, final int column,
      final ProjectionIndexScan.Op lowOp, final long lowLit,
      final ProjectionIndexScan.Op highOp, final long highLit) {
    final ProjectionIndexScan.ColumnPredicate[] unfused = {
        ProjectionIndexScan.ColumnPredicate.numeric(column, lowOp, lowLit),
        ProjectionIndexScan.ColumnPredicate.numeric(column, highOp, highLit)
    };
    return ProjectionIndexByteScan.conjunctiveCount(leaves, unfused);
  }

  private static long countFused(final List<byte[]> leaves, final int column,
      final ProjectionIndexScan.Op lowOp, final long lowLit,
      final ProjectionIndexScan.Op highOp, final long highLit) {
    final ProjectionIndexScan.ColumnPredicate[] fused = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(column, lowOp, lowLit, highOp, highLit)
    };
    return ProjectionIndexByteScan.conjunctiveCount(leaves, fused);
  }

  @Test
  void betweenGtLtParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[]{-10L, 0L, 10L, 25L, 50L, 99L, 110L}) {
      for (final long hi : new long[]{-5L, 5L, 30L, 60L, 99L, 200L}) {
        assertEquals(
            countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            "GT_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGtLeParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[]{0L, 25L, 49L, 99L}) {
      for (final long hi : new long[]{0L, 30L, 50L, 99L}) {
        assertEquals(
            countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LE, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LE, hi),
            "GT_LE lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGeLtParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[]{0L, 1L, 50L, 99L, 100L}) {
      for (final long hi : new long[]{0L, 2L, 50L, 99L, 100L}) {
        assertEquals(
            countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LT, hi),
            "GE_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenGeLeParity() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    for (final long lo : new long[]{-5L, 0L, 50L, 99L, 100L}) {
      for (final long hi : new long[]{-5L, 0L, 50L, 99L, 100L}) {
        assertEquals(
            countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LE, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GE, lo, ProjectionIndexScan.Op.LE, hi),
            "GE_LE lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenEmptyRangeYieldsZero() {
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    // lo > hi → no row can satisfy.
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, 60L,
        ProjectionIndexScan.Op.LT, 30L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GT, 60L,
        ProjectionIndexScan.Op.LT, 30L);
    assertEquals(0L, unfused);
    assertEquals(0L, fused);
  }

  @Test
  void betweenSingleValueRangeGeLe() {
    // GE 50 AND LE 50 ⇒ v == 50
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, 50L,
        ProjectionIndexScan.Op.LE, 50L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GE, 50L,
        ProjectionIndexScan.Op.LE, 50L);
    assertEquals(unfused, fused);
    // Also assert it matches a direct EQ predicate.
    final ProjectionIndexScan.ColumnPredicate[] eqPred = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 50L)
    };
    assertEquals(ProjectionIndexByteScan.conjunctiveCount(leaves, eqPred), fused);
  }

  @Test
  void betweenAllMatchWidth() {
    // lo < min, hi > max ⇒ every row matches.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GE, -1000L,
        ProjectionIndexScan.Op.LE, 1000L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GE, -1000L,
        ProjectionIndexScan.Op.LE, 1000L);
    assertEquals(1024L, unfused);
    assertEquals(1024L, fused);
  }

  @Test
  void betweenNoMatchByZoneMap() {
    // hi < min ⇒ zone-map rules out the whole leaf.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    // values are (i*17)%100 ∈ [0, 99]. A BETWEEN(200, 300) excludes all.
    final long unfused = countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, 200L,
        ProjectionIndexScan.Op.LT, 300L);
    final long fused = countFused(leaves, 0, ProjectionIndexScan.Op.GT, 200L,
        ProjectionIndexScan.Op.LT, 300L);
    assertEquals(0L, unfused);
    assertEquals(0L, fused);
  }

  @Test
  void betweenMixedWithBooleanParity() {
    // age BETWEEN 30 AND 70 AND active == true.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final ProjectionIndexScan.ColumnPredicate[] unfused = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 30L),
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LT, 70L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    final ProjectionIndexScan.ColumnPredicate[] fused = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(0,
            ProjectionIndexScan.Op.GT, 30L, ProjectionIndexScan.Op.LT, 70L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    assertEquals(
        ProjectionIndexByteScan.conjunctiveCount(leaves, unfused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  @Test
  void betweenMixedWithStringEqParity() {
    // age BETWEEN 25 AND 75 AND dept == "Eng".
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 1024));
    final ProjectionIndexScan.ColumnPredicate[] unfused = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GE, 25L),
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LE, 75L),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    final ProjectionIndexScan.ColumnPredicate[] fused = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(0,
            ProjectionIndexScan.Op.GE, 25L, ProjectionIndexScan.Op.LE, 75L),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(
        ProjectionIndexByteScan.conjunctiveCount(leaves, unfused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  @Test
  void betweenMultiLeafParity() {
    // Zone-map kicks in on some leaves and not others — verify we don't
    // short-circuit wrong.
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeafBetween(0L, 1024));
    leaves.add(buildLeafBetween(1024L, 1024));
    leaves.add(buildLeafBetween(2048L, 512));  // partial leaf
    for (final long lo : new long[]{0L, 40L, 99L}) {
      for (final long hi : new long[]{50L, 99L, 500L}) {
        assertEquals(
            countUnfused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            countFused(leaves, 0, ProjectionIndexScan.Op.GT, lo, ProjectionIndexScan.Op.LT, hi),
            "multi-leaf GT_LT lo=" + lo + " hi=" + hi);
      }
    }
  }

  @Test
  void betweenMaterializingScanParity() {
    // Cross-check the materialising ProjectionIndexScan (not the byte
    // scan) handles BETWEEN correctly too — both paths share the
    // test discipline.
    final List<byte[]> leaves = List.of(buildLeafBetween(0L, 256));
    final ProjectionIndexScan.ColumnPredicate[] fused = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(0,
            ProjectionIndexScan.Op.GT, 20L, ProjectionIndexScan.Op.LE, 70L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, fused),
        ProjectionIndexByteScan.conjunctiveCount(leaves, fused));
  }

  // ---------------------------------------------------------------------
  // iter#10 dense group-by parity tests.
  //
  // For every supported input shape, the dense long[N] accumulator path
  // must produce the same Object2LongOpenHashMap<String> counts as the
  // legacy hashmap path. The dense path is an optimization — never a
  // semantic change.
  // ---------------------------------------------------------------------

  /**
   * Build a leaf with a caller-specified dept/city dictionary. Columns:
   * [numeric age, boolean active, STRING_DICT dept, STRING_DICT city].
   * Row {@code i} gets {@code depts[i % depts.length]} and
   * {@code cities[(i * 3) % cities.length]} — stride-3 on city keeps the
   * two dicts from moving in lock-step so fallback cases actually exercise
   * per-leaf dict divergence.
   */
  private static byte[] buildLeafWithDepts(final long baseKey, final int rowCount,
      final String[] depts, final String[] cities) {
    final byte[] kinds = {
        ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT,
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
    };
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(kinds);
    for (int i = 0; i < rowCount; i++) {
      final long[] nums = {20L + (i % 50), 0L, 0L, 0L};
      final boolean[] bools = {false, (i & 1) == 0, false, false};
      final String[] strs = {null, null, depts[i % depts.length],
          cities[(i * 3) % cities.length]};
      p.appendRow(baseKey + i, nums, bools, strs);
    }
    return p.serialize();
  }

  /**
   * Run both dense and hashmap paths and assert the resulting
   * Object2LongOpenHashMap<String> are byte-for-byte equal.
   */
  private static void assertDenseParity(final List<byte[]> leaves,
      final ProjectionIndexScan.ColumnPredicate[] preds, final int groupColumn,
      final byte[][] canonicalDict) {
    // Hashmap (legacy) path — ground truth.
    final Object2LongOpenHashMap<String> hash = new Object2LongOpenHashMap<>();
    hash.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, preds, groupColumn, hash);

    // Dense path — counts + fallback accumulator.
    final long[] counts = new long[canonicalDict.length];
    final Object2LongOpenHashMap<String> fallback = new Object2LongOpenHashMap<>();
    fallback.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroupDense(
        leaves, preds, groupColumn, canonicalDict, counts, fallback);
    // Merge dense counts + fallback into a single map to compare.
    final Object2LongOpenHashMap<String> dense = new Object2LongOpenHashMap<>();
    dense.defaultReturnValue(0L);
    for (int i = 0; i < canonicalDict.length; i++) {
      if (counts[i] != 0L) {
        dense.put(new String(canonicalDict[i], StandardCharsets.UTF_8), counts[i]);
      }
    }
    final var it = fallback.object2LongEntrySet().fastIterator();
    while (it.hasNext()) {
      final var e = it.next();
      dense.addTo(e.getKey(), e.getLongValue());
    }

    assertEquals(hash, dense, "dense vs hashmap group-by counts must match");
  }

  @Test
  void denseGroupBy_emptyPreds_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    assertEquals(8, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_boolEq_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    assertDenseParity(leaves, preds, 2, canonical);
  }

  @Test
  void denseGroupBy_numericBetween_8Depts() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0", "C1", "C2", "C3", "C4", "C5", "C6", "C7"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(0,
            ProjectionIndexScan.Op.GE, 30L, ProjectionIndexScan.Op.LE, 50L)
    };
    assertDenseParity(leaves, preds, 2, canonical);
  }

  @Test
  void denseGroupBy_n256_boundaryAccepted() {
    // 256 distinct strings — right at the limit.
    final String[] depts = new String[256];
    for (int i = 0; i < 256; i++) depts[i] = "D" + i;
    final String[] cities = {"C0"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    assertEquals(256, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_n257_aboveThresholdReturnsNull() {
    // 257 distinct strings — above the limit of 256.
    final String[] depts = new String[257];
    for (int i = 0; i < 257; i++) depts[i] = "D" + i;
    final String[] cities = {"C0"};
    final List<byte[]> leaves = List.of(buildLeafWithDepts(0L, 1024, depts, cities));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical,
        "probe should return null when cardinality > cardLimit");
  }

  @Test
  void denseGroupBy_crossLeafDictVariation() {
    // Two leaves with the SAME 4 values but in different dict positions.
    // Leaf 1: D0, D1, D2, D3. Leaf 2: D3, D2, D1, D0 (rotated).
    final List<byte[]> leaves = new ArrayList<>();
    final String[] cities = {"C0"};
    leaves.add(buildLeafWithDepts(0L, 12, new String[]{"D0", "D1", "D2", "D3"}, cities));
    leaves.add(buildLeafWithDepts(1000L, 12, new String[]{"D3", "D2", "D1", "D0"}, cities));
    // Canonical dict built from probing — union is {D0,D1,D2,D3}.
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    assertEquals(4, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_missingDictValueTriggersFallback() {
    // Probe sees only 2 leaves (D0, D1). A third leaf introduces D99.
    final List<byte[]> leaves = new ArrayList<>();
    final String[] cities = {"C0"};
    leaves.add(buildLeafWithDepts(0L, 8, new String[]{"D0", "D1"}, cities));
    leaves.add(buildLeafWithDepts(100L, 8, new String[]{"D0", "D1"}, cities));
    // Build canonical from the first 2 leaves only — probeLeaves=2.
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 2, 256);
    assertEquals(2, canonical.length);
    // NOW add a third leaf with a new value.
    leaves.add(buildLeafWithDepts(200L, 8, new String[]{"D99"}, cities));
    // Dense path must fall back for the third leaf. Parity holds.
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_singleValueN1() {
    final List<byte[]> leaves = List.of(
        buildLeafWithDepts(0L, 16, new String[]{"OnlyOne"}, new String[]{"C0"}));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    assertEquals(1, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void denseGroupBy_multiLeafAccumulates() {
    final String[] depts = {"D0", "D1", "D2", "D3", "D4", "D5", "D6", "D7"};
    final String[] cities = {"C0"};
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeafWithDepts(0L, 1024, depts, cities));
    leaves.add(buildLeafWithDepts(1024L, 1024, depts, cities));
    leaves.add(buildLeafWithDepts(2048L, 512, depts, cities));  // partial
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 2, 16, 256);
    assertEquals(8, canonical.length);
    assertDenseParity(leaves, new ProjectionIndexScan.ColumnPredicate[0], 2, canonical);
  }

  @Test
  void probeCanonicalDict_ineligibleForNumericColumn() {
    // Group column 0 is numeric — not STRING_DICT; probe returns null.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        leaves, 0, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical);
  }

  @Test
  void probeCanonicalDict_emptyListReturnsNull() {
    final byte[][] canonical = ProjectionIndexByteScan.probeCanonicalDict(
        List.of(), 2, 16, 256);
    org.junit.jupiter.api.Assertions.assertNull(canonical);
  }

  // ---------------------------------------------------------------------
  // iter#22 FOR-BP parity tests. Serialisation upgrades every numeric
  // column to COLUMN_KIND_NUMERIC_LONG_FOR_BP; the scan's dispatch must
  // produce identical row counts to the materialising scan, regardless
  // of the value-range spread (which drives the encoder's bitWidth).
  // ---------------------------------------------------------------------

  @Test
  void forBpNumericGtParityNarrowRange() {
    // Values in [40, 49] → bitWidth 4, fast-path gate mixed.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    // Sanity: the serialised wire kind for column 0 should be FOR-BP.
    assertEquals(ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG_FOR_BP, leaves.get(0)[24]);
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpNumericEqParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 42L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpNumericBetweenParity() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 100));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numericBetween(
            0, ProjectionIndexScan.Op.GE, 45L, ProjectionIndexScan.Op.LT, 80L)
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpZoneMapPruneAllNone() {
    // Predicate outside [40, 49] — zone-skip triggers both on the 16-byte
    // column min/max prefix (unchanged from raw) and on the FOR-BP
    // bit-width gate.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 10000L)
    };
    assertEquals(0L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
    assertEquals(0L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpZoneMapPruneAllMatch() {
    // Predicate covers entire range — both scans should return rowCount.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, -100L)
    };
    assertEquals(10L, ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
    assertEquals(10L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpConstantRunParity() {
    // All rows share the same numeric value → bitWidth == 0 constant-run fast path.
    final byte[] kinds = {
        ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
        ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
        ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
    };
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(kinds);
    for (int i = 0; i < 50; i++) {
      final long[] nums = {42L, 0L, 0L};
      final boolean[] bools = {false, false, false};
      final String[] strs = {null, null, "X"};
      p.appendRow(1000L + i, nums, bools, strs);
    }
    final List<byte[]> leaves = List.of(p.serialize());
    // EQ 42 — constant-run all-match.
    final ProjectionIndexScan.ColumnPredicate[] eq = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 42L)
    };
    assertEquals(50L, ProjectionIndexByteScan.conjunctiveCount(leaves, eq));
    // EQ 41 — constant-run no-match.
    final ProjectionIndexScan.ColumnPredicate[] ne = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 41L)
    };
    assertEquals(0L, ProjectionIndexByteScan.conjunctiveCount(leaves, ne));
  }

  @Test
  void forBpWideRangeParity() {
    // Values with moderate spread (0..1023) → bitWidth 10.
    final byte[] kinds = {ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG};
    final ProjectionIndexLeafPage p = new ProjectionIndexLeafPage(kinds);
    for (int i = 0; i < 1024; i++) {
      final long[] nums = {(long) i};
      p.appendRow(i, nums, new boolean[]{false}, new String[]{null});
    }
    final List<byte[]> leaves = List.of(p.serialize());
    for (final long threshold : new long[]{0L, 100L, 500L, 1000L, 1024L}) {
      final ProjectionIndexScan.ColumnPredicate[] preds = {
          ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.LT, threshold)
      };
      assertEquals(
          ProjectionIndexScan.conjunctiveCount(leaves, preds),
          ProjectionIndexByteScan.conjunctiveCount(leaves, preds),
          "threshold=" + threshold);
    }
  }

  @Test
  void forBpCombinedPredicatesParity() {
    // Tri-column predicate on FOR-BP numeric + raw boolean + string-dict
    // exercises the full evaluateLeafMask path.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 100));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GE, 50L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(
        ProjectionIndexScan.conjunctiveCount(leaves, preds),
        ProjectionIndexByteScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void forBpGroupByParity() {
    // Group by STRING_DICT column (col 2) while filtering on FOR-BP
    // numeric column (col 0). Validates that the group-by dispatch
    // walks past a FOR-BP-encoded column correctly.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 100));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GE, 50L)
    };
    final Object2LongOpenHashMap<String> out = new Object2LongOpenHashMap<>();
    out.defaultReturnValue(0L);
    ProjectionIndexByteScan.conjunctiveCountByGroup(leaves, preds, 2, out);
    long total = 0L;
    for (final String k : new String[]{"Eng", "Sales", "Ops"}) {
      total += out.getLong(k);
    }
    // 90 rows match (i where 40+i ≥ 50 ⇒ i ≥ 10 ⇒ rows 10..99) across 3 depts.
    assertEquals(90L, total);
  }
}
