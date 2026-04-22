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
}
