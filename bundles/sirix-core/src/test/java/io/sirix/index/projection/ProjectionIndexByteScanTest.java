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
}
