/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Scan-side round-trip: build leaf pages via {@link ProjectionIndexLeafPage#appendRow}
 * + {@link ProjectionIndexLeafPage#serialize()}, then count matching rows via
 * {@link ProjectionIndexScan}. Exercises the conjunctive scan, zone-map
 * pruning, and each per-column kernel.
 */
final class ProjectionIndexScanTest {

  private static final byte[] KINDS_NUM_BOOL_STR = {
      ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG,
      ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN,
      ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT
  };

  /** Build one leaf of rowCount rows with deterministic values. */
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
  void countRowsAcrossLeaves() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(1000L, 10));
    leaves.add(buildLeaf(2000L, 5));
    assertEquals(15L, ProjectionIndexScan.countRows(leaves));
  }

  @Test
  void numericGreaterThanPredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    // age > 43 → rows 4..9 → 6 matches
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)
    };
    assertEquals(6L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void numericEqualsPredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.EQ, 42L)
    };
    assertEquals(1L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanTruePredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    // active == true → even rows 0,2,4,6,8 → 5 matches
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true)
    };
    assertEquals(5L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void booleanFalsePredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, false)
    };
    assertEquals(5L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqPredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    // dept == "Eng" → rows 0,3,6 → 3 matches
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(3L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void stringEqPredicateAbsentLiteral() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "NotInDict".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(0L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void conjunctiveMultiColumnPredicate() {
    final List<byte[]> leaves = List.of(buildLeaf(0L, 9));
    // age > 40 (rows 1..8 = 8) AND active (even rows: 2,4,6,8 = 4) AND dept=="Eng"
    // "Eng" rows (0,3,6). Intersection with active-even {2,4,6,8}: {6}. age>40 at row 6 is 46 → yes.
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 40L),
        ProjectionIndexScan.ColumnPredicate.booleanEq(1, true),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "Eng".getBytes(StandardCharsets.UTF_8))
    };
    assertEquals(1L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void zoneMapPrunesLeafOutOfRange() {
    // Leaf values 40..49. Predicate age > 1000 → min/max prune → no deserialization,
    // zero matches. Observable only via correctness, but exercises the prune branch.
    final List<byte[]> leaves = List.of(buildLeaf(0L, 10));
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 1000L)
    };
    assertEquals(0L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }

  @Test
  void scanAcrossMultipleLeaves() {
    final List<byte[]> leaves = new ArrayList<>();
    leaves.add(buildLeaf(0L, 10));
    leaves.add(buildLeaf(1000L, 10));
    // age > 43 within each 0..9 block → 6 per leaf, 12 total
    final ProjectionIndexScan.ColumnPredicate[] preds = {
        ProjectionIndexScan.ColumnPredicate.numeric(0, ProjectionIndexScan.Op.GT, 43L)
    };
    assertEquals(12L, ProjectionIndexScan.conjunctiveCount(leaves, preds));
  }
}
