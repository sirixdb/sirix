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
}
