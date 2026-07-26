/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.query.scan;

import io.sirix.index.projection.ProjectionIndexScan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * Unit tests for the iter#07 range-fusion pass in
 * {@link SirixVectorizedExecutor#fuseRangePredicates}.
 *
 * <p>The fusion pass is a stand-alone static method on the executor; these
 * tests exercise it directly without spinning up a Sirix DB. Correctness
 * against real projection-index leaves is covered by
 * {@code ProjectionIndexByteScanTest.between*Parity}.
 */
public final class RangeFusionPassTest {

  private static ProjectionIndexScan.ColumnPredicate num(final int col,
      final ProjectionIndexScan.Op op, final long lit) {
    return ProjectionIndexScan.ColumnPredicate.numeric(col, op, lit);
  }

  private static ProjectionIndexScan.ColumnPredicate bool(final int col, final boolean lit) {
    return ProjectionIndexScan.ColumnPredicate.booleanEq(col, lit);
  }

  /**
   * {@code age > 30 AND age < 50} ⇒ one fused {@code BETWEEN_GT_LT}.
   * The canonical compoundAndFilterCount shape.
   */
  @Test
  void fusesTwoBoundsOnSameColumn() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 30L),
        num(0, ProjectionIndexScan.Op.LT, 50L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertEquals(1, out.length);
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[0].op);
    assertEquals(0, out[0].column);
    assertEquals(30L, out[0].longLit);
    assertEquals(50L, out[0].highLit);
  }

  @Test
  void fusesReversedOrderBounds() {
    // LT comes before GT in input array — fusion must still work.
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.LT, 50L),
        num(0, ProjectionIndexScan.Op.GT, 30L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertEquals(1, out.length);
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[0].op);
    assertEquals(30L, out[0].longLit);
    assertEquals(50L, out[0].highLit);
  }

  @Test
  void fusesAllFourOpCombinations() {
    // (GT, LT) → BETWEEN_GT_LT
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT,
        SirixVectorizedExecutor.fuseRangePredicates(new ProjectionIndexScan.ColumnPredicate[] {
            num(0, ProjectionIndexScan.Op.GT, 1L),
            num(0, ProjectionIndexScan.Op.LT, 10L)})[0].op);
    // (GT, LE) → BETWEEN_GT_LE
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LE,
        SirixVectorizedExecutor.fuseRangePredicates(new ProjectionIndexScan.ColumnPredicate[] {
            num(0, ProjectionIndexScan.Op.GT, 1L),
            num(0, ProjectionIndexScan.Op.LE, 10L)})[0].op);
    // (GE, LT) → BETWEEN_GE_LT
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GE_LT,
        SirixVectorizedExecutor.fuseRangePredicates(new ProjectionIndexScan.ColumnPredicate[] {
            num(0, ProjectionIndexScan.Op.GE, 1L),
            num(0, ProjectionIndexScan.Op.LT, 10L)})[0].op);
    // (GE, LE) → BETWEEN_GE_LE
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GE_LE,
        SirixVectorizedExecutor.fuseRangePredicates(new ProjectionIndexScan.ColumnPredicate[] {
            num(0, ProjectionIndexScan.Op.GE, 1L),
            num(0, ProjectionIndexScan.Op.LE, 10L)})[0].op);
  }

  /**
   * {@code age > 30 AND age < 50 AND active} ⇒ one BETWEEN + unchanged bool.
   * Fused predicate precedes the unchanged predicate in the output.
   */
  @Test
  void fusesAndPreservesOtherPredicates() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 30L),
        num(0, ProjectionIndexScan.Op.LT, 50L),
        bool(1, true)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertEquals(2, out.length);
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[0].op);
    assertEquals(ProjectionIndexScan.Op.EQ, out[1].op);  // booleanEq uses EQ
    assertEquals(1, out[1].column);
    assertEquals(true, out[1].boolLit);
  }

  /**
   * Two GT on same column — no fusion (not a range). Input returned unchanged.
   */
  @Test
  void noFuseSameDirectionBounds() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 10L),
        num(0, ProjectionIndexScan.Op.GT, 20L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertSame(in, out);  // unchanged array reference — zero alloc
  }

  /**
   * EQ + LT on same column — EQ is not a low-bound, no fusion.
   */
  @Test
  void noFuseEqWithRange() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.EQ, 10L),
        num(0, ProjectionIndexScan.Op.LT, 50L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertSame(in, out);
  }

  /**
   * Bounds on different columns — no fusion.
   */
  @Test
  void noFuseDifferentColumns() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 10L),
        num(1, ProjectionIndexScan.Op.LT, 50L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertSame(in, out);
  }

  /**
   * More than one fusible pair — e.g. two range columns in one query.
   */
  @Test
  void fusesMultiplePairsOnDifferentColumns() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 30L),
        num(0, ProjectionIndexScan.Op.LT, 50L),
        num(1, ProjectionIndexScan.Op.GE, 5L),
        num(1, ProjectionIndexScan.Op.LE, 15L),
        bool(2, true)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertEquals(3, out.length);
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[0].op);
    assertEquals(0, out[0].column);
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GE_LE, out[1].op);
    assertEquals(1, out[1].column);
    assertEquals(ProjectionIndexScan.Op.EQ, out[2].op);  // bool
  }

  /**
   * Three bounds on same column (e.g. {@code a > 10 AND a > 20 AND a < 50}):
   * conservative policy fuses one pair (the first fusible match) and leaves
   * the extra alone. Result is valid — AND is associative, so (fused) AND
   * (extra) still produces the correct count.
   */
  @Test
  void fusesOnePairLeavesExtraAlone() {
    final ProjectionIndexScan.ColumnPredicate[] in = {
        num(0, ProjectionIndexScan.Op.GT, 10L),
        num(0, ProjectionIndexScan.Op.GT, 20L),
        num(0, ProjectionIndexScan.Op.LT, 50L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    // Expected: fuse (GT 10, LT 50) — first pair found — leaves (GT 20) alone.
    assertEquals(2, out.length);
    // Fused BETWEEN comes first (at the position of GT 10).
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[0].op);
    assertEquals(10L, out[0].longLit);
    assertEquals(50L, out[0].highLit);
    assertEquals(ProjectionIndexScan.Op.GT, out[1].op);
    assertEquals(20L, out[1].longLit);
  }

  /**
   * Null / empty / single-predicate arrays — no-op, return input unchanged.
   */
  @Test
  void emptyArraysPassThrough() {
    assertSame(null, SirixVectorizedExecutor.fuseRangePredicates(null));
    final ProjectionIndexScan.ColumnPredicate[] empty = new ProjectionIndexScan.ColumnPredicate[0];
    assertSame(empty, SirixVectorizedExecutor.fuseRangePredicates(empty));
    final ProjectionIndexScan.ColumnPredicate[] one = {num(0, ProjectionIndexScan.Op.GT, 5L)};
    assertSame(one, SirixVectorizedExecutor.fuseRangePredicates(one));
  }

  /**
   * Preserve input order of non-fused predicates. For filterGroupBy +
   * subsequent scan, the executor relies on the order (columns touched in
   * declared order) — fusion must not reorder non-fused predicates.
   */
  @Test
  void preservesOrderOfNonFusedPredicates() {
    // (bool, GT-on-col1, STR_EQ-on-col2, LT-on-col1) → fused pair moves to
    // earliest fused position; other predicates keep relative order.
    final ProjectionIndexScan.ColumnPredicate[] in = {
        bool(0, true),
        num(1, ProjectionIndexScan.Op.GT, 10L),
        ProjectionIndexScan.ColumnPredicate.stringEq(2, "x".getBytes()),
        num(1, ProjectionIndexScan.Op.LT, 50L)
    };
    final ProjectionIndexScan.ColumnPredicate[] out =
        SirixVectorizedExecutor.fuseRangePredicates(in);
    assertEquals(3, out.length);
    // bool stays first.
    assertEquals(0, out[0].column);
    // BETWEEN emitted at low-bound (index 1) position.
    assertEquals(ProjectionIndexScan.Op.BETWEEN_GT_LT, out[1].op);
    assertEquals(1, out[1].column);
    // stringEq stays last.
    assertNotNull(out[2].stringLitBytes);
    assertEquals(2, out[2].column);
  }
}
