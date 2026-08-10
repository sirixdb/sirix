/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionIndexScan.Op;

import java.util.Arrays;

/**
 * Predicate evaluation over RUN-LENGTH ENCODED columns, without expanding them.
 *
 * <h2>Why this exists</h2>
 *
 * <p>BtrBlocks lists RLE among its schemes and vectorizes its DECOMPRESSION (§5). Our scan paths do
 * not decompress at all — they evaluate predicates over the encoded bytes — and a positional kernel
 * cannot address a run-encoded column, because value {@code i} is only reachable by walking the run
 * lengths. That left RLE unusable for us despite the ratio it offers, which is the one place our
 * design and the paper's genuinely pull apart: their headline is decompression-bound, ours is
 * scan-bound.
 *
 * <p>The resolution is not to decompress faster but to compare LESS. A predicate is a function of
 * the value, so it holds for every row of a run or for none of them: one comparison per RUN decides
 * as many rows as the run is long. Where a positional kernel does {@code rows} comparisons, this
 * does {@code runs}, and RLE is only ever chosen when {@code runs} is far smaller.
 *
 * <h2>The two shapes</h2>
 *
 * <ul>
 *   <li>{@link #countMatching} — no mask at all. A count over a run-encoded column is a sum of the
 *       lengths of the matching runs: {@code O(runs)} arithmetic for any number of rows. This is
 *       asymptotically better than the positional path rather than a constant-factor win.</li>
 *   <li>{@link #evalInto} — for a conjunction, where the running mask must be narrowed. Matching
 *       runs leave their rows alone and non-matching runs clear theirs, WORD AT A TIME: clearing a
 *       range of bits costs {@code O(range/64)}, so even here no per-row work is done.</li>
 * </ul>
 *
 * <p>Both take the runs as plain arrays rather than the encoded bytes. Unpacking two bit-packed
 * arrays of length {@code runs} is proportional to the runs, not the rows, so it does not put the
 * per-row cost back — and it keeps this kernel independent of the block's byte layout.
 */
public final class RleScan {

  private RleScan() {
  }

  /**
   * Rows matching {@code op} against {@code lit}/{@code highLit}, counted from the runs alone.
   *
   * <p>No mask is written and no row is visited. The caller uses this when the predicate is the
   * only one on the column and the answer is a count — the shape that dominates analytical scans.
   *
   * @param runValues one value per run
   * @param runLengths one length per run, same order
   * @param runs number of runs
   * @return matching row count, or {@code -1} when the arrays are inconsistent
   */
  public static long countMatching(final long[] runValues, final int[] runLengths, final int runs,
      final Op op, final long lit, final long highLit) {
    if (runValues == null || runLengths == null || runs < 0
        || runs > runValues.length || runs > runLengths.length) {
      return -1;
    }
    long matched = 0;
    for (int r = 0; r < runs; r++) {
      final int length = runLengths[r];
      if (length < 0) {
        return -1;
      }
      if (matches(runValues[r], op, lit, highLit)) {
        matched += length;
      }
    }
    return matched;
  }

  /**
   * Narrow {@code mask} to the rows whose run satisfies the predicate.
   *
   * <p>The mask arrives holding the conjunction so far and is ANDed in place, which is the
   * convention every other kernel here follows. A matching run is left untouched — its rows keep
   * whatever earlier predicates decided — and a non-matching run has its whole row range cleared.
   *
   * @param rowCount rows the mask covers; the runs must sum to exactly this
   * @return {@code false} when the runs do not describe {@code rowCount} rows, in which case the
   *         mask is left as it was and the caller must fall back rather than trust a partial narrow
   */
  public static boolean evalInto(final long[] runValues, final int[] runLengths, final int runs,
      final int rowCount, final Op op, final long lit, final long highLit, final long[] mask) {
    if (runValues == null || runLengths == null || mask == null || runs < 0
        || runs > runValues.length || runs > runLengths.length) {
      return false;
    }
    if (mask.length < (rowCount + 63) >>> 6) {
      return false;
    }
    // Verified BEFORE any bit is touched: a run set that does not cover the rows exactly would
    // otherwise leave the mask half-narrowed, and a half-narrowed conjunction is a wrong answer
    // rather than a slow one.
    long total = 0;
    for (int r = 0; r < runs; r++) {
      if (runLengths[r] < 0) {
        return false;
      }
      total += runLengths[r];
    }
    if (total != rowCount) {
      return false;
    }
    int row = 0;
    for (int r = 0; r < runs; r++) {
      final int length = runLengths[r];
      if (!matches(runValues[r], op, lit, highLit)) {
        clearRange(mask, row, row + length);
      }
      row += length;
    }
    return true;
  }

  /**
   * Clear bits {@code [from, to)} a word at a time.
   *
   * <p>The reason the conjunctive shape stays run-proportional: a run of ten thousand rows costs
   * the ~156 words it spans, not ten thousand bit writes.
   */
  static void clearRange(final long[] mask, final int from, final int to) {
    if (from >= to) {
      return;
    }
    final int firstWord = from >>> 6;
    final int lastWord = (to - 1) >>> 6;
    // -1L << (from & 63) selects the bits at and above `from`; Java's shift is mod 64, which makes
    // an aligned start select the whole word rather than nothing.
    final long headMask = -1L << (from & 63);
    final int lastBit = (to - 1) & 63;
    final long tailMask = lastBit == 63 ? -1L : (1L << (lastBit + 1)) - 1L;
    if (firstWord == lastWord) {
      mask[firstWord] &= ~(headMask & tailMask);
      return;
    }
    mask[firstWord] &= ~headMask;
    if (lastWord > firstWord + 1) {
      Arrays.fill(mask, firstWord + 1, lastWord, 0L);
    }
    mask[lastWord] &= ~tailMask;
  }

  /** Set bits {@code [from, to)} a word at a time — the mirror of {@link #clearRange}. */
  static void setRange(final long[] mask, final int from, final int to) {
    if (from >= to) {
      return;
    }
    final int firstWord = from >>> 6;
    final int lastWord = (to - 1) >>> 6;
    final long headMask = -1L << (from & 63);
    final int lastBit = (to - 1) & 63;
    final long tailMask = lastBit == 63 ? -1L : (1L << (lastBit + 1)) - 1L;
    if (firstWord == lastWord) {
      mask[firstWord] |= headMask & tailMask;
      return;
    }
    mask[firstWord] |= headMask;
    if (lastWord > firstWord + 1) {
      Arrays.fill(mask, firstWord + 1, lastWord, -1L);
    }
    mask[lastWord] |= tailMask;
  }

  /**
   * The predicate, evaluated once for a whole run.
   *
   * <p>Deliberately the same comparison set and the same operand order as the positional kernels:
   * a run-aware path that disagreed with them on a boundary would produce answers that depend on
   * which scheme the compressor happened to pick for a block.
   */
  private static boolean matches(final long value, final Op op, final long lit, final long high) {
    return switch (op) {
      case GT -> value > lit;
      case LT -> value < lit;
      case GE -> value >= lit;
      case LE -> value <= lit;
      case EQ -> value == lit;
      case BETWEEN_GT_LT -> value > lit && value < high;
      case BETWEEN_GT_LE -> value > lit && value <= high;
      case BETWEEN_GE_LT -> value >= lit && value < high;
      case BETWEEN_GE_LE -> value >= lit && value <= high;
    };
  }
}
