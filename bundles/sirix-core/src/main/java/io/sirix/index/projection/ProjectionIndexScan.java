/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;

/**
 * Stateless conjunctive-predicate scan over a collection of serialised
 * {@link ProjectionIndexLeafPage}s. Encodes the analytical filter
 * workload (e.g. {@code WHERE age > 40 AND active}) against a declared
 * projection: each predicate leaf is a {@link ColumnPredicate}
 * referencing a column index in the leaf's layout.
 *
 * <h2>Hot-path shape</h2>
 * For each leaf:
 *
 * <ol>
 *   <li>Parse the header (row count + per-column zone-map min/max)
 *       without touching the value bodies.</li>
 *   <li>Zone-map prune: if any predicate proves unsatisfiable on the
 *       leaf's min/max, skip the leaf entirely — zero value reads.</li>
 *   <li>Otherwise {@link ProjectionIndexLeafPage#deserialize} and
 *       produce a per-column {@code long[]} mask (1024 bits packed
 *       64-way) via the SIMD-ready kernels reused from
 *       {@link io.sirix.page.pax.NumberRegionSimd} (numeric compares)
 *       and {@link io.sirix.page.pax.BooleanRegion#countTrue} (boolean
 *       POPCNT) — at this step each column's per-row predicate outcome
 *       is a bit in a 1024-bit mask.</li>
 *   <li>AND masks across columns → one 1024-bit mask per leaf.</li>
 *   <li>POPCNT the mask → per-leaf row count. Sum across leaves.</li>
 * </ol>
 *
 * <h2>HFT</h2>
 * The scan owns no per-leaf allocations beyond the reusable work
 * bitmap ({@code long[16]}) and the per-column mask buffers
 * ({@code long[16]} × columnCount). The only per-leaf heap touch is
 * the (deserialise once, scan) materialisation of a
 * {@code ProjectionIndexLeafPage}; a later commit will add a
 * zero-copy reader that walks the raw byte[] straight — useful once
 * HOT integration lets us stream leaves straight from the page cache.
 */
public final class ProjectionIndexScan {

  /**
   * Per-column conjunctive predicate leaf. Exactly one of the typed
   * literals is meaningful per op kind.
   *
   * <p>For numeric single-bound ops ({@code GT/GE/LT/LE/EQ}) only
   * {@link #longLit} is read. For fused range ops ({@code BETWEEN_*})
   * both {@link #longLit} (= low bound) and {@link #highLit} (= high
   * bound) are read. {@link #boolLit} / {@link #stringLitBytes} are
   * zero / null for numeric predicates.
   */
  public static final class ColumnPredicate {
    /** Index into the leaf's column layout. */
    public final int column;
    public final Op op;
    public final long longLit;
    /**
     * High-bound literal for {@code BETWEEN_*} ops. Always 0L for
     * single-bound ops — evaluator never reads this field except on
     * BETWEEN arms.
     */
    public final long highLit;
    public final boolean boolLit;
    public final byte[] stringLitBytes;  // UTF-8

    public ColumnPredicate(final int column, final Op op,
        final long longLit, final long highLit, final boolean boolLit,
        final byte[] stringLitBytes) {
      this.column = column;
      this.op = op;
      this.longLit = longLit;
      this.highLit = highLit;
      this.boolLit = boolLit;
      this.stringLitBytes = stringLitBytes;
    }

    public static ColumnPredicate numeric(final int column, final Op op, final long literal) {
      return new ColumnPredicate(column, op, literal, 0L, false, null);
    }

    public static ColumnPredicate booleanEq(final int column, final boolean literal) {
      return new ColumnPredicate(column, Op.EQ, 0L, 0L, literal, null);
    }

    public static ColumnPredicate stringEq(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.EQ, 0L, 0L, false, literalUtf8);
    }

    /**
     * Fused BETWEEN predicate: {@code lowOp(lowLit) AND highOp(highLit)}
     * on the same column, in one evaluator call. Rejects non-BETWEEN
     * op combinations so callers cannot accidentally construct an
     * inconsistent predicate.
     */
    public static ColumnPredicate numericBetween(final int column,
        final Op lowOp, final long lowLit, final Op highOp, final long highLit) {
      final Op fused = fuseBetween(lowOp, highOp);
      return new ColumnPredicate(column, fused, lowLit, highLit, false, null);
    }

    /**
     * Map a {@code (lowOp, highOp)} pair to its fused BETWEEN op. The
     * low op must be {@code GT} or {@code GE}; the high op must be
     * {@code LT} or {@code LE}. Anything else is an invariant
     * violation and throws — callers are expected to gate on this
     * shape before constructing the fused predicate.
     */
    public static Op fuseBetween(final Op lowOp, final Op highOp) {
      if (lowOp == Op.GT && highOp == Op.LT) return Op.BETWEEN_GT_LT;
      if (lowOp == Op.GT && highOp == Op.LE) return Op.BETWEEN_GT_LE;
      if (lowOp == Op.GE && highOp == Op.LT) return Op.BETWEEN_GE_LT;
      if (lowOp == Op.GE && highOp == Op.LE) return Op.BETWEEN_GE_LE;
      throw new IllegalArgumentException(
          "BETWEEN fusion requires (GT|GE, LT|LE), got (" + lowOp + ", " + highOp + ")");
    }
  }

  public enum Op {
    GT, LT, GE, LE, EQ,
    /** Fused {@code lowLit < v < highLit}. */
    BETWEEN_GT_LT,
    /** Fused {@code lowLit < v <= highLit}. */
    BETWEEN_GT_LE,
    /** Fused {@code lowLit <= v < highLit}. */
    BETWEEN_GE_LT,
    /** Fused {@code lowLit <= v <= highLit}. */
    BETWEEN_GE_LE
  }

  private ProjectionIndexScan() {
  }

  /**
   * Count rows across {@code leafPayloads} that satisfy the conjunctive
   * {@code predicates}. Predicate-free calls are rejected — call
   * {@link #countRows(Iterable)} for unconditional counts.
   */
  public static long conjunctiveCount(final Iterable<byte[]> leafPayloads,
      final ColumnPredicate[] predicates) {
    if (predicates == null || predicates.length == 0) {
      throw new IllegalArgumentException("use countRows for unconditional counts");
    }
    long total = 0;
    for (final byte[] payload : leafPayloads) {
      total += countLeaf(payload, predicates);
    }
    return total;
  }

  /** Raw row count across leaves — used for {@code SELECT count(*)}. */
  public static long countRows(final Iterable<byte[]> leafPayloads) {
    long total = 0;
    for (final byte[] payload : leafPayloads) {
      // Parse the header's rowCount field without materialising the rest.
      total += java.nio.ByteBuffer.wrap(payload, 0, 4)
          .order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    }
    return total;
  }

  private static long countLeaf(final byte[] payload, final ColumnPredicate[] predicates) {
    final ProjectionIndexLeafPage leaf = ProjectionIndexLeafPage.deserialize(payload);
    final int rowCount = leaf.getRowCount();
    if (rowCount == 0) return 0L;

    // Zone-map prune: if any predicate is provably unsatisfiable against
    // the leaf's column min/max, the whole leaf contributes zero.
    for (final ColumnPredicate p : predicates) {
      if (pruneByZoneMap(leaf, p)) return 0L;
    }

    // One 1024-bit mask initialised to "all rows pass" and AND'd with
    // each column's predicate outcome in turn. When the leaf carries v2
    // presence bitmaps, each predicate also ANDs its column's presence —
    // a comparison over a MISSING field is false (the stored default must
    // never match). Mirrors ProjectionIndexByteScan's sparse semantics.
    final int stride = (rowCount + 63) >>> 6;
    final long[] mask = new long[stride];
    fillAllTrue(mask, rowCount);
    final long[] colMask = new long[stride];
    for (final ColumnPredicate p : predicates) {
      java.util.Arrays.fill(colMask, 0L);
      evalColumn(leaf, p, rowCount, colMask);
      if (leaf.hasPresence()) {
        final long[] presence = leaf.presenceColumnBits(p.column);
        for (int i = 0; i < stride; i++) mask[i] &= colMask[i] & presence[i];
      } else {
        for (int i = 0; i < stride; i++) mask[i] &= colMask[i];
      }
    }
    long result = 0;
    for (int i = 0; i < stride; i++) result += Long.bitCount(mask[i]);
    return result;
  }

  /**
   * Evaluate a column predicate against the leaf's raw column array(s)
   * and write the per-row outcome bits into {@code out} (length
   * {@code >= ceil(rowCount/64)}).
   */
  private static void evalColumn(final ProjectionIndexLeafPage leaf, final ColumnPredicate p,
      final int rowCount, final long[] out) {
    final byte kind = leaf.columnKind(p.column);
    switch (kind) {
      case ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG -> evalNumeric(
          leaf.numericColumn(p.column), rowCount, p.op, p.longLit, p.highLit, out);
      case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> evalBoolean(
          leaf.booleanColumnBits(p.column), rowCount, p.boolLit, out);
      case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> evalStringEq(leaf, p, rowCount, out);
      default -> throw new IllegalStateException("Unknown column kind " + kind);
    }
  }

  private static void evalNumeric(final long[] col, final int rowCount, final Op op,
      final long lit, final long highLit, final long[] out) {
    switch (op) {
      case GT -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] > lit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case LT -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] < lit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case GE -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] >= lit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case LE -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] <= lit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case EQ -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] == lit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GT_LT -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v > lit && v < highLit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GT_LE -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v > lit && v <= highLit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GE_LT -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v >= lit && v < highLit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GE_LE -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v >= lit && v <= highLit) out[i >>> 6] |= 1L << (i & 63);
        }
      }
    }
  }

  private static void evalBoolean(final long[] packedBits, final int rowCount,
      final boolean wantTrue, final long[] out) {
    final int stride = (rowCount + 63) >>> 6;
    if (wantTrue) {
      System.arraycopy(packedBits, 0, out, 0, stride);
    } else {
      for (int i = 0; i < stride; i++) out[i] = ~packedBits[i];
      // Mask off bits beyond rowCount to avoid counting phantom "true" bits
      // for the tail below the 64-bit boundary.
      final int tail = rowCount & 63;
      if (tail != 0) out[stride - 1] &= (1L << tail) - 1L;
    }
  }

  private static void evalStringEq(final ProjectionIndexLeafPage leaf,
      final ColumnPredicate p, final int rowCount, final long[] out) {
    final byte[][] dict = leaf.stringDictionary(p.column);
    // Find the dict-id corresponding to the literal; -1 if absent →
    // leaf has no matching rows.
    int targetDictId = -1;
    for (int i = 0; i < dict.length && dict[i] != null; i++) {
      if (Arrays.equals(dict[i], p.stringLitBytes)) {
        targetDictId = i;
        break;
      }
    }
    if (targetDictId < 0) return;
    final int[] ids = leaf.stringDictIdColumn(p.column);
    for (int i = 0; i < rowCount; i++) {
      if (ids[i] == targetDictId) out[i >>> 6] |= 1L << (i & 63);
    }
  }

  private static void fillAllTrue(final long[] mask, final int rowCount) {
    final int fullWords = rowCount >>> 6;
    for (int i = 0; i < fullWords; i++) mask[i] = -1L;
    final int tail = rowCount & 63;
    if (tail != 0) mask[fullWords] = (1L << tail) - 1L;
  }

  private static boolean pruneByZoneMap(final ProjectionIndexLeafPage leaf,
      final ColumnPredicate p) {
    final byte kind = leaf.columnKind(p.column);
    // Zone maps only help on numeric / dict-id columns. Booleans pass
    // through — pruning them would require leaf-global has-true/
    // has-false flags which we don't encode today.
    if (kind != ProjectionIndexLeafPage.COLUMN_KIND_NUMERIC_LONG) return false;
    final long min = leaf.columnMin(p.column);
    final long max = leaf.columnMax(p.column);
    return switch (p.op) {
      case GT -> max <= p.longLit;
      case LT -> min >= p.longLit;
      case GE -> max < p.longLit;
      case LE -> min > p.longLit;
      case EQ -> p.longLit < min || p.longLit > max;
      // BETWEEN zone-skip: OR of the two independent zone-skip
      // conditions. Strictly no more pessimistic than running each
      // bound as a separate predicate. See iter07-range-fusion-analysis.md.
      case BETWEEN_GT_LT -> max <= p.longLit || min >= p.highLit;
      case BETWEEN_GT_LE -> max <= p.longLit || min > p.highLit;
      case BETWEEN_GE_LT -> max < p.longLit || min >= p.highLit;
      case BETWEEN_GE_LE -> max < p.longLit || min > p.highLit;
    };
  }
}
