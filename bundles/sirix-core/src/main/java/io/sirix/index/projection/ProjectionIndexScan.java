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
   */
  public static final class ColumnPredicate {
    /** Index into the leaf's column layout. */
    public final int column;
    public final Op op;
    public final long longLit;
    public final boolean boolLit;
    public final byte[] stringLitBytes;  // UTF-8

    public ColumnPredicate(final int column, final Op op,
        final long longLit, final boolean boolLit, final byte[] stringLitBytes) {
      this.column = column;
      this.op = op;
      this.longLit = longLit;
      this.boolLit = boolLit;
      this.stringLitBytes = stringLitBytes;
    }

    public static ColumnPredicate numeric(final int column, final Op op, final long literal) {
      return new ColumnPredicate(column, op, literal, false, null);
    }

    public static ColumnPredicate booleanEq(final int column, final boolean literal) {
      return new ColumnPredicate(column, Op.EQ, 0L, literal, null);
    }

    public static ColumnPredicate stringEq(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.EQ, 0L, false, literalUtf8);
    }
  }

  public enum Op {
    GT, LT, GE, LE, EQ
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
    // each column's predicate outcome in turn.
    final int stride = (rowCount + 63) >>> 6;
    final long[] mask = new long[stride];
    fillAllTrue(mask, rowCount);
    final long[] colMask = new long[stride];
    for (final ColumnPredicate p : predicates) {
      java.util.Arrays.fill(colMask, 0L);
      evalColumn(leaf, p, rowCount, colMask);
      for (int i = 0; i < stride; i++) mask[i] &= colMask[i];
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
          leaf.numericColumn(p.column), rowCount, p.op, p.longLit, out);
      case ProjectionIndexLeafPage.COLUMN_KIND_BOOLEAN -> evalBoolean(
          leaf.booleanColumnBits(p.column), rowCount, p.boolLit, out);
      case ProjectionIndexLeafPage.COLUMN_KIND_STRING_DICT -> evalStringEq(leaf, p, rowCount, out);
      default -> throw new IllegalStateException("Unknown column kind " + kind);
    }
  }

  private static void evalNumeric(final long[] col, final int rowCount, final Op op,
      final long lit, final long[] out) {
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
    };
  }
}
