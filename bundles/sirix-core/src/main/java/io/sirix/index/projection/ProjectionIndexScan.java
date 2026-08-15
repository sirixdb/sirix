/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.util.Arrays;

/**
 * Stateless conjunctive-predicate scan over a collection of serialised
 * {@link ProjectionIndexRowGroupPage}s. Encodes the analytical filter workload (e.g.
 * {@code WHERE age > 40 AND active}) against a declared projection: each predicate leaf is a
 * {@link ColumnPredicate} referencing a column index in the leaf's layout.
 *
 * <h2>Hot-path shape</h2> For each leaf:
 *
 * <ol>
 * <li>Parse the header (row count + per-column zone-map min/max) without touching the value
 * bodies.</li>
 * <li>Zone-map prune: if any predicate proves unsatisfiable on the leaf's min/max, skip the leaf
 * entirely — zero value reads.</li>
 * <li>Otherwise {@link ProjectionIndexRowGroupPage#deserialize} and produce a per-column
 * {@code long[]} mask (1024 bits packed 64-way) via the SIMD-ready kernels reused from
 * {@link io.sirix.page.pax.NumberRegionSimd} (numeric compares) and
 * {@link io.sirix.page.pax.BooleanRegion#countTrue} (boolean POPCNT) — at this step each column's
 * per-row predicate outcome is a bit in a 1024-bit mask.</li>
 * <li>AND masks across columns → one 1024-bit mask per leaf.</li>
 * <li>POPCNT the mask → per-leaf row count. Sum across leaves.</li>
 * </ol>
 *
 * <h2>HFT</h2> The scan owns no per-leaf allocations beyond the reusable work bitmap
 * ({@code long[16]}) and the per-column mask buffers ({@code long[16]} × columnCount). The only
 * per-leaf heap touch is the (deserialise once, scan) materialisation of a
 * {@code ProjectionIndexRowGroupPage}; a later commit will add a zero-copy reader that walks the
 * raw byte[] straight — useful once HOT integration lets us stream leaves straight from the page
 * cache.
 */
public final class ProjectionIndexScan {

  /**
   * Per-column conjunctive predicate leaf. Exactly one of the typed literals is meaningful per op
   * kind.
   *
   * <p>
   * For numeric single-bound ops ({@code GT/GE/LT/LE/EQ}) only {@link #longLit} is read. For fused
   * range ops ({@code BETWEEN_*}) both {@link #longLit} (= low bound) and {@link #highLit} (= high
   * bound) are read. {@link #boolLit} / {@link #stringLitBytes} are zero / null for numeric
   * predicates.
   */
  public static final class ColumnPredicate {
    /** Index into the leaf's column layout. */
    public final int column;
    public final Op op;
    public final long longLit;
    /**
     * High-bound literal for {@code BETWEEN_*} ops. Always 0L for single-bound ops — evaluator never
     * reads this field except on BETWEEN arms.
     */
    public final long highLit;
    public final boolean boolLit;
    public final byte[] stringLitBytes; // UTF-8

    public ColumnPredicate(final int column, final Op op, final long longLit, final long highLit, final boolean boolLit,
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

    /**
     * {@code column != literal} on a BOOLEAN column. Folded to an equality against the complement
     * rather than carried as an NE op: over a two-valued domain the two are identical for PRESENT
     * cells, and presence is applied by the caller either way.
     */
    public static ColumnPredicate booleanNe(final int column, final boolean literal) {
      return new ColumnPredicate(column, Op.EQ, 0L, 0L, !literal, null);
    }

    public static ColumnPredicate stringEq(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.EQ, 0L, 0L, false, literalUtf8);
    }

    /** {@code column != literal} on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringNe(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.NE, 0L, 0L, false, literalUtf8);
    }

    /**
     * Fused BETWEEN predicate: {@code lowOp(lowLit) AND highOp(highLit)} on the same column, in one
     * evaluator call. Rejects non-BETWEEN op combinations so callers cannot accidentally construct an
     * inconsistent predicate.
     */
    public static ColumnPredicate numericBetween(final int column, final Op lowOp, final long lowLit, final Op highOp,
        final long highLit) {
      final Op fused = fuseBetween(lowOp, highOp);
      return new ColumnPredicate(column, fused, lowLit, highLit, false, null);
    }

    /**
     * Map a {@code (lowOp, highOp)} pair to its fused BETWEEN op. The low op must be {@code GT} or
     * {@code GE}; the high op must be {@code LT} or {@code LE}. Anything else is an invariant violation
     * and throws — callers are expected to gate on this shape before constructing the fused predicate.
     */
    public static Op fuseBetween(final Op lowOp, final Op highOp) {
      if (lowOp == Op.GT && highOp == Op.LT)
        return Op.BETWEEN_GT_LT;
      if (lowOp == Op.GT && highOp == Op.LE)
        return Op.BETWEEN_GT_LE;
      if (lowOp == Op.GE && highOp == Op.LT)
        return Op.BETWEEN_GE_LT;
      if (lowOp == Op.GE && highOp == Op.LE)
        return Op.BETWEEN_GE_LE;
      throw new IllegalArgumentException("BETWEEN fusion requires (GT|GE, LT|LE), got (" + lowOp + ", " + highOp + ")");
    }
  }

  public enum Op {
    GT, LT, GE, LE, EQ,
    /**
     * {@code v != lit}.
     *
     * <p>
     * NOT the negation of {@link #EQ}: every leaf mask here is two-valued with <b>missing ⇒ false</b>,
     * and {@code !EQ} would flip a missing cell to TRUE. In JSONiq a missing field dereferences to the
     * empty sequence, {@code () != "x"} is the empty sequence, and a {@code where} treats that as false
     * — so a record lacking the field must NOT match {@code != ""}, which is exactly what {@code !EQ}
     * would get wrong. NE is therefore its own op, evaluated only over present cells, and never
     * rewritten as a negation.
     */
    NE,
    /** Fused {@code lowLit < v < highLit}. */
    BETWEEN_GT_LT,
    /** Fused {@code lowLit < v <= highLit}. */
    BETWEEN_GT_LE,
    /** Fused {@code lowLit <= v < highLit}. */
    BETWEEN_GE_LT,
    /** Fused {@code lowLit <= v <= highLit}. */
    BETWEEN_GE_LE
  }

  /**
   * Compiled boolean predicate TREE over column predicates — the general form the conjunction-only
   * {@code ColumnPredicate[]} cannot express: arbitrary AND/OR nesting.
   *
   * <p>
   * <b>Program encoding.</b> {@code program} is a postfix (RPN) walk over leaf masks: an entry
   * {@code >= 0} pushes leaf {@code program[i]}'s mask; {@link #OP_AND} pops two masks and pushes
   * their intersection; {@link #OP_OR} pushes their union. A well-formed program leaves exactly one
   * mask on the stack. Leaf masks encode two-valued predicate truth with missing ⇒ {@code false}
   * (presence AND) — under AND/OR composition this is exactly the interpreter's general-comparison
   * semantics, which is why NOT is deliberately NOT representable here:
   * {@code not(missing-comparison)} flips missing ⇒ {@code true}, a semantic the mask algebra must
   * model explicitly before negation can be offered (callers fall back to the generic pipeline for
   * NOT).
   *
   * <p>
   * Stack depth is bounded by {@link #MAX_LEAVES}; {@link #of} validates shape.
   */
  public static final class PredicateTree {
    /** Max leaf predicates (= max program stack depth) — bounds kernel scratch. */
    public static final int MAX_LEAVES = 16;

    public static final byte OP_AND = -1;
    public static final byte OP_OR = -2;

    public final ColumnPredicate[] leaves;
    public final byte[] program;

    private PredicateTree(final ColumnPredicate[] leaves, final byte[] program) {
      this.leaves = leaves;
      this.program = program;
    }

    /**
     * Validated construction: every leaf referenced, ops in range, stack discipline sound (never
     * underflows, ends at depth 1, never exceeds {@link #MAX_LEAVES}).
     *
     * @throws IllegalArgumentException on a malformed program
     */
    public static PredicateTree of(final ColumnPredicate[] leaves, final byte[] program) {
      if (leaves == null || program == null || leaves.length == 0 || leaves.length > MAX_LEAVES) {
        throw new IllegalArgumentException("leaves must have 1.." + MAX_LEAVES + " entries");
      }
      int depth = 0;
      for (final byte insn : program) {
        if (insn >= 0) {
          if (insn >= leaves.length) {
            throw new IllegalArgumentException("leaf index " + insn + " out of range");
          }
          depth++;
          if (depth > MAX_LEAVES) {
            throw new IllegalArgumentException("program stack exceeds " + MAX_LEAVES);
          }
        } else if (insn == OP_AND || insn == OP_OR) {
          if (depth < 2) {
            throw new IllegalArgumentException("combinator underflow at depth " + depth);
          }
          depth--;
        } else {
          throw new IllegalArgumentException("unknown program op " + insn);
        }
      }
      if (depth != 1) {
        throw new IllegalArgumentException("program ends at stack depth " + depth + " (want 1)");
      }
      return new PredicateTree(leaves.clone(), program.clone());
    }

    /** Whether any combinator is an OR — pure-AND trees should use the flat conjunctive form. */
    public boolean hasOr() {
      for (final byte insn : program) {
        if (insn == OP_OR) {
          return true;
        }
      }
      return false;
    }
  }

  private ProjectionIndexScan() {}

  /**
   * Count rows across {@code rowGroupPayloads} that satisfy the conjunctive {@code predicates}.
   * Predicate-free calls are rejected — call {@link #countRows(Iterable)} for unconditional counts.
   */
  public static long conjunctiveCount(final Iterable<byte[]> rowGroupPayloads, final ColumnPredicate[] predicates) {
    if (predicates == null || predicates.length == 0) {
      throw new IllegalArgumentException("use countRows for unconditional counts");
    }
    long total = 0;
    for (final byte[] payload : rowGroupPayloads) {
      total += countRowGroup(payload, predicates);
    }
    return total;
  }

  /** Raw row count across leaves — used for {@code SELECT count(*)}. */
  public static long countRows(final Iterable<byte[]> rowGroupPayloads) {
    long total = 0;
    for (final byte[] payload : rowGroupPayloads) {
      // Parse the header's rowCount field without materialising the rest.
      total += java.nio.ByteBuffer.wrap(payload, 0, 4).order(java.nio.ByteOrder.LITTLE_ENDIAN).getInt();
    }
    return total;
  }

  private static long countRowGroup(final byte[] payload, final ColumnPredicate[] predicates) {
    final ProjectionIndexRowGroupPage leaf = ProjectionIndexRowGroupPage.deserialize(payload);
    final int rowCount = leaf.getRowCount();
    if (rowCount == 0)
      return 0L;

    // Zone-map prune: if any predicate is provably unsatisfiable against
    // the leaf's column min/max, the whole leaf contributes zero.
    for (final ColumnPredicate p : predicates) {
      if (pruneByZoneMap(leaf, p))
        return 0L;
    }

    // One 1024-bit mask initialised to "all rows pass" and AND'd with
    // each column's predicate outcome in turn. Each predicate also ANDs
    // its column's presence bitmap — a comparison over a MISSING field is
    // false (the stored default must never match). Mirrors
    // ProjectionIndexByteScan's sparse semantics.
    final int stride = (rowCount + 63) >>> 6;
    final long[] mask = new long[stride];
    fillAllTrue(mask, rowCount);
    final long[] colMask = new long[stride];
    for (final ColumnPredicate p : predicates) {
      java.util.Arrays.fill(colMask, 0L);
      evalColumn(leaf, p, rowCount, colMask);
      final long[] presence = leaf.presenceColumnBits(p.column);
      for (int i = 0; i < stride; i++)
        mask[i] &= colMask[i] & presence[i];
    }
    long result = 0;
    for (int i = 0; i < stride; i++)
      result += Long.bitCount(mask[i]);
    return result;
  }

  /**
   * Evaluate a column predicate against the leaf's raw column array(s) and write the per-row outcome
   * bits into {@code out} (length {@code >= ceil(rowCount/64)}).
   */
  private static void evalColumn(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p, final int rowCount,
      final long[] out) {
    final byte kind = leaf.columnKind(p.column);
    switch (kind) {
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE ->
        evalNumeric(leaf.numericColumn(p.column), rowCount, p.op, p.longLit, p.highLit, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
        evalBoolean(leaf.booleanColumnBits(p.column), rowCount, p.boolLit, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> evalStringEq(leaf, p, rowCount, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> evalStringSetContains(leaf, p, rowCount, out);
      default -> throw new IllegalStateException("Unknown column kind " + kind);
    }
  }

  private static void evalNumeric(final long[] col, final int rowCount, final Op op, final long lit, final long highLit,
      final long[] out) {
    switch (op) {
      case GT -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] > lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case LT -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] < lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case GE -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] >= lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case LE -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] <= lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case EQ -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] == lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case NE -> {
        for (int i = 0; i < rowCount; i++) {
          if (col[i] != lit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GT_LT -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v > lit && v < highLit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GT_LE -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v > lit && v <= highLit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GE_LT -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v >= lit && v < highLit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
      case BETWEEN_GE_LE -> {
        for (int i = 0; i < rowCount; i++) {
          final long v = col[i];
          if (v >= lit && v <= highLit)
            out[i >>> 6] |= 1L << (i & 63);
        }
      }
    }
  }

  private static void evalBoolean(final long[] packedBits, final int rowCount, final boolean wantTrue,
      final long[] out) {
    final int stride = (rowCount + 63) >>> 6;
    if (wantTrue) {
      System.arraycopy(packedBits, 0, out, 0, stride);
    } else {
      for (int i = 0; i < stride; i++)
        out[i] = ~packedBits[i];
      // Mask off bits beyond rowCount to avoid counting phantom "true" bits
      // for the tail below the 64-bit boundary.
      final int tail = rowCount & 63;
      if (tail != 0)
        out[stride - 1] &= (1L << tail) - 1L;
    }
  }

  private static void evalStringEq(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p, final int rowCount,
      final long[] out) {
    // Op-aware: this used to assume equality because EQ was the only string op, which would make a
    // NE predicate silently answer the EQ question. The byte kernel had the same assumption.
    final boolean wantEqual = switch (p.op) {
      case EQ -> true;
      case NE -> false;
      default -> throw new IllegalStateException("STRING_DICT column supports EQ/NE only, got " + p.op);
    };
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
    if (targetDictId < 0) {
      if (wantEqual) {
        return;
      }
      // The literal is absent from this leaf's dictionary, so EVERY present row differs from it.
      // Presence is ANDed in by the caller, so setting all rows here cannot resurrect a missing one.
      for (int i = 0; i < rowCount; i++) {
        out[i >>> 6] |= 1L << (i & 63);
      }
      return;
    }
    final int[] ids = leaf.stringDictIdColumn(p.column);
    for (int i = 0; i < rowCount; i++) {
      if ((ids[i] == targetDictId) == wantEqual)
        out[i >>> 6] |= 1L << (i & 63);
    }
  }

  /**
   * Set membership over a hydrated leaf — the in-memory twin of the byte and sliced kernels.
   *
   * <p>
   * Same shape as {@link #evalStringEq}, with the row's element run in place of its single id: the
   * literal resolves against the leaf dictionary once, an absent literal leaves the mask untouched,
   * and the cursor advances over every row's elements so the flat run stays aligned.
   */
  private static void evalStringSetContains(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p,
      final int rowCount, final long[] out) {
    final byte[][] dict = leaf.stringDictionary(p.column);
    int targetDictId = -1;
    for (int i = 0; i < dict.length && dict[i] != null; i++) {
      if (Arrays.equals(dict[i], p.stringLitBytes)) {
        targetDictId = i;
        break;
      }
    }
    if (targetDictId < 0) {
      return;
    }
    final int[] counts = leaf.stringSetCountColumn(p.column);
    final int[] elems = leaf.stringSetIdColumn(p.column);
    int cursor = 0;
    for (int r = 0; r < rowCount; r++) {
      final int n = counts[r];
      for (int k = 0; k < n; k++) {
        if (elems[cursor + k] == targetDictId) {
          out[r >>> 6] |= 1L << (r & 63);
          break;
        }
      }
      cursor += n;
    }
  }

  private static void fillAllTrue(final long[] mask, final int rowCount) {
    final int fullWords = rowCount >>> 6;
    for (int i = 0; i < fullWords; i++)
      mask[i] = -1L;
    final int tail = rowCount & 63;
    if (tail != 0)
      mask[fullWords] = (1L << tail) - 1L;
  }

  private static boolean pruneByZoneMap(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p) {
    final byte kind = leaf.columnKind(p.column);
    // Zone maps only help on numeric / dict-id columns. Booleans pass
    // through — pruning them would require leaf-global has-true/
    // has-false flags which we don't encode today.
    if (!ProjectionIndexRowGroupPage.isNumericKind(kind))
      return false;
    final long min = leaf.columnMin(p.column);
    final long max = leaf.columnMax(p.column);
    return switch (p.op) {
      case GT -> max <= p.longLit;
      case LT -> min >= p.longLit;
      case GE -> max < p.longLit;
      case LE -> min > p.longLit;
      case EQ -> p.longLit < min || p.longLit > max;
      // Skippable only when the whole zone collapses onto the literal, so every value equals it and
      // NE is false for the entire row group. See ProjectionIndexByteScan#zoneSkip.
      case NE -> min == max && min == p.longLit;
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
