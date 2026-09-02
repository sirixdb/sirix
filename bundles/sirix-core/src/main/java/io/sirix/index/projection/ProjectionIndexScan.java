/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.jspecify.annotations.Nullable;

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

    /**
     * Per-id verdict bitset over a resource-wide dictionary's id space, or {@code null} for every
     * ordinary predicate.
     *
     * <p>
     * A {@code STRING_GLOBAL} column stores dictionary IDS in its long lane, so a string question
     * (containment, ordering) is answered in two phases: the caller evaluates the predicate ONCE per
     * distinct id against the global dictionary's bytes — the same per-entry authority
     * {@link ProjectionIndexScan#stringDictEntryMatches} gives the per-leaf dictionaries — and this
     * bitset carries the verdicts; the kernels then test {@code verdict[id]} per row, pure integer
     * work. Bit {@code id} (1-based, bit 0 unused) is set iff the value interned under {@code id}
     * satisfies the predicate.
     *
     * <p>
     * The predicate keeps its STRING op — the ops every numeric kernel THROWS on — so a kernel that
     * has not been taught this form fails loud instead of comparing ids as numbers. Kernels that
     * prune by numeric zone maps or exact-value fingerprints must ignore verdict predicates: the
     * long lane holds ids, and neither an id range nor a value fingerprint says anything about a
     * substring or ordering verdict.
     */
    public final long @Nullable [] globalIdVerdict;

    /** Ids covered by {@link #globalIdVerdict}: valid ids are {@code 1 .. globalIdVerdictCount}. */
    public final int globalIdVerdictCount;

    public ColumnPredicate(final int column, final Op op, final long longLit, final long highLit, final boolean boolLit,
        final byte[] stringLitBytes) {
      this(column, op, longLit, highLit, boolLit, stringLitBytes, null, 0);
    }

    private ColumnPredicate(final int column, final Op op, final long longLit, final long highLit,
        final boolean boolLit, final byte[] stringLitBytes, final long @Nullable [] globalIdVerdict,
        final int globalIdVerdictCount) {
      this.column = column;
      this.op = op;
      this.longLit = longLit;
      this.highLit = highLit;
      this.boolLit = boolLit;
      this.stringLitBytes = stringLitBytes;
      this.globalIdVerdict = globalIdVerdict;
      this.globalIdVerdictCount = globalIdVerdictCount;
    }

    /**
     * A string predicate over a {@code STRING_GLOBAL} column, pre-evaluated per dictionary id.
     *
     * @param op one of the string ops ({@code STR_*}, {@code EQ}, {@code NE}); the numeric ops make
     *        no sense against a verdict and are refused
     * @param verdict bit {@code id} set iff the value interned under {@code id} matches
     * @param idCount ids the verdict covers; the bitset must span it
     */
    public static ColumnPredicate globalStringVerdict(final int column, final Op op, final byte[] literalUtf8,
        final long[] verdict, final int idCount) {
      if (verdict == null) {
        throw new NullPointerException("verdict");
      }
      // Ids are 1-BASED (bit 0 unused), so id == idCount lives in word idCount >>> 6 — an idCount
      // that is a multiple of 64 needs one word more than a 0-based ceil would grant.
      if (idCount < 0 || idCount >>> 6 >= verdict.length) {
        throw new IllegalArgumentException("verdict bitset spans " + (verdict.length * 64L - 1) + " ids, needs " + idCount);
      }
      switch (op) {
        case EQ, NE, STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> {
        }
        default -> throw new IllegalArgumentException("not a string op: " + op);
      }
      return new ColumnPredicate(column, op, 0L, 0L, false, literalUtf8, verdict, idCount);
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

    /** {@code column < literal} (string order) on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringLt(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.STR_LT, 0L, 0L, false, literalUtf8);
    }

    /** {@code column <= literal} (string order) on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringLe(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.STR_LE, 0L, 0L, false, literalUtf8);
    }

    /** {@code column > literal} (string order) on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringGt(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.STR_GT, 0L, 0L, false, literalUtf8);
    }

    /** {@code column >= literal} (string order) on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringGe(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.STR_GE, 0L, 0L, false, literalUtf8);
    }

    /** {@code fn:contains(column, literal)} on a STRING_DICT column; missing cells do NOT match. */
    public static ColumnPredicate stringContains(final int column, final byte[] literalUtf8) {
      return new ColumnPredicate(column, Op.STR_CONTAINS, 0L, 0L, false, literalUtf8);
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
    BETWEEN_GE_LE,
    /**
     * String ordering {@code v < lit} on a STRING_DICT column — its OWN op, never the numeric
     * {@link #LT} with string bytes: the numeric switches assume long semantics (zone maps on a dict
     * column hold dict IDS, so pruning by them on a string range drops matching leaves), and a distinct
     * op makes every exhaustive switch a compile error instead of a silent wrong answer. Interpreter
     * collation contract: {@code Str#cmp} is UTF-16 code-unit order; raw UTF-8 byte order diverges
     * exactly when a 4-byte sequence (lead {@code >= 0xF0}) meets a BMP char in U+E000..U+FFFF —
     * evaluators must detect that and fall back to decoded comparison. Missing cells do NOT match.
     */
    STR_LT,
    /** String ordering {@code v <= lit}; see {@link #STR_LT} for the collation contract. */
    STR_LE,
    /** String ordering {@code v > lit}; see {@link #STR_LT} for the collation contract. */
    STR_GT,
    /** String ordering {@code v >= lit}; see {@link #STR_LT} for the collation contract. */
    STR_GE,
    /**
     * Substring containment {@code fn:contains(v, lit)}. No collation subtlety (UTF-8 is
     * self-synchronizing, a byte-wise needle match IS a codepoint substring match), but exact-value
     * fingerprints must never prune it: a leaf whose every URL CONTAINS "google" fingerprints none of
     * them as the whole string "google". Missing cells do NOT match.
     */
    STR_CONTAINS
  }

  /**
   * Compiled boolean predicate TREE over column predicates — the general form the conjunction-only
   * {@code ColumnPredicate[]} cannot express: arbitrary AND/OR nesting.
   *
   * <p>
   * <b>Program encoding.</b> {@code program} is a postfix (RPN) walk over leaf masks: an entry
   * {@code >= 0} pushes leaf {@code program[i]}'s mask; {@link #OP_AND} pops two masks and pushes
   * their intersection; {@link #OP_OR} pushes their union; {@link #OP_NOT} pops one and pushes its
   * complement. A well-formed program leaves exactly one mask on the stack. Leaf masks encode
   * two-valued predicate truth with missing ⇒ {@code false} (presence AND) — under AND/OR composition
   * this is exactly the interpreter's general-comparison semantics, and it is ALSO what makes
   * {@link #OP_NOT} exact: the complement of {@code present AND matches} is
   * {@code missing OR (present AND !matches)}, precisely {@code fn:not} over a comparison whose
   * missing-deref operand made it false (an empty existential, {@code contains} over {@code ""}, a
   * false EBV). The leaf null gate is load-bearing here: JSON-null cells order differently from
   * missing under the interpreter's total order, so every leaf declines null-bearing columns BEFORE
   * the algebra runs — with or without negation above it.
   *
   * <p>
   * Stack depth is bounded by {@link #MAX_LEAVES}; {@link #of} validates shape.
   */
  public static final class PredicateTree {
    /** Max leaf predicates (= max program stack depth) — bounds kernel scratch. */
    public static final int MAX_LEAVES = 16;

    public static final byte OP_AND = -1;
    public static final byte OP_OR = -2;
    public static final byte OP_NOT = -3;

    public final ColumnPredicate[] leaves;
    public final byte[] program;
    /** Whether any instruction negates — decided once, read per leaf by the tree evaluator. */
    private final boolean hasNot;

    private PredicateTree(final ColumnPredicate[] leaves, final byte[] program) {
      this.leaves = leaves;
      this.program = program;
      boolean negates = false;
      for (final byte insn : program) {
        negates |= insn == OP_NOT;
      }
      this.hasNot = negates;
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
        } else if (insn == OP_NOT) {
          if (depth < 1) {
            throw new IllegalArgumentException("negation underflow at depth " + depth);
          }
        } else {
          throw new IllegalArgumentException("unknown program op " + insn);
        }
      }
      if (depth != 1) {
        throw new IllegalArgumentException("program ends at stack depth " + depth + " (want 1)");
      }
      return new PredicateTree(leaves.clone(), program.clone());
    }

    /**
     * Whether the program negates anywhere. A whole-leaf prune (every operand all-zero) is exact
     * under AND/OR but NOT would flip it to all-true, so negating trees keep the exact per-leaf
     * evaluation.
     */
    public boolean hasNot() {
      return hasNot;
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
      // A temporal predicate reaches here already mapped to numeric bounds (see the executor's
      // literal-to-bound rule), so the cells and the literal are in the same units.
      case ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_LONG,
          ProjectionIndexRowGroupPage.COLUMN_KIND_NUMERIC_DOUBLE,
          ProjectionIndexRowGroupPage.COLUMN_KIND_TIMESTAMP, ProjectionIndexRowGroupPage.COLUMN_KIND_DATE ->
        evalNumeric(leaf.numericColumn(p.column), rowCount, p.op, p.longLit, p.highLit, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_BOOLEAN ->
        evalBoolean(leaf.booleanColumnBits(p.column), rowCount, p.boolLit, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_DICT -> evalStringDict(leaf, p, rowCount, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_SET -> evalStringSetContains(leaf, p, rowCount, out);
      case ProjectionIndexRowGroupPage.COLUMN_KIND_STRING_GLOBAL -> {
        // Same dispatch contract as the byte kernel's global arm: a verdict predicate sweeps ids
        // against the caller-built bitset, an id-resolved EQ/NE evaluates numerically, and a string
        // literal without a verdict is a routing defect that must throw rather than compare bytes
        // to ids.
        if (p.globalIdVerdict != null) {
          final long[] ids = leaf.numericColumn(p.column);
          final long[] verdict = p.globalIdVerdict;
          final int idCount = p.globalIdVerdictCount;
          for (int i = 0; i < rowCount; i++) {
            final long id = ids[i];
            if (id >= 1 && id <= idCount && (verdict[(int) (id >>> 6)] & 1L << (id & 63)) != 0L) {
              out[i >>> 6] |= 1L << (i & 63);
            }
          }
        } else if (p.stringLitBytes != null) {
          throw new IllegalStateException("column " + p.column + " is STRING_GLOBAL, but the " + p.op
              + " predicate still carries a string literal — it was never resolved to a dictionary id");
        } else {
          evalNumeric(leaf.numericColumn(p.column), rowCount, p.op, p.longLit, p.highLit, out);
        }
      }
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
      // A statement switch swallows unlisted ops as a NO-OP — an all-false mask, i.e. a silent
      // under-count. String ops on a numeric column are a routing defect: throw.
      case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS ->
        throw new IllegalStateException("string op on a numeric column: " + op);
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

  private static void evalStringDict(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p,
      final int rowCount, final long[] out) {
    final int dictSize = leaf.stringDictionarySize(p.column);
    // Two-phase: evaluate the predicate ONCE per dictionary entry into an id bitset (dictSize
    // string operations, bounded by rowCount by format invariant), then sweep the rows as one
    // bit test each. The old single-target loop only expressed EQ/NE; the per-entry form serves
    // every string op with one code path — and "NE with an absent literal matches every present
    // row" falls out as a consequence instead of a special case.
    final long[] idBits = new long[dictSize + 63 >>> 6];
    boolean any = false;
    final boolean litHasSupplementary = hasFourByteUtf8(p.stringLitBytes, 0, p.stringLitBytes.length);
    for (int i = 0; i < dictSize; i++) {
      if (stringDictEntryMatches(leaf.stringDictionaryEntryBacking(p.column, i),
          leaf.stringDictionaryEntryOffset(p.column, i), leaf.stringDictionaryEntryLength(p.column, i), p.op,
          p.stringLitBytes, litHasSupplementary)) {
        idBits[i >>> 6] |= 1L << (i & 63);
        any = true;
      }
    }
    if (!any) {
      return;
    }
    final int[] ids = leaf.stringDictIdColumn(p.column);
    for (int i = 0; i < rowCount; i++) {
      final int id = ids[i];
      if ((idBits[id >>> 6] & 1L << (id & 63)) != 0L) {
        out[i >>> 6] |= 1L << (i & 63);
      }
    }
  }

  /**
   * Does one dictionary entry satisfy a string predicate? The single per-entry authority every dict
   * kernel (hydrated, byte, sliced) evaluates through, so the op semantics cannot drift between
   * paths.
   *
   * <p>
   * Ordering ops honor the interpreter's collation ({@code Str#cmp} = {@code String.compareTo} =
   * UTF-16 code-unit order): raw unsigned UTF-8 byte order equals CODEPOINT order, which diverges
   * exactly when a supplementary character (4-byte UTF-8, lead {@code >= 0xF0}) meets a BMP character
   * in U+E000..U+FFFF — so if EITHER side carries a 4-byte sequence, both decode and compare as
   * Strings. {@code contains} needs no such gate: UTF-8 is self-synchronizing, a byte-wise needle
   * match IS a codepoint substring match.
   */
  static boolean stringDictEntryMatches(final byte[] entry, final int off, final int len, final Op op, final byte[] lit,
      final boolean litHasSupplementary) {
    return switch (op) {
      case EQ -> Arrays.equals(entry, off, off + len, lit, 0, lit.length);
      case NE -> !Arrays.equals(entry, off, off + len, lit, 0, lit.length);
      case STR_CONTAINS -> containsBytes(entry, off, len, lit);
      case STR_LT, STR_LE, STR_GT, STR_GE -> {
        final int cmp;
        if (litHasSupplementary || hasFourByteUtf8(entry, off, len)) {
          cmp = new String(entry, off, len, StandardCharsets.UTF_8).compareTo(new String(lit, StandardCharsets.UTF_8));
        } else {
          cmp = Arrays.compareUnsigned(entry, off, off + len, lit, 0, lit.length);
        }
        yield switch (op) {
          case STR_LT -> cmp < 0;
          case STR_LE -> cmp <= 0;
          case STR_GT -> cmp > 0;
          default -> cmp >= 0;
        };
      }
      default -> throw new IllegalStateException("not a string op: " + op);
    };
  }

  /** Byte-wise substring search — sound for UTF-8 (self-synchronizing). */
  static boolean containsBytes(final byte[] hay, final int off, final int len, final byte[] needle) {
    final int n = needle.length;
    if (n == 0) {
      return true;
    }
    if (n > len) {
      return false;
    }
    final byte first = needle[0];
    final int last = off + len - n;
    outer: for (int i = off; i <= last; i++) {
      if (hay[i] != first) {
        continue;
      }
      for (int k = 1; k < n; k++) {
        if (hay[i + k] != needle[k]) {
          continue outer;
        }
      }
      return true;
    }
    return false;
  }

  /** Any 4-byte UTF-8 lead ({@code >= 0xF0}) in the slice — the supplementary-character gate. */
  static boolean hasFourByteUtf8(final byte[] bytes, final int off, final int len) {
    final int end = off + len;
    for (int i = off; i < end; i++) {
      if ((bytes[i] & 0xFF) >= 0xF0) {
        return true;
      }
    }
    return false;
  }

  /**
   * Set membership over a hydrated leaf — the in-memory twin of the byte and sliced kernels.
   *
   * <p>
   * Same shape as {@link #evalStringDict}, with the row's element run in place of its single id: the
   * literal resolves against the leaf dictionary once, an absent literal leaves the mask untouched,
   * and the cursor advances over every row's elements so the flat run stays aligned.
   */
  private static void evalStringSetContains(final ProjectionIndexRowGroupPage leaf, final ColumnPredicate p,
      final int rowCount, final long[] out) {
    if (p.op != Op.EQ) {
      // Membership over a SEQUENCE-valued field is EQ-only; ordering/substring over a set is a
      // different question no caller asks yet — fail loud, mirroring the byte kernel's guard.
      throw new IllegalStateException("STRING_SET supports EQ membership only, got " + p.op);
    }
    int targetDictId = -1;
    final int dictSize = leaf.stringDictionarySize(p.column);
    for (int i = 0; i < dictSize; i++) {
      final byte[] backing = leaf.stringDictionaryEntryBacking(p.column, i);
      final int offset = leaf.stringDictionaryEntryOffset(p.column, i);
      final int length = leaf.stringDictionaryEntryLength(p.column, i);
      if (Arrays.equals(backing, offset, offset + length, p.stringLitBytes, 0, p.stringLitBytes.length)) {
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
    if (!ProjectionIndexRowGroupPage.isNumericKind(kind) && !ProjectionIndexRowGroupPage.isTemporalKind(kind))
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
      // NEVER prune string ops: a dict column's zone map holds min/max dict IDS — meaningless
      // for value order or content, and a prune here silently drops matching rows.
      case STR_LT, STR_LE, STR_GT, STR_GE, STR_CONTAINS -> false;
    };
  }
}
