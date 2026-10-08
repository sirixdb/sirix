/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import org.jspecify.annotations.Nullable;

/**
 * A query-local DERIVED numeric column: one slice per leaf holding, for every row on which every
 * operand is present, the value of a postfix {@code +,-,*} program over the operand columns — the
 * grouped twin of {@link ProjectionIndexByteScan#conjunctiveAggregateComputed}. A grouped
 * {@code sum($r.cost * $r.qty)} then folds through the ordinary group kernels as if the product were
 * a stored column: no kernel learns about programs, and every arm that takes resident slices serves
 * it unchanged.
 *
 * <p>
 * Semantics follow the interpreter: arithmetic over a missing operand is the empty sequence, so such
 * a row is MISSING in the derived column (presence bit clear, contributes nothing). Arithmetic is
 * exact or DECLINES: an {@link ArithmeticException} from {@code Math.*Exact} propagates, and the
 * executor routes it to the generic pipeline, whose {@code xs:integer} math promotes to decimal.
 * </p>
 *
 * <p>
 * The program encoding is {@link ProjectionIndexByteScan#COMPUTED_OP_ADD} and friends: slots
 * {@code >= COMPUTED_CONST_BASE} push a constant, slots {@code >= 0} push operand {@code slot}'s
 * value, negative slots apply an operator. Only leaves kept by {@code keepWords} are evaluated (a
 * dropped leaf is the rowless sentinel the kernels never read); the whole leaf is evaluated when it
 * is kept, so the kernel's per-row mask decides which values count.
 * </p>
 */
public final class ProjectionComputedColumn {

  /** Programs deeper than this are refused at construction (the kernels' own bound). */
  private static final int MAX_STACK = 64;

  private ProjectionComputedColumn() {}

  /**
   * Evaluate the program over {@code operands} (index-aligned with the program's operand slots) into
   * one derived slice per leaf.
   *
   * @param operands the operand columns' slices, one array per operand, each one slice per leaf
   * @param code the postfix program
   * @param consts the constants the program pushes
   * @param keepWords the leaf keep mask, or {@code null} for every leaf
   * @return one slice per leaf; a dropped or rowless leaf is a shared rowless sentinel
   * @throws ArithmeticException when any evaluated row overflows a {@code long}
   * @throws IllegalArgumentException on a malformed program or mismatched operand shapes
   */
  public static ColumnSlice[] evaluate(final ColumnSlice[][] operands, final int[] code, final long[] consts,
      final long @Nullable [] keepWords) {
    if (operands == null || operands.length == 0 || code == null || code.length == 0 || consts == null) {
      throw new IllegalArgumentException("a computed column needs operands and a program");
    }
    validate(code, consts, operands.length);
    final int leaves = operands[0].length;
    for (final ColumnSlice[] operand : operands) {
      if (operand.length != leaves) {
        throw new IllegalArgumentException("operand columns disagree on the leaf count");
      }
    }
    final ColumnSlice[] out = new ColumnSlice[leaves];
    final long[] stack = new long[MAX_STACK];
    final long[] operandValues = new long[operands.length];
    for (int leaf = 0; leaf < leaves; leaf++) {
      final boolean kept =
          keepWords == null || (leaf >>> 6) < keepWords.length && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) != 0L;
      if (!kept) {
        out[leaf] = ProjectionColumnStore.prunedSlice();
        continue;
      }
      out[leaf] = evaluateLeaf(operands, leaf, code, consts, stack, operandValues);
    }
    return out;
  }

  private static ColumnSlice evaluateLeaf(final ColumnSlice[][] operands, final int leaf, final int[] code,
      final long[] consts, final long[] stack, final long[] operandValues) {
    final int operandCount = operands.length;
    int rowCount = -1;
    for (int c = 0; c < operandCount; c++) {
      final ColumnSlice slice = operands[c][leaf];
      if (slice == null || slice.rowCount() <= 0) {
        return ProjectionColumnStore.prunedSlice();
      }
      if (slice.numericValues() == null) {
        throw new IllegalArgumentException("computed operand " + c + " is not a long-lane column on leaf " + leaf);
      }
      if (rowCount < 0) {
        rowCount = slice.rowCount();
      } else if (rowCount != slice.rowCount()) {
        throw new IllegalArgumentException("operand columns disagree on the row count of leaf " + leaf);
      }
    }
    final int stride = (rowCount + 63) >>> 6;
    final long[] presence = new long[stride];
    final long[] values = new long[rowCount];
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    // Presence of the derived value = every operand present (the interpreter's empty arithmetic).
    for (int w = 0; w < stride; w++) {
      long word = -1L;
      for (int c = 0; c < operandCount; c++) {
        word &= operands[c][leaf].presenceWords()[w];
      }
      final int tailBits = rowCount - (w << 6);
      if (tailBits < 64) {
        word &= (1L << tailBits) - 1L;
      }
      presence[w] = word;
      final int rowBase = w << 6;
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        for (int c = 0; c < operandCount; c++) {
          operandValues[c] = operands[c][leaf].numericValues()[row];
        }
        final long v = run(code, consts, operandValues, stack);
        values[row] = v;
        if (v < min) {
          min = v;
        }
        if (v > max) {
          max = v;
        }
      }
    }
    return new ColumnSlice(rowCount, (byte) 0, min, max, presence, values, null, null, null, null);
  }

  /** One evaluation of the postfix program; exact arithmetic, overflow throws. */
  static long run(final int[] code, final long[] consts, final long[] operandValues, final long[] stack) {
    int sp = 0;
    for (final int op : code) {
      if (op >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
        stack[sp++] = consts[op - ProjectionIndexByteScan.COMPUTED_CONST_BASE];
      } else if (op >= 0) {
        stack[sp++] = operandValues[op];
      } else {
        final long b = stack[--sp];
        final long a = stack[--sp];
        stack[sp++] = switch (op) {
          case ProjectionIndexByteScan.COMPUTED_OP_ADD -> Math.addExact(a, b);
          case ProjectionIndexByteScan.COMPUTED_OP_SUB -> Math.subtractExact(a, b);
          case ProjectionIndexByteScan.COMPUTED_OP_MUL -> Math.multiplyExact(a, b);
          default -> throw new IllegalStateException("unknown computed opcode " + op);
        };
      }
    }
    return stack[0];
  }

  /**
   * Validate a program: every slot in range, every opcode known, stack discipline sound, final depth
   * exactly one, depth never above {@link #MAX_STACK}. The same contract the executor re-checks at
   * its untrusted boundary; here it protects the shared stack.
   */
  public static void validate(final int[] code, final long[] consts, final int operandCount) {
    int depth = 0;
    for (final int op : code) {
      if (op >= ProjectionIndexByteScan.COMPUTED_CONST_BASE) {
        if (op - ProjectionIndexByteScan.COMPUTED_CONST_BASE >= consts.length) {
          throw new IllegalArgumentException("computed program pushes an absent constant");
        }
        depth++;
      } else if (op >= 0) {
        if (op >= operandCount) {
          throw new IllegalArgumentException("computed program pushes an absent operand " + op);
        }
        depth++;
      } else {
        if (op != ProjectionIndexByteScan.COMPUTED_OP_ADD && op != ProjectionIndexByteScan.COMPUTED_OP_SUB
            && op != ProjectionIndexByteScan.COMPUTED_OP_MUL) {
          throw new IllegalArgumentException("unknown computed opcode " + op);
        }
        if (depth < 2) {
          throw new IllegalArgumentException("computed program underflows its stack");
        }
        depth--;
      }
      if (depth > MAX_STACK) {
        throw new IllegalArgumentException("computed program exceeds the stack bound");
      }
    }
    if (depth != 1) {
      throw new IllegalArgumentException("computed program must leave exactly one value, left " + depth);
    }
  }
}
