/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;

/**
 * A query-local DERIVED numeric column: one slice per leaf holding, for every row on which every
 * operand is present, the value of a postfix {@code +,-,*} program over the operand columns — the
 * grouped twin of {@link ProjectionIndexByteScan#conjunctiveAggregateComputed}. A grouped
 * {@code sum($r.cost * $r.qty)} then folds through the ordinary group kernels as if the product
 * were a stored column: no kernel learns about programs. The executor admits only resident slice
 * arms that can consume the derived lane.
 *
 * <p>
 * Semantics follow the interpreter: arithmetic over a missing operand is the empty sequence, so
 * such a row is MISSING in the derived column (presence bit clear, contributes nothing). Arithmetic
 * is exact or DECLINES: an {@link ArithmeticException} from {@code Math.*Exact} propagates, and the
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

  /** Rows per selection word. */
  private static final int WINDOW_ROWS = ProjectionColumnGroupScan.WINDOW_ROWS;

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
      final long @Nullable [] keepWords, final MemorySegment target) {
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
    // Hoisted once for the whole pass and refilled per leaf: the operand lanes are read where the
    // store decoded them, so no operand lane is ever materialized on the heap.
    final MemorySegment[] operandSeg = new MemorySegment[operands.length];
    final long[] operandBase = new long[operands.length];
    final long[][] operandPresence = new long[operands.length][];
    long laneBase = 0L;
    if (target.byteSize() < byteSize(operands, keepWords)) {
      throw new IllegalArgumentException("computed target is too small");
    }
    for (int leaf = 0; leaf < leaves; leaf++) {
      final boolean kept =
          keepWords == null || ((leaf >>> 6) < keepWords.length && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) != 0L);
      if (!kept) {
        out[leaf] = ProjectionColumnStore.prunedSlice();
        continue;
      }
      out[leaf] = evaluateLeaf(operands, leaf, code, consts, stack, operandValues, operandSeg, operandBase,
          operandPresence, target, laneBase);
      laneBase += (long) out[leaf].rowCount() * Long.BYTES;
    }
    return out;
  }

  private static ColumnSlice evaluateLeaf(final ColumnSlice[][] operands, final int leaf, final int[] code,
      final long[] consts, final long[] stack, final long[] operandValues, final MemorySegment[] operandSeg,
      final long[] operandBase, final long[][] operandPresence, final MemorySegment target, final long laneBase) {
    final int operandCount = operands.length;
    int rowCount = -1;
    for (int c = 0; c < operandCount; c++) {
      final ColumnSlice slice = operands[c][leaf];
      if (slice == null || slice.rowCount() <= 0) {
        return ProjectionColumnStore.prunedSlice();
      }
      if (!slice.hasNumericLane()) {
        throw new IllegalArgumentException("computed operand " + c + " is not a long-lane column on leaf " + leaf);
      }
      operandSeg[c] = slice.laneSegment();
      operandBase[c] = slice.laneBase();
      operandPresence[c] = slice.presenceWords();
      if (rowCount < 0) {
        rowCount = slice.rowCount();
      } else if (rowCount != slice.rowCount()) {
        throw new IllegalArgumentException("operand columns disagree on the row count of leaf " + leaf);
      }
    }
    final int stride = (rowCount + 63) >>> 6;
    final long[] presence = new long[stride];
    for (int w = 0; w < stride; w++) {
      long word = -1L;
      for (int c = 0; c < operandCount; c++) {
        word &= operandPresence[c][w]; // hoisted per leaf above, not re-dereferenced per word
      }
      final int validRows = rowCount - (w << 6);
      if (validRows < WINDOW_ROWS) {
        word &= (1L << validRows) - 1L;
      }
      presence[w] = word;
    }
    final NumericLane lane = new NumericLane(target, laneBase, rowCount);
    target.asSlice(laneBase, (long) rowCount * Long.BYTES).fill((byte) 0);
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int w = 0; w < stride; w++) {
      long word = presence[w];
      final int rowBase = w << 6;
      final int count = Long.bitCount(word);
      for (int i = 0; i < count; i++) {
        final int row = rowBase + Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final long offset = (long) row << 3;
        for (int c = 0; c < operandCount; c++) {
          operandValues[c] = SegmentAccess.getLong(operandSeg[c], operandBase[c] + offset);
        }
        final long value = run(code, consts, operandValues, stack);
        SegmentAccess.putLong(target, laneBase + offset, value);
        min = Math.min(min, value);
        max = Math.max(max, value);
      }
    }
    return new ColumnSlice(rowCount, (byte) 0, min, max, presence, lane);
  }

  public static long byteSize(final ColumnSlice[][] operands, final long @Nullable [] keepWords) {
    if (operands == null || operands.length == 0 || operands[0] == null) {
      throw new IllegalArgumentException("a computed column needs operands");
    }
    long bytes = 0L;
    for (int leaf = 0; leaf < operands[0].length; leaf++) {
      if (keepWords == null || ((leaf >>> 6) < keepWords.length && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) != 0L)) {
        bytes = Math.addExact(bytes, (long) operands[0][leaf].rowCount() * Long.BYTES);
      }
    }
    return bytes;
  }

  /**
   * One evaluation of the postfix program over {@code operandValues}; exact arithmetic, overflow
   * throws. {@code stack} is the caller's scratch of at least the program's depth.
   */
  public static long run(final int[] code, final long[] consts, final long[] operandValues, final long[] stack) {
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
