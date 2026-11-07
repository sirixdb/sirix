/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.index.projection.ProjectionColumnStore.ColumnSlice;
import io.sirix.node.SegmentAccess;
import org.jspecify.annotations.Nullable;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;

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

  /** Rows per selection word, hence rows per operand in the gather window. */
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
    // Hoisted once for the whole pass and refilled per leaf: the operand lanes are read where the
    // store decoded them, so no operand lane is ever materialized on the heap.
    final MemorySegment[] operandSeg = new MemorySegment[operands.length];
    final long[] operandBase = new long[operands.length];
    final long[][] operandPresence = new long[operands.length][];
    final Scratch scratch = new Scratch(operands.length);
    for (int leaf = 0; leaf < leaves; leaf++) {
      final boolean kept =
          keepWords == null || ((leaf >>> 6) < keepWords.length && (keepWords[leaf >>> 6] & 1L << (leaf & 63)) != 0L);
      if (!kept) {
        out[leaf] = ProjectionColumnStore.prunedSlice();
        continue;
      }
      out[leaf] = evaluateLeaf(operands, leaf, code, consts, stack, operandValues, operandSeg, operandBase,
          operandPresence, scratch);
    }
    return out;
  }

  private static ColumnSlice evaluateLeaf(final ColumnSlice[][] operands, final int leaf, final int[] code,
      final long[] consts, final long[] stack, final long[] operandValues, final MemorySegment[] operandSeg,
      final long[] operandBase, final long[][] operandPresence, final Scratch scratch) {
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
    // ONE PRESENCE PASS FIRST, for the whole leaf: presence of the derived value is every operand
    // present (the interpreter's empty arithmetic), and how many rows that leaves decides the read
    // form. The pass costs one word per 64 rows and saves deciding per word.
    int present = 0;
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
      present += Long.bitCount(word);
    }
    if (present == 0) {
      // No row carries every operand. The zone map stays inverted (min > max), which is how every
      // evaluator reads "this leaf holds no derived value".
      return new ColumnSlice(rowCount, (byte) 0, Long.MAX_VALUE, Long.MIN_VALUE, presence, new long[rowCount], null,
          null, null, null);
    }
    // EVERY ROW SURVIVES — the shape of every measured route, where a kept leaf is dense because the
    // group kernel's own per-row mask decides later which values count. Then the rows to read are the
    // leaf's own, consecutive, so ONE counted copy per operand stages the whole lane into a buffer
    // reused for the pass and the walk is counted too. Per word the same bytes move in 64-row pieces,
    // which is 16 times the copy calls for one leaf of this size: measured on the t250k anti-join
    // group route the per-word calls, not their bytes, were what the gather still cost. A leaf the
    // row mask thins keeps the per-word form, which wastes nothing on a sparse selection.
    final long[] staged = present == rowCount
        ? scratch.stage(operandSeg, operandBase, operandCount, rowCount)
        : null;
    // The two walks are their own methods and stay small on purpose: the program interpreter is
    // called once per row, and a caller it cannot be inlined into pays that call 468,000 times on the
    // measured route. Folding both walks into this method cost 17% of the interpreter's samples.
    return staged != null
        ? evaluateStagedRows(code, consts, stack, operandValues, operandCount, rowCount, presence, staged)
        : evaluateKeptRows(code, consts, stack, operandValues, operandSeg, operandBase, operandCount, rowCount, stride,
            presence, scratch.window);
  }

  /**
   * Every row of the leaf carries every operand and its operand lanes are staged, so the walk is a
   * counted loop over the leaf's rows and operand {@code c} of row {@code row} is
   * {@code staged[c * rowCount + row]}.
   */
  private static ColumnSlice evaluateStagedRows(final int[] code, final long[] consts, final long[] stack,
      final long[] operandValues, final int operandCount, final int rowCount, final long[] presence,
      final long[] staged) {
    final long[] values = new long[rowCount];
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int row = 0; row < rowCount; row++) {
      for (int c = 0, slot = row; c < operandCount; c++, slot += rowCount) {
        operandValues[c] = staged[slot];
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
    return new ColumnSlice(rowCount, (byte) 0, min, max, presence, values, null, null, null, null);
  }

  /**
   * Some row of the leaf lacks an operand, so the walk is by presence word, with one read-form
   * decision per word taken before the walk consumes it. The crossover is the one the group folds
   * already dispatch on ({@link ProjectionColumnGroupScan#gatherPays}), so the area keeps a single
   * crossover: a bit walk's row index comes out of {@code numberOfTrailingZeros}, the loop is
   * therefore not counted over linear offsets and no compiler lifts a segment's bounds and alignment
   * checks out of it, so every surviving row would pay its own. Above the crossover one counted,
   * intrinsified copy per operand pays one check per vector instead.
   */
  private static ColumnSlice evaluateKeptRows(final int[] code, final long[] consts, final long[] stack,
      final long[] operandValues, final MemorySegment[] operandSeg, final long[] operandBase, final int operandCount,
      final int rowCount, final int stride, final long[] presence, final long[] window) {
    final long[] values = new long[rowCount];
    long min = Long.MAX_VALUE;
    long max = Long.MIN_VALUE;
    for (int w = 0; w < stride; w++) {
      long word = presence[w];
      if (word == 0L) {
        continue; // no row of this word has every operand: nothing to read and nothing to gather
      }
      final int rowBase = w << 6;
      final boolean gathered = ProjectionColumnGroupScan.gatherPays(word);
      if (gathered) {
        gatherOperands(operandSeg, operandBase, operandCount, rowBase, Math.min(WINDOW_ROWS, rowCount - rowBase),
            window, WINDOW_ROWS);
      }
      while (word != 0L) {
        final int bit = Long.numberOfTrailingZeros(word);
        word &= word - 1L;
        final int row = rowBase + bit;
        if (gathered) {
          for (int c = 0, slot = bit; c < operandCount; c++, slot += WINDOW_ROWS) {
            operandValues[c] = window[slot];
          }
        } else {
          final long at = (long) row << 3;
          for (int c = 0; c < operandCount; c++) {
            operandValues[c] = SegmentAccess.getLong(operandSeg[c], operandBase[c] + at);
          }
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

  /**
   * Copy {@code rows} rows of every operand lane into {@code buffer}, operand {@code c} at
   * {@code c * stride}, with one counted, intrinsified copy per operand. The caller clamps
   * {@code rows} to the leaf's row count, so neither the tail word nor the last leaf reads past a
   * lane.
   */
  private static void gatherOperands(final MemorySegment[] segs, final long[] bases, final int operandCount,
      final int rowBase, final int rows, final long[] buffer, final int stride) {
    final long from = (long) rowBase << 3;
    for (int c = 0; c < operandCount; c++) {
      MemorySegment.copy(segs[c], ValueLayout.JAVA_LONG_UNALIGNED, bases[c] + from, buffer, c * stride, rows);
    }
  }

  /**
   * The read scratch of one {@link #evaluate} pass: the per-word gather window, always needed, and
   * the per-leaf staging buffer, allocated once the first fully present leaf asks for it. Both are
   * flat rather than one array per operand, so a read is a single bounds-checked load of a
   * loop-invariant array instead of a two-dimensional indirection.
   */
  private static final class Scratch {
    private final int operandCount;

    private final long[] window;

    private long @Nullable [] leaf;

    Scratch(final int operandCount) {
      this.operandCount = operandCount;
      window = new long[operandCount * WINDOW_ROWS];
    }

    /**
     * The whole lane of every operand, staged for a leaf of {@code rows} rows, or {@code null} when the
     * leaf is wider than a row group may be — then the caller reads per word, which has no such bound.
     * Operand {@code c} starts at {@code c * rows}.
     */
    long @Nullable [] stage(final MemorySegment[] segs, final long[] bases, final int columns, final int rows) {
      if (rows > ProjectionIndexRowGroupPage.MAX_ROWS) {
        return null;
      }
      long[] buffer = this.leaf;
      if (buffer == null) {
        buffer = new long[operandCount * ProjectionIndexRowGroupPage.MAX_ROWS];
        this.leaf = buffer;
      }
      gatherOperands(segs, bases, columns, 0, rows, buffer, rows);
      return buffer;
    }
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
