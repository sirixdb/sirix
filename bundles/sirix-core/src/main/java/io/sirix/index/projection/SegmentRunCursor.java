/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

import io.sirix.node.ValueDictionaryEntryNode;
import org.jspecify.annotations.Nullable;

/**
 * A sequential reader over ONE segment dictionary's storage positions, for a merge that walks every
 * segment in collation order at once.
 *
 * <p>
 * Why a cursor and not the read view's per-cell path: the merge compares heads, and a head is a
 * POSITION of a run whose neighbours it will visit next. The per-cell path answers a MINT — it
 * translates the mint to its position through the forward rank table (a random read), looks the
 * position's block up in a direct-mapped cache keyed by the mint (which random mints thrash), and
 * checks the reader's revision on every call. Measured at 100M on {@code GROUP BY URL}, that path was
 * three quarters of the merge's CPU and the byte comparison itself a fifth. A cursor holds the block
 * and the inverse rank-table record it is in and moves on only when the walk leaves them, so the
 * merge reads each block ONCE per range and compares bytes it already holds.
 * </p>
 *
 * <p>
 * The slice a {@link #seek} leaves behind is exposed as FIELDS rather than accessors so that the
 * merge's comparison — the hot instruction stream of every ordered dictionary operation — reads
 * them without a call: {@link #backing}/{@link #offset}/{@link #length} for a packed value,
 * {@link #spill} for one too large for a block. Exactly one of {@code backing} and {@code spill} is
 * non-null after a seek. The array behind {@code backing} is the block's own and MUST NOT be
 * mutated.
 * </p>
 *
 * <p>
 * A cursor is private to the thread that obtained it: it caches records off a view that is itself
 * thread-private, and it carries mutable scratch. Obtain one per run per range, on the thread that
 * walks it.
 * </p>
 */
public abstract class SegmentRunCursor {

  /** Backing array of the value at the sought position, or {@code null} when it spilled. */
  protected byte @Nullable [] backing;

  /** Start of the value within {@link #backing}. */
  protected int offset;

  /** Length of the value within {@link #backing}. */
  protected int length;

  /** The record of a spilled value, or {@code null} when the value is packed in a block. */
  protected @Nullable ValueDictionaryEntryNode spill;

  /** Blocks, buckets and rank-table records this cursor fetched — the witness that a walk read each once. */
  protected long loads;

  /**
   * Position the cursor at storage {@code position} ({@code 1..entries}), leaving the value's slice
   * in {@link #backing}/{@link #offset}/{@link #length} or its record in {@link #spill}.
   *
   * <p>
   * Positions of one walk ascend, and an implementation exploits that by keeping the block it holds
   * until a seek leaves it; a seek anywhere else is correct and merely loads.
   * </p>
   *
   * @throws IllegalStateException if the position is outside the dictionary or its value is missing
   */
  public abstract void seek(int position);

  /**
   * The MINT stored at storage {@code position} — the id the rows carry — without loading its value.
   * Answers {@code -1} or a mint outside the segment when the dictionary's inverse table is
   * inconsistent; the caller refuses on either.
   */
  public abstract int mintAt(int position);

  /** Records fetched so far; see {@link #loads}. */
  public final long loads() {
    return loads;
  }

  /**
   * Order the values two positioned cursors hold, under the dictionary's UTF-16 collation, reading
   * both in place: negative, zero or positive as {@code left} orders before, with, or after
   * {@code right}.
   */
  public static int compare(final SegmentRunCursor left, final SegmentRunCursor right) {
    return compare(left.backing, left.offset, left.length, left.spill, right.backing, right.offset, right.length,
        right.spill);
  }

  /**
   * Order two slices as {@link #compare(SegmentRunCursor, SegmentRunCursor)} does, for a caller that
   * copied a cursor's fields out (the previous winner of a merge, whose cursor has moved on).
   */
  public static int compare(final byte @Nullable [] leftBacking, final int leftOffset, final int leftLength,
      final @Nullable ValueDictionaryEntryNode leftSpill, final byte @Nullable [] rightBacking,
      final int rightOffset, final int rightLength, final @Nullable ValueDictionaryEntryNode rightSpill) {
    if (leftSpill == null) {
      return rightSpill == null
          ? ValueDictionaryEntryNode.compareUtf16Range(leftBacking, leftOffset, leftLength, rightBacking, rightOffset,
              rightLength)
          : -rightSpill.compareToRange(leftBacking, leftOffset, leftLength);
    }
    return rightSpill == null
        ? leftSpill.compareToRange(rightBacking, rightOffset, rightLength)
        : leftSpill.compareValueUtf16(rightSpill);
  }

  /**
   * Order the value a positioned cursor holds against a caller-owned byte range, under the same
   * collation.
   */
  public static int compareToRange(final SegmentRunCursor cursor, final byte[] bytes, final int offset,
      final int length) {
    final ValueDictionaryEntryNode spill = cursor.spill;
    return spill == null
        ? ValueDictionaryEntryNode.compareUtf16Range(cursor.backing, cursor.offset, cursor.length, bytes, offset,
            length)
        : spill.compareToRange(bytes, offset, length);
  }

  /** A COPY of the value at the sought position, for the few values a caller keeps (a range's pivots). */
  public final byte[] copyValue() {
    final ValueDictionaryEntryNode spilled = spill;
    if (spilled != null) {
      return spilled.getValue();
    }
    final byte[] bytes = new byte[length];
    System.arraycopy(backing, offset, bytes, 0, length);
    return bytes;
  }
}
