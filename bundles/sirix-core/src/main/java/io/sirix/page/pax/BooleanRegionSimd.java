/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorOperators;

import java.lang.foreign.MemorySegment;

/**
 * SIMD kernels over {@link BooleanRegion}'s packed-bit column.
 *
 * <h2>Why a boolean column needs a kernel at all</h2>
 *
 * <p>A boolean column is already the densest encoding there is — one bit per value — so the
 * temptation is to treat counting over it as trivial and move on. That is what the original
 * {@code countTrue} did: it walked the range one <em>byte</em> at a time, calling
 * {@code Integer.bitCount} on eight bits and discarding 56 bits of every register it touched. At
 * one popcount per byte, a boolean predicate over a page cost more than the same predicate over a
 * bit-packed integer column, which is the wrong way round.
 *
 * <p>These kernels count 512 values per instruction group instead of 8: whole 64-bit words, eight
 * lanes at a time, through the vector unit's per-lane population count. The partial words at each
 * end are masked rather than looped over, so a range costs one masked popcount for its head, a
 * vector sweep for its body, and one masked popcount for its tail — no per-bit control flow
 * anywhere.
 *
 * <h2>Fusing with the other columns</h2>
 *
 * <p>{@link #andInto} exists because a boolean field is almost never the whole predicate: it
 * qualifies one — {@code active AND age > 30}. Rather than counting the boolean column and
 * intersecting row sets afterwards, the column is AND-ed straight into the bitmap a numeric or
 * string kernel already produced, so a multi-column filter stays one pass over one bitmap. That is
 * the shape DuckDB, Umbra and ClickHouse all converge on, and it is what the boolean region was
 * introduced for in the first place.
 */
public final class BooleanRegionSimd {

  private static final int LANES = ColumnLoad.LANES;

  /** Bits governed by one vector of 64-bit words. */
  private static final int BITS_PER_VECTOR = LANES * Long.SIZE;

  private BooleanRegionSimd() {
  }

  /**
   * Count set bits in {@code [start, start + n)} of the region's value bits.
   *
   * @param payload the region payload
   * @param bitsOffset byte offset of the packed bit array, from the header
   * @param start first bit index, absolute within the region
   * @param n number of bits to count
   */
  public static long countTrue(final MemorySegment payload, final int bitsOffset, final int start,
      final int n) {
    if (n <= 0) {
      return 0L;
    }
    final int end = start + n;
    long count = 0;
    int bit = start;

    // Head: the partial word the range starts inside, masked rather than walked bit by bit.
    if ((bit & 63) != 0) {
      final int word = bit >>> 6;
      final int from = bit & 63;
      final int to = Math.min(64, end - (word << 6));
      count += Long.bitCount(readWord(payload, bitsOffset, word) & bitRange(from, to));
      bit = Math.min(end, (word + 1) << 6);
    }

    // Body: whole words, eight at a time through the vector unit's population count.
    final int lastFullWord = end >>> 6;
    int word = bit >>> 6;
    if (end - bit >= BITS_PER_VECTOR && BitUnpackSimd.vectorProfitable(end - bit)) {
      LongVector acc = LongVector.zero(ColumnLoad.LONG_SPECIES);
      boolean any = false;
      for (; word + LANES <= lastFullWord; word += LANES) {
        final long byteOff = (long) bitsOffset + ((long) word << 3);
        if (!ColumnLoad.canLoad(payload, byteOff)) {
          break;
        }
        acc = acc.add(ColumnLoad.loadWords(payload, byteOff)
                                .lanewise(VectorOperators.BIT_COUNT));
        any = true;
      }
      if (any) {
        count += acc.reduceLanes(VectorOperators.ADD);
      }
      bit = word << 6;
    }

    // Whole words the vector sweep could not cover (fewer than a full vector, or past the buffer).
    for (; word < lastFullWord; word++) {
      count += Long.bitCount(readWord(payload, bitsOffset, word));
      bit = (word + 1) << 6;
    }

    // Tail: the partial word the range ends inside.
    if (bit < end) {
      count += Long.bitCount(readWord(payload, bitsOffset, bit >>> 6) & bitRange(0, end - bit));
    }
    return count;
  }

  /**
   * Live-value counterpart of {@link #countTrue}: count set bits that a newer fragment has not
   * shadowed.
   *
   * <p>{@code liveBits} is indexed RELATIVE to {@code start}, as everywhere else in this package.
   * The value bits are indexed absolutely, so the two are brought into alignment by reading a
   * 64-bit window of the value column at an arbitrary <em>bit</em> offset — which costs two word
   * loads and a funnel shift, and lets the whole thing stay one AND and one popcount per 64 values
   * rather than a per-value test.
   */
  public static long countTrueMasked(final MemorySegment payload, final int bitsOffset, final int start,
      final int n, final long[] liveBits) {
    if (n <= 0) {
      return 0L;
    }
    if (liveBits.length < (n + 63) >>> 6) {
      throw new IllegalArgumentException(
          "liveness bitmap too small: " + liveBits.length + " words for " + n + " bits");
    }
    long count = 0;
    for (int relative = 0; relative < n; relative += 64) {
      final int remaining = Math.min(64, n - relative);
      final long values = readBits64(payload, bitsOffset, (long) start + relative);
      final long live = liveBits[relative >>> 6];
      count += Long.bitCount(values & live & bitRange(0, remaining));
    }
    return count;
  }

  /**
   * AND this boolean column into {@code target}, a bitmap indexed relative to {@code start}.
   *
   * <p>Lets a boolean predicate be applied to a row set another column's kernel already produced,
   * without materializing either side as a list of row ids.
   *
   * @param invert when {@code true} the column is inverted first, i.e. {@code NOT field}
   * @return the number of bits still set in {@code target} over {@code [0, n)}
   */
  /**
   * Write the column (optionally inverted) into {@code target} as a selection bitmap.
   *
   * <p>The boolean column IS a bitmap, so this is the cheapest leaf in the engine: a masked copy,
   * no comparison at all. {@link #andInto} composes into an existing selection; this one
   * establishes it, which is what a leaf of a predicate tree needs.
   *
   * @param invert select {@code false} rows instead of {@code true} ones — a negation answered
   *        without a complement pass, since the column already holds both answers
   * @return the number of rows selected
   */
  public static long selectInto(final MemorySegment payload, final int bitsOffset, final int start,
      final int n, final long[] target, final boolean invert) {
    if (n <= 0) {
      return 0L;
    }
    if (target.length < (n + 63) >>> 6) {
      throw new IllegalArgumentException(
          "target bitmap too small: " + target.length + " words for " + n + " bits");
    }
    long selected = 0;
    for (int relative = 0; relative < n; relative += 64) {
      final int width = Math.min(64, n - relative);
      long values = readBits64(payload, bitsOffset, (long) start + relative);
      if (invert) {
        values = ~values;
      }
      final long v = values & bitRange(0, width);
      target[relative >>> 6] = v;
      selected += Long.bitCount(v);
    }
    return selected;
  }

  public static long andInto(final MemorySegment payload, final int bitsOffset, final int start,
      final int n, final long[] target, final boolean invert) {
    if (n <= 0) {
      return 0L;
    }
    if (target.length < (n + 63) >>> 6) {
      throw new IllegalArgumentException(
          "target bitmap too small: " + target.length + " words for " + n + " bits");
    }
    long remainingSet = 0;
    for (int relative = 0; relative < n; relative += 64) {
      final int width = Math.min(64, n - relative);
      long values = readBits64(payload, bitsOffset, (long) start + relative);
      if (invert) {
        values = ~values;
      }
      final int wordIndex = relative >>> 6;
      final long merged = target[wordIndex] & values & bitRange(0, width);
      target[wordIndex] = merged;
      remainingSet += Long.bitCount(merged);
    }
    return remainingSet;
  }

  // ───────────────────────────────────────────────────────────────── helpers

  /** Whole word {@code index} of the bit array, zero-filled past the end of the payload. */
  private static long readWord(final MemorySegment payload, final int bitsOffset, final int index) {
    return BitUnpackSimd.readWordSafe(payload, (long) bitsOffset + ((long) index << 3));
  }

  /**
   * 64 bits of the column starting at an arbitrary bit offset.
   *
   * <p>Byte-aligned offsets take a single word load; the rest funnel two words together. The shift
   * count stays in 1..7, so there is no shift-by-64 hazard to guard.
   */
  private static long readBits64(final MemorySegment payload, final int bitsOffset, final long bitOffset) {
    final long byteOff = bitsOffset + (bitOffset >>> 3);
    final int shift = (int) (bitOffset & 7L);
    final long low = BitUnpackSimd.readWordSafe(payload, byteOff);
    if (shift == 0) {
      return low;
    }
    final long high = BitUnpackSimd.readWordSafe(payload, byteOff + Long.BYTES);
    return (low >>> shift) | (high << (64 - shift));
  }

  /** Mask with bits {@code [from, to)} set; {@code to} may be 64. */
  private static long bitRange(final int from, final int to) {
    final long above = to >= 64 ? ~0L : (1L << to) - 1L;
    final long below = from <= 0 ? ~0L : ~((1L << from) - 1L);
    return above & below;
  }
}
