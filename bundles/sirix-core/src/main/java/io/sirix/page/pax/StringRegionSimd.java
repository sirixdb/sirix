/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.page.pax;

import jdk.incubator.vector.LongVector;
import jdk.incubator.vector.VectorMask;
import jdk.incubator.vector.VectorOperators;
import jdk.incubator.vector.VectorSpecies;

/**
 * SIMD kernels for {@link StringRegion}'s dict-id column — the counterpart to
 * {@link NumberRegionSimd} for string equality.
 *
 * <p>A string equality predicate never compares strings during a scan. The dictionary lookup
 * resolves the literal to ONE dict id, and the scan then counts occurrences of that id in a
 * fixed-width bit-packed column — the same shape as {@code NumberRegion}'s {@code BIT_PACKED}
 * encoding, and equally vectorizable. Doing it scalar-wise left string predicates as the one
 * column kernel with no SIMD at all, on the single-fragment path as well as the merge.
 *
 * <h2>Why a per-lane gather</h2>
 *
 * <p>Values are packed at {@code bitWidth} bits with no alignment guarantee, so a lane's value can
 * straddle a byte and, for wide ids, sit at any bit offset. Each lane therefore loads the 64-bit
 * word containing its value and shifts it into place, exactly as
 * {@link NumberRegionSimd#countBitPacked} does. Consecutive lanes usually hit the SAME word — at
 * the typical 3-8 bit dict widths a 64-bit window covers 8 to 21 values — so the loads are L1 hits
 * and the win is the vectorized compare and popcount replacing a compare and an increment per
 * value.
 *
 * <p>Widths above 56 would need a second load per lane for the cross-word straddle and are
 * declined; dict ids are capped at 32 bits, so that bound is never reached in practice.
 */
public final class StringRegionSimd {

  private static final VectorSpecies<Long> LONG_SPECIES = LongVector.SPECIES_PREFERRED;
  private static final int LANES = LONG_SPECIES.length();

  /** Widths past this need a second load per lane for the straddle; see the class comment. */
  private static final int MAX_SIMD_BIT_WIDTH = 56;

  private StringRegionSimd() {
  }

  /** Whether the kernels can serve this width. */
  public static boolean supports(final int bitWidth) {
    return bitWidth >= 1 && bitWidth <= MAX_SIMD_BIT_WIDTH;
  }

  /**
   * Count values in {@code [start, start + n)} whose dict id equals {@code dictId}.
   *
   * @param payload the region payload
   * @param dictIdsOffset byte offset of the packed dict-id column (from the header)
   * @param bitWidth bits per dict id; must satisfy {@link #supports(int)}
   * @return the match count, or {@code -1} when the width is not supported
   */
  public static long countDictId(final byte[] payload, final int dictIdsOffset, final int bitWidth,
      final int start, final int n, final int dictId) {
    if (!supports(bitWidth)) {
      return -1L;
    }
    final long mask = (1L << bitWidth) - 1L;
    final long[] unpacked = new long[LANES];
    final LongVector target = LongVector.broadcast(LONG_SPECIES, dictId & mask);
    long count = 0;
    int i = 0;
    for (; i <= n - LANES; i += LANES) {
      unpackLanes(payload, dictIdsOffset, bitWidth, mask, start + i, unpacked);
      count += LongVector.fromArray(LONG_SPECIES, unpacked, 0).compare(VectorOperators.EQ, target)
                         .trueCount();
    }
    for (; i < n; i++) {
      if (decode(payload, dictIdsOffset, bitWidth, mask, start + i) == (dictId & mask)) {
        count++;
      }
    }
    return count;
  }

  /**
   * Live-value counterpart of {@link #countDictId}, for a versioned merge in which some values are
   * shadowed by a newer fragment. Shadowed lanes are unpacked and compared like any other and are
   * then simply not counted — the branch that would skip them is what the kernel exists to remove.
   *
   * <p>{@code liveBits} is indexed RELATIVE to {@code start}: bit {@code k} governs value
   * {@code start + k}. Lane groups advance by a lane count that divides 64, so a group never
   * straddles two words and the mask is one shift.
   *
   * <p>Callers with nothing shadowed must use {@link #countDictId} rather than an all-ones bitmap.
   *
   * @return the number of LIVE matches, or {@code -1} when the width is not supported
   */
  public static long countDictIdMasked(final byte[] payload, final int dictIdsOffset,
      final int bitWidth, final int start, final int n, final int dictId, final long[] liveBits) {
    if (!supports(bitWidth)) {
      return -1L;
    }
    final long mask = (1L << bitWidth) - 1L;
    final long[] unpacked = new long[LANES];
    final LongVector target = LongVector.broadcast(LONG_SPECIES, dictId & mask);
    long count = 0;
    int i = 0;
    for (; i <= n - LANES; i += LANES) {
      unpackLanes(payload, dictIdsOffset, bitWidth, mask, start + i, unpacked);
      final VectorMask<Long> live = VectorMask.fromLong(LONG_SPECIES, liveBits[i >>> 6] >>> (i & 63));
      count += LongVector.fromArray(LONG_SPECIES, unpacked, 0).compare(VectorOperators.EQ, target)
                         .and(live).trueCount();
    }
    for (; i < n; i++) {
      if ((liveBits[i >>> 6] & (1L << (i & 63))) == 0L) {
        continue;
      }
      if (decode(payload, dictIdsOffset, bitWidth, mask, start + i) == (dictId & mask)) {
        count++;
      }
    }
    return count;
  }

  /** Unpack {@link #LANES} consecutive dict ids starting at absolute index {@code from}. */
  private static void unpackLanes(final byte[] payload, final int dictIdsOffset, final int bitWidth,
      final long mask, final int from, final long[] out) {
    for (int lane = 0; lane < LANES; lane++) {
      out[lane] = decode(payload, dictIdsOffset, bitWidth, mask, from + lane);
    }
  }

  private static long decode(final byte[] payload, final int dictIdsOffset, final int bitWidth,
      final long mask, final int index) {
    final long bitOff = (long) index * bitWidth;
    final int byteOff = dictIdsOffset + (int) (bitOff >>> 3);
    final int shift = (int) (bitOff & 7L);
    return (StringRegion.readUpToLongLE(payload, byteOff) >>> shift) & mask;
  }
}
