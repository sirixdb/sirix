/*
 * Copyright (c) 2026, SirixDB. All rights reserved.
 */
package io.sirix.index.projection;

/**
 * Order-preserving encoding of {@code double} values into {@code long}s for
 * {@code NUMERIC_DOUBLE} projection columns (docs/PROJECTION_INDEX_STORAGE_REDESIGN.md §2.6).
 *
 * <p>The transform (the classic sortable-bits involution): non-negative doubles keep their
 * raw bits; negative doubles flip the low 63 bits (sign bit preserved). The result is
 * <b>order-isomorphic to signed-long order</b> —
 * {@code a < b ⟺ encode(a) < encode(b)} for all finite doubles (and ±∞) — which
 * is precisely why double columns store TRANSFORMED bits in the same {@code long[]} bodies as
 * {@code NUMERIC_LONG} columns: every existing signed-long compare surface (zone-map folding
 * in {@code appendRow}, {@code zoneSkip}, the numeric predicate kernels, FOR bit-packing in
 * the codec) works unchanged. Predicate literals are transformed once at plan time; only
 * value-materialising consumers (aggregate kernels, row serving) pay the two-op inverse.
 *
 * <p>NaN has no place in the order and cannot arise from JSON sources (projection maintenance
 * is JSON-only); extraction defensively classifies non-finite values as unrepresentable
 * before this transform is applied, so no stored bit pattern collides with the zone-map
 * sentinels ({@code Long.MAX_VALUE}/{@code Long.MIN_VALUE} map back to NaN payloads that are
 * never stored).
 */
public final class ProjectionDoubleEncoding {

  private static final long MAGNITUDE_MASK = 0x7FFF_FFFF_FFFF_FFFFL;

  private ProjectionDoubleEncoding() {
  }

  /** Order-preserving bits of {@code value}. Caller guarantees {@code value} is finite. */
  public static long encode(final double value) {
    final long bits = Double.doubleToRawLongBits(value);
    // Branch-free involution: negatives flip their low 63 bits, non-negatives pass through.
    return bits ^ ((bits >> 63) & MAGNITUDE_MASK);
  }

  /** Inverse of {@link #encode} (the transform is an involution on the bit patterns). */
  public static double decode(final long encoded) {
    return Double.longBitsToDouble(encoded ^ ((encoded >> 63) & MAGNITUDE_MASK));
  }
}
